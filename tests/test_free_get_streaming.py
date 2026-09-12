"""免费 GET 透传改为**流式转发**后的契约（``relay.stream_upstream`` + ``StreamingResponse``）。

为什么要单独成篇：换向后的免费 GET（``GET /queue/{path}`` 末段不是本地 task_id）
可能取的是图片/二进制产物，旧实现把 ``resp.content`` 整段读进内存——大产物会打爆
进程。改流式后**很容易出现「看起来流式、实际仍全缓冲」的假绿**，所以这里的核心不是
「内容对不对」，而是**实证它没有把 body 提前读满**：

- ``SpyStream`` 是自定义 ``httpx.AsyncByteStream``，每次被拉取一块就 +1；
  ``stream_upstream`` 返回后立刻断言 ``pulled == 0``（body 一个字节都还没读），
  迭代完再断言 ``pulled == 分块数``（逐块拉取，不是一次性拼成整段 bytes）；
- 复现探测/提交路径的缓冲行为**不在此篇**（那里必须拿完整报文才能改写 id / 落快照）。

覆盖：
1. 非全缓冲 + media_type 保真（relay 层）；
2. 路由层二进制流式 + 媒体类型保真；
3. 直接调 ``free_queue_get`` 断言返回的是 ``StreamingResponse``（body_iterator 可迭代）；
4. 声明长度超上限 → 502 且**未开始流式**（body 不消费、连接已关）；
5. 未声明长度（chunked）→ 正常流式，不因缺 Content-Length 被误拒；
6. 守卫仍在：非白名单 host 400、空基址 599、缺 token 401、熔断打开零出站。
"""

from __future__ import annotations

import httpx
import pytest
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from starlette.requests import Request as StarletteRequest

from app.main import app
from app.redis import K_BREAKER
from app.services import relay, relayflow, upstream

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"

#: 刻意用多块：``pulled`` 等于分块数才证明是逐块拉取而非一次读满
CHUNKS = [b"\x89PNG\r\n\x1a\n", b"chunk-1-", b"chunk-2-", b"chunk-3"]


class SpyStream(httpx.AsyncByteStream):
    """记录「被拉取多少块 / 是否被关闭」的自定义上游字节流。"""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.pulled = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            self.pulled += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Upstream-Base-Url": UP_BASE, **extra}


def _stream_response(spy: SpyStream, *, content_type: str,
                     content_length: int | None = None) -> httpx.Response:
    headers = {"content-type": content_type}
    if content_length is not None:
        headers["content-length"] = str(content_length)
    return httpx.Response(200, stream=spy, headers=headers)


def _starlette_request(path: str, headers: dict[str, str]) -> StarletteRequest:
    raw = [(name.lower().encode(), value.encode()) for name, value in headers.items()]
    return StarletteRequest({
        "type": "http", "method": "GET", "scheme": "http",
        "path": path, "raw_path": path.encode(), "query_string": b"",
        "headers": raw, "client": ("10.9.9.9", 4321), "server": ("gw.test", 80),
    })


@pytest.fixture
def stream_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    return settings


# ---------------------------------------------------------------------------
# 1. relay 层：非全缓冲 + media_type 保真
# ---------------------------------------------------------------------------


async def test_stream_upstream_does_not_read_body_eagerly(
    respx_router, stream_settings, patch_redis,
):
    spy = SpyStream(CHUNKS)
    respx_router.get(f"{UP_BASE}/v1/assets/big.png").mock(
        return_value=_stream_response(spy, content_type="image/png")
    )

    status, content_type, stream = await relay.stream_upstream(
        "GET", UP_BASE, "v1/assets/big.png", token="sk-user-1",
    )

    assert status == 200
    assert content_type == "image/png"                 # 媒体类型原样带出
    # 返回的是迭代器而非已读满的 bytes；且此刻上游一个字节都还没被拉走
    assert not isinstance(stream, bytes)
    assert hasattr(stream, "__aiter__")
    assert spy.pulled == 0                             # 关键实证：未提前读体

    got = [chunk async for chunk in stream]
    assert b"".join(got) == b"".join(CHUNKS)           # 内容完整
    assert len(got) == len(CHUNKS)                     # 逐块产出，不是一次性拼接
    assert spy.pulled == len(CHUNKS)
    assert spy.closed is True                          # 迭代结束关闭上游响应


async def test_stream_upstream_missing_content_type_falls_back_to_json(
    respx_router, stream_settings, patch_redis,
):
    spy = SpyStream([b"{}"])
    respx_router.get(f"{UP_BASE}/v1/nolabel").mock(
        return_value=httpx.Response(200, stream=spy)   # 不声明 content-type
    )
    _, content_type, stream = await relay.stream_upstream(
        "GET", UP_BASE, "v1/nolabel", token="t",
    )
    assert content_type == "application/json"          # 照既有口径回退
    assert b"".join([c async for c in stream]) == b"{}"


# ---------------------------------------------------------------------------
# 2. 路由层：二进制流式透传 + 媒体类型保真
# ---------------------------------------------------------------------------


async def test_free_get_route_streams_binary_and_preserves_media_type(
    stream_settings, patch_redis, respx_router,
):
    spy = SpyStream(CHUNKS)
    respx_router.get(f"{UP_BASE}/v1/assets/cover.png").mock(
        return_value=_stream_response(spy, content_type="image/png")
    )

    async with _client() as client:
        resp = await client.get("/queue/v1/assets/cover.png", headers=_headers())

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/png"  # 不硬写 JSON
    assert resp.content == b"".join(CHUNKS)
    assert spy.pulled == len(CHUNKS)
    assert spy.closed is True


async def test_free_queue_get_returns_streaming_response(
    stream_settings, patch_redis, respx_router,
):
    """直接调 free_queue_get：拿到的是 StreamingResponse（body_iterator 可迭代）。"""
    spy = SpyStream([b"part-1", b"part-2"])
    respx_router.get(f"{UP_BASE}/v1/stream.bin").mock(
        return_value=_stream_response(spy, content_type="application/octet-stream")
    )

    response = await relayflow.free_queue_get(
        "v1/stream.bin", _starlette_request("/queue/v1/stream.bin", _headers())
    )

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    assert response.media_type == "application/octet-stream"
    chunks = [chunk async for chunk in response.body_iterator]
    assert b"".join(chunks) == b"part-1part-2"
    assert spy.pulled == 2


# ---------------------------------------------------------------------------
# 3. 声明长度上限：快路径拒绝（未开始流式）
# ---------------------------------------------------------------------------


async def test_stream_upstream_rejects_declared_oversize_before_streaming(
    respx_router, stream_settings, patch_redis,
):
    stream_settings.upstream_response_max_bytes = 1024
    spy = SpyStream([b"x" * 4096])
    respx_router.get(f"{UP_BASE}/v1/big.bin").mock(
        return_value=_stream_response(spy, content_type="application/octet-stream",
                                      content_length=4096)
    )

    with pytest.raises(relay.RelayError) as exc:
        await relay.stream_upstream("GET", UP_BASE, "v1/big.bin", token="t")

    assert exc.value.status == 502
    assert spy.pulled == 0                             # 未开始流式：body 未被消费
    assert spy.closed is True                          # 连接已关闭，不留悬挂


async def test_free_get_route_declared_oversize_is_502(
    stream_settings, patch_redis, respx_router,
):
    stream_settings.upstream_response_max_bytes = 1024
    spy = SpyStream([b"x" * 4096])
    respx_router.get(f"{UP_BASE}/v1/big.bin").mock(
        return_value=_stream_response(spy, content_type="application/octet-stream",
                                      content_length=4096)
    )

    async with _client() as client:
        resp = await client.get("/queue/v1/big.bin", headers=_headers())

    assert resp.status_code == 502
    assert spy.pulled == 0


async def test_stream_upstream_under_limit_passes(
    respx_router, stream_settings, patch_redis,
):
    stream_settings.upstream_response_max_bytes = 1024
    spy = SpyStream([b"ok"])
    respx_router.get(f"{UP_BASE}/v1/small.bin").mock(
        return_value=_stream_response(spy, content_type="application/octet-stream",
                                      content_length=2)
    )
    status, _, stream = await relay.stream_upstream("GET", UP_BASE, "v1/small.bin", token="t")
    assert status == 200
    assert b"".join([c async for c in stream]) == b"ok"


async def test_stream_upstream_chunked_without_length_is_not_rejected(
    respx_router, stream_settings, patch_redis,
):
    """未声明 Content-Length（chunked）不因上限被误拒，也不中途截断。"""
    stream_settings.upstream_response_max_bytes = 1     # 上限极低也不该拦住 chunked
    spy = SpyStream([b"a", b"b", b"c"])
    respx_router.get(f"{UP_BASE}/v1/chunked").mock(
        return_value=_stream_response(spy, content_type="application/octet-stream")
    )
    status, _, stream = await relay.stream_upstream("GET", UP_BASE, "v1/chunked", token="t")
    assert status == 200
    assert b"".join([c async for c in stream]) == b"abc"   # 完整，无截断
    assert spy.pulled == 3


async def test_repeated_oversize_rejections_do_not_open_breaker(
    respx_router, stream_settings, patch_redis,
):
    """超限是**我方尺寸策略拒绝**，上游其实健康（多半就是 200）——不得计入熔断。

    若记成失败，客户端反复要一个大产物即可把该上游对**所有租户**熔断
    （自伤式、跨租户 DoS）。这里阈值故意设 2：正确实现下连续 5 次超限后熔断仍
    不打开，随后一次正常请求照常出站；错误实现下第 3 次起被 `breaker_guard` 拦下。
    """
    stream_settings.upstream_response_max_bytes = 1024
    stream_settings.upstream_breaker_threshold = 2
    big = respx_router.get(f"{UP_BASE}/v1/big.bin").mock(
        side_effect=lambda request: _stream_response(
            SpyStream([b"x" * 4096]), content_type="application/octet-stream",
            content_length=4096)
    )
    small = respx_router.get(f"{UP_BASE}/v1/small.bin").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )

    for _ in range(5):
        with pytest.raises(relay.RelayError) as exc:
            await relay.stream_upstream("GET", UP_BASE, "v1/big.bin", token="t")
        assert exc.value.status == 502

    assert len(big.calls) == 5                          # 5 次都真的到了上游，没被误熔断拦住

    # 熔断未打开：随后一次正常请求仍能出站
    status, _, stream = await relay.stream_upstream("GET", UP_BASE, "v1/small.bin", token="t")
    assert status == 200
    assert b"".join([c async for c in stream])
    assert len(small.calls) == 1                        # 正常请求真的发出去了


# ---------------------------------------------------------------------------
# 4. 守卫仍在
# ---------------------------------------------------------------------------


async def test_stream_upstream_empty_base_is_599(stream_settings, patch_redis):
    with pytest.raises(relay.RelayError) as exc:
        await relay.stream_upstream("GET", "", "v1/x", token="t")
    assert exc.value.status == 599


async def test_stream_upstream_rejects_host_outside_allowlist(
    respx_router, stream_settings, patch_redis,
):
    with pytest.raises(HTTPException) as exc:
        await relay.stream_upstream("GET", "http://evil.example", "v1/x", token="t")
    assert exc.value.status_code == 400


async def test_stream_upstream_breaker_open_blocks_without_outbound(
    respx_router, stream_settings, patch_redis,
):
    """熔断打开时**拦截**（不是只记账）：``BreakerOpenError`` 且一个出站包都不发。"""
    stream_settings.upstream_breaker_threshold = 2
    await patch_redis.set(K_BREAKER.format(host="upstream.test"), 2)

    with pytest.raises(upstream.BreakerOpenError):
        await relay.stream_upstream("GET", UP_BASE, "v1/x", token="t")

    assert len(respx_router.calls) == 0


async def test_free_get_missing_token_is_401(stream_settings, patch_redis):
    async with _client() as client:
        resp = await client.get("/queue/v1/models",
                                headers={"X-Upstream-Base-Url": UP_BASE})
    assert resp.status_code == 401
