"""``app/services/relay`` 单测：约定式提取纯函数 + 出站契约（ADR-010）。

覆盖：
- ``extract_upstream_task_id``：``id`` 优先、``task_id`` 回退、都缺 / 非标量 → None；
- ``upstream_status``：``status`` 原话、缺失 / 非字符串 → None；
- ``call_upstream``：URL 拼接（不产生双斜杠、query 原样）、Bearer 透传、
  content-type / body 转发、空基址 599、白名单拒绝 400、传输错误 599。

不触网：出站由 respx 拦截；Redis 走 FakeRedis（熔断计数）。
"""

from __future__ import annotations

import json

import httpx
import pytest
from fastapi import HTTPException

from app.services import relay


@pytest.fixture
def relay_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    return settings


# ---------------------------------------------------------------------------
# extract_upstream_task_id
# ---------------------------------------------------------------------------


def test_extract_prefers_id():
    assert relay.extract_upstream_task_id({"id": "abc", "task_id": "xyz"}) == "abc"


def test_extract_falls_back_to_task_id():
    assert relay.extract_upstream_task_id({"task_id": "xyz"}) == "xyz"


def test_extract_falls_back_when_id_is_null():
    assert relay.extract_upstream_task_id({"id": None, "task_id": "xyz"}) == "xyz"


def test_extract_numeric_id_is_stringified():
    assert relay.extract_upstream_task_id({"id": 2090071565996011520}) == "2090071565996011520"


@pytest.mark.parametrize("payload", [
    {},                                       # 两者都无
    {"other": "x"},
    {"id": {"nested": "x"}},                  # 非标量
    {"id": ["x"]},
    {"id": True},                             # bool 不是 id
    {"id": ""},
    {"id": None, "task_id": 3.14},            # 回退项也非标量
    "not-a-dict",
    None,
])
def test_extract_invalid_returns_none(payload):
    assert relay.extract_upstream_task_id(payload) is None


# ---------------------------------------------------------------------------
# upstream_status
# ---------------------------------------------------------------------------


def test_upstream_status_reads_status():
    assert relay.upstream_status({"status": "processing"}) == "processing"


def test_upstream_status_strips_whitespace():
    assert relay.upstream_status({"status": "  succeeded "}) == "succeeded"


@pytest.mark.parametrize("payload", [{}, {"status": None}, {"status": 7}, {"status": "  "},
                                     "nope", None])
def test_upstream_status_missing_or_invalid(payload):
    assert relay.upstream_status(payload) is None


# ---------------------------------------------------------------------------
# call_upstream
# ---------------------------------------------------------------------------


async def test_call_upstream_forwards_method_query_body_and_bearer(
    respx_router, relay_settings, patch_redis,
):
    route = respx_router.post("http://upstream.test/v1/tasks?a=1&b=2").mock(
        return_value=httpx.Response(200, json={"id": "up-1", "status": "queued"})
    )
    status, body, content_type = await relay.call_upstream(
        "POST", "http://upstream.test", "/v1/tasks", token="sk-user-1",
        query="a=1&b=2", body=b'{"model":"m"}', content_type="application/json",
    )
    assert status == 200
    assert json.loads(body) == {"id": "up-1", "status": "queued"}
    assert content_type == "application/json"
    request = route.calls[0].request
    assert request.headers["authorization"] == "Bearer sk-user-1"
    assert request.headers["content-type"] == "application/json"
    assert request.content == b'{"model":"m"}'


async def test_call_upstream_joins_base_and_path_without_double_slash(
    respx_router, relay_settings, patch_redis,
):
    route = respx_router.get("http://upstream.test/v1/ping").mock(
        return_value=httpx.Response(200, content=b"ok")
    )
    status, body, content_type = await relay.call_upstream(
        "GET", "http://upstream.test/", "/v1/ping", token="t",
    )
    assert (status, body) == (200, b"ok")
    assert content_type == "application/json"      # 缺失时回退 JSON
    assert route.called


async def test_call_upstream_preserves_upstream_content_type(
    respx_router, relay_settings, patch_redis,
):
    """非 JSON 媒体类型（图片/二进制）必须原样带出，不硬写 application/json。"""
    respx_router.get("http://upstream.test/v1/image").mock(
        return_value=httpx.Response(200, content=b"\x89PNG", headers={"content-type": "image/png"})
    )
    status, body, content_type = await relay.call_upstream(
        "GET", "http://upstream.test", "v1/image", token="t",
    )
    assert status == 200 and body == b"\x89PNG"
    assert content_type == "image/png"


async def test_call_upstream_empty_base_is_599(relay_settings, patch_redis):
    with pytest.raises(relay.RelayError) as exc:
        await relay.call_upstream("GET", "", "v1/x", token="t")
    assert exc.value.status == 599


async def test_call_upstream_transport_error_is_599(respx_router, relay_settings, patch_redis):
    respx_router.get("http://upstream.test/v1/x").mock(
        side_effect=httpx.ConnectError("boom")
    )
    with pytest.raises(relay.RelayError) as exc:
        await relay.call_upstream("GET", "http://upstream.test", "v1/x", token="t")
    assert exc.value.status == 599


async def test_call_upstream_rejects_host_outside_allowlist(
    respx_router, relay_settings, patch_redis,
):
    with pytest.raises(HTTPException) as exc:
        await relay.call_upstream("GET", "http://evil.example", "v1/x", token="t")
    assert exc.value.status_code == 400


async def test_call_upstream_empty_allowlist_fails_closed(respx_router, relay_settings,
                                                          patch_redis):
    relay_settings.upstream_allowlist = ""
    with pytest.raises(HTTPException) as exc:
        await relay.call_upstream("GET", "http://upstream.test", "v1/x", token="t")
    assert exc.value.status_code == 400


async def test_call_upstream_returns_upper_status_without_raising(
    respx_router, relay_settings, patch_redis,
):
    """上游 4xx/5xx 不由出站层抛异常：原样返回状态给调用方分流。"""
    respx_router.get("http://upstream.test/v1/x").mock(
        return_value=httpx.Response(429, json={"error": "rate limited"})
    )
    status, body, _ = await relay.call_upstream("GET", "http://upstream.test", "v1/x", token="t")
    assert status == 429
    assert json.loads(body)["error"] == "rate limited"
