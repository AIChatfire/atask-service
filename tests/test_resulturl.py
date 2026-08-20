"""产物直链改写（转存/镜像）测试：渠道配 ``result_url_template`` 后，
客户端在**所有**出口看到的都是网关地址，上游原始直链只留在 tasks.data。

出口清单：``/v1/tasks`` GET 视图、videos 视图、用户回调载荷、原生查询报文
（活跃态转发 + 终态快照回放）。
"""

from __future__ import annotations

import json

import httpx
import pytest

from app.main import app
from app.services import resulturl
from app.services.polling import poll_one
from app.services.registry import registry
from app.services.submit import submit_one

UP_ID = "424010985738629"
UP_URL = "http://cdn.upstream.test/out.mp4?sign=abc%2Fdef&exp=1"
TEMPLATE = "https://myhost.com/{upstream_result_url}"

GATEWAY = {
    "biz": "minimax",
    "submit_path": "/v2/video_generation",
    "probe_path": "/v2/query/video_generation/{upstream_task_id}",
    "status_path": "task.status",
    "result_path": "task.content.url",
    "error_path": "task.error",
    "result_url_template": TEMPLATE,
    "settle_usage_map": {"duration": "task.usage.output_seconds"},
    "billing": {"rule": "def calulate(request):\n    return 0.13", "type": "second"},
}

BODY = {"model": "MiniMax-H3", "duration": 5, "callback_url": "https://user.test/done"}
AUTH = {"Authorization": "Bearer sk-user-42"}


def _channel(**gateway_overrides):
    gateway = {**GATEWAY, **gateway_overrides}
    return {"id": 7, "name": "minimax-main", "base_url": "http://upstream.test",
            "setting": {"gateway": gateway}}


@pytest.fixture
def _reset_registry():
    registry._cache.clear()
    yield
    registry._cache.clear()


@pytest.fixture
def mirror_mocks(respx_router):
    m = type("Mocks", (), {})()
    m.channel = _channel()
    respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 42, "token_id": 9}))
    respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}}))
    respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}}))
    m.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        side_effect=lambda request: httpx.Response(200, json={
            "code": 0, "message": "ok",
            "data": {"channel_id": 7, "key_index": 1, "key": "sk-real",
                     "base_url": "http://upstream.test", "epoch": "e1",
                     "channel": m.channel},
        })
    )
    m.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": UP_ID}))
    m.probe = respx_router.get(
        f"http://upstream.test/v2/query/video_generation/{UP_ID}").mock(
        return_value=httpx.Response(200, json={
            "task": {"id": UP_ID, "status": "succeeded",
                     "content": {"url": UP_URL},
                     "usage": {"output_seconds": 4}},
        }))
    return m


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


# ---------------------------------------------------------------------------
# 纯函数：模板渲染
# ---------------------------------------------------------------------------


def test_render_all_placeholders():
    url = "https://cdn.test/a/b.mp4?x=1"
    assert resulturl.render("https://my.host/{upstream_result_url}", url) == \
        f"https://my.host/{url}"
    assert resulturl.render("https://my.host/?u={upstream_result_url_encoded}", url) == \
        "https://my.host/?u=https%3A%2F%2Fcdn.test%2Fa%2Fb.mp4%3Fx%3D1"
    assert resulturl.render("https://my.host/{upstream_result_url_no_scheme}", url) == \
        "https://my.host/cdn.test/a/b.mp4?x=1"
    assert resulturl.render("https://my.host/{upstream_result_host}", url) == \
        "https://my.host/cdn.test"
    assert resulturl.render("https://my.host/{upstream_result_path}", url) == \
        "https://my.host/a/b.mp4?x=1"
    assert resulturl.render("https://my.host/{task_id}/f.mp4", url, "minimax_1") == \
        "https://my.host/minimax_1/f.mp4"


def test_render_noop_without_template_or_url():
    assert resulturl.render("", "https://cdn.test/a") == "https://cdn.test/a"
    assert resulturl.render("https://my.host/{upstream_result_url}", "") == ""


def test_transform_handles_list_and_non_url(route_factory):
    route = route_factory(result_url_template=TEMPLATE)
    assert resulturl.transform(route, ["http://a/1.mp4", "http://a/2.mp4"]) == \
        ["https://myhost.com/http://a/1.mp4", "https://myhost.com/http://a/2.mp4"]
    assert resulturl.transform(route, {"url": "http://a/1.mp4"}) == {"url": "http://a/1.mp4"}
    assert resulturl.transform(route, None) is None
    # 未配模板 → 原样返回
    assert resulturl.transform(route_factory(), "http://a/1.mp4") == "http://a/1.mp4"


def test_rewrite_bytes_keeps_rest_of_payload_identical(route_factory):
    route = route_factory(result_url_template=TEMPLATE)
    raw = json.dumps({"task": {"status": "succeeded", "content": {"url": UP_URL},
                               "usage": {"output_seconds": 4}}},
                     ensure_ascii=False).encode()
    out = resulturl.rewrite_bytes(raw, resulturl.pairs(route, UP_URL))
    parsed = json.loads(out)
    assert parsed["task"]["content"]["url"] == f"https://myhost.com/{UP_URL}"
    assert parsed["task"]["usage"] == {"output_seconds": 4}       # 其余字段原样


def test_rewrite_bytes_hides_upstream_url_with_encoded_template(route_factory):
    """用 ``{upstream_result_url_encoded}`` 模板时，改写后的报文里不再出现
    上游原始直链的明文形态（真正意义上的"藏源站"）。"""
    route = route_factory(
        result_url_template="https://myhost.com/f?u={upstream_result_url_encoded}")
    raw = json.dumps({"task": {"content": {"url": UP_URL}}}, ensure_ascii=False).encode()
    out = resulturl.rewrite_bytes(raw, resulturl.pairs(route, UP_URL))
    assert UP_URL.encode() not in out
    assert json.loads(out)["task"]["content"]["url"].startswith("https://myhost.com/f?u=")


def test_rewrite_bytes_noop_without_pairs():
    raw = b'{"url":"http://a/1.mp4"}'
    assert resulturl.rewrite_bytes(raw, []) == raw
    assert resulturl.rewrite_bytes(b"", [("a", "b")]) == b""


# ---------------------------------------------------------------------------
# 端到端：所有出口都是网关地址
# ---------------------------------------------------------------------------


async def test_finalize_rewrites_result_across_all_views(
    _reset_registry, mirror_mocks, test_settings, patch_redis, task_store, queue_events,
):
    async with _client() as client:
        task_id = (await client.post("/minimax/v1/videos", json=BODY,
                                     headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        await poll_one(task_id)                      # 探测终态 → finalize

        got = await client.get(f"/minimax/v1/tasks/{task_id}")
        video = await client.get(f"/minimax/v1/videos/{task_id}")

    mirrored = f"https://myhost.com/{UP_URL}"
    row = task_store.rows[task_id]
    assert row["status"] == "SUCCESS"
    assert row["data"]["result"] == mirrored                     # 落库即改写
    assert row["data"]["upstream_result"] == UP_URL              # 原始链留档对账

    assert got.json()["result"] == mirrored                      # tasks 视图
    assert video.json()["result"] == mirrored                    # videos 视图
    notify = queue_events["notify"][-1]["payload"]
    assert notify["result"] == mirrored                          # 用户回调载荷
    # 结算闭环不受影响
    assert queue_events["settle"][0]["request_id"] == task_id
    assert queue_events["settle"][0]["actual_amount"] == pytest.approx(0.13)


async def test_native_query_rewrites_result_url_live_and_replayed(
    _reset_registry, mirror_mocks, test_settings, patch_redis, task_store, queue_events,
):
    """原生查询：活跃态转发的报文与终态快照回放，直链都被改写，
    且报文其余部分保持同构。"""
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation", json=BODY,
                                     headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        live = await client.get(f"/minimax/v2/query/video_generation/{task_id}")
        replayed = await client.get(f"/minimax/v2/query/video_generation/{task_id}")

    mirrored = f"https://myhost.com/{UP_URL}"
    expected = {"task": {"id": task_id, "status": "succeeded",
                         "content": {"url": mirrored},
                         "usage": {"output_seconds": 4}}}
    assert live.json() == expected                  # 活跃态：转发后字节级改写
    assert UP_ID not in live.text                   # 上游任务 id 不外泄
    assert len(mirror_mocks.probe.calls) == 1       # 终态后不再打上游
    assert replayed.json() == expected              # 快照回放同样改写


async def test_no_template_keeps_upstream_url(
    _reset_registry, mirror_mocks, test_settings, patch_redis, task_store, queue_events,
):
    """未配 ``result_url_template``：行为与改造前完全一致（零影响）。"""
    mirror_mocks.channel = _channel(result_url_template="")
    async with _client() as client:
        task_id = (await client.post("/minimax/v1/videos", json=BODY,
                                     headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        await poll_one(task_id)
        got = await client.get(f"/minimax/v1/tasks/{task_id}")

    assert got.json()["result"] == UP_URL
    assert "upstream_result" not in task_store.rows[task_id]["data"]
