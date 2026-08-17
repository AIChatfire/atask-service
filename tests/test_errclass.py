"""错误分类表（errclass）与探测链路 key 时效接线测试。

覆盖：内置默认表 / 渠道 error_classify 覆盖 / 429 Retry-After 解析 /
keypool 40001 retry_after_ms 结构化 / 探测 key 级失效上报（钉回渠道换 key）/
限流拉长退避 / preflight 503 透传 Retry-After。
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest

from app.services import errclass
from app.services.polling import poll_one
from app.services.upstream import UpstreamError

# ---------------------------------------------------------------------------
# errclass：内置默认表与渠道覆盖
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("status", "expected"), [
    (401, errclass.KEY_LEVEL),
    (403, errclass.KEY_LEVEL),
    (429, errclass.RATE_LIMITED),
    (500, errclass.AMBIGUOUS),
    (599, errclass.AMBIGUOUS),          # 网络/超时封装：模糊失败，重试探测
    (400, errclass.TASK_LEVEL),
    (404, errclass.TASK_LEVEL),
    (418, errclass.TASK_LEVEL),         # 拿不准的 4xx 一律按任务级
])
def test_classify_default_table(status, expected, route_factory):
    route = route_factory()
    assert errclass.classify(route, UpstreamError("minimax", status, "boom")) == expected


def test_classify_envelope_is_task_level(route_factory):
    exc = UpstreamError("minimax", 200, "envelope code=1001", envelope=True)
    assert errclass.classify(route_factory(), exc) == errclass.TASK_LEVEL


def test_classify_channel_overrides(route_factory):
    """渠道名单优先于默认表：403=欠费 的渠道显式配 account_level。"""
    route = route_factory(error_classify={
        "account_level": [403],
        "task_level": [401],            # 渠道自行把 401 降为任务级
    })
    assert errclass.classify(route, UpstreamError("m", 403, "forbidden")) == errclass.ACCOUNT_LEVEL
    assert errclass.classify(route, UpstreamError("m", 401, "bad key")) == errclass.TASK_LEVEL


def test_classify_account_level_message_substring(route_factory):
    route = route_factory(error_classify={
        "account_level_messages": ["insufficient balance"],
    })
    exc = UpstreamError("m", 400, '{"error": "Insufficient Balance"}')
    assert errclass.classify(route, exc) == errclass.ACCOUNT_LEVEL
    # 未配置名单时同一报文回落任务级（拿不准按任务级）
    assert errclass.classify(route_factory(), exc) == errclass.TASK_LEVEL


def test_classify_tolerates_malformed_codes(route_factory):
    route = route_factory(error_classify={"key_level": ["401", "oops", None]})
    assert errclass.classify(route, UpstreamError("m", 401, "x")) == errclass.KEY_LEVEL


# ---------------------------------------------------------------------------
# 上游 429 Retry-After 解析
# ---------------------------------------------------------------------------


async def test_probe_429_parses_retry_after(respx_router, route_factory, key_lease_factory,
                                            patch_redis):
    from app.services import upstream

    route = route_factory()
    key = key_lease_factory()
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(429, text="slow down", headers={"Retry-After": "30"})
    )
    with pytest.raises(UpstreamError) as exc_info:
        await upstream.probe(route, key, "mm-1")
    assert exc_info.value.retry_after_ms == 30_000


# ---------------------------------------------------------------------------
# keypool 40001 → retry_after_ms 结构化
# ---------------------------------------------------------------------------


async def test_keypool_40001_structured_retry_after(respx_router, test_settings):
    from app.services.providers import KeyLeaseError
    from app.services.providers.keypool import KeypoolProvider

    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(
            503, json={"code": 40001, "message": "no available key",
                       "data": {"retry_after_ms": 1000}})
    )
    with pytest.raises(KeyLeaseError) as exc_info:
        await KeypoolProvider().lease("minimax", model="x")
    assert exc_info.value.retry_after_ms == 1000


# ---------------------------------------------------------------------------
# 探测链路接线（polling 分类分流）
# ---------------------------------------------------------------------------

_KEYPOOL_SELECT = {
    "code": 0, "message": "ok",
    "data": {
        "channel_id": 7, "key_index": 0, "key": "sk-upstream-key",
        "base_url": "http://upstream.test", "epoch": "e1",
        "channel": {
            "id": 7, "base_url": "http://upstream.test",
            "setting": {"gateway": {
                "submit_path": "/v2/video_generation",
                "probe_path": "/v2/query/video_generation/{upstream_task_id}",
                "status_path": "task.status",
                "billing": {"rule": "def calulate(request):\n    return 0.13"},
            }},
        },
    },
}


def _seed(task_store) -> str:
    task_id = "e" + "3" * 31
    now = int(time.time()) - 10
    task_store.rows[task_id] = {
        "task_id": task_id, "platform": "gateway", "action": "video",
        "status": "QUEUED", "progress": "0%", "fail_reason": "",
        "data": {
            "biz": "minimax", "model": "MiniMax-H3", "token_hash": "h",
            "callback_url": None, "freeze_amount": 0.13, "settled": False,
            "key_id": 7, "request_body": {"model": "MiniMax-H3"},
            "upstream_task_id": "mm-1",
        },
        "user_id": 7, "channel_id": 7,
        "submit_time": now, "created_at": now, "updated_at": now, "finish_time": 0,
    }
    return task_id


def _mock_lease(respx_router):
    return respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_KEYPOOL_SELECT)
    )


async def test_probe_key_level_reports_and_reschedules(
    respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """probe 401（key 级失效）→ report(ok=False) 驱动 keypool 禁用 + 重投下轮。"""
    _mock_lease(respx_router)
    report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "disabled"}})
    )
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(401, text="invalid api key")
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    await asyncio.sleep(0)                       # report 为 fire-and-forget
    await asyncio.sleep(0)

    assert report.calls, "key 级失效必须上报 keypool"
    body = json.loads(report.calls.last.request.content)
    assert body["success"] is False and body["status_code"] == 401
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]  # 下轮换 key 再探
    assert task_store.rows[task_id]["status"] == "QUEUED"              # 任务不受影响


async def test_probe_rate_limited_lengthens_backoff(
    respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """probe 429 + Retry-After: 30 → 重投延迟按 hint 拉长，且不上报（key 没坏）。"""
    _mock_lease(respx_router)
    report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(429, text="slow down", headers={"Retry-After": "30"})
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert not report.calls                      # 限流不是 key 失效，不上报
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 30}]


async def test_poll_lease_40001_uses_retry_hint(
    respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """keypool 无可用 key 带 retry_after_ms=10000 → 重投延迟 ≥ hint（10s）。"""
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(503, json={"code": 40001, "message": "no key",
                                               "data": {"retry_after_ms": 10000}})
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 10}]


# ---------------------------------------------------------------------------
# preflight 503 透传 Retry-After
# ---------------------------------------------------------------------------


async def test_preflight_503_carries_retry_after(respx_router, test_settings,
                                                 patch_redis, task_store):
    from app.main import app

    respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 1, "token_id": 1})
    )
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(503, json={"code": 40001, "message": "no key",
                                               "data": {"retry_after_ms": 1000}})
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        resp = await client.post(
            "/minimax/v1/tasks", json={"model": "MiniMax-H3"},
            headers={"Authorization": "Bearer sk-user-1"},
        )
    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "1"
    assert "error" in resp.json()


async def test_poll_lease_40001_without_hint_keeps_ladder(
    respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """无 retry_after_ms 时维持退避阶梯（回归既有行为）。"""
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(503, json={"code": 40001, "message": "no key"})
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]
