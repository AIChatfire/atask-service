"""任务生命周期测试：创建（幂等/渠道覆盖/失败补偿）与终态推进（三档结算）。

边界：providers 用记录器替换（除单独声明外），上游 HTTP 走 respx，
taskstore 内存实现，queue 发布门面记录器，Redis FakeRedis。
"""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import HTTPException

from app.schemas import FAILURE, QUEUED, SUCCESS, Quote
from app.services import flow, providers, tokensession


def _make_preflight(route, key, *, amount: float = 0.13, body: dict | None = None,
                    idem_key: str | None = None, token_raw: str = "sk-user-1"):
    from app.deps.auth import TokenCtx
    from app.deps.preflight import Preflight
    from app.schemas import UserIdentity

    return Preflight(
        biz=route.biz, route=route,
        token=TokenCtx(raw=token_raw, hash=hashlib.sha256(token_raw.encode()).hexdigest()),
        identity=UserIdentity(user_id=7, token_id=3),
        quote=Quote(amount=amount, metric="second", logic="rule"),
        key=key, model="MiniMax-H3", amount=amount,
        task_id="t" + "0" * 31, idem_key=idem_key, body=body or {},
    )


@pytest.fixture
def key_recorder(monkeypatch: pytest.MonkeyPatch):
    """providers.keys 记录器（lease/report 不伤真服务）。"""
    calls: list[dict[str, Any]] = []

    async def _lease(*a, **kw):
        raise AssertionError("lease 不应在 flow 单测中触发（preflight 已持有租约）")

    async def _report(key, ok, status_code=0, latency_ms=0, error="", usage=None):
        calls.append({"ok": ok, "status_code": status_code, "error": error})

    monkeypatch.setattr(providers, "keys", SimpleNamespace(lease=_lease, report=_report))
    return calls


# ---------------------------------------------------------------------------
# create_task
# ---------------------------------------------------------------------------


async def test_create_task_success_full_chain(
    respx_router, route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder,
):
    """提交全链路：渠道覆盖报文 → 落库 → 探测排程 → key 上报。"""
    key = key_lease_factory(param_override={"aigc_watermark": False})
    route = route_factory()
    http = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "mm-1"})
    )
    body = {"model": "MiniMax-H3",
            "content": [{"type": "text", "text": "a cat"}],
            "duration": 5, "resolution": "2K",
            "callback_url": "https://user.test/hook"}
    pf = _make_preflight(route, key, body=body)

    view = await flow.create_task("minimax", body, pf, action="video", source="videos")

    assert view["status"] == QUEUED and view["upstream_task_id"] == "mm-1"
    # 渠道 param_override 合并进提交体；用户回调不直接带上游
    sent = json.loads(http.calls.last.request.content)
    assert sent["aigc_watermark"] is False
    assert sent["content"][0]["text"] == "a cat"
    assert "callback_url" not in sent       # supports_callback=False 不注入
    # 落库与台账
    row = await task_store.get(pf.task_id)
    assert row["status"] == QUEUED
    assert row["data"]["upstream_task_id"] == "mm-1"
    assert row["data"]["request_body"]["duration"] == 5     # 结算重估基底
    assert row["channel_id"] == 7
    # 不支持回调 → 进探测队列；key 成功上报
    assert queue_events["poll"] == [{"task_id": pf.task_id, "delay": 5}]
    assert key_recorder == [{"ok": True, "status_code": 0, "error": ""}]


async def test_create_task_idempotent_replay(
    respx_router, route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder,
):
    """同 Idempotency-Key 重放：直接返回原任务，不产生第二次提交/扣费。"""
    route = route_factory()
    key = key_lease_factory()
    http = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "mm-1"})
    )
    body = {"model": "MiniMax-H3", "duration": 5}
    pf = _make_preflight(route, key, body=body, idem_key="idem-1")
    await flow.create_task("minimax", body, pf, action="video", source="videos")
    assert len(http.calls) == 1

    pf2 = _make_preflight(route, key, body=body, idem_key="idem-1")
    view2 = await flow.create_task("minimax", body, pf2, action="video", source="videos")
    assert view2["task_id"] == pf.task_id   # 返回首个任务
    assert len(http.calls) == 1             # 没有第二次上游提交


async def test_create_task_upstream_rejected_compensates(
    respx_router, route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder,
):
    """上游 4xx 拒绝：任务 FAILURE + 取消冻结（用户令牌）+ key 失败上报。"""
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(400, json={"error": "content policy"})
    )
    pf = _make_preflight(route_factory(), key_lease_factory(),
                         body={"model": "MiniMax-H3"})

    with pytest.raises(HTTPException) as exc_info:
        await flow.create_task("minimax", {"model": "MiniMax-H3"}, pf,
                               action="video", source="videos")
    assert exc_info.value.status_code == 502
    row = await task_store.get(pf.task_id)
    assert row["status"] == FAILURE
    assert queue_events["cancel"] == [{"request_id": pf.task_id, "user_sk": "sk-user-1"}]
    assert key_recorder[0]["ok"] is False and key_recorder[0]["status_code"] == 400


async def test_create_task_tail_gather_poll_failure(
    respx_router, route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder, monkeypatch,
):
    """收尾并行化失败语义：schedule_poll 抛错时——
    ① 异常照常传播到外层补偿（释放并发槽 + 取消冻结）；
    ② 同 gather 的 idem.set_task_id 不被取消、落键成功
       （gather 默认不取消兄弟协程，客户端重试可回放）。"""
    import app.queue as q
    from unittest.mock import AsyncMock

    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "mm-1"})
    )
    monkeypatch.setattr(q, "schedule_poll",
                        AsyncMock(side_effect=RuntimeError("redis down")))
    body = {"model": "MiniMax-H3", "duration": 5}
    pf = _make_preflight(route_factory(), key_lease_factory(),
                         body=body, idem_key="idem-tail")

    with pytest.raises(RuntimeError, match="redis down"):
        await flow.create_task("minimax", body, pf, action="video", source="videos")

    # 外层补偿：取消冻结（用户令牌）已发布
    assert queue_events["cancel"] == [{"request_id": pf.task_id, "user_sk": "sk-user-1"}]
    # 兄弟协程未被取消：幂等键仍落库，重试可回放原任务而非双建双扣
    from app.services import idem
    assert await idem.get_task_id(pf.token.hash, "idem-tail") == pf.task_id


async def test_create_task_missing_task_id_visible(
    respx_router, route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder,
):
    """傻瓜式防护：提交响应提取不到 task_id（task_id_path 配错）→ 立即 502。"""
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"unexpected": "shape"})
    )
    pf = _make_preflight(route_factory(), key_lease_factory(),
                         body={"model": "MiniMax-H3"})
    with pytest.raises(HTTPException) as exc_info:
        await flow.create_task("minimax", {"model": "MiniMax-H3"}, pf,
                               action="video", source="videos")
    assert exc_info.value.status_code == 502
    assert "missing task id" in str(exc_info.value.detail)
    assert queue_events["cancel"]            # 冻结已取消补偿


async def test_create_task_submit_path_not_configured(
    route_factory, key_lease_factory, patch_redis, task_store, queue_events,
):
    """渠道没配 setting.gateway.submit_path → 502 且明确提示，不静默挂起。"""
    pf = _make_preflight(route_factory(submit_path=""), key_lease_factory(),
                         body={"model": "MiniMax-H3"})
    with pytest.raises(HTTPException) as exc_info:
        await flow.create_task("minimax", {"model": "MiniMax-H3"}, pf,
                               action="video", source="videos")
    assert exc_info.value.status_code == 502
    assert "submit_path" in str(exc_info.value.detail)


# ---------------------------------------------------------------------------
# finalize_task：三档结算
# ---------------------------------------------------------------------------


async def _seed_task(task_store, route, *, freeze_amount=0.13, callback_url=None):
    data = {
        "biz": route.biz, "model": "MiniMax-H3", "token_hash": "h",
        "callback_url": callback_url, "freeze_amount": freeze_amount,
        "settled": False, "key_id": 7,
        "request_body": {"model": "MiniMax-H3", "duration": 5},
    }
    task_id = "t" + "1" * 31
    await task_store.create(task_id=task_id, user_id=7, channel_id=7,
                            action="video", data=data)
    return task_id


async def test_finalize_success_settle_requote(
    route_factory, patch_redis, task_store, queue_events,
):
    """成功终态：settle_usage_map 提取实际秒数 → 重跑渠道计费规则 → 多退少补。"""
    route = route_factory(billing_rule="duration * 0.026", billing_type="second")
    task_id = await _seed_task(task_store, route, callback_url="https://user.test/hook")
    await tokensession.store(task_id, "sk-user-1")

    task = await task_store.get(task_id)
    raw = {"task": {"status": "succeeded",
                    "content": {"url": "http://cdn.test/v.mp4"},
                    "usage": {"output_seconds": 4}}}
    ok = await flow.finalize_task(task, SUCCESS, raw, route=route)

    assert ok is True
    # 重估：duration 被实际产出秒数覆盖（5 → 4），4 × 0.026 = 0.104
    assert queue_events["settle"] == [{
        "request_id": task_id, "actual_amount": pytest.approx(0.104), "user_sk": "sk-user-1",
        "units": 4, "attrs": {"biz": "minimax", "model": "MiniMax-H3", "duration": 4},
    }]
    # 结果 URL 落库 + 用户回调通知 + 令牌会话清除
    row = await task_store.get(task_id)
    assert row["status"] == SUCCESS
    assert row["data"]["result"] == "http://cdn.test/v.mp4"
    assert queue_events["notify"][0]["url"] == "https://user.test/hook"
    assert queue_events["notify"][0]["payload"]["result"] == "http://cdn.test/v.mp4"
    assert await tokensession.get(task_id) is None


async def test_finalize_settle_fallback_to_freeze_amount(
    route_factory, patch_redis, task_store, queue_events,
):
    """终态报文缺用量字段：回退冻结金额（绝不静默按 0 结算，不重估）。"""
    route = route_factory(billing_rule="duration * 0.026", billing_type="second")
    task_id = await _seed_task(task_store, route)
    await tokensession.store(task_id, "sk-user-1")

    task = await task_store.get(task_id)
    raw = {"task": {"status": "succeeded", "content": {"url": "http://v"}}}
    await flow.finalize_task(task, SUCCESS, raw, route=route)
    assert queue_events["settle"][0]["actual_amount"] == 0.13


async def test_finalize_actual_amount_path_priority(
    route_factory, patch_redis, task_store, queue_events,
):
    """上游直接给出实收金额（actual_amount_path）→ 最高优先，不重估。"""
    route = route_factory(actual_amount_path="task.billing.amount",
                          billing_rule="duration * 0.026")
    task_id = await _seed_task(task_store, route)
    await tokensession.store(task_id, "sk-user-1")

    task = await task_store.get(task_id)
    raw = {"task": {"status": "succeeded", "content": {"url": "http://v"},
                    "billing": {"amount": 0.09}}}
    await flow.finalize_task(task, SUCCESS, raw, route=route)
    assert queue_events["settle"][0]["actual_amount"] == 0.09


async def test_finalize_rule_error_fallback_to_freeze(
    route_factory, patch_redis, task_store, queue_events,
):
    """渠道计费规则本身坏了（求值抛错）：回退冻结金额并告警，绝不按 0 结算。"""
    route = route_factory(billing_rule="duration *", billing_type="second")  # 语法错误
    task_id = await _seed_task(task_store, route)
    await tokensession.store(task_id, "sk-user-1")

    task = await task_store.get(task_id)
    raw = {"task": {"status": "succeeded", "content": {"url": "http://v"},
                    "usage": {"output_seconds": 4}}}
    await flow.finalize_task(task, SUCCESS, raw, route=route)
    assert queue_events["settle"][0]["actual_amount"] == 0.13   # 冻结兜底


async def test_finalize_failure_cancels_freeze(
    route_factory, patch_redis, task_store, queue_events,
):
    """失败终态：全额解冻（cancel 携带用户令牌）。"""
    route = route_factory()
    task_id = await _seed_task(task_store, route)
    await tokensession.store(task_id, "sk-user-1")
    task = await task_store.get(task_id)
    raw = {"task": {"status": "failed", "error": "content moderated"}}
    ok = await flow.finalize_task(task, FAILURE, raw, route=route)
    assert ok is True
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
    row = await task_store.get(task_id)
    assert "content moderated" in row["fail_reason"]


async def test_finalize_missing_token_session_defers(
    route_factory, patch_redis, task_store, queue_events,
):
    """令牌会话丢失：不发计费事件（billing 冻结 TTL 兜底），任务仍正常终态。"""
    route = route_factory()
    task_id = await _seed_task(task_store, route)
    task = await task_store.get(task_id)
    raw = {"task": {"status": "succeeded", "content": {"url": "http://v"}}}
    ok = await flow.finalize_task(task, SUCCESS, raw, route=route)
    assert ok is True
    assert queue_events["settle"] == [] and queue_events["cancel"] == []
    row = await task_store.get(task_id)
    assert row["status"] == SUCCESS
    assert row["data"]["settled"] is False   # sweeper 持续对账/人工介入


async def test_finalize_cas_lost_is_noop(route_factory, patch_redis, task_store, queue_events):
    """CAS 竞态落败（已终态）：迟到快照丢弃，不重复发事件。"""
    route = route_factory()
    task_id = await _seed_task(task_store, route)
    await tokensession.store(task_id, "sk-user-1")
    task = await task_store.get(task_id)
    raw = {"task": {"status": "succeeded", "content": {"url": "http://v"}}}
    assert await flow.finalize_task(task, SUCCESS, raw, route=route) is True
    assert await flow.finalize_task(task, SUCCESS, raw, route=route) is False
    assert len(queue_events["settle"]) == 1   # 只结算一次
