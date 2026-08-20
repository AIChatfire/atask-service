"""任务生命周期测试：创建（异步提交受理/幂等/补偿）、查询（上游 id 反查、
耗时序列化）与终态推进（三档结算）。

边界：providers 用记录器替换（除单独声明外），上游 HTTP 走 respx，
taskstore 内存实现，queue 发布门面记录器，Redis FakeRedis。
worker 侧提交执行（submit_one）的测试在 test_submit.py。
"""

from __future__ import annotations

import hashlib
import json
import time
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import HTTPException

from app.schemas import FAILURE, SUBMITTED, SUCCESS, Quote
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
# create_task（异步提交受理：落库 → 返回本地 id → 提交事件入队）
# ---------------------------------------------------------------------------


async def test_create_task_returns_local_id_immediately(
    respx_router, route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder,
):
    """创建链路不再同步等上游：零上游出站调用，落库即返回本地 task_id，
    上游提交事件入队（worker 异步执行，见 test_submit.py）。"""
    key = key_lease_factory(param_override={"aigc_watermark": False})
    route = route_factory()
    upstream_mock = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "mm-1"})
    )
    body = {"model": "MiniMax-H3",
            "content": [{"type": "text", "text": "a cat"}],
            "duration": 5, "resolution": "2K",
            "callback_url": "https://user.test/hook"}
    pf = _make_preflight(route, key, body=body)

    view = await flow.create_task("minimax", body, pf, action="video", source="videos")

    assert view == {"task_id": pf.task_id, "status": SUBMITTED}
    assert not upstream_mock.calls                     # 请求内零上游调用（立即返回）
    row = await task_store.get(pf.task_id)
    assert row["status"] == SUBMITTED
    assert row["data"]["request_body"]["duration"] == 5   # 提交体重建/结算重估基底
    assert row["data"]["callback_url"] == "https://user.test/hook"
    assert row["channel_id"] == 7
    assert queue_events["submit"] == [pf.task_id]      # 提交事件已入队
    assert queue_events["poll"] == []                  # 拿到上游 id 前不排探测


async def test_create_task_idempotent_replay(
    respx_router, route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder,
):
    """同 Idempotency-Key 重放：回放同一本地 task_id，不产生第二次提交事件。"""
    route = route_factory()
    key = key_lease_factory()
    body = {"model": "MiniMax-H3", "duration": 5}
    pf = _make_preflight(route, key, body=body, idem_key="idem-1")
    view1 = await flow.create_task("minimax", body, pf, action="video", source="videos")
    assert view1["task_id"] == pf.task_id
    assert queue_events["submit"] == [pf.task_id]

    # 幂等键在落库后即回填：worker 尚未提交时重试也回放同一本地 id
    pf2 = _make_preflight(route, key, body=body, idem_key="idem-1")
    view2 = await flow.create_task("minimax", body, pf2, action="video", source="videos")
    assert view2["task_id"] == pf.task_id              # 返回首个任务
    assert queue_events["submit"] == [pf.task_id]      # 没有第二次提交事件
    assert len(task_store.rows) == 1


async def test_create_task_publish_failure_compensates(
    route_factory, key_lease_factory,
    patch_redis, task_store, queue_events, key_recorder, monkeypatch,
):
    """提交事件入队失败（Redis 故障）：异常传播 + 外层补偿（释放并发槽 +
    取消冻结）；幂等键已回填（重试回放该行，孤儿 sweep 会收口判死）。"""
    import app.queue as q
    from unittest.mock import AsyncMock

    monkeypatch.setattr(q, "publish_submit",
                        AsyncMock(side_effect=RuntimeError("redis down")))
    body = {"model": "MiniMax-H3", "duration": 5}
    pf = _make_preflight(route_factory(), key_lease_factory(),
                         body=body, idem_key="idem-tail")

    with pytest.raises(RuntimeError, match="redis down"):
        await flow.create_task("minimax", body, pf, action="video", source="videos")

    # 外层补偿：取消冻结（用户令牌）已发布
    assert queue_events["cancel"] == [{"request_id": pf.task_id, "user_sk": "sk-user-1"}]
    # 幂等键已落：客户端重试回放同一行而非双建双扣
    from app.services import idem
    assert await idem.get_task_id(pf.token.hash, "idem-tail") == pf.task_id


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
    assert queue_events["submit"] == []


# ---------------------------------------------------------------------------
# view_task：上游 id 反查 + 耗时序列化
# ---------------------------------------------------------------------------


async def test_view_task_resolves_upstream_task_id(
    route_factory, patch_redis, task_store, queue_events,
):
    """GET 兼容入口：客户端持上游任务 id 轮询 → 反查本地任务（返回视图里
    仍是本地 task_id，全链路以本地 id 为准）。"""
    task_id = await _seed_task(task_store, route_factory())
    await task_store.patch_data(task_id, {"upstream_task_id": "up-424010"})

    view = await flow.view_task("up-424010")
    assert view["task_id"] == task_id
    assert view["status"] == SUBMITTED
    # 反查是兜底入口，绝不意味着上游 id 可以回显给客户端
    assert "upstream_task_id" not in view
    assert "up-424010" not in json.dumps(view, ensure_ascii=False)

    with pytest.raises(HTTPException) as exc_info:
        await flow.view_task("no-such-id")
    assert exc_info.value.status_code == 404


async def test_public_view_strips_internal_fields(
    route_factory, patch_redis, task_store, queue_events,
):
    """对外契约收紧：public_view 白名单序列化——upstream_task_id/token_hash/
    freeze_amount 等内部实现细节绝不泄给客户端（上游 id 只留 tasks.data 内部，
    供轮询/结算/对账与 ops 诊断使用）。"""
    task_id = await _seed_task(task_store, route_factory())
    await task_store.patch_data(task_id, {"upstream_task_id": "up-424010"})

    task = await task_store.get(task_id)
    view = flow.public_view(task)

    assert set(view) == {"task_id", "status", "progress", "fail_reason", "result",
                         "created_at", "finish_time", "duration"}
    serialized = json.dumps(view, ensure_ascii=False)
    assert "upstream_task_id" not in view
    assert "up-424010" not in serialized          # 上游 id 值本身也不出现
    assert "token_hash" not in serialized
    assert "freeze_amount" not in serialized


async def test_cancel_task_resolves_upstream_task_id(
    route_factory, patch_redis, task_store, queue_events,
):
    """取消同一兼容入口：按上游 id 反查后正常终态收口。"""
    task_id = await _seed_task(task_store, route_factory())
    await task_store.patch_data(task_id, {"upstream_task_id": "up-424010"})
    await tokensession.store(task_id, "sk-user-1")

    view = await flow.cancel_task("up-424010")
    assert view["task_id"] == task_id
    assert view["status"] == "CANCELED"
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]


# ---------------------------------------------------------------------------
# duration：耗时序列化（秒；终态时间缺失/毫秒混入不产出天文数字）
# ---------------------------------------------------------------------------


def _duration_task(**overrides) -> dict:
    now = int(time.time())
    task = {"task_id": "d" + "0" * 31, "status": SUCCESS, "data": {},
            "created_at": now - 271, "finish_time": now}
    task.update(overrides)
    return task


async def test_duration_seconds_success_and_failure():
    """成功/失败同一公式：耗时 = 终态时间 - 创建时间（秒）。"""
    ok = flow.duration_seconds(_duration_task(status=SUCCESS))
    assert ok == 271
    failed = flow.duration_seconds(_duration_task(status=FAILURE))
    assert failed == 271
    assert flow.public_view(_duration_task())["duration"] == 271


async def test_duration_seconds_non_terminal_is_zero():
    """非终态（finish_time=0/缺失）耗时为 0，不产出"当前时间"级天文数字。"""
    assert flow.duration_seconds(_duration_task(status=SUBMITTED, finish_time=0)) == 0
    task = _duration_task(status=SUBMITTED)
    del task["finish_time"]
    assert flow.duration_seconds(task) == 0
    assert flow.duration_seconds(_duration_task(created_at=0)) == 0


async def test_duration_seconds_millisecond_timestamp_normalized():
    """毫秒时间戳混入（如 new-api 原生任务模块 UnixMilli 写法）→ 归一为秒，
    绝不把 ~1e12 的毫秒值当秒输出。"""
    now_ms = int(time.time() * 1000)
    task = _duration_task(finish_time=now_ms, created_at=now_ms - 271_000)
    assert flow.duration_seconds(task) == 271
    # 终态为毫秒、创建时间为秒的混合单位同样归一
    mixed = _duration_task(finish_time=now_ms)
    assert 0 <= flow.duration_seconds(mixed) <= 300
    # 终态缺失但创建时间毫秒混入：仍然 0
    assert flow.duration_seconds(_duration_task(finish_time=0, created_at=now_ms)) == 0


async def test_taskstore_row_time_columns_normalized():
    """taskstore 读侧单点归一：行内全部时间列的毫秒值 → 秒（共享表其他
    写入方 UnixMilli 污染的兜底，消费方拿到的永远是秒）。"""
    from app.services import taskstore

    now = int(time.time())
    row = taskstore._row_to_dict({
        "task_id": "t1", "status": SUBMITTED, "data": None,
        "submit_time": now * 1000, "start_time": now,
        "finish_time": (now + 3) * 1000, "created_at": now * 1000,
        "updated_at": now,
    })
    assert row["submit_time"] == now
    assert row["created_at"] == now
    assert row["finish_time"] == now + 3
    assert row["start_time"] == now and row["updated_at"] == now   # 秒值不动
    assert taskstore.as_unix_seconds(None) == 0
    assert taskstore.as_unix_seconds("bogus") == 0


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
    await task_store.patch_data(task_id, {"upstream_task_id": "up-424010"})
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
    # 回调投递载荷与 public_view 同一白名单：不含上游任务 id（字段与值都不出现）
    notify_payload = queue_events["notify"][0]["payload"]
    assert "upstream_task_id" not in notify_payload
    assert "up-424010" not in json.dumps(notify_payload, ensure_ascii=False)
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
