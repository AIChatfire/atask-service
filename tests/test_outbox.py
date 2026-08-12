"""W3 事务性 outbox worker 测试（SPEC §3.11.4/§4.7；DB/Redis/HTTP 全 mock）。

**零自有表（决策 A-4/A-5/A-9）**：队列载体 Redis 延迟队列（obx:due/obx:{id}/
obx:lease/obx:dead，Lua 原子领取 + 租约回收），欠费单 Redis（debt:order:{rid}
HASH + debt:orders SET），计费审计改 logfire 结构化日志。

覆盖：settle/cancel/charge 重放成功（出队 + billing_state 回写带 platform
条件 + 历史分片逐个 cancel）、settle 重估（成功/保持挂起不计 attempts）、
指数退避重排、attempts>20 死信、charge 402 欠费三连、欠费清偿、
非 charge 402 死信、sksess 取回 user_sk、lease 回收。
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest

from app import redis_queue
from app.billing.outbox import (
    NS,
    OutboxWorker,
    clear_debt_block,
    enqueue_outbox,
    is_debt_blocked,
    set_debt_block,
    write_debt_order,
)
from app.billing.pricing import PricingEvalError, PricingLogic
from tests.conftest import FakeRedis
from tests.w3_fakes import FakeSession, FakeSessionFactory

SK = "sk-user-token-0123456789abcdef"


@pytest.fixture
def qredis(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    redis = FakeRedis()

    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr("app.billing.outbox.get_redis", _get_redis)
    monkeypatch.setattr("app.auth.get_redis", _get_redis)  # sksess（user_sk 取回）
    return redis


def _worker(
    sessions: list[FakeSession],
    billing: AsyncMock | None = None,
    pricing: AsyncMock | None = None,
) -> OutboxWorker:
    billing = billing or AsyncMock()
    pricing = pricing or AsyncMock()
    return OutboxWorker(billing, pricing, FakeSessionFactory(sessions))


async def _seed(
    qredis: FakeRedis, op: str, payload: dict, attempts: int = 0
) -> str:
    outbox_id = await enqueue_outbox(task_id="task_a", op=op, payload=payload)
    if attempts:
        await qredis.hset(f"{NS}:{outbox_id}", "attempts", str(attempts))
    return outbox_id


# ---------- 成功路径 ----------


async def test_settle_success_full_side_effects(qredis: FakeRedis) -> None:
    billing = AsyncMock()
    billing.settle.return_value = {"settled_amount": 200}
    payload = {"request_id": "task_a:2", "actual_amount": "0.4", "reevaluate": False,
               "cancel_prev_shards": ["task_a:0", "task_a:1"],
               "user_sk": SK, "user_id": 7, "biz": "kling"}
    outbox_id = await _seed(qredis, "settle", payload)
    session = FakeSession()
    processed = await _worker([session], billing=billing)._sweep_once()

    assert processed == 1
    billing.settle.assert_awaited_once()
    kw = billing.settle.await_args.kwargs
    assert kw["request_id"] == "task_a:2"  # request_id 原样（幂等）
    assert kw["actual_usd"] == Decimal("0.4") and kw["user_sk"] == SK
    # 历史分片逐个 cancel（幂等收口）
    assert [c.kwargs["request_id"] for c in billing.cancel.await_args_list] == [
        "task_a:0", "task_a:1"]

    # 成功出队：HASH 删除 + due/lease 摘除
    assert await qredis.hgetall(f"{NS}:{outbox_id}") == {}
    assert await qredis.zcard(f"{NS}:due") == 0
    assert await qredis.zcard(f"{NS}:lease") == 0
    # billing_state 回写：platform 前缀条件（§4.5），不校验 status（§4.7）
    state = session.statements_containing("$.gateway.billing_state")
    assert state and state[0][1]["state"] == "settled"
    assert "platform LIKE" in state[0][0]
    assert "status" not in state[0][0].split("WHERE")[1]
    assert session.commits == 1


async def test_cancel_op_success(qredis: FakeRedis) -> None:
    billing = AsyncMock()
    await _seed(qredis, "cancel",
                {"request_id": "task_a:0", "user_sk": SK, "user_id": 7})
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    billing.cancel.assert_awaited_once()
    assert billing.cancel.await_args.kwargs["request_id"] == "task_a:0"
    state = session.statements_containing("$.gateway.billing_state")
    assert state and state[0][1]["state"] == "cancelled"


async def test_charge_success_marks_charged(qredis: FakeRedis) -> None:
    billing = AsyncMock()
    billing.charge.return_value = {"charged": True}
    payload = {"request_id": "pt:7:abc", "biz_type": "kling_video", "metric": "call",
               "amount": "0.05", "verify_only": False, "user_sk": SK, "user_id": 7}
    await _seed(qredis, "charge", payload)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    assert billing.charge.await_args.kwargs["amount_usd"] == Decimal("0.05")
    state = session.statements_containing("$.gateway.billing_state")
    assert state and state[0][1]["state"] == "charged"


async def test_settle_reevaluate_success(qredis: FakeRedis) -> None:
    """payload.reevaluate=true → 先 settle 相位重估再打 settle。"""
    billing = AsyncMock()
    pricing = AsyncMock()
    pricing.get_logic_for_task.return_value = PricingLogic(
        expr="duration*0.08", expr_type="asteval", version=3,
        fallback_amount_usd=Decimal("1"))
    pricing.evaluate.return_value = Decimal("0.333333")
    payload = {"request_id": "task_a:0", "actual_amount": None, "reevaluate": True,
               "context": {"duration": 4.0}, "user_sk": SK, "user_id": 7}
    await _seed(qredis, "settle", payload)
    session = FakeSession()
    await _worker([session], billing=billing, pricing=pricing)._sweep_once()
    assert pricing.evaluate.await_args.kwargs["phase"] == "settle"
    assert billing.settle.await_args.kwargs["actual_usd"] == Decimal("0.333333")


async def test_settle_reevaluate_failure_keeps_pending(qredis: FakeRedis) -> None:
    """重估仍失败：保持挂起（不打 settle、不出队、不静默顶格、不计 attempts）。"""
    billing = AsyncMock()
    pricing = AsyncMock()
    pricing.get_logic_for_task.side_effect = PricingEvalError("expr boom")
    payload = {"request_id": "task_a:0", "actual_amount": None,
               "reevaluate": True, "user_sk": SK, "user_id": 7}
    outbox_id = await _seed(qredis, "settle", payload)
    session = FakeSession()
    await _worker([session], billing=billing, pricing=pricing)._sweep_once()
    billing.settle.assert_not_awaited()
    item = await redis_queue.get_item(qredis, NS, outbox_id)
    assert item is not None
    assert item["attempts"] == "0"  # 挂起不计 attempts
    assert item["state"] == "pending"
    assert "reevaluate" in item["last_error"]
    assert await qredis.zcard(f"{NS}:due") == 1  # 重排回 due


# ---------- 失败重排与死信 ----------


async def test_failure_exponential_backoff(qredis: FakeRedis) -> None:
    billing = AsyncMock()
    billing.cancel.side_effect = httpx.ConnectError("boom")
    outbox_id = await _seed(
        qredis, "cancel", {"request_id": "task_a:0", "user_sk": SK}, attempts=2)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    item = await redis_queue.get_item(qredis, NS, outbox_id)
    assert item is not None
    assert item["attempts"] == "3"  # attempts+1
    assert item["last_error"]
    assert item["state"] == "pending"
    assert await qredis.zcard(f"{NS}:due") == 1  # 退避重排回 due


async def test_dead_letter_after_max_attempts(qredis: FakeRedis) -> None:
    billing = AsyncMock()
    billing.cancel.side_effect = httpx.ConnectError("boom")
    outbox_id = await _seed(
        qredis, "cancel", {"request_id": "task_a:0", "user_sk": SK}, attempts=20)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    item = await redis_queue.get_item(qredis, NS, outbox_id)
    assert item is not None
    assert item["state"] == "dead"
    assert item["attempts"] == "21"
    assert item["dead_reason"].startswith("dead_letter:")
    assert await qredis.zcard(f"{NS}:dead") == 1  # 死信清单
    assert await qredis.zcard(f"{NS}:due") == 0
    assert not session.statements_containing("$.gateway.billing_state")


async def test_non_charge_402_dead_letter(qredis: FakeRedis) -> None:
    from app.billing.client import InsufficientBalance

    billing = AsyncMock()
    billing.settle.side_effect = InsufficientBalance()
    payload = {"request_id": "task_a:0", "actual_amount": "0.4",
               "reevaluate": False, "user_sk": SK, "user_id": 7}
    outbox_id = await _seed(qredis, "settle", payload)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    item = await redis_queue.get_item(qredis, NS, outbox_id)
    assert item is not None and item["state"] == "dead"
    assert item["dead_reason"] == "unexpected 402"
    assert await qredis.zcard(f"{NS}:dead") == 1


# ---------- charge 402 欠费三连（决策 A-9：欠费单 Redis） ----------


async def test_charge_402_debt_triple(qredis: FakeRedis) -> None:
    """①欠费单 Redis（debt:order:{rid} HASH + debt:orders SET）
    ②debt:{user_id} 熔断名单 ③保持重试（充值后扣回）。"""
    from app.billing.client import InsufficientBalance

    billing = AsyncMock()
    billing.charge.side_effect = InsufficientBalance()
    payload = {"request_id": "pt:7:abc", "biz_type": "kling_video", "metric": "call",
               "amount": "0.5", "user_sk": SK, "user_id": 7, "debt": True}
    outbox_id = await _seed(qredis, "charge", payload)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()

    debt = await qredis.hgetall("debt:order:pt:7:abc")
    assert debt["user_id"] == "7" and debt["amount"] == "0.5"
    assert debt["status"] == "open"
    assert await qredis.sismember("debt:orders", "pt:7:abc")
    assert await qredis.exists("debt:7")  # 熔断名单
    item = await redis_queue.get_item(qredis, NS, outbox_id)
    assert item is not None and item["state"] == "pending"  # 保持重试


async def test_debt_order_idempotent(qredis: FakeRedis) -> None:
    """欠费单 request_id 幂等：重复写不重复计（对齐原 INSERT IGNORE）。"""
    await write_debt_order(user_id=7, task_id=None, request_id="pt:7:abc",
                           amount_usd=Decimal("0.5"))
    first = await qredis.hgetall("debt:order:pt:7:abc")
    await write_debt_order(user_id=7, task_id=None, request_id="pt:7:abc",
                           amount_usd=Decimal("9.9"))
    assert await qredis.hgetall("debt:order:pt:7:abc") == first


async def test_debt_cleared_on_charge_success(qredis: FakeRedis) -> None:
    await write_debt_order(user_id=7, task_id=None, request_id="pt:7:abc",
                           amount_usd=Decimal("0.5"))
    await set_debt_block(7)
    billing = AsyncMock()
    billing.charge.return_value = {"charged": True}
    payload = {"request_id": "pt:7:abc", "biz_type": "kling_video", "metric": "call",
               "amount": "0.5", "user_sk": SK, "user_id": 7, "debt": True}
    await _seed(qredis, "charge", payload)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    debt = await qredis.hgetall("debt:order:pt:7:abc")
    assert debt["status"] == "cleared" and debt["cleared_at"]
    assert not await qredis.sismember("debt:orders", "pt:7:abc")  # open 清单摘除
    assert not await qredis.exists("debt:7")  # 熔断名单解除


async def test_debt_block_helpers(qredis: FakeRedis) -> None:
    assert await is_debt_blocked(7) is False
    await set_debt_block(7)
    assert await is_debt_blocked(7) is True
    await clear_debt_block(7)
    assert await is_debt_blocked(7) is False


# ---------- user_sk 取回（sksess:{task_id}） ----------


async def test_user_sk_via_sksess(qredis: FakeRedis) -> None:
    billing = AsyncMock()
    qredis._data["sksess:task_a"] = "sk-resolved"  # submit 时写入的 sksess
    payload = {"request_id": "task_a:0", "actual_amount": "0.4",
               "reevaluate": False, "user_id": 9}
    await _seed(qredis, "settle", payload)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    assert billing.settle.await_args.kwargs["user_sk"] == "sk-resolved"


async def test_user_sk_unavailable_retries(qredis: FakeRedis) -> None:
    billing = AsyncMock()
    payload = {"request_id": "task_a:0", "actual_amount": "0.4",
               "reevaluate": False, "user_id": 9}  # 无 user_sk 且无 sksess
    outbox_id = await _seed(qredis, "settle", payload)
    session = FakeSession()
    await _worker([session], billing=billing)._sweep_once()
    billing.settle.assert_not_awaited()
    item = await redis_queue.get_item(qredis, NS, outbox_id)
    assert item is not None and item["attempts"] == "1"  # 告警 + 退避重试
    assert item["state"] == "pending"


# ---------- 队列原语：lease 回收与死信重放 ----------


async def test_claim_skips_future_and_reclaims_expired_lease(
    qredis: FakeRedis,
) -> None:
    """未到期条目不领取；lease 过期条目回收回 due 后可再领（副本死亡兜底）。"""
    future = await enqueue_outbox(task_id="t1", op="cancel", payload={})
    await redis_queue.reschedule(qredis, NS, future, delay_seconds=3600)
    claimed = await redis_queue.claim(qredis, NS, limit=10, lease_seconds=60)
    assert claimed == []

    # 领取后 lease 过期 → 回收回 due → 再次可领
    due = await enqueue_outbox(task_id="t2", op="cancel", payload={})
    claimed = await redis_queue.claim(qredis, NS, limit=10, lease_seconds=-1)
    assert claimed == [due]
    assert await qredis.zcard(f"{NS}:lease") == 1
    reclaimed = await redis_queue.reclaim_expired_leases(qredis, NS)
    assert reclaimed == [due]
    item = await redis_queue.get_item(qredis, NS, due)
    assert item is not None and item["state"] == "pending"


async def test_replay_dead_only_when_dead(qredis: FakeRedis) -> None:
    outbox_id = await enqueue_outbox(task_id="t3", op="cancel", payload={})
    assert await redis_queue.replay_dead(qredis, NS, outbox_id) is False
    await redis_queue.dead_letter(qredis, NS, outbox_id, reason="max attempts",
                                  attempts=21)
    assert await redis_queue.replay_dead(qredis, NS, outbox_id) is True
    item = await redis_queue.get_item(qredis, NS, outbox_id)
    assert item is not None
    assert item["state"] == "pending" and item["attempts"] == "0"
    assert await qredis.zcard(f"{NS}:dead") == 0
    assert await qredis.zcard(f"{NS}:due") == 1
