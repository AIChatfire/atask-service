"""W3 FreezeRenewer 分片续期测试（SPEC §3.11.5，架构 §5.4.1；DB/Redis/HTTP 全 mock）。

覆盖：到期前续期（freeze 新分片 → cancel 旧分片 → Redis 热台账 +
JSON_SET 三键回写 seq/amount/expires 带 platform 前缀条件 + logfire 审计）、
多分片序列（:0→:1→:2 后终态 settle 收口）、健康分片跳过、NX 锁互斥、
Redis 台账丢失 tasks 行回读重建（决策 A-7）、金额不可恢复告警跳过、
sksess 取不到告警下轮重试（freeze 幂等重放安全）、续期失败下轮重试。
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest

from app.billing.renewer import FREEZE_SHARD_TTL, RENEW_WINDOW, FreezeRenewer
from app.registry import BizConfig
from tests.w3_fakes import FakeRedis, FakeSession, FakeSessionFactory

SK = "sk-user-token-0123456789abcdef"


def _now() -> int:
    return int(time.time())


def _task_row(seq: int = 0, deadline_in: int = 50_000,
              *, with_shard_keys: bool = True) -> dict:
    gw: dict = {"billing_state": "frozen", "biz": "kling",
                "freeze_shard_seq": seq,
                "deadline_unix": _now() + deadline_in}
    if with_shard_keys:
        gw["freeze_shard_amount_usd"] = "1.5"
        gw["freeze_shard_expires_at"] = _now() + 100
    pdata = {"gateway": gw}
    return {"task_id": "task_a", "user_id": 7, "private_data": json.dumps(pdata)}


def _biz_cfg() -> BizConfig:
    return BizConfig(
        biz="kling", adapter="kling", upstream_base_url="http://up",
        auth_type="aksk_jwt", auth_secret_ref="X", native_prefixes=["v1"],
        enabled=True, billing_keys={"biz_type": "kling_video", "metric": "call"},
        default_freeze_amount_usd="2.5", rate_limit={},
        newapi_channel_id=None, version=1,
    )


@pytest.fixture
def redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def _patch_infra(monkeypatch: pytest.MonkeyPatch, redis: FakeRedis) -> None:
    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr("app.billing.renewer.get_redis", _get_redis)
    monkeypatch.setattr("app.billing.renewer.registry.get",
                        AsyncMock(return_value=_biz_cfg()))
    # user_sk 取回走 sksess:{task_id}（submit 写入、终态 DEL）
    monkeypatch.setattr("app.auth.get_redis", _get_redis)
    redis.strings["sksess:task_a"] = SK


def _seed_shard(redis: FakeRedis, seq: int, expires_in: int) -> None:
    redis.hashes["freeze:shard:task_a"] = {
        "seq": str(seq), "amount_usd": "1.5", "expires_at": str(_now() + expires_in)}


def _recheck() -> FakeSession:
    """终态竞态复查会话：任务仍在途且 billing_state='frozen'。"""
    return FakeSession([[{"status": "IN_PROGRESS", "billing_state": "frozen"}]])


async def test_renew_happy_path(_patch_infra: None, redis: FakeRedis) -> None:
    _seed_shard(redis, seq=0, expires_in=100)  # 距到期 100s < RENEW_WINDOW
    billing = AsyncMock()
    sweep = FakeSession([[_task_row()]])
    registry_session = FakeSession()
    write = FakeSession()
    worker = FreezeRenewer(billing, FakeSessionFactory(
        [sweep, _recheck(), registry_session, write]))
    await worker._sweep_once()

    # freeze 新分片：request_id={task_id}:{seq+1}、同金额、ttl=min(剩余, 82800)
    fkw = billing.freeze.await_args.kwargs
    assert fkw["request_id"] == "task_a:1"
    assert fkw["amount_usd"] == Decimal("1.5")
    assert fkw["biz_type"] == "kling_video" and fkw["metric"] == "call"
    assert fkw["user_sk"] == SK
    assert 49_000 < fkw["ttl_seconds"] <= min(50_000, FREEZE_SHARD_TTL)
    # 主动 cancel 旧分片立即释放资金
    assert billing.cancel.await_args.kwargs["request_id"] == "task_a:0"

    # Redis 热台账推进
    shard = redis.hashes["freeze:shard:task_a"]
    assert shard["seq"] == "1" and shard["amount_usd"] == "1.5"

    # 持久真相源 JSON_SET 三键回写（带 platform 前缀条件，§4.5/§5.4.1，
    # 决策 A-7：seq/金额/到期齐全即 Redis 丢失重建源）
    json_set = write.statements_containing("$.gateway.freeze_shard_seq")
    assert json_set and json_set[0][1]["seq"] == 1
    assert json_set[0][1]["amount"] == "1.5"
    assert json_set[0][1]["expires"] > 0
    assert "platform LIKE" in json_set[0][0]
    assert "$.gateway.freeze_shard_amount_usd" in json_set[0][0]
    assert "$.gateway.freeze_shard_expires_at" in json_set[0][0]
    # 零自有表：无任何 gateway_ 表写入（审计走 logfire）
    assert not write.statements_containing("gateway_")
    assert write.commits == 1


async def test_multi_shard_sequence_then_settle(_patch_infra: None,
                                                redis: FakeRedis,
                                                monkeypatch: pytest.MonkeyPatch) -> None:
    """分片 :0→:1→:2 续期序列；终态 settle 打当前活跃分片 + 历史分片逐个 cancel。"""
    billing = AsyncMock()
    sessions = [FakeSession([[_task_row(seq=0)]]), _recheck(), FakeSession(),
                FakeSession(),
                FakeSession([[_task_row(seq=1)]]), _recheck(), FakeSession(),
                FakeSession()]
    worker = FreezeRenewer(billing, FakeSessionFactory(sessions))

    _seed_shard(redis, seq=0, expires_in=100)
    await worker._sweep_once()  # :0 → :1
    redis.strings.pop("freeze:renew:task_a", None)  # 锁 120s TTL 已过
    _seed_shard(redis, seq=1, expires_in=100)  # 模拟 23h 后再次进入续期窗口
    await worker._sweep_once()  # :1 → :2

    assert [c.kwargs["request_id"] for c in billing.freeze.await_args_list] == [
        "task_a:1", "task_a:2"]
    assert [c.kwargs["request_id"] for c in billing.cancel.await_args_list] == [
        "task_a:0", "task_a:1"]
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "2"

    # 终态统一收口（终态副作用入队 Redis obx 队列，由 worker 重放，决策 A-4）
    from app.billing.outbox import OutboxWorker, enqueue_outbox
    from tests.conftest import FakeRedis as QRedis

    qredis = QRedis()

    async def _obx_redis() -> QRedis:
        return qredis

    monkeypatch.setattr("app.billing.outbox.get_redis", _obx_redis)
    payload = {"request_id": "task_a:2", "actual_amount": "0.42", "reevaluate": False,
               "cancel_prev_shards": ["task_a:0", "task_a:1"],
               "user_sk": SK, "user_id": 7, "biz": "kling"}
    await enqueue_outbox(task_id="task_a", op="settle", payload=payload)
    ob = OutboxWorker(billing, AsyncMock(),
                      FakeSessionFactory([FakeSession()]))
    await ob._sweep_once()
    assert billing.settle.await_args.kwargs["request_id"] == "task_a:2"  # 当前活跃分片
    prev = [c.kwargs["request_id"] for c in billing.cancel.await_args_list[-2:]]
    assert prev == ["task_a:0", "task_a:1"]  # 历史分片逐个 cancel（幂等）
    assert await qredis.hgetall("obx:due") == {}  # 已成功出队


async def test_healthy_shard_skipped(_patch_infra: None, redis: FakeRedis) -> None:
    _seed_shard(redis, seq=0, expires_in=RENEW_WINDOW + 500)
    billing = AsyncMock()
    worker = FreezeRenewer(billing, FakeSessionFactory([FakeSession([[_task_row()]])]))
    await worker._sweep_once()
    billing.freeze.assert_not_awaited()


async def test_renew_lock_contention_skipped(_patch_infra: None,
                                             redis: FakeRedis) -> None:
    _seed_shard(redis, seq=0, expires_in=100)
    redis.strings["freeze:renew:task_a"] = "1"  # 另一副本持锁
    billing = AsyncMock()
    worker = FreezeRenewer(billing, FakeSessionFactory([FakeSession([[_task_row()]])]))
    await worker._sweep_once()
    billing.freeze.assert_not_awaited()


async def test_redis_ledger_rebuilt_from_tasks_row(_patch_infra: None,
                                                   redis: FakeRedis) -> None:
    """Redis 热台账丢失 → tasks 行三键回读重建（决策 A-7，金额/到期齐全）。"""
    billing = AsyncMock()
    row = _task_row(seq=3)
    pdata = json.loads(row["private_data"])
    pdata["gateway"]["freeze_shard_amount_usd"] = "2.0"
    row["private_data"] = json.dumps(pdata)
    sweep = FakeSession([[row]])
    worker = FreezeRenewer(
        billing, FakeSessionFactory(
            [sweep, _recheck(), FakeSession(), FakeSession()]))
    await worker._sweep_once()
    assert billing.freeze.await_args.kwargs["request_id"] == "task_a:4"
    assert billing.freeze.await_args.kwargs["amount_usd"] == Decimal("2.0")
    assert billing.cancel.await_args.kwargs["request_id"] == "task_a:3"
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "4"  # 重建后再推进


async def test_ledger_unrecoverable_skips_with_alert(_patch_infra: None,
                                                     redis: FakeRedis) -> None:
    """tasks 行缺金额键 → 金额不可恢复：告警跳过（绝不按 0 续冻）。"""
    billing = AsyncMock()
    sweep = FakeSession([[_task_row(seq=3, with_shard_keys=False)]])
    worker = FreezeRenewer(billing, FakeSessionFactory([sweep]))
    await worker._sweep_once()
    billing.freeze.assert_not_awaited()


async def test_sk_unavailable_alerts_and_retries_next_round(
    _patch_infra: None, redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis.strings.pop("sksess:task_a", None)  # sksess 缺失：取不到 sk
    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    sessions = [FakeSession([[_task_row()]]), _recheck(),  # 第 1 轮：sk 取不到
                FakeSession([[_task_row()]]), _recheck(), FakeSession(),
                FakeSession()]  # 第 2 轮
    worker = FreezeRenewer(billing, FakeSessionFactory(sessions))
    await worker._sweep_once()
    billing.freeze.assert_not_awaited()
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "0"  # 台账未推进
    redis.strings.pop("freeze:renew:task_a", None)  # 锁 120s TTL 已过

    redis.strings["sksess:task_a"] = SK  # 下轮 sksess 恢复
    await worker._sweep_once()  # 下轮重试成功（freeze 幂等重放安全）
    assert billing.freeze.await_args.kwargs["request_id"] == "task_a:1"
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "1"


async def test_renew_billing_5xx_retried_next_sweep(_patch_infra: None,
                                                    redis: FakeRedis) -> None:
    """续期失败（计费 5xx）→ 不推进台账，下轮重试同 request_id（幂等）。"""
    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    billing.freeze.side_effect = [
        httpx.ConnectError("billing down"), {"status": "frozen"}]
    sessions = [FakeSession([[_task_row()]]), _recheck(), FakeSession(),  # 第 1 轮
                FakeSession([[_task_row()]]), _recheck(), FakeSession(),
                FakeSession()]  # 第 2 轮
    worker = FreezeRenewer(billing, FakeSessionFactory(sessions))
    await worker._sweep_once()  # 失败：无台账回写
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "0"
    redis.strings.pop("freeze:renew:task_a", None)  # 锁 120s TTL 已过
    await worker._sweep_once()  # 下轮重试成功
    assert [c.kwargs["request_id"] for c in billing.freeze.await_args_list] == [
        "task_a:1", "task_a:1"]
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "1"


# ---------------------------------------------------------------------------
# 终态竞态防护（执行前复查 + seq 回写在途条件 + rowcount=0 补偿）
# ---------------------------------------------------------------------------


async def test_renew_skipped_when_task_already_terminal(
    _patch_infra: None, redis: FakeRedis
) -> None:
    """扫描快照后任务已被收敛到终态 → 复查拦截：不 freeze、不推进台账。"""
    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    sweep = FakeSession([[_task_row()]])
    recheck = FakeSession([[{"status": "SUCCESS", "billing_state": "settled"}]])
    worker = FreezeRenewer(billing, FakeSessionFactory([sweep, recheck]))
    await worker._sweep_once()
    billing.freeze.assert_not_awaited()
    billing.cancel.assert_not_awaited()
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "0"


class _ZeroRowcountSession(FakeSession):
    """模拟 seq 回写 UPDATE 在途条件不命中（rowcount=0）的会话。"""

    async def execute(self, stmt, params=None):  # type: ignore[no-untyped-def]
        res = await super().execute(stmt, params)
        if "freeze_shard_seq" in str(stmt):
            res.rowcount = 0
        return res


async def test_writeback_lost_terminal_race_compensates_new_shard(
    _patch_infra: None, redis: FakeRedis
) -> None:
    """freeze/cancel 完成后任务被终态通道收敛（rowcount=0）：
    台账回滚 + 补偿 cancel 新分片 + Redis 热台账摘除（绝不指向已取消分片）。"""
    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    sweep = FakeSession([[_task_row()]])
    write = _ZeroRowcountSession()
    worker = FreezeRenewer(
        billing, FakeSessionFactory([sweep, _recheck(), FakeSession(), write]))
    await worker._sweep_once()

    assert billing.freeze.await_args.kwargs["request_id"] == "task_a:1"
    cancels = [c.kwargs["request_id"] for c in billing.cancel.await_args_list]
    assert cancels == ["task_a:0", "task_a:1"]  # 旧分片 + 补偿取消新分片
    assert "freeze:shard:task_a" not in redis.hashes  # 热台账摘除
    assert write.rollbacks == 1 and write.commits == 0
    assert not write.statements_containing("ON DUPLICATE KEY UPDATE")
    assert not write.statements_containing("INSERT INTO gateway_billing_audit")


async def test_seq_writeback_has_inflight_status_condition(
    _patch_infra: None, redis: FakeRedis
) -> None:
    """seq 回写 UPDATE 带在途状态条件（终态竞态 SQL 级防护）。"""
    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    write = FakeSession()
    worker = FreezeRenewer(billing, FakeSessionFactory(
        [FakeSession([[_task_row()]]), _recheck(), FakeSession(), write]))
    await worker._sweep_once()
    json_set = write.statements_containing("$.gateway.freeze_shard_seq")
    assert json_set
    assert "status IN ('SUBMITTED','QUEUED','IN_PROGRESS')" in json_set[0][0]


# ---------------------------------------------------------------------------
# 续期 402 语义：收敛 + 止损（不与 5xx 同等下轮重试）
# ---------------------------------------------------------------------------


async def test_renew_402_converges_task_and_stops_loss(
    _patch_infra: None, redis: FakeRedis
) -> None:
    """402 → ①立即 cancel 旧分片止损 ②transition timeout 收敛（终态 outbox
    全额解冻兜底）；不推进台账、不再按 5xx 下轮重试。"""
    from app.billing.client import InsufficientBalance

    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    billing.freeze.side_effect = InsufficientBalance()
    tm = AsyncMock()
    tm.transition = AsyncMock(return_value=True)
    worker = FreezeRenewer(
        billing,
        FakeSessionFactory([FakeSession([[_task_row()]]), _recheck(),
                            FakeSession(), FakeSession()]),
        task_manager=tm,
    )
    await worker._sweep_once()

    # ①止损：旧分片立即 cancel
    assert billing.cancel.await_args.kwargs["request_id"] == "task_a:0"
    # ②收敛：timeout 快照经唯一仲裁点推进（status-CAS 天然防竞态重复）
    tm.transition.assert_awaited_once()
    kw = tm.transition.await_args.kwargs
    assert kw["task_id"] == "task_a" and kw["channel"] == "sweep"
    assert kw["snapshot"].status.name == "TIMEOUT"
    assert kw["snapshot"].error["code"] == "payment_required"
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "0"  # 台账未推进


async def test_renew_402_converge_real_task_manager(
    _patch_infra: None, redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """402 收敛走真实 TaskManager.transition：终态 outbox cancel 行落库
    （cancel_prev_shards 覆盖历史分片），资金不锁死。"""
    from app.billing.client import InsufficientBalance
    from app.tasks.manager import TaskManager

    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    billing.freeze.side_effect = InsufficientBalance()

    async def _get_redis() -> FakeRedis:
        return redis

    # TaskManager.current_freeze_shard 读 Redis（有台账 seq=0）
    monkeypatch.setattr("app.tasks.manager.get_redis", _get_redis)
    obx_calls: list[dict] = []

    async def _enqueue_outbox(**kw):  # type: ignore[no-untyped-def]
        obx_calls.append(kw)
        return "obx_t"

    monkeypatch.setattr("app.billing.outbox.enqueue_outbox", _enqueue_outbox)
    tm = TaskManager(billing, AsyncMock())
    transition_session = FakeSession([[
        {"status": "IN_PROGRESS", "user_id": 7,
         "private_data": json.dumps({"gateway": {
             "billing_state": "frozen", "biz": "kling", "freeze_shard_seq": 0,
             "callback_url": None, "request_snapshot": {}}})}]])
    worker = FreezeRenewer(
        billing,
        FakeSessionFactory([FakeSession([[_task_row()]]), _recheck(),
                            FakeSession(), transition_session]),
        task_manager=tm,
    )
    await worker._sweep_once()

    # transition CAS 胜出 → 终态 outbox cancel 条入队 Redis（全额解冻当前+
    # 历史分片，决策 A-4）
    ob = obx_calls[0]
    assert ob["op"] == "cancel"
    assert ob["payload"]["request_id"] == "task_a:0"
    assert transition_session.commits == 1


async def test_renew_402_without_task_manager_alerts_and_keeps_ledger(
    _patch_infra: None, redis: FakeRedis
) -> None:
    """TaskManager 未注入（并行开发兜底）：402 退回告警口径——止损 cancel 旧
    分片后保持台账，下轮重试（freeze 幂等）。"""
    from app.billing.client import InsufficientBalance

    _seed_shard(redis, seq=0, expires_in=100)
    billing = AsyncMock()
    billing.freeze.side_effect = InsufficientBalance()
    worker = FreezeRenewer(
        billing,
        FakeSessionFactory([FakeSession([[_task_row()]]), _recheck(),
                            FakeSession()]),
    )
    await worker._sweep_once()
    assert billing.cancel.await_args.kwargs["request_id"] == "task_a:0"
    assert redis.hashes["freeze:shard:task_a"]["seq"] == "0"
