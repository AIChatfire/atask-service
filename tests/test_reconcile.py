"""W3 每日对账入口测试（SPEC §3.11.6，架构 §5.5 五项；DB/HTTP 全 mock）。

零自有表（决策 A-4/A-5/A-8）：
- 漏结算（服务端非 frozen / billing_state 未收敛）→ 重新入队 Redis obx；
- 三方对账读计费服务 /billing/logs 资金流水；
- 报告 = 返回值 dict + stdout 单行 JSON（定时任务采集），不再落表。
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from app.billing.reconcile import run_daily_reconciliation
from tests.conftest import FakeRedis
from tests.w3_fakes import FakeSession, FakeSessionFactory

SK = "sk-user-token-0123456789abcdef"


def _pdata(seq: int = 0) -> str:
    return json.dumps({"gateway": {"billing_state": "frozen", "freeze_shard_seq": seq}})


@pytest.fixture
def qredis(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    redis = FakeRedis()

    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr("app.billing.outbox.get_redis", _get_redis)
    monkeypatch.setattr("app.auth.get_redis", _get_redis)  # sksess（user_sk 取回）
    for tid in ("task_a", "task_b", "task_c"):
        redis._data[f"sksess:{tid}"] = SK
    return redis


async def test_reconciliation_mismatch_report_and_reenqueue(
    qredis: FakeRedis, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    billing = AsyncMock()
    billing.get_freeze.return_value = {"status": "cancelled"}  # 服务端已解冻 → 差异
    billing.get_billing_logs.return_value = [
        {"op": "settle", "amount_usd": "0.9"}]  # settle 流水 0.90 vs 上游 1.00

    sessions = [
        # 1. frozen>24h：终态 SUCCESS 但服务端已解冻 → 差异 + 重新入队
        FakeSession([[{"task_id": "task_a", "user_id": 7, "status": "SUCCESS",
                       "private_data": _pdata(2)}]]),
        # 2. 台账比对：SUCCESS 但 billing_state 仍 frozen → 差异 + 重新入队
        FakeSession([[{"task_id": "task_b", "user_id": 7, "status": "SUCCESS",
                       "billing_state": "frozen", "private_data": _pdata(0)}]]),
        # 3. 三方对账：上游实收 1.00 vs 计费服务 settle 流水 0.90 → 差异 0.10
        FakeSession([[{"task_id": "task_c", "user_id": 7,
                       "private_data": _pdata(0), "upstream_amount": "1.0"}]]),
        # 5. 滞留巡检：一行
        FakeSession([[{"task_id": "task_d", "status": "IN_PROGRESS",
                       "submit_time": 1}]]),
    ]
    report = await run_daily_reconciliation(
        FakeSessionFactory(sessions), billing, AsyncMock())

    # 对账轮询打当前活跃分片
    assert billing.get_freeze.await_args.kwargs["request_id"] == "task_a:2"
    assert billing.get_freeze.await_args.kwargs["user_sk"] == SK
    # 三方对账读计费服务资金流水（决策 A-5）
    assert billing.get_billing_logs.await_args.kwargs["request_id"] == "task_c:0"

    assert report["total_count"] == 3
    assert report["mismatch_count"] == 3
    assert report["mismatch_amount_usd"] == "0.1"
    detail = report["detail"]
    assert detail["frozen_over_24h"][0]["task_id"] == "task_a"
    assert detail["frozen_over_24h"][0]["reenqueue"] is True
    assert detail["ledger_mismatches"][0]["task_id"] == "task_b"
    assert detail["three_way_mismatches"][0]["task_id"] == "task_c"
    assert detail["three_way_mismatches"][0]["diff_usd"] == "0.1"
    assert detail["stuck_tasks"][0]["task_id"] == "task_d"

    # 漏结算重新入队（决策 A-4 对账兜底）：task_a settle / task_b settle
    reenq = {e["task_id"]: e["outbox_id"] for e in detail["reenqueued"]}
    assert set(reenq) == {"task_a", "task_b"}
    item_a = await qredis.hgetall(f"obx:{reenq['task_a']}")
    assert item_a["op"] == "settle"
    payload = json.loads(item_a["payload"])
    assert payload["request_id"] == "task_a:2"
    assert payload["reevaluate"] is True
    assert payload["cancel_prev_shards"] == ["task_a:0", "task_a:1"]
    item_b = await qredis.hgetall(f"obx:{reenq['task_b']}")
    assert item_b["op"] == "settle"

    # stdout 单行 JSON 报告（定时任务采集，决策 A-8）
    line = capsys.readouterr().out.strip()
    out_report = json.loads(line)
    assert out_report["event"] == "reconciliation_report"
    assert out_report["mismatch_count"] == 3


async def test_reconciliation_clean(qredis: FakeRedis,
                                    monkeypatch: pytest.MonkeyPatch) -> None:
    billing = AsyncMock()
    sessions = [FakeSession([[]]), FakeSession([[]]), FakeSession([[]]),
                FakeSession([[]])]
    report = await run_daily_reconciliation(
        FakeSessionFactory(sessions), billing, AsyncMock())
    billing.get_freeze.assert_not_awaited()
    assert report["mismatch_count"] == 0
    assert report["detail"]["reenqueued"] == []


async def test_frozen_row_clean_when_server_frozen(
    qredis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    billing = AsyncMock()
    billing.get_freeze.return_value = {"status": "frozen"}  # 一致 → 无差异
    sessions = [
        FakeSession([[{"task_id": "task_a", "user_id": 7, "status": "IN_PROGRESS",
                       "private_data": _pdata(0)}]]),
        FakeSession([[]]), FakeSession([[]]), FakeSession([[]]),
    ]
    report = await run_daily_reconciliation(
        FakeSessionFactory(sessions), billing, AsyncMock())
    assert report["mismatch_count"] == 0
    assert report["total_count"] == 1


async def test_frozen_row_inflight_not_reenqueued(
    qredis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """在途任务服务端非 frozen → 差异报告但不重入队（终态后由第 2 项兜底）。"""
    billing = AsyncMock()
    billing.get_freeze.return_value = {"status": "cancelled"}
    sessions = [
        FakeSession([[{"task_id": "task_a", "user_id": 7, "status": "IN_PROGRESS",
                       "private_data": _pdata(0)}]]),
        FakeSession([[]]), FakeSession([[]]), FakeSession([[]]),
    ]
    report = await run_daily_reconciliation(
        FakeSessionFactory(sessions), billing, AsyncMock())
    assert report["mismatch_count"] == 1
    assert report["detail"]["frozen_over_24h"][0]["reenqueue"] is False
    assert report["detail"]["reenqueued"] == []


async def test_three_way_no_settle_log_flagged(
    qredis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """计费服务无 settle 流水 → 差异条目（issue=no_settle_log）。"""
    billing = AsyncMock()
    billing.get_billing_logs.return_value = []
    sessions = [
        FakeSession([[]]),
        FakeSession([[]]),
        FakeSession([[{"task_id": "task_c", "user_id": 7,
                       "private_data": _pdata(0), "upstream_amount": "1.0"}]]),
        FakeSession([[]]),
    ]
    report = await run_daily_reconciliation(
        FakeSessionFactory(sessions), billing, AsyncMock())
    entry = report["detail"]["three_way_mismatches"][0]
    assert entry["issue"] == "no_settle_log"
    assert report["mismatch_count"] == 1
