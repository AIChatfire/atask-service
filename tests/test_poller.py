"""W2 PollWorker 单元测试（SPEC §7.1；全 mock）。

覆盖：领取 SQL 只扫 ``platform LIKE 'gw\\_%'`` 前缀行 + SKIP LOCKED +
next_poll_at 游标过滤；claim-and-bump 退避升档；deadline 到期走
transition(channel="sweep") timeout 快照；正常行 adapter.poll →
transition(channel="poll")（SubmitContext.action 从行内取回）；
上游异常吞掉不中断。
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest
from test_task_manager import FakeResult, FakeSession, make_biz_cfg

import app.tasks.poller as poller_mod
from app.adapters.base import (
    TaskSnapshot,
    TaskStatus,
    UpstreamRateLimitError,
)
from app.tasks.models import GW_PLATFORM_LIKE
from app.tasks.poller import PollWorker, _next_delay


class FakeSessionFactory:
    """async_sessionmaker 形制的 fake：每次 with 返回同一 FakeSession。"""

    def __init__(self, session: FakeSession) -> None:
        self._session = session

    def __call__(self) -> FakeSessionFactory:
        return self

    async def __aenter__(self) -> FakeSession:
        return self._session

    async def __aexit__(self, *args: Any) -> bool:
        return False


@pytest.fixture(autouse=True)
def _upstream_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """SubmitContext.secrets 由 resolve_submit_secrets 按 auth_secret_ref 解析。"""
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-test")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-test")


def make_worker(
    session: FakeSession, transition: AsyncMock | None = None
) -> tuple[PollWorker, AsyncMock]:
    tm = AsyncMock()
    tm.transition = transition or AsyncMock(return_value=True)
    return PollWorker(task_manager=tm, session_factory=FakeSessionFactory(session)), tm.transition


def in_flight_row(next_poll_at: int = 0, updated_at: int = 100) -> FakeResult:
    return FakeResult(
        rows=[{"task_id": "task_abc", "action": "textGenerate",
               "updated_at": updated_at, "next_poll_at": next_poll_at}]
    )


def full_row(deadline_unix: int, biz: str | None = "kling-biz") -> FakeResult:
    pdata = {
        "upstream_task_id": "up-123",
        "gateway": {"biz": biz, "deadline_unix": deadline_unix, "freeze_shard_seq": 0},
    }
    return FakeResult(
        rows=[{"task_id": "task_abc", "action": "textGenerate",
               "private_data": json.dumps(pdata)}]
    )


# ---------------------------------------------------------------------------
# 领取纪律
# ---------------------------------------------------------------------------


async def test_claim_sql_gw_prefix_only() -> None:
    session = FakeSession()  # 无行
    worker, _ = make_worker(session)

    claimed = await worker._poll_once()

    assert claimed == 0
    claim_sql = session.calls[0][0]
    assert "platform LIKE :gw_like" in claim_sql
    assert session.calls[0][1]["gw_like"] == GW_PLATFORM_LIKE  # 'gw\_%'
    assert "FOR UPDATE SKIP LOCKED" in claim_sql
    assert "status IN ('SUBMITTED', 'QUEUED', 'IN_PROGRESS')" in claim_sql
    assert "next_poll_at" in claim_sql
    assert "ORDER BY submit_time" in claim_sql
    assert session.commits == 1


async def test_claim_bumps_next_poll_at_with_backoff() -> None:
    session = FakeSession()
    session.on("FOR UPDATE SKIP LOCKED", in_flight_row(next_poll_at=0, updated_at=100))
    worker, _ = make_worker(session)

    claimed = await worker._poll_once()

    assert claimed == 1
    bump = session.last_params("JSON_SET(private_data, '$.gateway.next_poll_at', :np)")
    # 首次 claim：prev_delay 推导为 0 → 升第一档 5s ±20% jitter
    assert 0 <= bump["np"] - bump["now"] <= 7
    assert bump["task_id"] == "task_abc"
    assert bump["gw_like"] == GW_PLATFORM_LIKE


def test_next_delay_escalation_and_cap() -> None:
    seq = (5, 15, 30, 120)
    assert 4 <= _next_delay(0, seq) <= 6          # 5 ±20%
    assert 12 <= _next_delay(5, seq) <= 18        # 15 ±20%
    assert 24 <= _next_delay(15, seq) <= 36       # 30 ±20%
    assert 96 <= _next_delay(30, seq) <= 144      # 120 ±20%
    assert 96 <= _next_delay(120, seq) <= 144     # 封顶末档
    assert 96 <= _next_delay(9999, seq) <= 144    # 超界仍封顶


# ---------------------------------------------------------------------------
# 推进路径
# ---------------------------------------------------------------------------


async def test_deadline_row_transitions_timeout_via_sweep() -> None:
    session = FakeSession()
    session.on("FOR UPDATE SKIP LOCKED", in_flight_row())
    session.on("SELECT task_id, action, private_data", full_row(deadline_unix=1))  # 已过期
    worker, transition = make_worker(session)

    await worker._poll_once()

    transition.assert_awaited_once()
    kw = transition.await_args.kwargs
    assert kw["task_id"] == "task_abc"
    assert kw["channel"] == "sweep"
    assert kw["snapshot"].status is TaskStatus.TIMEOUT
    assert kw["snapshot"].error["code"] == "deadline_exceeded"


async def test_poll_path_calls_adapter_and_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession()
    session.on("FOR UPDATE SKIP LOCKED", in_flight_row())
    session.on("SELECT task_id, action, private_data", full_row(deadline_unix=9999999999))

    seen_ctx: dict[str, Any] = {}

    class FakePollAdapter:
        name = "kling"
        callback_capability = True
        echoes_external_task_id = True

        async def poll(self, upstream_task_id: str, ctx: Any) -> TaskSnapshot:
            seen_ctx.update(
                upstream_task_id=upstream_task_id, biz=ctx.biz, action=ctx.action,
                upstream_base_url=ctx.upstream_base_url,
            )
            return TaskSnapshot(
                upstream_status="processing",
                status=TaskStatus.RUNNING,
                result=None, usage=None, error=None,
                event_id="kling:up-123:processing:1",
            )

    monkeypatch.setattr(poller_mod, "get_adapter", lambda name: FakePollAdapter())

    async def fake_registry_get(biz: str, session_arg: Any) -> Any:
        assert biz == "kling-biz"
        return make_biz_cfg()

    monkeypatch.setattr(poller_mod.registry, "get", fake_registry_get)

    worker, transition = make_worker(session)
    await worker._poll_once()

    # SubmitContext.action 从行内 action 取回（kling 旧版查询路径需要）
    assert seen_ctx["action"] == "textGenerate"
    assert seen_ctx["upstream_task_id"] == "up-123"
    assert seen_ctx["upstream_base_url"] == "https://upstream.example.com"
    transition.assert_awaited_once()
    kw = transition.await_args.kwargs
    assert kw["channel"] == "poll"
    assert kw["snapshot"].status is TaskStatus.RUNNING


async def test_poll_upstream_error_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    session = FakeSession()
    session.on("FOR UPDATE SKIP LOCKED", in_flight_row())
    session.on("SELECT task_id, action, private_data", full_row(deadline_unix=9999999999))

    class FlakyAdapter:
        name = "kling"
        callback_capability = True
        echoes_external_task_id = True

        async def poll(self, upstream_task_id: str, ctx: Any) -> TaskSnapshot:
            raise UpstreamRateLimitError("429", retry_after=3.0)

    monkeypatch.setattr(poller_mod, "get_adapter", lambda name: FlakyAdapter())
    monkeypatch.setattr(
        poller_mod.registry, "get", AsyncMock(return_value=make_biz_cfg())
    )

    worker, transition = make_worker(session)
    claimed = await worker._poll_once()  # 不抛异常

    assert claimed == 1
    transition.assert_not_awaited()  # 未推进；next_poll_at 已前推，下轮再试
