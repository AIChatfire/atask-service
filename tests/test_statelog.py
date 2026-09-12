"""``app/services/statelog``：状态迁移日志「恰好一条」+ logfire 开关。

旧链路的 ``statelog`` 靠 Redis 去重键保证「状态不变 → 零日志」。新链路把推进权
收敛到 CAS 单点（``taskstore.cas``），所以纪律变成：**只在 cas 成功分支里记一条**
——重复/迟到的观察者拿不到推进权，自然不会再记。本文件同时验证 logfire 事件
只在 ``LOGFIRE_ENABLED`` 时发出。
"""

from __future__ import annotations

import sys
import types

import httpx
import pytest
from loguru import logger

from app.main import app
from app.services import statelog

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"
TRANSITION = "task_status_changed"


@pytest.fixture
def transitions() -> list:
    """只捕获 ``task_status_changed`` 的 loguru record（其余日志不干扰计数）。"""
    records: list = []
    sink = logger.add(
        lambda m: records.append(m.record),
        level="INFO",
        filter=lambda r: TRANSITION in r["message"],
    )
    yield records
    logger.remove(sink)


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


@pytest.fixture
def batch_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    return settings


@pytest.fixture
def batch_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.queue as q

    async def _noop(*_a, **_k) -> None:
        return None

    monkeypatch.setattr(q, "publish_batch_submit", _noop)


# ---------------------------------------------------------------------------
# 单元：一次调用 → 恰好一条 INFO + 恰好一条 logfire 事件
# ---------------------------------------------------------------------------


def test_record_transition_emits_exactly_one_info(transitions):
    statelog.record_transition("t1", "QUEUED", "SUCCESS", "batch_finalize", detail="done")
    assert len(transitions) == 1
    assert transitions[0]["level"].name == "INFO"
    assert "t1 QUEUED -> SUCCESS" in transitions[0]["message"]
    assert "batch_finalize" in transitions[0]["message"]
    assert "done" in transitions[0]["message"]


def test_record_transition_calls_logfire_event_once(monkeypatch, transitions):
    calls: list[tuple] = []
    monkeypatch.setattr(statelog, "logfire_event",
                        lambda level, event, **fields: calls.append((level, event, fields)))

    statelog.record_transition("t1", "SUBMITTED", "QUEUED", "batch_submit")

    assert len(transitions) == 1
    assert calls == [("info", TRANSITION,
                      {"task_id": "t1", "from_status": "SUBMITTED",
                       "to_status": "QUEUED", "source": "batch_submit", "detail": ""})]


def test_logfire_event_noop_when_disabled(monkeypatch):
    """``LOGFIRE_ENABLED`` 关闭时绝不去 import logfire（观测链路不得成为依赖）。"""
    from app.config import settings
    from app.logging import logfire_event

    monkeypatch.setattr(settings, "logfire_enabled", False)
    monkeypatch.delitem(sys.modules, "logfire", raising=False)
    logfire_event("info", TRANSITION, task_id="t1")     # 不抛、不 import


def test_logfire_event_emits_when_enabled(monkeypatch):
    from app.config import settings
    from app.logging import logfire_event

    calls: list[tuple] = []
    fake = types.ModuleType("logfire")
    fake.info = lambda event, **fields: calls.append((event, fields))
    monkeypatch.setitem(sys.modules, "logfire", fake)
    monkeypatch.setattr(settings, "logfire_enabled", True)

    logfire_event("info", TRANSITION, task_id="t1", to_status="SUCCESS")

    assert calls == [(TRANSITION, {"task_id": "t1", "to_status": "SUCCESS"})]


# ---------------------------------------------------------------------------
# 集成：CAS 去重 → 重复观察者不重复记
# ---------------------------------------------------------------------------


async def test_terminal_transition_logged_exactly_once(
    transitions, batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    probe = respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"id": "up-1", "status": "succeeded"})
    )
    async with _client() as client:
        task_id = (await client.post("/batch/v1/tasks", json={"model": "m"},
                                     headers={**AUTH, "X-Upstream-Base-Url": UP_BASE})
                   ).json()["task_id"]
        await task_store.patch_data(task_id, {"upstream_task_id": "up-1"}, status="QUEUED")

        first = await client.get(f"/batch/v1/tasks/{task_id}")     # 推进到终态
        second = await client.get(f"/batch/v1/tasks/{task_id}")    # 已终态，零上游往返

    assert first.status_code == second.status_code == 200
    assert len(probe.calls) == 1                                  # 只探一次
    assert task_store.rows[task_id]["status"] == "SUCCESS"
    terminal = [r for r in transitions if "-> SUCCESS" in r["message"]]
    assert len(terminal) == 1                                     # 恰好一条
    assert "QUEUED -> SUCCESS" in terminal[0]["message"]


async def test_non_terminal_transition_logged_exactly_once(
    transitions, batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    """非终态推进（queued→processing）也在 CAS 成功分支记一条；状态不变则不记。"""
    respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"id": "up-1", "status": "processing"})
    )
    async with _client() as client:
        task_id = (await client.post("/batch/v1/tasks", json={"model": "m"},
                                     headers={**AUTH, "X-Upstream-Base-Url": UP_BASE})
                   ).json()["task_id"]
        await task_store.patch_data(task_id, {"upstream_task_id": "up-1"}, status="QUEUED")

        await client.get(f"/batch/v1/tasks/{task_id}")            # QUEUED -> IN_PROGRESS
        await client.get(f"/batch/v1/tasks/{task_id}")            # 仍是 processing：不记

    assert [r["message"] for r in transitions if "-> IN_PROGRESS" in r["message"]] \
        and len([r for r in transitions if "-> IN_PROGRESS" in r["message"]]) == 1


async def test_failed_finalize_does_not_log_twice(
    transitions, batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    """提交被 4xx 确定性拒绝：FAILURE 迁移恰好一条日志。"""
    from app.services import relayflow

    respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(400, json={"error": "bad request"})
    )
    async with _client() as client:
        task_id = (await client.post("/batch/v1/tasks", json={"model": "m"},
                                     headers={**AUTH, "X-Upstream-Base-Url": UP_BASE})
                   ).json()["task_id"]

    await relayflow.submit_batch_task(task_id)

    failures = [r for r in transitions if "-> FAILURE" in r["message"]]
    assert len(failures) == 1
    assert "SUBMITTED -> FAILURE" in failures[0]["message"]
