"""可观测性测试：statelog 状态变化去重 + taskiq 观测中间件降噪纪律。"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

from app.services import statelog


def _fake_logfire(calls: list[dict]):
    def _mk(level):
        def _emit(event, **fields):
            calls.append({"level": level, "event": event, "fields": fields})
        return _emit

    return SimpleNamespace(
        info=_mk("info"), warn=_mk("warn"), warning=_mk("warning"), error=_mk("error"),
    )


# ---------------------------------------------------------------------------
# statelog：同状态零记录，变化恰好一条
# ---------------------------------------------------------------------------


async def test_statelog_same_status_records_nothing(patch_redis, monkeypatch):
    calls: list[dict] = []
    monkeypatch.setitem(sys.modules, "logfire", _fake_logfire(calls))
    from app.config import settings

    monkeypatch.setattr(settings, "logfire_enabled", True)

    # 运行中连探多轮同状态：只有第一次产生事件，之后零记录
    assert await statelog.record_if_changed("t-1", "IN_PROGRESS", detail="poll") is True
    assert await statelog.record_if_changed("t-1", "IN_PROGRESS", detail="poll") is False
    assert await statelog.record_if_changed("t-1", "IN_PROGRESS", detail="get") is False
    assert len(calls) == 1
    assert calls[0]["event"] == "task_status_changed"
    assert calls[0]["fields"]["to_status"] == "IN_PROGRESS"

    # 状态变化：恰好再发一条
    assert await statelog.record_if_changed("t-1", "SUCCESS", detail="poll") is True
    assert len(calls) == 2
    assert calls[1]["fields"]["from_status"] == "IN_PROGRESS"


async def test_statelog_disabled_logfire_no_crash(patch_redis, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "logfire_enabled", False)
    assert await statelog.record_if_changed("t-2", "QUEUED") is True
    assert await statelog.record_if_changed("t-2", "QUEUED") is False


# ---------------------------------------------------------------------------
# taskiq ObservabilityMiddleware：成功 DEBUG 静默，失败才发事件
# ---------------------------------------------------------------------------


def _msg(name: str = "app.queue:poll_task", task_id: str = "m-1",
         attempts: int = 0) -> Any:
    return SimpleNamespace(task_id=task_id, task_name=name,
                           labels={"attempts": attempts} if attempts else {})


def _result(is_err: bool = False, error: Exception | None = None) -> Any:
    return SimpleNamespace(is_err=is_err, error=error, execution_time=0.2)


async def test_middleware_success_is_quiet(monkeypatch):
    from app import queue as q

    events: list[dict] = []
    monkeypatch.setattr(q, "_logfire_event",
                        lambda level, event, **fields: events.append(
                            {"level": level, "event": event, "fields": fields}))
    mw = q.ObservabilityMiddleware()
    msg = _msg()
    await mw.pre_execute(msg)
    await mw.post_execute(msg, _result())
    assert events == []                       # 成功路径零 logfire 事件（降噪）


async def test_middleware_failure_emits_without_args(monkeypatch):
    from app import queue as q

    events: list[dict] = []
    monkeypatch.setattr(q, "_logfire_event",
                        lambda level, event, **fields: events.append(
                            {"level": level, "event": event, "fields": fields}))
    mw = q.ObservabilityMiddleware()
    msg = _msg(name="app.queue:billing_settle_task", attempts=2)
    await mw.pre_execute(msg)
    await mw.post_execute(msg, _result(is_err=True, error=RuntimeError("billing 5xx")))

    assert len(events) == 1
    assert events[0]["level"] == "error" and events[0]["event"] == "taskiq_task_failed"
    fields = events[0]["fields"]
    assert fields["task_name"] == "app.queue:billing_settle_task"
    assert fields["attempts"] == 2
    assert "args" not in fields and "user_sk" not in str(fields)   # 绝不带任务参数


async def test_middleware_on_error_path(monkeypatch):
    from app import queue as q

    events: list[dict] = []
    monkeypatch.setattr(q, "_logfire_event",
                        lambda level, event, **fields: events.append(
                            {"level": level, "event": event, "fields": fields}))
    mw = q.ObservabilityMiddleware()
    msg = _msg(name="app.queue:poll_task")
    await mw.pre_execute(msg)
    await mw.on_error(msg, _result(), ValueError("boom"))
    assert len(events) == 1 and events[0]["fields"]["error"] == "boom"
