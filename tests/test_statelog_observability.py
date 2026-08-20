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


# ---------------------------------------------------------------------------
# 失败升档计数（轮询降噪补强）：仅 1/5/20 档产出事件，成功清零
# ---------------------------------------------------------------------------


async def test_failure_escalation_only_on_rungs(patch_redis, monkeypatch):
    calls: list[dict] = []
    monkeypatch.setitem(sys.modules, "logfire", _fake_logfire(calls))
    from app.config import settings

    monkeypatch.setattr(settings, "logfire_enabled", True)

    # 连续 6 次失败：恰好 2 条事件（count=1 与 count=5 两档）
    for expected in range(1, 7):
        count = await statelog.record_failure_escalated("poll:t-9", "probe boom")
        assert count == expected
    assert [c["fields"]["count"] for c in calls] == [1, 5]
    assert all(c["event"] == "failure_escalated" for c in calls)

    # 成功后清零：下一次失败从第 1 档重新升档
    await statelog.reset_failure("poll:t-9")
    count = await statelog.record_failure_escalated("poll:t-9", "probe boom again")
    assert count == 1
    assert len(calls) == 3


async def test_failure_escalation_disabled_logfire_no_crash(patch_redis, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "logfire_enabled", False)
    assert await statelog.record_failure_escalated("poll:t-10", "x") == 1
    await statelog.reset_failure("poll:t-10")


# ---------------------------------------------------------------------------
# loguru → logfire 桥接装配：disabled 零挂载 / enabled 幂等挂接 / 重建自动补挂
# ---------------------------------------------------------------------------


def test_attach_logfire_handler_lifecycle(monkeypatch):
    from app.config import settings
    from app.logging import attach_logfire_handler, setup_logging

    def _attached() -> bool:
        import app.logging

        return app.logging._logfire_attached

    try:
        monkeypatch.setattr(settings, "logfire_enabled", False)
        setup_logging()
        attach_logfire_handler()
        assert _attached() is False                  # disabled：零挂载

        monkeypatch.setattr(settings, "logfire_enabled", True)
        attach_logfire_handler()
        assert _attached() is True                   # 挂接成功
        attach_logfire_handler()
        assert _attached() is True                   # 重复挂接幂等（flag 不变）

        setup_logging()                              # 重建 sink 后自动补挂
        assert _attached() is True
    finally:
        # 清理全局 loguru 状态，不污染后续测试
        monkeypatch.setattr(settings, "logfire_enabled", False)
        setup_logging()


# ---------------------------------------------------------------------------
# logfire sink 过滤：taskiq-admin 看板上报的 httpx 刷屏日志精准丢弃
# ---------------------------------------------------------------------------


def test_logfire_sink_filter_predicate(monkeypatch):
    from app.config import settings
    from app.logging import _logfire_sink_filter as f

    noise = ('HTTP Request: POST http://taskiq-admin:3000/api/tasks/abc/executed'
             ' "HTTP/1.1 200 OK"')
    monkeypatch.setattr(settings, "taskiq_admin_url", "http://taskiq-admin:3000/")
    assert f({"message": noise}) is False          # 看板上报（含尾斜杠配置）→ 丢弃
    other = 'HTTP Request: POST http://keypool:8000/api/lease "HTTP/1.1 200 OK"'
    assert f({"message": other}) is True           # 其余 httpx 日志（4xx 排障靠它）保留
    fail = "taskiq-admin report failed: http://taskiq-admin:3000 unreachable"
    assert f({"message": fail}) is True            # 非 httpx 请求行即使含 URL 也保留
    monkeypatch.setattr(settings, "taskiq_admin_url", "")
    assert f({"message": noise}) is True           # 未配置管理台 → 全放行


def test_logfire_sink_end_to_end_filtering(monkeypatch):
    """假 sink 走完整 loguru 管线：看板噪音不进 logfire，其余日志照常。"""
    import logfire

    import app.logging as al
    from app.config import settings

    sent: list[str] = []
    monkeypatch.setattr(settings, "logfire_enabled", True)
    monkeypatch.setattr(settings, "taskiq_admin_url", "http://taskiq-admin:3000")
    monkeypatch.setattr(
        logfire, "loguru_handler",
        lambda: {"sink": lambda m: sent.append(str(m)), "format": "{message}"},
    )
    try:
        al.attach_logfire_handler()
        al.log.info('HTTP Request: POST http://taskiq-admin:3000/api/tasks/t1/started'
                    ' "HTTP/1.1 200 OK"')
        al.log.info('HTTP Request: POST http://taskiq-admin:3000/api/tasks/t1/executed'
                    ' "HTTP/1.1 200 OK"')
        al.log.info('HTTP Request: POST http://keypool:8000/lease "HTTP/1.1 200 OK"')
        assert [m.rstrip("\n") for m in sent] == [
            'HTTP Request: POST http://keypool:8000/lease "HTTP/1.1 200 OK"']
    finally:
        monkeypatch.setattr(settings, "logfire_enabled", False)
        al.setup_logging()
