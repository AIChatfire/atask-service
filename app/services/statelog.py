"""状态迁移日志：``/queue`` 链路**状态变化的唯一记录点**。

旧链路的 ``statelog`` 靠 Redis「最近已上报状态」键去抖，因为它的状态推进有多个
平等观察者（GET 轮询 / Poller / Callback）互相竞争。ADR-010 后的新链路把推进权
收敛到 **CAS 单点**（``taskstore.cas`` 的 rowcount 判定）：同一迁移的重复/迟到
观察者拿不到推进权、不会走到本模块，所以「状态变化 → **恰好一条**日志」由 CAS
本身保证，不再需要额外的去重键。

纪律：
- **只在 CAS 抢到推进权的那一次调用里记录**（调用方负责放在 ``cas(...) is True``
  分支内）——把本函数放到 CAS 之外会让重复观察者各记一条，恰好一次就破了；
- 本地 INFO 一条 + logfire 结构化事件（``LOGFIRE_ENABLED`` 时才真正发出），
  两者同源同参数，便于按 ``task_id`` 在日志与 trace 间对齐；
- 不记录上游探测回显的原话状态（``data.upstream_status``）——那是高频道上游
  回显、不是本地状态机迁移，记它等于每次轮询都刷一条。
"""

from __future__ import annotations

from app.logging import log, logfire_event


def record_transition(task_id: str, from_status: str | None, to_status: str,
                      source: str, detail: str = "") -> None:
    """记录一次**已抢到推进权**的状态迁移：恰好一条本地 INFO + 一条 logfire 事件。

    ``source`` 是推进来源（``queue_submit`` / ``queue_probe`` / ``queue_finalize``
    / ``queue_cancel``），用于定位是哪条路径推进的；``detail`` 只放面向排障的短
    文案（失败原因等），**绝不放用户令牌**。
    """
    log.info("task_status_changed: {} {} -> {} ({}{})",
             task_id, from_status, to_status, source,
             f": {detail}" if detail else "")
    logfire_event("info", "task_status_changed", task_id=task_id,
                  from_status=from_status, to_status=to_status,
                  source=source, detail=detail)
