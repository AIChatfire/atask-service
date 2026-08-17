"""补数对账（由 queue.sweep_task 每分钟调用）。
从 tasks 表事实源修复队列层的一切丢失：
a) 非终态且长时间未更新 → 重新入探测队列（进程崩溃/延迟任务丢失兜底）
b) 终态但 settled 未落 → 重发结算事件（billing 按 request_id 幂等，重发安全）
"""

from app import queue
from app.config import settings
from app.logging import log
from app.queue import publish_cancel, publish_settle, schedule_poll
from app.schemas import SUCCESS
from app.services import taskstore


async def _watch_queue() -> None:
    """队列阻塞可观测：积压/死信超阈值时产出告警日志与 Logfire 事件。
    处置：加 worker 副本或调大 --max-async-tasks；死信用 /ops/dlq/replay 补号。"""
    stats = await queue.queue_stats()
    if stats["pending"] > settings.queue_warn_depth or stats["dlq"] > 0:
        log.warning("queue backlog detected: {}", stats)
        if settings.logfire_enabled:
            try:
                import logfire

                logfire.warn("queue_backlog", **stats)
            except Exception:
                pass


async def sweep_once() -> None:
    await _watch_queue()
    stale = await taskstore.stale_active(settings.task_stale_seconds)
    for task_id in stale:
        await schedule_poll(task_id, 0)
    if stale:
        log.info("sweeper requeued {} stale tasks", len(stale))

    # 终态但结算未落：重发计费事件（需用户令牌；令牌会话丢失则冻结已由
    # billing TTL 兜底解冻，直接收口并告警，不再无限重发）
    unsettled = await taskstore.terminal_unsettled()
    for item in unsettled:
        data = item.get("data") or {}
        amount = float(data.get("freeze_amount") or 0)
        if amount <= 0:
            await taskstore.mark_settled(item["task_id"], 0)
            continue
        from app.services import tokensession

        user_sk = await tokensession.get(item["task_id"])
        if not user_sk:
            log.error("unsettled task {} missing token session, force-close "
                      "(freeze ttl covers refund)", item["task_id"])
            await taskstore.mark_settled(
                item["task_id"], float(data.get("settled_amount") or 0))
            continue
        if item["status"] == SUCCESS:
            await publish_settle(
                item["task_id"],
                float(data.get("settled_amount") or amount),
                user_sk,
            )
        else:
            await publish_cancel(item["task_id"], user_sk)
    if unsettled:
        log.warning("sweeper republished {} unsettled billing events", len(unsettled))
