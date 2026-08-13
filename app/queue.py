"""任务队列层（taskiq）—— 全部异步协同的唯一入口。

- broker：Redis ListQueueBroker（待执行消息 = Redis list `gw:taskiq`）
- 延迟任务：RedisScheduleSource（`gw:sched:*`），由 scheduler 进程到期派发。
  注意：with_labels(delay=...) 对 ListQueueBroker 不生效，延迟必须走 schedule_by_time。
- 补数：sweep 定时任务（cron 每分钟）从 tasks 表事实源重发缺失的结算/探测任务
- 并发：`taskiq worker app.queue:broker --max-async-tasks N`，多副本直接加进程
- 可观测：queue_stats() 队列深度/延迟任务数/死信数/任务状态分布，供 /ops/queue 与巡检告警
- 死信：Redis Stream gw:events:dlq，/ops/dlq/replay 可重放

运行：
  taskiq worker    app.queue:broker --max-async-tasks 100
  taskiq scheduler app.queue:scheduler

计费事件纪律（资金收口）：
- settle/cancel 携带**用户令牌**（billing 只认令牌身份，跨用户 403）；
- 5xx/网络错误 → 退避重试，超限落死信人工介入；
- 4xx（400 状态错误/403 跨用户/404 单不存在）= 确定性失败——重试无意义，
  直接 ``mark_settled`` 收口不再重发（资金由 billing 冻结 TTL/台账兜底）。
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

from taskiq import Context, TaskiqDepends, TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import ListQueueBroker, RedisScheduleSource

from app.config import settings
from app.redis import S_DLQ, r

log = logging.getLogger("gateway.queue")

QUEUE_NAME = "gw:taskiq"
SCHED_PREFIX = "gw:sched"

broker = ListQueueBroker(settings.redis_url, queue_name=QUEUE_NAME)
schedule_source = RedisScheduleSource(settings.redis_url, prefix=SCHED_PREFIX)
scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker), schedule_source])


def _at(delay_seconds: float) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=delay_seconds)


def _backoff(attempts: int) -> int:
    return min(2**attempts, 300)          # 2s → 4s → … → 300s 封顶


async def _retry_or_dlq(name: str, kicker, context: Context, args: tuple) -> None:
    attempts = int(context.message.labels.get("attempts", 0)) + 1
    if attempts >= settings.event_max_attempts:
        await r.xadd(S_DLQ, {
            "type": name,
            "payload": json.dumps({"args": list(args)}, ensure_ascii=False),
            "attempts": str(attempts),
        })
        log.error("task %s moved to DLQ: %s", name, args)
        return
    log.warning("task %s retry #%s: %s", name, attempts, args)
    await kicker.with_labels(attempts=attempts).schedule_by_time(
        schedule_source, _at(_backoff(attempts)), *args,
    )


# ---------------- 任务定义 ----------------

@broker.task
async def billing_settle_task(request_id: str, actual_amount: float, user_sk: str,
                              units: float | None = None, attrs: dict | None = None,
                              context: Context = TaskiqDepends()) -> None:
    from app.services import taskstore
    from app.services.providers import BillingError, billing
    try:
        await billing.settle(
            raw_token=user_sk, request_id=request_id,
            actual_amount=actual_amount, units=units, attrs=attrs,
        )
        await taskstore.mark_settled(request_id, actual_amount)
    except BillingError as exc:
        if not exc.retryable:
            # 4xx 确定性失败（冻结已过期/已结算/跨用户）：收口不重试
            log.error("billing_settle terminal failure %s: %s", request_id, exc.message)
            await taskstore.mark_settled(request_id, actual_amount)
            return
        log.exception("billing_settle failed: %s", request_id)
        await _retry_or_dlq("BILLING_SETTLE", billing_settle_task.kicker(), context,
                            (request_id, actual_amount, user_sk, units, attrs))
    except Exception:
        log.exception("billing_settle failed: %s", request_id)
        await _retry_or_dlq("BILLING_SETTLE", billing_settle_task.kicker(), context,
                            (request_id, actual_amount, user_sk, units, attrs))


@broker.task
async def billing_cancel_task(request_id: str, user_sk: str,
                              context: Context = TaskiqDepends()) -> None:
    from app.services import taskstore
    from app.services.providers import BillingError, billing
    try:
        await billing.cancel(raw_token=user_sk, request_id=request_id)
        await taskstore.mark_settled(request_id, 0)
    except BillingError as exc:
        if not exc.retryable:
            log.error("billing_cancel terminal failure %s: %s", request_id, exc.message)
            await taskstore.mark_settled(request_id, 0)
            return
        log.exception("billing_cancel failed: %s", request_id)
        await _retry_or_dlq("BILLING_CANCEL", billing_cancel_task.kicker(), context,
                            (request_id, user_sk))
    except Exception:
        log.exception("billing_cancel failed: %s", request_id)
        await _retry_or_dlq("BILLING_CANCEL", billing_cancel_task.kicker(), context,
                            (request_id, user_sk))


@broker.task
async def notify_task(task_id: str, url: str, payload: dict,
                      context: Context = TaskiqDepends()) -> None:
    from app.services import notify
    try:
        await notify.push(url, payload)
    except Exception:
        log.exception("notify failed: %s -> %s", task_id, url)
        await _retry_or_dlq("NOTIFY", notify_task.kicker(), context, (task_id, url, payload))


@broker.task
async def poll_task(task_id: str, context: Context = TaskiqDepends()) -> None:
    from app.services.polling import poll_one  # 延迟 import 防循环
    try:
        await poll_one(task_id)
    except Exception:
        log.exception("poll failed: %s", task_id)
        await _retry_or_dlq("POLL", poll_task.kicker(), context, (task_id,))


@broker.task(schedule=[{"cron": "*/1 * * * *"}])     # 每分钟补数巡检
async def sweep_task() -> None:
    from app.services.reconcile import sweep_once  # 延迟 import 防循环
    await sweep_once()


# ---------------- 发布门面（请求路径只依赖这里） ----------------

async def publish_settle(request_id: str, actual_amount: float, user_sk: str,
                         units: float | None = None, attrs: dict | None = None) -> None:
    await billing_settle_task.kiq(request_id, actual_amount, user_sk, units, attrs)


async def publish_cancel(request_id: str, user_sk: str) -> None:
    await billing_cancel_task.kiq(request_id, user_sk)


async def publish_notify(task_id: str, url: str, payload: dict) -> None:
    await notify_task.kiq(task_id, url, payload)


async def schedule_poll(task_id: str, delay: int | float) -> None:
    await poll_task.kicker().schedule_by_time(schedule_source, _at(delay), task_id)


# ---------------- 可观测与补号 ----------------

async def queue_stats() -> dict:
    """队列健康快照：待执行深度 / 延迟任务数 / 死信数 / 任务状态分布"""
    pending = await r.llen(QUEUE_NAME)
    delayed = 0
    async for key in r.scan_iter(f"{SCHED_PREFIX}:time:*"):
        delayed += await r.llen(key)
    dlq = await r.xlen(S_DLQ)

    from app.services import taskstore
    return {
        "pending": pending,          # 队列积压：>阈值应加 worker 副本或调大 --max-async-tasks
        "delayed": delayed,          # 延迟任务（探测回退/重试退避）
        "dlq": dlq,                  # 死信：>0 需要人工介入
        "tasks_by_status": await taskstore.counts_by_status(),
    }


_DLQ_TASKS = {
    "BILLING_SETTLE": billing_settle_task,
    "BILLING_CANCEL": billing_cancel_task,
    "NOTIFY": notify_task,
    "POLL": poll_task,
}


async def replay_dlq(limit: int = 100) -> int:
    """死信重放（补号）：重新入队并移除原死信记录"""
    entries = await r.xrange(S_DLQ, count=limit)
    replayed = 0
    for msg_id, fields in entries:
        task = _DLQ_TASKS.get(fields.get("type", ""))
        if task is None:
            continue
        args = json.loads(fields["payload"]).get("args", [])
        await task.kiq(*args)
        await r.xdel(S_DLQ, msg_id)
        replayed += 1
    if replayed:
        log.warning("replayed %d DLQ entries", replayed)
    return replayed
