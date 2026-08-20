"""任务队列层（taskiq）—— 全部异步协同的唯一入口。

- broker：Redis ListQueueBroker（待执行消息 = Redis list `gw:taskiq`）
- 延迟任务：RedisScheduleSource（`gw:sched:*`），由 scheduler 进程到期派发。
  注意：with_labels(delay=...) 对 ListQueueBroker 不生效，延迟必须走 schedule_by_time。
- 补数：sweep 定时任务（cron 每分钟）从 tasks 表事实源重发缺失的提交/结算/探测任务
- 并发：`taskiq worker app.queue:broker --max-async-tasks N`，多副本直接加进程
- 可观测：queue_stats() 队列深度/延迟任务数/死信数/任务状态分布，供 /ops/queue 与巡检告警
- 死信：Redis Stream gw:events:dlq，/ops/dlq/replay 可重放

运行（scheduler 已合并进 worker 进程；**scheduler 必须单副本**，worker 扩
多副本时把 scheduler 拆回独立服务）：
  sh -c "taskiq scheduler app.queue:scheduler & exec taskiq worker app.queue:broker --max-async-tasks 100"
看板：配置 GW_TASKIQ_ADMIN_URL / GW_TASKIQ_ADMIN_API_TOKEN 后 worker 自动
  挂接 TaskiqAdminReportMiddleware（args 脱敏不上报）；面板服务见 compose。

计费事件纪律（资金收口）：
- settle/cancel 携带**用户令牌**（billing 只认令牌身份，跨用户 403）；
- 5xx/网络错误 → 退避重试，超限落死信人工介入；
- 4xx（400 状态错误/403 跨用户/404 单不存在）= 确定性失败——重试无意义，
  直接 ``mark_settled`` 收口不再重发（资金由 billing 冻结 TTL/台账兜底）。
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from taskiq import Context, TaskiqDepends, TaskiqMessage, TaskiqResult, TaskiqScheduler
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend, RedisScheduleSource

from app.config import settings
from app.logging import attach_logfire_handler, log, logfire_event, setup_logging
from app.redis import K_QSTATS, S_DLQ, r

QUEUE_NAME = "gw:taskiq"
SCHED_PREFIX = "gw:sched"

# 公共发射点收敛到 app.logging（billing 等其他模块共用）；保留本模块别名，
# 兼容既有调用点与测试的 monkeypatch 面（queue._logfire_event）
_logfire_event = logfire_event


class ObservabilityMiddleware(TaskiqMiddleware):
    """taskiq 执行观测中间件（worker 进程的 logfire 装配点）。

    降噪纪律：
    - **成功路径只记 DEBUG**——poll_task 每几秒一轮，任务状态变化的唯一
      logfire 记录点是 ``app.services.statelog``（Redis 去重，只在状态变化
      时发射），队列层不重复刷事件；
    - **失败路径记 ERROR + logfire.error**（异常即信号，含 task/attempts/耗时；
      绝不带 args——billing 任务参数含用户令牌）。
    """

    def __init__(self) -> None:
        self._started: dict[str, float] = {}

    async def startup(self) -> None:
        """worker 进程启动：装配 loguru（web 进程由 app.main 装配）+ logfire。"""
        setup_logging()
        if not settings.logfire_enabled:
            return
        try:
            import logfire

            logfire.configure(
                service_name="atask-worker",
                service_version=settings.app_version,
                environment=settings.app_env,
                token=settings.logfire_token,
                send_to_logfire="if-token-present",
                scrubbing=logfire.ScrubbingOptions(
                    extra_patterns=["api_key", "access_token", "authorization", "sk-"]
                ),
                console=False,
            )
            # configure 成功后再挂 loguru→logfire 桥接（顺序颠倒会丢启动期日志）
            attach_logfire_handler()
        except Exception:
            log.opt(exception=True).warning("logfire setup failed in worker")

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        self._started[message.task_id] = time.monotonic()
        return message

    def _duration(self, message: TaskiqMessage, result: TaskiqResult[Any]) -> float:
        started = self._started.pop(message.task_id, None)
        if started is None:
            return float(result.execution_time or 0.0)
        return time.monotonic() - started

    async def post_execute(self, message: TaskiqMessage,
                           result: TaskiqResult[Any]) -> None:
        duration = self._duration(message, result)
        if result.is_err:
            self._emit_failure(message, result.error, duration)
        else:
            log.debug("taskiq {} ok in {:.2f}s", message.task_name, duration)

    async def on_error(self, message: TaskiqMessage, result: TaskiqResult[Any],
                       exception: BaseException) -> None:
        self._emit_failure(message, exception, self._duration(message, result))

    @staticmethod
    def _emit_failure(message: TaskiqMessage, error: BaseException | None,
                      duration: float) -> None:
        attempts = int(message.labels.get("attempts", 0)) if message.labels else 0
        log.error("taskiq {} failed in {:.2f}s (attempts={}): {}",
                  message.task_name, duration, attempts, error)
        _logfire_event(
            "error", "taskiq_task_failed",
            task_name=message.task_name, task_id=message.task_id,
            attempts=attempts, duration_s=round(duration, 3),
            error=str(error)[:300],
        )


broker = ListQueueBroker(settings.redis_url, queue_name=QUEUE_NAME)
# 结果后端（taskiq-admin 看板依赖；result_ex_time 兜底 TTL 24h 防膨胀——
# 任务均返回 None，无敏感数据）
broker = broker.with_result_backend(
    RedisAsyncResultBackend(settings.redis_url, result_ex_time=86400)
)
schedule_source = RedisScheduleSource(settings.redis_url, prefix=SCHED_PREFIX)
scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker), schedule_source])
broker.add_middlewares(ObservabilityMiddleware())


class TaskiqAdminReportMiddleware(TaskiqMiddleware):
    """taskiq-admin 看板上报中间件（官方中间件未随 taskiq 0.11 的 pip 包分发，
    按官方 API 契约自实现）：worker 把任务 started/executed 事件 POST 到看板。

    红线：**args/kwargs 一律脱敏不上报**（billing 任务参数含用户令牌）；
    上报失败只记 DEBUG，绝不影响任务执行。未配置 GW_TASKIQ_ADMIN_URL 不挂接。
    """

    def __init__(self, url: str, api_token: str, broker_name: str = "atask-worker"):
        super().__init__()
        self._url = url.rstrip("/")
        self._token = api_token
        self._broker_name = broker_name

    async def _post(self, path: str, payload: dict) -> None:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(
                    f"{self._url}{path}",
                    headers={"access-token": self._token},
                    json=payload,
                )
        except Exception:
            log.opt(exception=True).debug("taskiq-admin report failed")

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        await self._post(f"/api/tasks/{message.task_id}/started", {
            "args": [], "kwargs": {},          # 脱敏：绝不上报任务参数
            "taskName": message.task_name,
            "worker": self._broker_name,
            "startedAt": datetime.now(UTC).isoformat(),
        })
        return message

    async def post_execute(self, message: TaskiqMessage,
                           result: TaskiqResult[Any]) -> None:
        await self._post(f"/api/tasks/{message.task_id}/executed", {
            "error": None if result.error is None else repr(result.error)[:300],
            "executionTime": result.execution_time,
            "returnValue": {"return_value": None},
            "finishedAt": datetime.now(UTC).isoformat(),
        })


if settings.taskiq_admin_url and settings.taskiq_admin_api_token:
    broker.add_middlewares(TaskiqAdminReportMiddleware(
        settings.taskiq_admin_url, settings.taskiq_admin_api_token))


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
        log.error("task {} moved to DLQ (attempts={})", name, attempts)
        return
    # 注意：args 不落日志——billing 任务参数含用户令牌（DLQ payload 存 Redis 供重放）
    log.warning("task {} retry #{}", name, attempts)
    await kicker.with_labels(attempts=attempts).schedule_by_time(
        schedule_source, _at(_backoff(attempts)), *args,
    )


# ---------------- 任务定义 ----------------

@broker.task
async def submit_task(task_id: str, context: Context = TaskiqDepends()) -> None:
    """上游异步提交：创建链路落库即返回本地 task_id，提交在 worker 执行
    （进程重启不丢任务；基础设施异常退避重试，超限落死信，sweep 兜底补投）。"""
    from app.services.submit import submit_one  # 延迟 import 防循环
    try:
        await submit_one(task_id)
    except Exception:
        log.exception("submit failed: {}", task_id)
        await _retry_or_dlq("SUBMIT", submit_task.kicker(), context, (task_id,))


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
            # 4xx 确定性失败（冻结已过期/已结算/跨用户）：收口不重试。
            # 注意 409 锁竞争不在此列（retryable=True）——瞬时竞争退避重试，
            # 否则 settle 撞锁会被静默记为已结算而实际分文未扣（营收漏单）
            log.error("billing_settle terminal failure {}: {}", request_id, exc.message)
            await taskstore.mark_settled(request_id, actual_amount)
            return
        log.exception("billing_settle failed: {}", request_id)
        await _retry_or_dlq("BILLING_SETTLE", billing_settle_task.kicker(), context,
                            (request_id, actual_amount, user_sk, units, attrs))
    except Exception:
        log.exception("billing_settle failed: {}", request_id)
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
            # 409 锁竞争不在此列（retryable=True）：瞬时竞争退避重试，
            # 避免冻结干等 TTL 兜底才解冻（用户额度被多占 ~30min）
            log.error("billing_cancel terminal failure {}: {}", request_id, exc.message)
            await taskstore.mark_settled(request_id, 0)
            return
        log.exception("billing_cancel failed: {}", request_id)
        await _retry_or_dlq("BILLING_CANCEL", billing_cancel_task.kicker(), context,
                            (request_id, user_sk))
    except Exception:
        log.exception("billing_cancel failed: {}", request_id)
        await _retry_or_dlq("BILLING_CANCEL", billing_cancel_task.kicker(), context,
                            (request_id, user_sk))


@broker.task
async def notify_task(task_id: str, url: str, payload: dict,
                      context: Context = TaskiqDepends()) -> None:
    from app.services import notify
    try:
        await notify.push(url, payload)
    except Exception:
        log.exception("notify failed: {} -> {}", task_id, url)
        await _retry_or_dlq("NOTIFY", notify_task.kicker(), context, (task_id, url, payload))


@broker.task
async def poll_task(task_id: str, context: Context = TaskiqDepends()) -> None:
    from app.services.polling import poll_one  # 延迟 import 防循环
    try:
        await poll_one(task_id)
    except Exception:
        log.exception("poll failed: {}", task_id)
        await _retry_or_dlq("POLL", poll_task.kicker(), context, (task_id,))


@broker.task
async def resume_held_task(context: Context = TaskiqDepends()) -> None:
    """HELD 金丝雀排空（每次最老一只；详见 app.services.held）。"""
    from app.services.held import resume_held_once  # 延迟 import 防循环
    try:
        await resume_held_once()
    except Exception:
        log.exception("resume held failed")
        await _retry_or_dlq("RESUME_HELD", resume_held_task.kicker(), context, ())


@broker.task(schedule=[{"cron": "*/1 * * * *"}])     # 每分钟补数巡检
async def sweep_task() -> None:
    from app.services.reconcile import sweep_once  # 延迟 import 防循环
    await sweep_once()


# ---------------- 发布门面（请求路径只依赖这里） ----------------
# 纪律：user_sk 只作为任务参数传递，绝不进日志。

async def publish_submit(task_id: str) -> None:
    """发布上游异步提交事件（创建链路唯一依赖；消息落 Redis list，重启不丢）。"""
    log.debug("publish submit: {}", task_id)
    await submit_task.kiq(task_id)


async def publish_settle(request_id: str, actual_amount: float, user_sk: str,
                         units: float | None = None, attrs: dict | None = None) -> None:
    log.debug("publish settle: {} amount={} units={}", request_id, actual_amount, units)
    await billing_settle_task.kiq(request_id, actual_amount, user_sk, units, attrs)


async def publish_cancel(request_id: str, user_sk: str) -> None:
    log.debug("publish cancel: {}", request_id)
    await billing_cancel_task.kiq(request_id, user_sk)


async def publish_notify(task_id: str, url: str, payload: dict) -> None:
    log.debug("publish notify: {} -> {}", task_id, url)
    await notify_task.kiq(task_id, url, payload)


async def schedule_poll(task_id: str, delay: int | float) -> None:
    log.debug("schedule poll: {} in {}s", task_id, delay)
    await poll_task.kicker().schedule_by_time(schedule_source, _at(delay), task_id)


async def schedule_resume_held(delay: int | float) -> None:
    """调度 HELD 金丝雀排空（账户级故障恢复后按节奏重提交）。"""
    log.debug("schedule resume_held in {}s", delay)
    await resume_held_task.kicker().schedule_by_time(schedule_source, _at(delay))


# ---------------- 可观测与补号 ----------------

async def queue_stats() -> dict:
    """队列健康快照：待执行深度 / 延迟任务数 / 死信数 / 任务状态分布。

    带短缓存（``GW_QUEUE_STATS_CACHE_SECONDS``）：sweep 每分钟观测 + /ops
    人工查询共用，避免每次都付「scan 全部延迟键 + tasks 全表 GROUP BY」
    ——多副本 sweep/看板同时打时开销会叠乘。缓存失败降级为直算。"""
    try:
        cached = await r.get(K_QSTATS)
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    pending = await r.llen(QUEUE_NAME)
    delayed = 0
    async for key in r.scan_iter(f"{SCHED_PREFIX}:time:*"):
        delayed += await r.llen(key)
    dlq = await r.xlen(S_DLQ)

    from app.services import taskstore
    stats = {
        "pending": pending,          # 队列积压：>阈值应加 worker 副本或调大 --max-async-tasks
        "delayed": delayed,          # 延迟任务（探测回退/重试退避）
        "dlq": dlq,                  # 死信：>0 需要人工介入
        "tasks_by_status": await taskstore.counts_by_status(),
    }
    try:
        await r.set(K_QSTATS, json.dumps(stats, ensure_ascii=False),
                    ex=settings.queue_stats_cache_seconds)
    except Exception:
        pass
    return stats


_DLQ_TASKS: dict[str, Any] = {
    "SUBMIT": submit_task,
    "BILLING_SETTLE": billing_settle_task,
    "BILLING_CANCEL": billing_cancel_task,
    "NOTIFY": notify_task,
    "POLL": poll_task,
    "RESUME_HELD": resume_held_task,
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
        log.warning("replayed {} DLQ entries", replayed)
    return replayed
