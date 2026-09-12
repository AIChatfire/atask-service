"""任务队列层（taskiq）—— 全部异步协同的唯一入口。

- broker：Redis ListQueueBroker（待执行消息 = Redis list `atask:taskiq`）
- 延迟任务：RedisScheduleSource（`atask:sched:*`），由 scheduler 进程到期派发。
  注意：with_labels(delay=...) 对 ListQueueBroker 不生效，延迟必须走 schedule_by_time。
- 补数：``/queue`` 后台收敛（cron 每分钟）从 tasks 表事实源把非终态任务探到终态
- 并发：`taskiq worker app.queue:broker --max-async-tasks N`，多副本直接加进程
- 可观测：queue_stats() 队列深度/延迟任务数/死信数/任务状态分布，供 /ops/queue 与巡检告警
- 死信：Redis Stream atask:events:dlq，/ops/dlq/replay 可重放

运行（scheduler 已合并进 worker 进程；**scheduler 必须单副本**，worker 扩
多副本时把 scheduler 拆回独立服务）：
  sh -c "taskiq scheduler app.queue:scheduler & exec taskiq worker app.queue:broker --max-async-tasks 100"
看板：配置 TASKIQ_ADMIN_URL / TASKIQ_ADMIN_API_TOKEN 后 worker 自动
  挂接 TaskiqAdminReportMiddleware（args 脱敏不上报）；面板服务见 compose。
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
from app import observability
from app.logging import log, logfire_event, setup_logging
from app.redis import KEY_PREFIX, K_QSTATS, S_DLQ, r

QUEUE_NAME = f"{KEY_PREFIX}:taskiq"
SCHED_PREFIX = f"{KEY_PREFIX}:sched"

# 公共发射点收敛到 app.logging；保留本模块别名，兼容既有调用点与测试的
# monkeypatch 面（queue._logfire_event）
_logfire_event = logfire_event


class ObservabilityMiddleware(TaskiqMiddleware):
    """taskiq 执行观测中间件（worker 进程的装配点）。

    降噪纪律：
    - **成功路径只记 DEBUG**——队列层不刷事件；
    - **失败路径记 ERROR + logfire.error**（异常即信号，含 task/attempts/耗时；
      绝不带 args——notify 回调体可能含用户数据）。
    """

    def __init__(self) -> None:
        self._started: dict[str, float] = {}

    async def startup(self) -> None:
        """worker 进程启动：装配 loguru（web 进程由 app.main 装配）+ logfire。

        配置口径统一在 ``app.observability``——本处只声明进程形态，
        不再自己抄一份 ``logfire.configure``（两份手工同步必然漂移）。
        """
        setup_logging()
        observability.setup("worker")

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

    红线：**args/kwargs 一律脱敏不上报**；上报失败只记 DEBUG，绝不影响任务执行。
    未配置 TASKIQ_ADMIN_URL 不挂接。
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
    log.warning("task {} retry #{}", name, attempts)
    await kicker.with_labels(attempts=attempts).schedule_by_time(
        schedule_source, _at(_backoff(attempts)), *args,
    )


# ---------------- 任务定义 ----------------

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
async def queue_submit_task(task_id: str, context: Context = TaskiqDepends()) -> None:
    """``/queue`` 中继链路的异步提交（ADR-010：零资金动作）。

    只做「按落库的 upstream_base_url 原样转发」，失败没有资金分支可走
    （ADR-010 §3）。幂等短路与重试由 worker 函数与本层退避共同保证。"""
    from app.services.relayflow import submit_queue_task  # 延迟 import 防循环
    try:
        await submit_queue_task(task_id)
    except Exception:
        log.exception("queue submit failed: {}", task_id)
        await _retry_or_dlq("QUEUE_SUBMIT", queue_submit_task.kicker(), context, (task_id,))


@broker.task(schedule=[{"cron": "*/1 * * * *"}])     # 每分钟收敛 /queue 非终态任务
async def queue_sweep_task() -> None:
    """``/queue`` 中继链路的后台收敛（ADR-010）+ 攒批的超期兜底。

    只认 ``source='queue'`` 自有行、零资金动作。收敛本身不做重试编排——一轮失败
    下轮自然重来，是''幂等轮询''而非''一次性事件''，故不走 ``_retry_or_dlq``。"""
    from app.services.relayflow import sweep_queue_once  # 延迟 import 防循环
    await sweep_queue_once(limit=settings.queue_sweep_limit)


@broker.task
async def batch_release_task(key: str, source: str = "batch") -> None:
    """整批放行一个归组键的攒批成员（N 触发 / T 触发共用）。

    **刻意不做重试编排**（与 ``queue_sweep_task`` 同一条理由）：摘批是幂等的，没被
    成功放行的成员会留在 DB 的等待态里，由 sweep 的超期兜底按 ``batch_due_at`` 捞回；
    给一次可自愈的抖动配重试只会让同一批被反复摘取，收益为零而噪声翻倍。

    异常**不上抛也不吞**——直接交给 taskiq 的错误通道（中间件记 ERROR + logfire），
    静默吞掉会让看板全绿而任务一条都没出去。
    """
    from app.services import batching  # 延迟 import 防循环

    await batching.release(key, source=source)


@broker.task
async def queue_release_task(task_id: str) -> None:
    """单条放行（放行时占不到并发槽 → 退避重排到点的重新尝试）。

    入口是 ``relayflow.release_batched_task`` 而不是 ``submit_queue_task``：重排后
    仍然要**再占一次并发槽**。直接投递提交等于绕过并发闸门，那是「排队」语义的
    完全反面（提交链路里占槽点只在受理与放行两处）。
    """
    from app.services.relayflow import release_batched_task  # 延迟 import 防循环

    await release_batched_task(task_id, source="requeue")


# ---------------- 发布门面（请求路径只依赖这里） ----------------

async def publish_queue_submit(task_id: str) -> None:
    """发布 ``/queue`` 中继链路的提交事件（消息落 Redis list，重启不丢）。"""
    log.debug("publish queue submit: {}", task_id)
    await queue_submit_task.kiq(task_id)


async def publish_batch_release(
    key: str, source: str = "batch", *, due_at: int | None = None
) -> None:
    """投递整批放行：``due_at`` 为 None → 立即（N 触发）；否则排到该时刻（T 触发）。

    **延迟必须走 ``schedule_by_time``**：``with_labels(delay=...)`` 对
    ``ListQueueBroker`` 不生效（见模块 docstring），那样写会静默变成「立即执行」，
    批次窗口形同虚设。

    T 触发的精度取决于 scheduler 的轮询间隔（``--update-interval``，见 Makefile /
    docker-compose / app/standalone）。默认不设时最小粒度是 1 分钟——**正确性不依赖
    它**：投递丢失或迟到都由 sweep 的超期兜底接住，只是最坏多等一个周期。
    """
    if due_at is not None:
        delay = float(due_at) - time.time()
        if delay > 0:
            await batch_release_task.kicker().schedule_by_time(
                schedule_source, _at(delay), key, source,
            )
            log.debug("publish batch release (deferred {}s): key={}", int(delay), key)
            return
    log.debug("publish batch release (now): key={} source={}", key, source)
    await batch_release_task.kiq(key, source)


async def publish_task_release(task_id: str, due_at: int) -> None:
    """把单条任务的放行排到 ``due_at``（退避重排）。

    ``due_at`` 已过（抖动或时钟）则立即投递，绝不排一个负延迟的任务。
    """
    delay = float(due_at) - time.time()
    if delay <= 0:
        await queue_release_task.kiq(task_id)
        return
    await queue_release_task.kicker().schedule_by_time(schedule_source, _at(delay), task_id)


async def publish_notify(task_id: str, url: str, payload: dict) -> None:
    log.debug("publish notify: {} -> {}", task_id, url)
    await notify_task.kiq(task_id, url, payload)


# ---------------- 可观测与补号 ----------------

async def queue_stats() -> dict:
    """队列健康快照：待执行深度 / 延迟任务数 / 死信数 / 任务状态分布。

    带短缓存（``QUEUE_STATS_CACHE_SECONDS``）：sweep 每分钟观测 + /ops
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
        "delayed": delayed,          # 延迟任务（重试退避）
        "dlq": dlq,                  # 死信：>0 需要人工介入
        "tasks_by_status": await taskstore.counts_by_status(),
    }
    try:
        await r.set(K_QSTATS, json.dumps(stats, ensure_ascii=False),
                    ex=settings.queue_stats_cache_seconds)
    except Exception:
        pass
    return stats


#: 死信重放登记表：只登记**会经 ``_retry_or_dlq`` 落死信**的任务类型
#: （``replay_dlq`` 按此把死信 payload 重新 ``kiq``）。cron 巡检任务
#: （``queue_sweep_task``）一轮失败下轮自然重来、从不落死信，
#: 故**刻意不登记**——登记一个永不写入的键只会误导后来人以为它会被重投。
_DLQ_TASKS: dict[str, Any] = {
    "QUEUE_SUBMIT": queue_submit_task,
    "NOTIFY": notify_task,
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
