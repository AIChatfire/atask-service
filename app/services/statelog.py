"""状态变更记录器：**任务状态变化的唯一 logfire 记录点**。

GET 轮询 / Poller 探测 / Callback 共用"只在状态变化时记录"逻辑：
Redis 记录每个任务最近一次已上报的状态，变化才产出一条 task_status_changed
（多个观察者共享同一键，谁先发现变化谁记录，天然去重）。

降噪纪律（运行期每几秒一轮探测也不会刷事件）：
- 状态不变 → 零日志零事件（``record_if_changed`` 返回 False）；
- 状态变化 → 恰好一条（本地 INFO + logfire.info，GW_LOGFIRE_ENABLED 时）；
- 队列执行层（taskiq 中间件）不重复记录状态语义，只兜执行失败。
"""

from app.config import settings
from app.logging import log
from app.redis import r

K_SEEN = "gw:status_seen:{task_id}"
SEEN_TTL = 48 * 3600

K_FAIL = "gw:fail_count:{subject}"       # 连续失败计数（轮询降噪）
FAIL_ESCALATION = (1, 5, 20)             # 仅这些档位产出 warning/事件


async def record_if_changed(task_id: str, status: str, detail: str = "") -> bool:
    """状态变化才记录并返回 True；未变化返回 False（零日志零事件）。"""
    key = K_SEEN.format(task_id=task_id)
    try:
        last = await r.get(key)
        if last == status:
            return False
        await r.set(key, status, ex=SEEN_TTL)
    except Exception:
        log.opt(exception=True).debug("statelog redis error")
        last = None

    if settings.logfire_enabled:
        try:
            import logfire

            logfire.info(
                "task_status_changed",
                task_id=task_id, from_status=last, to_status=status, source=detail,
            )
        except Exception:
            pass
    log.info("task_status_changed: {} {} -> {} ({})", task_id, last, status, detail)
    return True


async def record_failure_escalated(subject: str, detail: str = "") -> int:
    """连续失败计数 +1，**仅在 1/5/20 档**产出 warning + logfire 事件（轮询降噪：
    探测每几秒一轮，连续失败时日志量从每轮一条降到三档三条）。
    恢复成功后由 ``reset_failure`` 清零。返回当前计数。"""
    key = K_FAIL.format(subject=subject)
    try:
        count = int(await r.incr(key))
        await r.expire(key, SEEN_TTL)
    except Exception:
        log.opt(exception=True).debug("statelog redis error")
        return 0
    if count in FAIL_ESCALATION:
        log.warning("failure escalation: {} count={} ({})", subject, count, detail)
        if settings.logfire_enabled:
            try:
                import logfire

                logfire.warn("failure_escalated",
                             subject=subject, count=count, detail=detail[:200])
            except Exception:
                pass
    else:
        log.debug("failure counted: {} count={} ({})", subject, count, detail)
    return count


async def reset_failure(subject: str) -> None:
    """成功后清零失败计数（下次故障从第 1 档重新升档）。"""
    try:
        await r.delete(K_FAIL.format(subject=subject))
    except Exception:
        log.opt(exception=True).debug("statelog redis error")
