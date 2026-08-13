"""状态变更记录器：**任务状态变化的唯一 logfire 记录点**。

GET 轮询 / Poller 探测 / Callback 共用"只在状态变化时记录"逻辑：
Redis 记录每个任务最近一次已上报的状态，变化才产出一条 task_status_changed
（多个观察者共享同一键，谁先发现变化谁记录，天然去重）。

降噪纪律（运行期每几秒一轮探测也不会刷事件）：
- 状态不变 → 零日志零事件（``record_if_changed`` 返回 False）；
- 状态变化 → 恰好一条（本地 INFO + logfire.info，GW_LOGFIRE_ENABLED 时）；
- 队列执行层（taskiq 中间件）不重复记录状态语义，只兜执行失败。
"""

import logging

from app.config import settings
from app.redis import r

log = logging.getLogger("gateway.statelog")

K_SEEN = "gw:status_seen:{task_id}"
SEEN_TTL = 48 * 3600


async def record_if_changed(task_id: str, status: str, detail: str = "") -> bool:
    """状态变化才记录并返回 True；未变化返回 False（零日志零事件）。"""
    key = K_SEEN.format(task_id=task_id)
    try:
        last = await r.get(key)
        if last == status:
            return False
        await r.set(key, status, ex=SEEN_TTL)
    except Exception:
        log.debug("statelog redis error", exc_info=True)
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
    log.info("task_status_changed: %s %s -> %s (%s)", task_id, last, status, detail)
    return True
