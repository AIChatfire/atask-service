"""上游状态探测（由 queue.poll_task 调用）。
退避按任务年龄升档；超 poll_max_age 转 FAILURE 并取消冻结；重投走 queue.schedule_poll。
租约按原 channel 钉回（key_id 直达），路由配置随租约从渠道元数据重建。
"""

from __future__ import annotations

import logging
import time

from app.config import settings
from app.queue import schedule_poll
from app.schemas import ACTIVE, FAILURE, TERMINAL
from app.services import flow, providers, statelog, statusmap, taskstore, upstream
from app.services.providers import KeyLeaseError
from app.services.registry import registry, route_from_lease

log = logging.getLogger("gateway.polling")


def _next_delay(age_seconds: float) -> int:
    ladder = settings.poll_ladder_seconds
    delay = ladder[0]
    for rung in ladder:
        if age_seconds >= rung * 4:      # 每档探测约 4 次后升档
            delay = rung
    return delay


async def poll_one(task_id: str) -> None:
    task = await taskstore.get(task_id)
    if not task or task["status"] in TERMINAL:
        return
    data = task.get("data") or {}
    biz = str(data.get("biz") or "")
    upstream_task_id = data.get("upstream_task_id")
    if not biz or not upstream_task_id:
        return

    age = time.time() - (task.get("submit_time") or time.time())
    if age > settings.poll_max_age_seconds:
        await flow.finalize_task(task, FAILURE, {}, fail_reason="poll timeout (>24h)")
        return

    try:
        key = await providers.keys.lease(           # 按原 channel 钉回（channel_id 直达）
            biz, model=str(data.get("model") or ""),
            key_id=data.get("key_id"),
        )
    except KeyLeaseError as exc:
        log.warning("probe %s key lease failed: %s", task_id, exc)
        await schedule_poll(task_id, _next_delay(age))                   # 租约失败下轮再来
        return
    route = registry.remember(route_from_lease(biz, key))
    if not route.probe_path:
        log.error("biz=%s channel setting.gateway.probe_path missing, cannot probe", biz)
        return

    try:
        resp = await upstream.probe(route, key, upstream_task_id)
    except Exception as exc:
        log.warning("probe %s failed: %s", task_id, exc)
        await schedule_poll(task_id, _next_delay(age))                   # 探测失败下轮再来
        return

    upstream_status = upstream.extract_path(resp, route.status_path)
    mapped = statusmap.map_status(route, upstream_status)

    if mapped is not None:
        await statelog.record_if_changed(task_id, mapped, detail="poll")

    if mapped is None:
        await schedule_poll(task_id, _next_delay(age))                   # 未识别状态：下轮再探
    elif mapped in TERMINAL:
        await flow.finalize_task(task, mapped, resp, route=route)
    elif mapped in ACTIVE:
        await taskstore.patch_data(task_id, {"upstream_status": upstream_status}, status=mapped)
        await schedule_poll(task_id, _next_delay(age))
    else:
        await schedule_poll(task_id, _next_delay(age))
