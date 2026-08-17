"""上游状态探测（由 queue.poll_task 调用）。
退避按任务年龄升档；超 poll_max_age 转 FAILURE 并取消冻结；重投走 queue.schedule_poll。
租约按原 channel 钉回（key_id 直达），路由配置随租约从渠道元数据重建。
"""

from __future__ import annotations

import time

from app.config import settings
from app.logging import log
from app.queue import schedule_poll
from app.schemas import ACTIVE, FAILURE, TERMINAL
from app.services import errclass, flow, providers, statelog, statusmap, taskstore, upstream
from app.services.providers import KeyLeaseError
from app.services.registry import registry, route_from_lease


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
        log.warning("poll timeout, finalize FAILURE: task_id={} age={:.0f}s", task_id, age)
        await flow.finalize_task(task, FAILURE, {}, fail_reason="poll timeout (>24h)")
        # 超时收口尽力调上游取消端点源头止损（渠道配 cancel_path 才动作）
        await flow.try_upstream_cancel(task)
        return

    try:
        key = await providers.keys.lease(           # 按原 channel 钉回（channel_id 直达）
            biz, model=str(data.get("model") or ""),
            key_id=data.get("key_id"),
        )
    except KeyLeaseError as exc:
        # 无可用 key：keypool 给出 retry_after_ms 时按 hint 拉长重投（退避升档兜底）；
        # 每轮 warning 走失败升档计数（1/5/20 档才告警，其余 DEBUG）
        delay = _next_delay(age)
        if exc.retry_after_ms:
            delay = max(delay, (exc.retry_after_ms + 999) // 1000)
        await statelog.record_failure_escalated(
            f"poll:{task_id}", f"lease failed, retry in {delay}s: {exc}")
        await schedule_poll(task_id, delay)                            # 租约失败下轮再来
        return
    route = registry.remember(route_from_lease(biz, key))
    if not route.probe_path:
        log.error("biz={} channel setting.gateway.probe_path missing, cannot probe", biz)
        return

    try:
        resp = await upstream.probe(route, key, upstream_task_id)
    except Exception as exc:
        delay = _next_delay(age)
        if isinstance(exc, upstream.UpstreamError):
            category = errclass.classify(route, exc)
            if category == errclass.KEY_LEVEL:
                # key 级失效：上报驱动 keypool 禁用坏 key；下轮仍钉回 channel_id，
                # 由 keypool 返回同渠道健康 key（key 时效轮换由此闭环）
                await providers.keys.report(
                    key, ok=False, status_code=exc.status, error=str(exc)[:200])
            elif category == errclass.RATE_LIMITED and exc.retry_after_ms:
                # 限流：不上报（不是 key 坏了），按上游 hint 拉长退避
                delay = max(delay, (exc.retry_after_ms + 999) // 1000)
            detail = f"{category}, retry in {delay}s: {exc}"
        else:
            detail = str(exc)
        await statelog.record_failure_escalated(f"poll:{task_id}", detail)
        await schedule_poll(task_id, delay)                            # 探测失败下轮再来
        return
    await statelog.reset_failure(f"poll:{task_id}")                    # 探测成功清零失败计数

    upstream_status = upstream.extract_path(resp, route.status_path)
    mapped = statusmap.map_status(route, upstream_status)
    log.debug("probe result: task_id={} upstream_status={} mapped={}",
              task_id, upstream_status, mapped)

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
