"""上游状态探测（由 queue.poll_task 调用）。
退避按任务年龄升档；超 poll_max_age 转 FAILURE 并取消冻结；重投走 queue.schedule_poll。
租约按原 key 钉回（``channel_id + key_index`` 精确直达，见 app.services.leasing），
路由配置随租约从渠道元数据重建。
"""

from __future__ import annotations

import time

from app.config import settings
from app.logging import log
from app.queue import schedule_poll
from app.schemas import ACTIVE, FAILURE, TERMINAL
from app.services import (
    errclass,
    flow,
    leasing,
    providers,
    statelog,
    statusmap,
    taskstore,
    upstream,
)
from app.services.providers import KeyLeaseError


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

    # 任务年龄：时间列可能被共享表的其他写入方写成毫秒（UnixMilli），
    # 归一为秒后再算；submit_time 缺失回退 created_at，负值（时钟漂移/
    # 脏数据）钳 0——绝不因单位混用把新任务秒判超时
    submit = taskstore.as_unix_seconds(task.get("submit_time")) \
        or taskstore.as_unix_seconds(task.get("created_at"))
    age = max(0.0, time.time() - submit) if submit else 0.0
    if age > settings.poll_max_age_seconds:
        log.warning("poll timeout, finalize FAILURE: task_id={} age={:.0f}s", task_id, age)
        await flow.finalize_task(
            task, FAILURE, {},
            fail_reason=f"poll timeout (>{settings.poll_max_age_seconds}s in-flight)")
        # 超时收口尽力调上游取消端点源头止损（渠道配 cancel_path 才动作）
        await flow.try_upstream_cancel(task)
        return

    try:
        # 按原 key 钉回（channel_id + key_index 精确直达，失败降级渠道直达）：
        # 同渠道多上游账号时，换账号的 key 查不到这条任务
        key, route = await leasing.route_for_task(biz, data, task)
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

    mapped = await advance_from_probe(task, route, resp, detail="poll")
    if mapped not in TERMINAL:
        await schedule_poll(task_id, _next_delay(age))                 # 非终态/未识别：下轮再探


async def advance_from_probe(task: dict, route, resp: dict,
                             detail: str = "probe") -> str | None:
    """一份上游探测快照 → 推进本地任务状态（不含重投排程）。

    poller 与**原生查询透传拦截**共用：客户端轮询原生查询端点时顺带驱动状态
    推进（结果更早可见；与 poller 并发无害——终态走 CAS 恰好一次，活跃态
    patch_data 幂等）。返回映射后的内部状态；未识别 → None。
    """
    task_id = task["task_id"]
    upstream_status = upstream.extract_path(resp, route.status_path)
    mapped = statusmap.map_status(route, upstream_status)
    log.debug("probe result: task_id={} upstream_status={} mapped={}",
              task_id, upstream_status, mapped)
    if mapped is None:
        return None
    await statelog.record_if_changed(task_id, mapped, detail=detail)
    if mapped in TERMINAL:
        await flow.finalize_task(task, mapped, resp, route=route)
    elif mapped in ACTIVE:
        await taskstore.patch_data(task_id, {"upstream_status": upstream_status},
                                   status=mapped)
    return mapped
