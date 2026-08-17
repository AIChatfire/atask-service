"""补数对账（由 queue.sweep_task 每分钟调用）。
从 tasks 表事实源修复队列层的一切丢失：
a) 非终态且长时间未更新 → 重新入探测队列（进程崩溃/延迟任务丢失兜底）
b) 终态但 settled 未落 → 重发结算事件（billing 按 request_id 幂等，重发安全）
c) 孤儿任务（非终态且无 upstream_task_id 超宽限期）→ FAILURE + 解冻收口
d) 反向对账：本地 FAILURE/已退 但上游 SUCCESS → 告警台账（钱追不回但要看得见亏）
"""

import time

from app import queue
from app.config import settings
from app.logging import log
from app.queue import publish_cancel, publish_settle, schedule_poll
from app.redis import r
from app.schemas import FAILURE, SUCCESS, TERMINAL
from app.services import flow, providers, statusmap, taskstore, tokensession, upstream
from app.services.providers import BillingError
from app.services.registry import registry, route_from_lease


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


async def _orphan_closeout() -> None:
    """孤儿任务收口：submit 前崩溃的残留（永远不会有上游任务）→ FAILURE + 解冻。
    （根治靠 submit 注入 client_request_id 反查补挂，这里是兜底）"""
    orphans = await taskstore.orphan_active(settings.orphan_grace_seconds)
    for task_id in orphans:
        task = await taskstore.get(task_id)
        if not task or task["status"] in TERMINAL:
            continue
        log.error("orphan task closed: {} (no upstream_task_id after {}s)",
                  task_id, settings.orphan_grace_seconds)
        await flow.finalize_task(
            task, FAILURE, {},
            fail_reason="orphan: submit never returned upstream task id",
        )


async def _reverse_reconcile() -> None:
    """反向对账：本地 FAILURE/已退 但上游 SUCCESS → logfire 告警 + 台账标记。
    小批量抽查（每轮 reverse_reconcile_batch 条，单任务每小时最多核对一次）。"""
    candidates = await taskstore.reconcile_candidates(
        settings.reverse_reconcile_window_seconds,
        settings.reverse_reconcile_recheck_seconds,
        settings.reverse_reconcile_batch,
    )
    for item in candidates:
        data = item.get("data") or {}
        biz = str(data.get("biz") or "")
        upstream_task_id = data.get("upstream_task_id")
        if not biz or not upstream_task_id:
            continue
        try:
            key = await providers.keys.lease(biz, model=str(data.get("model") or ""),
                                             key_id=data.get("key_id"))
            route = registry.remember(route_from_lease(biz, key))
            if not route.probe_path:
                continue
            resp = await upstream.probe(route, key, str(upstream_task_id))
        except Exception as exc:
            log.debug("reverse reconcile probe skipped: {} {}", item["task_id"], exc)
            continue
        mapped = statusmap.map_status(route, upstream.extract_path(resp, route.status_path))
        patch: dict = {"reconcile_checked_at": int(time.time())}
        if mapped == SUCCESS:
            patch["reconciled"] = True
            patch["reconcile_alert"] = "upstream_succeeded_but_local_failure"
            log.error("reverse reconcile alert: task {} local FAILURE but upstream "
                      "SUCCESS (upstream_task_id={})", item["task_id"], upstream_task_id)
            if settings.logfire_enabled:
                try:
                    import logfire

                    logfire.error("reverse_reconcile_alert", task_id=item["task_id"],
                                  biz=biz, upstream_task_id=str(upstream_task_id))
                except Exception:
                    pass
        elif mapped in TERMINAL:
            patch["reconciled"] = True        # 上游也失败/取消：账面一致，不再复查
        await taskstore.patch_data(item["task_id"], patch)
    if candidates:
        log.info("reverse reconcile checked {} tasks", len(candidates))


async def _renew_expiring_freezes() -> None:
    """冻结续期扫描：非终态任务 freeze 临期（< margin）→ 用令牌会话续期
    （只推 expires_at 不动钱；HELD/长任务防"freeze 过期 → settle 被 4xx 静默收口"）。
    失败矩阵：400（冻结已终态/超总量上限）→ 任务 FAILURE 止损 + 告警
    （钱已被 billing sweeper 退用户）；409/5xx → 下轮再来。"""
    expiring = await taskstore.expiring_freezes(settings.freeze_renew_margin_seconds,
                                                settings.freeze_renew_batch)
    for item in expiring:
        task_id = item["task_id"]
        user_sk = await tokensession.get(task_id)
        if not user_sk:
            log.error("freeze renew skipped, token session missing: {}", task_id)
            continue
        try:
            result = await providers.billing.renew(
                raw_token=user_sk, request_id=task_id,
                ttl_seconds=settings.freeze_ttl_seconds)
        except BillingError as exc:
            if exc.status == 400:
                log.error("freeze renew terminal rejection, fail task {}: {}",
                          task_id, exc.message)
                task = await taskstore.get(task_id)
                if task and task["status"] not in TERMINAL:
                    await flow.finalize_task(
                        task, FAILURE, {},
                        fail_reason="freeze renew rejected (funding lapsed)")
            else:
                log.warning("freeze renew failed (retry next round): {} {}",
                            task_id, exc.message)
            continue
        expires_at = int(result.get("expires_at") or 0) \
            or int(time.time()) + settings.freeze_ttl_seconds
        await taskstore.patch_data(task_id, {"freeze_expires_at": expires_at})
        log.info("freeze renewed: {} expires_at={}", task_id, expires_at)
    if expiring:
        log.info("freeze renew scan: {} candidates", len(expiring))


async def _held_maintenance() -> None:
    """HELD 维护：超 hold_max_age 判死（FAILURE + cancel 兜底收口）；
    仍有存活 HELD → 触发金丝雀排空（Redis 锁防每分钟 sweep 堆积调度）。"""
    for task_id in await taskstore.held_expired(settings.hold_max_age_seconds):
        task = await taskstore.get(task_id)
        if not task or task["status"] in TERMINAL:
            continue
        log.error("held task expired, finalize FAILURE: {}", task_id)
        await flow.finalize_task(task, FAILURE, {}, fail_reason="held timeout")
    if await taskstore.oldest_held():
        if await r.set("gw:held:resume_lock", "1", ex=60, nx=True):
            await queue.schedule_resume_held(0)


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

    await _renew_expiring_freezes()
    await _held_maintenance()
    await _orphan_closeout()
    await _reverse_reconcile()
