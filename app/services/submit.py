"""异步上游提交（由 queue.submit_task 在 worker 进程执行）。

创建链路（flow.create_task）在 preflight/落库后立即返回本地 task_id，
上游提交全部走本模块——taskiq 消息落 Redis list，web/worker 进程重启不丢
任务；queue 层异常（keypool/DB/Redis 故障）按退避重试、超限落死信。

生命周期纪律（与原同步提交路径语义一一对应）：

- **成功**：回填 upstream_task_id（重打换渠道时同步 key_id/key_index 与
  channel_id 对账口径）→ QUEUED → 非回调渠道在此刻才进探测队列
  （拿到上游 id 之前无探测意义）；
- **确定性拒绝（账户级/限流）**：HELD 挂起保留冻结，释放并发槽，调度金丝雀
  排空（与 held.py 恢复语义衔接）；
- **其余失败**：finalize FAILURE——取消冻结（令牌会话取用户令牌）、释放并发
  槽、用户回调 notify，与轮询/回调终态推进同一条 ``flow.finalize_task``
  路径（提交阶段上游未接单，失败单一律解冻，不走 failed_billing=charge）；
- **幂等/互斥**：终态或已有 upstream_task_id 直接返回；Redis 互斥锁防
  sweep 补投 / DLQ 重放与在飞提交并发双建——锁 TTL 按路由动态派生
  （``submit_lock_ttl``：重打次数 × 渠道 timeout + 余量，换渠道重打时按
  新路由刷新），绝不先于在飞提交过期（KI2 根治）；锁崩溃残留由 TTL 自动
  释放，提交事件整体丢失由 sweep 补投（补投前查锁让路）+ 孤儿收口兜底。
"""

from __future__ import annotations

import math
import time

from app import queue
from app.config import settings
from app.deps import ratelimit
from app.logging import log
from app.redis import K_SUBMIT_LOCK, r
from app.schemas import (
    ACTIVE,
    FAILURE,
    HELD,
    QUEUED,
    SUBMITTED,
    TERMINAL,
    KeyLease,
    RouteConfig,
)
from app.services import errclass, flow, providers, statelog, taskstore, upstream
from app.services.registry import registry, route_from_lease


def submit_lock_ttl(route: RouteConfig) -> int:
    """提交互斥锁 TTL 按路由动态派生（KI2 根治，不再硬编码 300s）：

    最坏提交窗口 = 全部重打都打满本渠道 timeout
    （``submit_max_attempts × route.timeout_sec``）+ 租约重建/落库/探测排程
    余量（``GW_SUBMIT_LOCK_BUFFER_SECONDS``）。锁绝不先于在飞提交过期，
    从源头消除「锁提前释放 → 补投与在飞提交并发 → 上游双建」窗口。
    """
    return (math.ceil(settings.submit_max_attempts * route.timeout_sec)
            + settings.submit_lock_buffer_seconds)


async def submit_one(task_id: str) -> None:
    """单任务上游提交（worker 入口；可重入，重复触发安全）。"""
    task = await taskstore.get(task_id)
    if not task or task["status"] in TERMINAL:
        return
    data = task.get("data") or {}
    if data.get("upstream_task_id"):
        return                     # 已提交（sweep 补投/DLQ 重放幂等短路）
    if task["status"] not in (SUBMITTED, QUEUED):
        return                     # HELD 由 resume_held 金丝雀排空负责
    if not data.get("biz"):
        log.error("submit skipped, task missing biz: {}", task_id)
        return
    # 快速让路（非原子预检，零出站）：锁在 = 有在飞提交；原子互斥（SET NX）
    # 在 _submit 拿到路由后按动态 TTL 建立
    if await r.get(K_SUBMIT_LOCK.format(task_id=task_id)):
        log.debug("submit already in flight: {}", task_id)
        return
    await _submit(task)


async def _submit(task: dict) -> None:
    task_id = task["task_id"]
    data = task.get("data") or {}
    biz = str(data["biz"])
    model = str(data.get("model") or "")

    # 首打钉回原渠道（key_id 直达，与 preflight 租约/对账口径一致）；
    # key 级确定性拒绝重打换新鲜租约（不钉渠道，keypool 剔除坏 key 后自动切
    # 健康渠道）。租约失败（keypool 故障/无 key）直接抛给 queue 层退避重试。
    key = await providers.keys.lease(biz, model=model, key_id=data.get("key_id"))
    route = registry.remember(route_from_lease(biz, key))
    if not route.submit_path:
        # 渠道配置在创建后被改坏：立即可见（FAILURE + 解冻），不静默挂起
        log.error("biz {!r} submit_path missing at async submit: task_id={}", biz, task_id)
        await flow.finalize_task(
            task, FAILURE, {}, route=route, failed_charge=False,
            fail_reason=f"biz {biz!r} upstream not configured "
                        f"(channel setting.gateway.submit_path missing)",
        )
        return

    # 原子互斥（SET NX）：并发补投/DLQ 重放只活一个（防上游双重创建双扣费）；
    # TTL 按路由动态派生（KI2 根治），覆盖最坏提交窗口
    lock_key = K_SUBMIT_LOCK.format(task_id=task_id)
    if not await r.set(lock_key, "1", ex=submit_lock_ttl(route), nx=True):
        log.debug("submit already in flight: {}", task_id)
        return
    try:
        await _submit_locked(task, key, route, lock_key)
    finally:
        await r.delete(lock_key)


async def _submit_locked(task: dict, key: KeyLease, route: RouteConfig,
                         lock_key: str) -> None:
    """持锁提交体：有限重打 + 成功回填 + 失败分流（锁已由 _submit 建立）。"""
    task_id = task["task_id"]
    data = task.get("data") or {}
    biz = str(data["biz"])
    model = str(data.get("model") or "")
    body = dict(data.get("request_body") or {})

    callback_url = (
        route.callback_url_for(settings.gateway_public_base_url, task_id)
        if route.supports_callback
        else None
    )
    submit_body = upstream.build_submit_body(
        route, key, body, callback_url, client_request_id=task_id)

    # 提交重打纪律（与原同步路径一致）：仅换 key 可能改变结果的确定性拒绝
    # （默认 401/403/429，GW_SUBMIT_RETRYABLE_STATUS_CODES 可配）重打——
    # 确定性拒绝 = 上游明确未接单，重打安全；模糊失败（超时/5xx/连接中断）
    # 绝不重试（防双重创建双扣费）；任务级 4xx 重打同一报文无意义。
    resp: dict | None = None
    last_exc: upstream.UpstreamError | None = None
    started = time.monotonic()
    for attempt in range(1, settings.submit_max_attempts + 1):
        try:
            resp = await upstream.submit(route, key, submit_body)
            break
        except upstream.UpstreamError as exc:
            last_exc = exc
            category = errclass.classify(route, exc)
            retryable = (
                attempt < settings.submit_max_attempts
                and not exc.envelope
                and exc.status in settings.submit_retryable_status_codes
            )
            if not retryable:
                break
            if category == errclass.KEY_LEVEL:
                await providers.keys.report(
                    key, ok=False, status_code=exc.status, error=str(exc)[:200])
            log.warning(
                "submit rejected ({}), retry with fresh lease: task_id={} attempt={}/{}",
                category, task_id, attempt, settings.submit_max_attempts,
            )
            try:
                key = await providers.keys.lease(biz, model=model)
            except Exception as lease_exc:
                log.warning("submit retry re-lease failed: task_id={} {}",
                            task_id, lease_exc)
                break
            # 渠道覆盖按新租约重建（model_mapping/param_override 逐渠道生效）
            route = registry.remember(route_from_lease(biz, key))
            # 换渠道重打：新渠道 timeout 可能更长——按新路由刷新锁 TTL
            # （xx 仅锁在时刷新；丢失说明已超最坏窗口，仅告警不阻塞当次提交）
            if not await r.set(lock_key, "1", ex=submit_lock_ttl(route), xx=True):
                log.warning("submit lock lost during retry: {}", task_id)
            submit_body = upstream.build_submit_body(
                route, key, body, callback_url, client_request_id=task_id)

    if resp is None:
        assert last_exc is not None
        await _submit_rejected(task, route, key, last_exc)
        return

    latency_ms = int((time.monotonic() - started) * 1000)
    await providers.keys.report(key, ok=True, latency_ms=latency_ms)

    upstream_task_id = upstream.extract_path(resp, route.task_id_path)
    if not upstream_task_id:
        # 傻瓜式防护：submit_path 配置的 biz 必须能提取到上游任务 id，
        # 提取不到 = task_id_path 配错或上游非异步——立即可见，不静默挂起
        log.warning("submit response missing task id: task_id={} biz={} path={!r}",
                    task_id, route.biz, route.task_id_path)
        await flow.finalize_task(
            task, FAILURE, {}, route=route, failed_charge=False,
            fail_reason=f"upstream response missing task id at path {route.task_id_path!r}",
        )
        return

    patch = {"upstream_task_id": upstream_task_id}
    channel_id = None
    if key.key_id != data.get("key_id"):
        # 重打落到别的渠道：探测钉回与对账口径以实际渠道为准
        patch["key_id"] = key.key_id
        patch["key_index"] = key.key_index
        channel_id = key.key_id
    # 终态守卫（KI-D）：提交在飞期间任务可能已被并发判死（孤儿收口/用户取消/
    # 探测超时）——CAS 只允许活跃态落 QUEUED，绝不复活终态、不覆盖退款事实；
    # 此时上游这单成为孤儿，靠 client_request_id 反查对账/人工处理
    queued = await taskstore.cas(task_id, (SUBMITTED, QUEUED), QUEUED, patch=patch)
    if not queued:
        log.warning("submit succeeded but task already terminal, upstream task {} "
                    "orphaned (reconcile via client_request_id): task_id={}",
                    upstream_task_id, task_id)
        return
    if channel_id:
        await taskstore.patch_data(task_id, {}, channel_id=channel_id)
    log.info("task submitted (async): task_id={} upstream_task_id={} latency_ms={}",
             task_id, upstream_task_id, latency_ms)

    # 拿到上游 id 才进探测闭环（不支持回调的渠道）
    if not route.supports_callback:
        await queue.schedule_poll(task_id, settings.poll_ladder_seconds[0])


async def _submit_rejected(task: dict, route, key, err: upstream.UpstreamError) -> None:
    """重打耗尽后的分流：账户级/限流 → HELD 保留冻结；其余 → FAILURE + 解冻。"""
    task_id = task["task_id"]
    data = task.get("data") or {}
    category = errclass.classify(route, err)
    if category in (errclass.ACCOUNT_LEVEL, errclass.RATE_LIMITED):
        # 账户级故障（欠费/封禁）或上游限流（429）：挂起而非判死——HELD
        # 保留冻结，sweep 续期保活，resume_held 金丝雀排空（恢复时不钉渠道）。
        # 挂起即释放并发槽；账户级/限流都不上报 keypool（不是单个 key 坏了）。
        # CAS 没抢到（并发取消/判死已收口并释槽）则不重复释槽、不再调度恢复。
        held = await taskstore.cas(task_id, ACTIVE, HELD,
                                   patch={"held_reason": category},
                                   fail_reason=str(err)[:500])
        if not held:
            return
        await ratelimit.conc_release(data.get("token_hash"))
        await statelog.record_if_changed(task_id, HELD, detail="submit")
        log.warning("task HELD ({}): task_id={} biz={} err={}",
                    category, task_id, route.biz, str(err)[:200])
        if settings.logfire_enabled:
            try:
                import logfire

                logfire.warn("task_held", task_id=task_id, biz=route.biz,
                             error=str(err)[:200])
            except Exception:
                pass
        # 首次排空节奏：限流固定 5m（上游限速窗口语义），账户级 1m 起
        await queue.schedule_resume_held(
            settings.held_rate_limited_backoff_seconds
            if category == errclass.RATE_LIMITED else 60)
        return
    # 提交阶段上游明确未接单：一律解冻（失败单 charge 策略只覆盖生成失败，
    # 不覆盖提交拒绝），走 finalize 统一终态路径（令牌会话解冻 + 回调通知 +
    # 并发槽释放）
    await providers.keys.report(
        key, ok=False,
        status_code=0 if err.envelope else err.status,
        error=str(err)[:200],
    )
    log.warning("upstream rejected at async submit: task_id={} biz={} class={} err={}",
                task_id, route.biz, category, str(err)[:200])
    await flow.finalize_task(task, FAILURE, {}, route=route,
                             failed_charge=False, fail_reason=str(err)[:500])
