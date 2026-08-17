"""HELD 挂起排空（[6]）：账户级故障（欠费/封禁）恢复后的金丝雀重提交。

设计纪律：
- **金丝雀策略**：每次只取最老一个 HELD 任务试提交；再撞账户级 → 退避
  1m→5m→15m 封顶重投；成功 → 按 5s 节奏排下一只；
- **不钉渠道**：恢复排空重新 ``keys.lease``（双账号红利：自动切健康账号）；
- **并发槽**：挂起时已释放（flow.create_task），提交前重新 acquire，
  拿不到下轮再来；
- **冻结保活**：挂起期间由 sweep 续期扫描（[7]）维持 freeze 不过期；
  ``hold_max_age``（默认 4h）兜底判死（FAILURE + cancel）；
- **幂等**：重提交带 ``client_request_id = task_id``（渠道配
  ``client_request_id_param`` 时），上游支持幂等可防双重创建。
"""

from __future__ import annotations

from app.config import settings
from app.deps import ratelimit
from app.logging import log
from app.queue import schedule_poll, schedule_resume_held
from app.schemas import FAILURE, HELD, QUEUED
from app.services import errclass, flow, providers, taskstore, upstream
from app.services.providers import KeyLeaseError
from app.services.registry import registry, route_from_lease

#: 账户级退避阶梯：1m → 5m → 15m 封顶
_BACKOFF = (60, 300, 900)

#: 排空节奏：一只成功后 5s 排下一只（渠道限速友好）
_DRAIN_INTERVAL = 5


def _backoff(attempts: int) -> int:
    return _BACKOFF[min(max(attempts - 1, 0), len(_BACKOFF) - 1)]


async def resume_held_once() -> None:
    """金丝雀单步：取最老 HELD 试提交一次（成败都会按需自调度下一步）。"""
    task_id = await taskstore.oldest_held()
    if not task_id:
        return
    task = await taskstore.get(task_id)
    if not task or task["status"] != HELD:
        return
    data = task.get("data") or {}
    biz = str(data.get("biz") or "")
    token_hash = str(data.get("token_hash") or "")

    # 并发槽：挂起即释放、提交前重新 acquire（拿不到下轮再来，不计退避档）
    if token_hash and not await ratelimit.conc_try_acquire(token_hash):
        log.debug("held resume deferred (user concurrency full): {}", task_id)
        await schedule_resume_held(30)
        return

    try:
        key = await providers.keys.lease(           # 不钉渠道：自动切健康账号
            biz, model=str(data.get("model") or ""))
    except KeyLeaseError as exc:
        await ratelimit.conc_release(token_hash)
        attempts = int(data.get("held_attempts") or 0) + 1
        await taskstore.patch_data(task_id, {"held_attempts": attempts})
        delay = _backoff(attempts)
        log.warning("held resume lease failed, backoff {}s: {} {}", delay, task_id, exc)
        await schedule_resume_held(delay)
        return

    route = registry.remember(route_from_lease(biz, key))
    callback_url = (
        route.callback_url_for(settings.gateway_public_base_url, task_id)
        if route.supports_callback else None
    )
    body = upstream.build_submit_body(
        route, key, dict(data.get("request_body") or {}),
        callback_url, client_request_id=task_id,   # 幂等反查：task_id 随体重交
    )
    try:
        resp = await upstream.submit(route, key, body)
    except upstream.UpstreamError as exc:
        await ratelimit.conc_release(token_hash)
        category = errclass.classify(route, exc)
        attempts = int(data.get("held_attempts") or 0) + 1
        await taskstore.patch_data(task_id, {"held_attempts": attempts})
        if category == errclass.TASK_LEVEL:
            # 任务本身被拒（内容审核等）：挂起无意义，判死 + 解冻
            log.warning("held task rejected at task level, finalize FAILURE: {} {}",
                        task_id, str(exc)[:200])
            fresh = await taskstore.get(task_id)
            if fresh:
                await flow.finalize_task(fresh, FAILURE, {}, fail_reason=str(exc)[:500])
            return
        delay = _backoff(attempts)
        log.warning("held resume failed ({}), backoff {}s: {}", category, delay, task_id)
        await schedule_resume_held(delay)
        return

    upstream_task_id = upstream.extract_path(resp, route.task_id_path)
    if not upstream_task_id:
        await ratelimit.conc_release(token_hash)
        log.error("held resume missing task id: {} path={!r}", task_id, route.task_id_path)
        fresh = await taskstore.get(task_id)
        if fresh:
            await flow.finalize_task(
                fresh, FAILURE, {},
                fail_reason=f"upstream response missing task id at path {route.task_id_path!r}")
        return

    # 恢复成功：回填上游任务与**实际渠道**（可能换了账号），转 QUEUED 进探测闭环
    await taskstore.patch_data(
        task_id,
        {"upstream_task_id": upstream_task_id, "key_id": key.key_id,
         "key_index": key.key_index, "held_attempts": 0},
        status=QUEUED, channel_id=key.key_id,
    )
    log.info("held task resumed: {} -> upstream {} (channel {})",
             task_id, upstream_task_id, key.key_id)
    await schedule_poll(task_id, settings.poll_ladder_seconds[0])

    # 还有 HELD → 按节奏排下一只金丝雀
    if await taskstore.oldest_held():
        await schedule_resume_held(_DRAIN_INTERVAL)
