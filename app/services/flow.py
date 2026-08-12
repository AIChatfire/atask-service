"""任务生命周期共享逻辑：创建 / 查询 / 取消，以及终态推进的统一入口。
被 tasks / videos / proxy 三个路由和 callback / poller 两个入站复用。
"""

import logging
import time

from fastapi import HTTPException

from app import queue
from app.config import settings
from app.deps import ratelimit
from app.deps.preflight import Preflight
from app.schemas import ACTIVE, FAILURE, QUEUED, SUCCESS, TERMINAL
from app.services import idem, providers, statelog, taskstore, upstream

log = logging.getLogger("gateway.flow")


def public_view(task: dict) -> dict:
    """对外视图：不暴露 key/freeze/token_hash 等内部字段；task_id 即凭证，无需鉴权"""
    data = task.get("data") or {}
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "progress": task.get("progress", "0%"),
        "fail_reason": task.get("fail_reason") or "",
        "result": data.get("result"),
        "created_at": task.get("created_at"),
        "finish_time": task.get("finish_time") or 0,
    }


async def create_task(biz: str, body: dict, pf: Preflight, action: str, source: str) -> dict:
    # 1) 幂等短路：客户端重试直接返回原任务，不产生新扣费
    if pf.idem_key:
        existing = await idem.get_task_id(pf.token.hash, pf.idem_key)
        if existing:
            task = await taskstore.get(existing)
            if task:
                return public_view(task)

    # 2) 并发占用（终态推进时释放）
    await ratelimit.conc_acquire(pf.token.hash)

    data = {
        "biz": biz,
        "source": source,
        "model": pf.model,
        "key_group": pf.route.key_group,
        "token_hash": pf.token.hash,
        "idempotency_key": pf.idem_key,
        "callback_url": body.get("callback_url") or body.get("webhook"),
        "freeze_amount": pf.amount,
        "settled": pf.amount <= 0,          # 免费任务无需结算闭环
        "key_id": pf.key.key_id,
        "key_index": pf.key.key_index,
    }
    try:
        await taskstore.create(
            task_id=pf.task_id,
            user_id=pf.identity.user_id,
            channel_id=pf.key.key_id,
            action=action,
            data=data,
        )

        # 3) 提交上游（创建类操作绝不重试，失败走取消冻结）
        started = time.monotonic()
        try:
            resp = await upstream.submit(pf.route, pf.key, body)
        except upstream.UpstreamError as exc:
            await taskstore.cas(pf.task_id, ACTIVE, FAILURE, fail_reason=str(exc)[:500])
            if pf.amount > 0:
                await queue.publish_cancel(pf.task_id)
            await ratelimit.conc_release(pf.token.hash)
            await providers.keys.report(pf.key, ok=False, status_code=exc.status, error=str(exc)[:200])
            raise HTTPException(502, f"upstream rejected: {exc}") from exc
        await providers.keys.report(pf.key, ok=True, latency_ms=int((time.monotonic() - started) * 1000))

        upstream_task_id = upstream.extract_path(resp, pf.route.task_id_path)
        await taskstore.patch_data(
            pf.task_id,
            {"upstream_task_id": upstream_task_id},
            status=QUEUED,
        )

        # 4) 上游不支持回调 → 进延迟探测队列
        if not pf.route.supports_callback and upstream_task_id:
            await queue.schedule_poll(pf.task_id, settings.poll_ladder_seconds[0])

        if pf.idem_key:
            await idem.set_task_id(pf.token.hash, pf.idem_key, pf.task_id)

        view = {"task_id": pf.task_id, "status": QUEUED, "upstream_task_id": upstream_task_id}
        return view
    except HTTPException:
        raise
    except Exception:
        # 创建链路异常：尽力释放占用并取消冻结，防止泄漏
        await ratelimit.conc_release(pf.token.hash)
        if pf.amount > 0:
            await queue.publish_cancel(pf.task_id)
        raise


async def view_task(task_id: str) -> dict:
    task = await taskstore.get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    await statelog.record_if_changed(task_id, task["status"], detail="get")
    return public_view(task)


async def finalize_task(task: dict, to_status: str, raw: dict, fail_reason: str = "") -> bool:
    """终态推进统一入口（callback / poller 共用）。
    CAS 抢到推进权才发事件；结算金额取 actual_amount_path 或回退冻结额。
    """
    route_task_id = task["task_id"]
    data = task.get("data") or {}
    from app.services.registry import registry

    route = registry.get(data.get("biz", ""))
    patch = {"upstream_status": upstream.extract_path(raw, route.status_path) if route else None}
    if to_status == SUCCESS and route and route.result_path:
        patch["result"] = upstream.extract_path(raw, route.result_path)

    ok = await taskstore.cas(route_task_id, ACTIVE, to_status, patch=patch, fail_reason=fail_reason[:500])
    if not ok:
        return False

    # 终态落点：结构化记录 result（Logfire 采集），便于按 task_id 追溯成品直链
    log.info(
        "task finalized: task_id=%s status=%s result=%s fail_reason=%s",
        route_task_id, to_status, patch.get("result"), fail_reason[:200],
    )

    freeze_amount = float(data.get("freeze_amount") or 0)
    if freeze_amount > 0 and not data.get("settled"):
        if to_status == SUCCESS:
            actual = freeze_amount
            if route and route.actual_amount_path:
                extracted = upstream.extract_path(raw, route.actual_amount_path)
                if isinstance(extracted, (int, float)):
                    actual = float(extracted)
            await queue.publish_settle(route_task_id, actual)
        else:
            await queue.publish_cancel(route_task_id)

    if data.get("callback_url"):
        fresh = await taskstore.get(route_task_id)
        await queue.publish_notify(route_task_id, data["callback_url"], public_view(fresh or task))

    await ratelimit.conc_release(data.get("token_hash"))
    return True


async def cancel_task(task_id: str) -> dict:
    """按 task_id 取消（持有即凭证，与 GET 同一安全假设）"""
    task = await taskstore.get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    if task["status"] in TERMINAL:
        return public_view(task)
    await finalize_task(task, "CANCELED", {}, fail_reason="canceled by user")
    # TODO: 如上游支持取消，调用上游取消端点
    fresh = await taskstore.get(task_id)
    return public_view(fresh or task)
