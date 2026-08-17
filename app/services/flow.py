"""任务生命周期共享逻辑：创建 / 查询 / 取消，以及终态推进的统一入口。
被 tasks / videos / proxy 三个路由和 callback / poller 两个入站复用。
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import HTTPException

from app import queue
from app.config import settings
from app.deps import ratelimit
from app.deps.preflight import Preflight
from app.logging import log
from app.schemas import ACTIVE, FAILURE, QUEUED, SUCCESS, TERMINAL
from app.services import (
    idem,
    pricing,
    providers,
    statelog,
    taskstore,
    tokensession,
    upstream,
)
from app.services.providers import PricingError


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
    # 0) 幂等重放短路（preflight 已在 freeze 前判定；此处兜底同一语义）：
    #    客户端重试直接返回原任务，不产生新扣费
    if pf.replay_task_id:
        task = await taskstore.get(pf.replay_task_id)
        if task:
            return public_view(task)
    if pf.idem_key:
        existing = await idem.get_task_id(pf.token.hash, pf.idem_key)
        if existing:
            task = await taskstore.get(existing)
            if task:
                return public_view(task)

    assert pf.route is not None and pf.key is not None and pf.identity is not None

    # 2) 并发占用（终态推进时释放）
    await ratelimit.conc_acquire(pf.token.hash)

    route = pf.route
    if not route.submit_path:
        # 渠道 setting.gateway.submit_path 未配置：接入未完成，立即可见
        raise HTTPException(
            502, f"biz {biz!r} upstream not configured "
                 f"(channel setting.gateway.submit_path missing)"
        )
    log.info(
        "task creating: task_id={} biz={} model={} user_id={} channel_id={} freeze={}",
        pf.task_id, route.biz, pf.model, pf.identity.user_id, pf.key.key_id, pf.amount,
    )
    callback_url = (
        route.callback_url_for(settings.gateway_public_base_url, pf.task_id)
        if route.supports_callback
        else None
    )
    # 提交体在路由侧一次塑形：default_params < 用户 body < 渠道 param_override，
    # model_mapping 改写 + 回调注入（详见 upstream.build_submit_body）
    submit_body = upstream.build_submit_body(route, pf.key, body, callback_url)

    data = {
        "biz": route.biz,                     # 权威 biz 来自渠道元数据（非 URL 段）
        "source": source,
        "model": pf.model,
        "token_hash": pf.token.hash,
        "idempotency_key": pf.idem_key,
        "callback_url": body.get("callback_url") or body.get("webhook"),
        "freeze_amount": pf.amount,
        "settled": pf.amount <= 0,          # 免费任务无需结算闭环
        "key_id": pf.key.key_id,
        "key_index": pf.key.key_index,
        # 原始请求快照：终态结算重估的基底（settle_usage_map 覆盖实际用量）
        "request_body": body,
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
            resp = await upstream.submit(route, pf.key, submit_body)
        except upstream.UpstreamError as exc:
            await taskstore.cas(pf.task_id, ACTIVE, FAILURE, fail_reason=str(exc)[:500])
            if pf.amount > 0:
                await queue.publish_cancel(pf.task_id, pf.token.raw)
            await ratelimit.conc_release(pf.token.hash)
            await providers.keys.report(
                pf.key, ok=False,
                status_code=0 if exc.envelope else exc.status,
                error=str(exc)[:200],
            )
            log.warning("upstream rejected at submit: task_id={} biz={} err={}",
                        pf.task_id, route.biz, str(exc)[:200])
            raise HTTPException(502, f"upstream rejected: {exc}") from exc
        latency_ms = int((time.monotonic() - started) * 1000)
        await providers.keys.report(pf.key, ok=True, latency_ms=latency_ms)

        upstream_task_id = upstream.extract_path(resp, route.task_id_path)
        if not upstream_task_id:
            # 傻瓜式防护：submit_path 配置的 biz 必须能提取到上游任务 id，
            # 提取不到 = task_id_path 配错或上游非异步——立即可见，不静默挂起
            await taskstore.cas(
                pf.task_id, ACTIVE, FAILURE,
                fail_reason=f"upstream response missing task id at path {route.task_id_path!r}",
            )
            if pf.amount > 0:
                await queue.publish_cancel(pf.task_id, pf.token.raw)
            await ratelimit.conc_release(pf.token.hash)
            log.warning("submit response missing task id: task_id={} biz={} path={!r}",
                        pf.task_id, route.biz, route.task_id_path)
            raise HTTPException(
                502, f"upstream response missing task id (task_id_path={route.task_id_path!r})"
            )
        await taskstore.patch_data(
            pf.task_id,
            {"upstream_task_id": upstream_task_id},
            status=QUEUED,
        )
        log.info("task submitted: task_id={} upstream_task_id={} latency_ms={}",
                 pf.task_id, upstream_task_id, latency_ms)

        # 4) 上游不支持回调 → 进延迟探测队列
        if not route.supports_callback:
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
            await queue.publish_cancel(pf.task_id, pf.token.raw)
        raise


async def view_task(task_id: str) -> dict:
    task = await taskstore.get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    await statelog.record_if_changed(task_id, task["status"], detail="get")
    return public_view(task)


async def _settle_amount(route, task: dict, raw: dict) -> tuple[float, dict[str, Any]]:
    """终态实收金额与用量，优先级（傻瓜式三档）：

    ① ``actual_amount_path``：上游直接给出实收金额（最可信）；
    ② ``settle_usage_map``：终态报文提取实际用量覆盖原始请求体，重跑
       渠道计费规则（随租约下发的 billing.rule）得出实收
       （如 duration ← task.usage.output_seconds）；
    ③ 回退冻结金额（顶格预估即实收，多退少补语义退化为不补不退）。
    """
    data = task.get("data") or {}
    freeze_amount = float(data.get("freeze_amount") or 0)
    if route is None:
        return freeze_amount, {}
    if route.actual_amount_path:
        extracted = upstream.extract_path(raw, route.actual_amount_path)
        if isinstance(extracted, int | float):
            return float(extracted), {}
    if route.settle_usage_map:
        request_body = dict(data.get("request_body") or {})
        usage: dict[str, Any] = {}
        for field, path in route.settle_usage_map.items():
            value = upstream.extract_path(raw, path)
            if value is not None:
                request_body[field] = value
                usage[field] = value
        if usage:
            try:
                quote = pricing.quote_from_route(route, request_body)
                return quote.amount, usage
            except PricingError:
                # 重估失败绝不静默多扣/少扣：回退冻结额并告警（sweeper 可对账）
                log.exception("settle re-quote failed, fallback to freeze amount: {}",
                              task.get("task_id"))
    return freeze_amount, {}


async def finalize_task(task: dict, to_status: str, raw: dict, fail_reason: str = "",
                        route=None) -> bool:
    """终态推进统一入口（callback / poller 共用）。
    CAS 抢到推进权才发事件；结算金额取 actual_amount_path / 重估 / 冻结额三档。
    ``route`` 由调用方（已持有租约构建的路由）传入；缺省读进程缓存兜底。
    """
    route_task_id = task["task_id"]
    data = task.get("data") or {}
    if route is None:
        from app.services.registry import registry

        route = registry.get_cached(data.get("biz", ""))
    patch = {"upstream_status": upstream.extract_path(raw, route.status_path) if route else None}
    if to_status == SUCCESS and route and route.result_path:
        patch["result"] = upstream.extract_path(raw, route.result_path)
    if not fail_reason and route and route.error_path:
        fail_reason = str(upstream.extract_path(raw, route.error_path) or "")

    ok = await taskstore.cas(route_task_id, ACTIVE, to_status, patch=patch, fail_reason=fail_reason[:500])
    if not ok:
        return False

    # 终态落点：结构化记录 result（日志采集），便于按 task_id 追溯成品直链
    log.info(
        "task finalized: task_id={} status={} result={} fail_reason={}",
        route_task_id, to_status, patch.get("result"), fail_reason[:200],
    )

    freeze_amount = float(data.get("freeze_amount") or 0)
    if freeze_amount > 0 and not data.get("settled"):
        user_sk = await tokensession.get(route_task_id)
        if not user_sk:
            # 令牌会话丢失（Redis 故障/超 TTL）：billing freeze TTL 到期自动解冻
            # 兜底，这里只告警；settled 保持 False 由 sweeper 持续重试/对账
            log.error("user token session missing, billing event deferred: {}", route_task_id)
        elif to_status == SUCCESS:
            actual, usage = await _settle_amount(route, task, raw)
            await queue.publish_settle(
                route_task_id, actual, user_sk,
                units=next(iter(usage.values()), None),
                attrs={"biz": data.get("biz"), "model": data.get("model"), **usage},
            )
        else:
            await queue.publish_cancel(route_task_id, user_sk)
        if user_sk:
            await tokensession.clear(route_task_id)

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
