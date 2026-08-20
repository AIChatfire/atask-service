"""任务生命周期共享逻辑：创建 / 查询 / 取消，以及终态推进的统一入口。
被 tasks / videos / proxy 三个路由和 callback / poller 两个入站复用。

创建链路为**异步提交**：preflight（限流/幂等/内省/租约/冻结）→ 落 tasks 表
→ 立即返回本地 task_id → 上游提交由 queue.submit_task 在 worker 进程执行
（app/services/submit.py），拿到上游 id 后回写并进探测/回调闭环。
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from app import queue
from app.deps import ratelimit
from app.deps.preflight import Preflight
from app.logging import log
from app.schemas import (
    ACTIVE,
    FAILURE,
    HELD,
    QUEUED,
    SUBMITTED,
    SUCCESS,
    TERMINAL,
)
from app.services import (
    idem,
    leasing,
    nativeapi,
    pricing,
    resulturl,
    statelog,
    taskstore,
    tokensession,
    upstream,
)
from app.services.providers import PricingError
from app.services.registry import registry

#: 时间归一单点在 taskstore（读侧已统一毫秒→秒）；此处仅为消费别名
_as_unix_seconds = taskstore.as_unix_seconds


def duration_seconds(task: dict) -> int:
    """耗时 = 终态时间 - 创建时间（统一秒）。终态时间缺失（非终态/为 0）或
    字段异常时返回 0——绝不产出天文数字。"""
    finish = _as_unix_seconds(task.get("finish_time"))
    created = _as_unix_seconds(task.get("created_at"))
    if not finish or not created:
        return 0
    return max(0, finish - created)


def public_view(task: dict) -> dict:
    """对外视图（白名单序列化）：upstream_task_id/key/freeze/token_hash 等内部
    字段绝不暴露——上游任务 id 是内部实现细节，只留 tasks.data 供轮询/结算/
    对账与 ops 诊断使用；task_id 即凭证，无需鉴权。
    HELD（账户级挂起）对外映射为 QUEUED——调用方无需理解挂起语义。"""
    data = task.get("data") or {}
    status = task["status"]
    return {
        "task_id": task["task_id"],
        "status": QUEUED if status == HELD else status,
        "progress": task.get("progress", "0%"),
        "fail_reason": task.get("fail_reason") or "",
        "result": data.get("result"),
        "created_at": task.get("created_at"),
        "finish_time": task.get("finish_time") or 0,
        "duration": duration_seconds(task),
    }


async def create_task(biz: str, body: dict, pf: Preflight, action: str, source: str) -> dict:
    # 0) 幂等重放短路（preflight 已在 freeze 前判定；此处兜底同一语义）：
    #    客户端重试直接返回原任务，不产生新扣费
    if pf.replay_task_id:
        task = await taskstore.get(pf.replay_task_id)
        if task:
            return public_view(task)
        # 重放目标已不存在（行被清理）：preflight 重放短路未做 freeze/route，
        # 不能 fall through（route=None 必撞 assert 500），显式 409 让客户端摘键重试
        raise HTTPException(
            409, "idempotent replay target missing; retry without Idempotency-Key")
    if pf.idem_key:
        existing = await idem.get_task_id(pf.token.hash, pf.idem_key)
        if existing:
            task = await taskstore.get(existing)
            if task:
                return public_view(task)

    assert pf.route is not None and pf.key is not None and pf.identity is not None

    # 1) 并发占用（终态推进时释放）
    try:
        await ratelimit.conc_acquire(pf.token.hash)
    except Exception:
        # 占用失败（429 超限）= 创建链路失败：取消预冻结（资金不无任务挂账
        # 至 freeze TTL）+ CAS 归还幂等占位，与下方创建异常分支同一收口纪律；
        # 槽位未抢到（LUA 自减），无需 conc_release
        if pf.amount > 0:
            await queue.publish_cancel(pf.task_id, pf.token.raw)
        if pf.idem_key:
            await idem.release(pf.token.hash, pf.idem_key)
        raise

    route = pf.route
    if not route.submit_path:
        # 渠道 setting.gateway.submit_path 未配置：接入未完成，立即可见
        await ratelimit.conc_release(pf.token.hash)   # 及时还槽（TTL 兜底 + sweep 校准只是保险）
        if pf.amount > 0:
            await queue.publish_cancel(pf.task_id, pf.token.raw)
        if pf.idem_key:
            # 占位者提前出局（无任务可回填）：CAS 归还占位，同键重试立即可重建
            await idem.release(pf.token.hash, pf.idem_key)
        raise HTTPException(
            502, f"biz {biz!r} upstream not configured "
                 f"(channel setting.gateway.submit_path missing)"
        )
    log.info(
        "task creating: task_id={} biz={} model={} user_id={} channel_id={} freeze={}",
        pf.task_id, route.biz, pf.model, pf.identity.user_id, pf.key.key_id, pf.amount,
    )
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
        "freeze_expires_at": pf.freeze_expires_at,
        # 原始请求快照：异步提交体重建与终态结算重估的基底
        "request_body": body,
    }
    try:
        # 2) 立即落库（SUBMITTED）→ 回填幂等键 → 提交事件入队（Redis list，
        #    进程重启不丢；sweep 对丢失的提交事件兜底补投）
        await taskstore.create(
            task_id=pf.task_id,
            user_id=pf.identity.user_id,
            channel_id=pf.key.key_id,
            action=action,
            data=data,
        )
        if pf.idem_key:
            await idem.set_task_id(pf.token.hash, pf.idem_key, pf.task_id)
        await queue.publish_submit(pf.task_id)
    except HTTPException:
        raise
    except Exception:
        # 创建链路异常：尽力释放占用并取消冻结，防止泄漏
        await ratelimit.conc_release(pf.token.hash)
        if pf.amount > 0:
            await queue.publish_cancel(pf.task_id, pf.token.raw)
        if pf.idem_key:
            # 幂等占位未回填（任务未建成）：CAS 归还占位（已回填则 no-op），
            # 同键重试立即可重建而不是干等占位 TTL
            await idem.release(pf.token.hash, pf.idem_key)
        raise

    # 3) 立即返回本地 task_id——上游提交在 worker 异步执行（submit.submit_one），
    #    失败补偿（解冻/HELD 挂起）由 worker 侧收口；GET/轮询/回调全程本地 id
    log.info("task accepted (async submit): task_id={} biz={}", pf.task_id, route.biz)
    return {"task_id": pf.task_id, "status": SUBMITTED}


async def resolve_task(task_id: str) -> dict | None:
    """本地 task_id 优先；未命中按上游任务 id 反查——客户端持上游 id
    （如同步提交时代/proxy 透传响应里的上游 id）轮询的兼容入口。
    原生透传路径的 GET 拦截同样复用它（URL 里的 id 段两种形态都认）。"""
    task = await taskstore.get(task_id)
    if task:
        return task
    return await taskstore.get_by_upstream_id(task_id)


async def view_task(task_id: str) -> dict:
    task = await resolve_task(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    await statelog.record_if_changed(task["task_id"], task["status"], detail="get")
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
                        route=None, failed_charge: bool = True) -> bool:
    """终态推进统一入口（callback / poller / 异步提交失败补偿共用）。
    CAS 抢到推进权才发事件；结算金额取 actual_amount_path / 重估 / 冻结额三档。
    ``route`` 由调用方（已持有租约构建的路由）传入；缺省读进程缓存兜底。
    ``failed_charge=False``：提交阶段失败（上游未接单）一律解冻，不适用渠道
    失败单 charge 策略（该策略只覆盖生成失败的厂商条款收费）。
    """
    route_task_id = task["task_id"]
    data = task.get("data") or {}
    if route is None:
        route = registry.get_cached(data.get("biz", ""))
    patch = {"upstream_status": upstream.extract_path(raw, route.status_path) if route else None}
    if to_status == SUCCESS and route and route.result_path:
        extracted = upstream.extract_path(raw, route.result_path)
        # 产物直链改写（渠道配了 result_url_template 才动作）：对外统一走网关
        # 自己的域名/转存服务；原始直链另存 upstream_result 供对账与回源
        patch["result"] = resulturl.transform(route, extracted, route_task_id)
        if patch["result"] != extracted:
            patch["upstream_result"] = extracted
    if not fail_reason and route and route.error_path:
        fail_reason = str(upstream.extract_path(raw, route.error_path) or "")
    # 终态上游原始报文快照：原生查询拦截据此逐字段同构回放（usage/trace_id 等
    # 网关不认识的字段全都在），且终态查询零上游往返。小体积才落，见 nativeapi
    snapshot = nativeapi.capture_snapshot(raw)
    if snapshot is not None:
        patch["upstream_snapshot"] = snapshot

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
        elif (to_status == FAILURE and failed_charge and route
                and route.failed_billing == "charge"):
            # 失败单计费策略 charge（厂商条款失败也收费）：先查 actual_amount_path
            # 实收 → settle_usage_map 重估 → 冻结额兜底（与成功单同一三档）
            actual, usage = await _settle_amount(route, task, raw)
            log.warning("failed task charged per channel policy: task_id={} amount={}",
                        route_task_id, actual)
            await queue.publish_settle(
                route_task_id, actual, user_sk,
                units=next(iter(usage.values()), None),
                attrs={"biz": data.get("biz"), "model": data.get("model"),
                       "failed_charge": True, **usage},
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


async def try_upstream_cancel(task: dict, route=None) -> None:
    """尽力调上游取消端点止损（渠道配 ``cancel_path`` 才动作）。
    用户取消 / 探测超时收口时调用；任何失败只告警，绝不阻塞本地收口。"""
    data = task.get("data") or {}
    upstream_task_id = data.get("upstream_task_id")
    biz = str(data.get("biz") or "")
    if route is None:
        route = registry.get_cached(biz)
    if route is None or not route.cancel_path or not upstream_task_id:
        return
    try:
        # 钉回创建时那把 key（精确直达）：同渠道多账号时换 key 取消不到
        key = await leasing.lease_for_task(biz, data, task)
        if await upstream.cancel_task_remote(route, key, str(upstream_task_id)):
            log.info("upstream cancel ok: task_id={} upstream_task_id={}",
                     task["task_id"], upstream_task_id)
    except Exception as exc:
        log.warning("upstream cancel attempt failed: task_id={} err={}",
                    task["task_id"], exc)


async def cancel_task(task_id: str) -> dict:
    """按 task_id 取消（持有即凭证，与 GET 同一安全假设；兼容上游 id 反查）"""
    task = await resolve_task(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    if task["status"] in TERMINAL:
        return public_view(task)
    await finalize_task(task, "CANCELED", {}, fail_reason="canceled by user")
    # 渠道配了 cancel_path 时尽力源头止损（失败不阻塞，本地已收口）
    await try_upstream_cancel(task)
    fresh = await taskstore.get(task["task_id"])
    return public_view(fresh or task)
