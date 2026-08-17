"""创建类请求的预检（依赖注入）：
限流 → 并行(身份内省 ∥ key租约) → 路由配置+本地报价（规则随租约下发）→
freeze → 令牌暂存。
微服务调用全部走 providers 适配层；freeze 是唯一必须同步的资金操作，request_id = task_id。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field

from fastapi import Header, HTTPException, Request

from app.config import settings
from app.deps import ratelimit
from app.deps.auth import TokenCtx, extract_token, resolve_identity
from app.logging import log
from app.schemas import KeyLease, Quote, RouteConfig, UserIdentity
from app.services import providers, tokensession
from app.services.pricing import quote_from_route
from app.services.providers import (
    BillingError,
    KeyLeaseError,
    PricingError,
)
from app.services.registry import registry, route_from_lease


@dataclass
class Preflight:
    biz: str
    token: TokenCtx
    model: str
    amount: float
    task_id: str
    idem_key: str | None
    route: RouteConfig | None = None
    identity: UserIdentity | None = None
    quote: Quote | None = None
    key: KeyLease | None = None
    body: dict = field(default_factory=dict)
    # 幂等重放短路：同 token + Idempotency-Key 已有任务时置位，
    # preflight 在 freeze 前返回（绝不重复冻结），flow 直接回放首个任务视图
    replay_task_id: str | None = None
    # 冻结到期时刻（billing freeze 响应 expires_at；缺省 now+ttl）：
    # 落 tasks.data，sweep 续期扫描依此判定临期
    freeze_expires_at: int = 0


async def preflight(
    biz: str,
    request: Request,
    authorization: str | None = Header(None),
    idempotency_key: str | None = Header(None),
) -> Preflight:
    token = extract_token(authorization)
    await ratelimit.check_rate(f"tok:{token.hash}")

    # 大请求体（文件上传透传）不做 JSON 解析，计费模型取 body.model
    body: dict = {}
    content_length = int(request.headers.get("content-length") or 0)
    if content_length <= 1_048_576 and request.headers.get("content-type", "").startswith("application/json"):
        try:
            parsed = await request.json()   # starlette 会缓存 body，下游可再次读取
            if isinstance(parsed, dict):
                body = parsed
        except Exception:
            body = {}

    # 计费模型：body.model / body.model_name（keypool 选渠道需要）
    model = body.get("model") or body.get("model_name")

    # 幂等重放短路必须在 freeze 之前：同 Idempotency-Key 直接回放首个任务，
    # 不产生第二次冻结/租约/报价（计费重复防线第一重，billing request_id 唯一约束兜底）
    if idempotency_key:
        from app.services import idem

        replay_task_id = await idem.get_task_id(token.hash, idempotency_key)
        if replay_task_id:
            return Preflight(
                biz=biz, token=token, model=model or "", amount=0.0,
                task_id=replay_task_id, idem_key=idempotency_key,
                body=body, replay_task_id=replay_task_id,
            )
    if not model:
        raise HTTPException(400, "missing model")

    # 无依赖的远程调用全部并行：身份内省 ∥ key 租约（统一分组 GW_KEY_GROUP +
    # model 选渠道）；计费规则随租约下发，报价在路由构建后本地沙箱求值
    try:
        identity, key = await asyncio.gather(
            resolve_identity(token),
            providers.keys.lease(biz, model=model),
        )
    except HTTPException:
        raise
    except BillingError as exc:
        raise HTTPException(exc.status, exc.message) from exc
    except KeyLeaseError as exc:
        # 无可用 key：透传 keypool 建议退避为 Retry-After 响应头（秒，至少 1）
        headers = None
        if exc.retry_after_ms:
            headers = {"Retry-After": str(max(1, (exc.retry_after_ms + 999) // 1000))}
        raise HTTPException(503, str(exc), headers=headers) from exc

    # 路由配置随租约从渠道元数据构建（零本地路由文件）并回填进程缓存；
    # 报价 = 渠道 gateway 块 billing.rule 本地求值（规则唯一事实源 = keypool）
    route = registry.remember(route_from_lease(biz, key))
    try:
        quote = quote_from_route(route, body)
    except PricingError as exc:
        log.error("billing rule error: biz={} model={} err={}", biz, model, exc)
        raise HTTPException(500, f"billing rule error: {exc}") from exc

    task_id = uuid.uuid4().hex
    log.debug("preflight: biz={} model={} user_id={} channel_id={} quote={} {}",
              route.biz, model, identity.user_id, key.key_id, quote.amount, quote.metric)

    # 同步预冻结（金额 > 0 才计费；freeze 即第二重身份校验）
    freeze_expires_at = 0
    if quote.amount > 0:
        try:
            frozen = await providers.billing.freeze(
                raw_token=token.raw,
                request_id=task_id,
                biz_type=route.pricing_biz_type or biz,
                metric=quote.metric,
                amount=quote.amount,
                ttl_seconds=settings.freeze_ttl_seconds,
                attrs={"gateway": True, "biz": biz, "model": model, "path": str(request.url.path)},
            )
        except BillingError as exc:
            raise HTTPException(exc.status, exc.message) from exc
        # 终态 settle/cancel 仍须用户令牌（billing 只认令牌身份；new-api 渠道侧
        # 轮询不带 sk）——按 task_id 暂存 Redis 作为唯一的 taskid→token 查询处
        # （终态清除；冻结 TTL 是资金兜底）
        await tokensession.store(task_id, token.raw)
        # 冻结到期时刻落 tasks.data：sweep 续期扫描（HELD/长任务防过期）依此判定
        freeze_expires_at = int(frozen.get("expires_at") or 0) \
            or int(time.time()) + settings.freeze_ttl_seconds

    return Preflight(
        biz=biz, route=route, token=token, identity=identity, quote=quote, key=key,
        model=model, amount=quote.amount, task_id=task_id, idem_key=idempotency_key,
        body=body, freeze_expires_at=freeze_expires_at,
    )
