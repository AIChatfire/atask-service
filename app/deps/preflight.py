"""创建类请求的预检（依赖注入）：
限流 → 路由配置 → 并行(身份内省 ∥ 模型报价 ∥ key租约) → freeze → 令牌暂存。
微服务调用全部走 providers 适配层；freeze 是唯一必须同步的资金操作，request_id = task_id。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field

from fastapi import Header, HTTPException, Request

from app.config import settings
from app.deps import ratelimit
from app.deps.auth import TokenCtx, extract_token, resolve_identity
from app.schemas import KeyLease, Quote, RouteConfig, UserIdentity
from app.services import providers, tokensession
from app.services.providers import (
    BillingError,
    KeyLeaseError,
    ModelUnavailableError,
    PricingError,
)
from app.services.registry import registry, route_from_lease

log = logging.getLogger("gateway.preflight")


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

    # 计费模型：body.model / body.model_name（pricing 报价与 keypool 选渠道都需要）
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

    # 无依赖的远程调用全部并行，省 2~3 个 RTT；统一分组（GW_KEY_GROUP）+ model 选渠道
    try:
        identity, quote, key = await asyncio.gather(
            resolve_identity(token),
            providers.pricing.quote(model, body),
            providers.keys.lease(biz, model=model),
        )
    except HTTPException:
        raise
    except ModelUnavailableError as exc:
        raise HTTPException(400, str(exc)) from exc  # 模型不可用：客户端错误
    except BillingError as exc:
        raise HTTPException(exc.status, exc.message) from exc
    except PricingError as exc:
        raise HTTPException(503, str(exc)) from exc
    except KeyLeaseError as exc:
        raise HTTPException(503, str(exc)) from exc

    # 路由配置随租约从渠道元数据构建（零本地路由文件）并回填进程缓存
    route = registry.remember(route_from_lease(biz, key))

    task_id = uuid.uuid4().hex

    # 同步预冻结（金额 > 0 才计费；freeze 即第二重身份校验）
    if quote.amount > 0:
        try:
            await providers.billing.freeze(
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
        # 终态 settle/cancel 仍须用户令牌（billing 只认令牌身份）：
        # 按 task_id 暂存 Redis（终态清除；冻结 TTL 是资金兜底）
        await tokensession.store(task_id, token.raw)

    return Preflight(
        biz=biz, route=route, token=token, identity=identity, quote=quote, key=key,
        model=model, amount=quote.amount, task_id=task_id, idem_key=idempotency_key, body=body,
    )
