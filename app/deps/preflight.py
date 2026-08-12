"""创建类请求的预检（依赖注入）：
限流 → 路由配置 → 并行(身份内省 ∥ 模型报价 ∥ key租约) → freeze。
微服务调用全部走 providers 适配层；freeze 是唯一必须同步的资金操作，request_id = task_id。
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field

from fastapi import Header, HTTPException, Request

from app.config import settings
from app.deps import ratelimit
from app.deps.auth import TokenCtx, extract_token, resolve_identity
from app.schemas import KeyLease, Quote, RouteConfig, UserIdentity
from app.services import providers
from app.services.providers import BillingError, KeyLeaseError, PricingError
from app.services.registry import registry

log = logging.getLogger("gateway.preflight")


@dataclass
class Preflight:
    biz: str
    route: RouteConfig
    token: TokenCtx
    identity: UserIdentity
    quote: Quote
    key: KeyLease
    model: str
    amount: float
    task_id: str
    idem_key: str | None
    body: dict = field(default_factory=dict)


async def preflight(
    biz: str,
    request: Request,
    authorization: str | None = Header(None),
    idempotency_key: str | None = Header(None),
) -> Preflight:
    token = extract_token(authorization)
    await ratelimit.check_rate(f"tok:{token.hash}")

    route = registry.get(biz)
    if route is None:
        raise HTTPException(404, f"unknown biz: {biz}")

    # 大请求体（文件上传透传）不做 JSON 解析，计费模型走路由默认
    body: dict = {}
    content_length = int(request.headers.get("content-length") or 0)
    if content_length <= 1_048_576 and request.headers.get("content-type", "").startswith("application/json"):
        try:
            parsed = await request.json()   # starlette 会缓存 body，下游可再次读取
            if isinstance(parsed, dict):
                body = parsed
        except Exception:
            body = {}

    # 计费模型：body.model / body.model_name / 路由默认
    model = body.get("model") or body.get("model_name") or route.default_model
    if not model:
        raise HTTPException(400, "missing model for billing")

    # 无依赖的远程调用全部并行，省 2~3 个 RTT
    try:
        identity, quote, key = await asyncio.gather(
            resolve_identity(token),
            providers.pricing.quote(model, body),
            providers.keys.lease(biz, model=model, group=route.key_group),
        )
    except HTTPException:
        raise
    except BillingError as exc:
        raise HTTPException(exc.status_code, exc.message)
    except PricingError as exc:
        raise HTTPException(503, str(exc))
    except KeyLeaseError as exc:
        raise HTTPException(503, str(exc))

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
            raise HTTPException(exc.status_code, exc.message)

    return Preflight(
        biz=biz, route=route, token=token, identity=identity, quote=quote, key=key,
        model=model, amount=quote.amount, task_id=task_id, idem_key=idempotency_key, body=body,
    )
