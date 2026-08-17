"""Bearer 鉴权：不读 tokens 表，身份由 billing 服务的 /auth/inspect 提供。
结果按 sha256(token) 缓存 30s。GET 类接口全程不涉及鉴权（task_id 即凭证）。
"""

import hashlib
import json
from dataclasses import dataclass

from fastapi import Header, HTTPException

from app.config import settings
from app.logging import log
from app.redis import K_INSPECT, r
from app.schemas import UserIdentity
from app.services.providers import billing


@dataclass
class TokenCtx:
    raw: str
    hash: str


def extract_token(authorization: str | None) -> TokenCtx:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing or invalid Authorization header")
    raw = authorization[7:].strip()
    if not raw:
        raise HTTPException(401, "empty token")
    return TokenCtx(raw=raw, hash=hashlib.sha256(raw.encode()).hexdigest())


async def require_token(authorization: str | None = Header(None)) -> TokenCtx:
    return extract_token(authorization)


async def resolve_identity(ctx: TokenCtx) -> UserIdentity:
    """内省 + 缓存。无效 token → 401。"""
    key = K_INSPECT.format(token_hash=ctx.hash)
    cached = await r.get(key)
    if cached:
        return UserIdentity(**json.loads(cached))
    identity = await billing.inspect(ctx.raw)
    if identity is None:
        raise HTTPException(401, "invalid or expired token")
    await r.set(key, identity.model_dump_json(), ex=settings.auth_cache_ttl)
    log.debug("identity introspected: user_id={} token_id={}", identity.user_id, identity.token_id)
    return identity
