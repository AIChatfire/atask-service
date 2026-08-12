"""Bearer 认证：委托计费服务鉴权 + 委托结论缓存 + 系统跳过开关（SPEC §3.9）。

管线：① ``KEY_RE`` 格式门禁（无 I/O）→ ② SHA-256 → ③ L1 进程内 LRU(60s) /
L2 Redis ``apikey:{sha256(sk)}``(300s)（缓存「委托结论」非自建鉴权）→
④ 委托 ``GET {BILLING_SERVICE_URL}/api/v1/billing/balance``（Bearer 用户 sk）：
200 取 user_id/group；401/403 即无效；5xx/超时 fail-closed 503（绝不放行）。
跳过开关 ``X-Skip-Auth-Billing: true`` + 有效 ``X-System-Token``（env
``SYSTEM_API_TOKEN``，未配置则头一律忽略）→ system 身份（user_id=0）跳过
鉴权且计费 no-op，记审计 event=skip_auth_billing + client_ip；令牌错误按
正常 sk 流程走（防探测）。后台 user_sk 走 ``sksess:{task_id}``（EX=deadline+1h，
终态 transition 时 DEL）。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from dataclasses import dataclass
from typing import Annotated, Any

import httpx
import logfire
from fastapi import Header, Request
from sqlalchemy import text

from app import errors
from app.config import settings
from app.db import get_session_factory
from app.http_clients import billing_client
from app.redis_client import get_redis

KEY_RE = re.compile(r"^sk-[A-Za-z0-9]{20,64}$")  # new-api sk- 令牌形制（§3.9.1）
SKSESS_GRACE_SECONDS = 3600  # sksess TTL 在任务 deadline 之上再宽限 1h
SYSTEM_USER_ID = 0           # 系统跳过开关身份


class BillingAuthUnavailable(Exception):
    """计费服务鉴权委托不可达（5xx/超时/传输错误）→ fail-closed 503。"""


@dataclass
class TokenInfo:
    """委托鉴权结论；``raw`` 仅驻留内存供本次请求计费透传（不落缓存）。"""

    user_id: int
    sk_hash: str                            # sha256(raw)，缓存/限流索引
    group: str = "default"
    raw: str = ""
    is_system: bool = False

    def dump(self) -> str:
        """缓存序列化；不落原始令牌。"""
        return json.dumps({"user_id": self.user_id, "sk_hash": self.sk_hash,
                           "group": self.group}, ensure_ascii=False)

    @classmethod
    def parse(cls, blob: str, raw: str) -> TokenInfo:
        """从缓存 blob 还原；``raw`` 由调用方从请求头补回。"""
        d = json.loads(blob)
        return cls(user_id=int(d["user_id"]), sk_hash=str(d["sk_hash"]),
                   group=str(d.get("group") or "default"), raw=raw)


class LocalTTLCache:
    """进程内 LRU+TTL（L1）；容量有界防内存膨胀。"""

    def __init__(self, maxsize: int = 10_000, ttl: float = 60.0) -> None:
        self._data: dict[str, tuple[float, TokenInfo]] = {}
        self._maxsize = maxsize
        self._ttl = ttl

    def get(self, key: str) -> TokenInfo | None:
        item = self._data.pop(key, None)
        if item is None or time.monotonic() - item[0] >= self._ttl:
            return None
        self._data[key] = item  # LRU：命中即移到末尾
        return item[1]

    def set(self, key: str, value: TokenInfo) -> None:
        self._data.pop(key, None)
        if len(self._data) >= self._maxsize:
            self._data.pop(next(iter(self._data)))  # 逐出最久未用
        self._data[key] = (time.monotonic(), value)

    def clear(self) -> None:
        self._data.clear()


local_cache = LocalTTLCache(settings.token_l1_maxsize, settings.token_l1_ttl_seconds)


def token_hash(raw: str) -> str:
    """SHA-256 哈希（缓存索引均用哈希不落明文）。"""
    return hashlib.sha256(raw.encode()).hexdigest()


async def verify_bearer(raw: str) -> TokenInfo | None:
    """委托鉴权管线：命中缓存或委托 200 返回 TokenInfo，401/403 返回 None；
    计费服务不可达抛 :class:`BillingAuthUnavailable`（fail-closed 503）。"""
    if not KEY_RE.match(raw):                          # ① 格式门禁（无 I/O）
        return None
    h = token_hash(raw)                                # ② 哈希即索引
    if info := local_cache.get(h):                     # ③a L1 进程内 LRU
        info.raw = raw
        return info

    redis, info = None, None
    try:
        redis = await get_redis()
        if blob := await redis.get(f"apikey:{h}"):     # ③b L2 Redis（委托结论）
            info = TokenInfo.parse(blob, raw)
    except Exception:
        logfire.exception("token L2 cache read failed", hash_prefix=h[:8])
    if info is None:                                   # ④ 委托计费服务
        info = await _delegate_to_billing(raw, h)
        if info is None:
            return None
        if redis is not None:
            try:
                await redis.set(f"apikey:{h}", info.dump(),
                                ex=settings.token_redis_ttl_seconds)
            except Exception:
                logfire.exception("token L2 writeback failed", hash_prefix=h[:8])
    local_cache.set(h, info)
    return info


async def _delegate_to_billing(raw: str, h: str) -> TokenInfo | None:
    """委托计费服务（唯一事实源）：200 取 user_id/group（兼容 ``{"data": {...}}``
    信封与平铺）；401/403 → None；其余非 200 与传输异常 → fail-closed。"""
    try:
        resp = await billing_client().get(
            "/api/v1/billing/balance", headers={"Authorization": f"Bearer {raw}"})
        if resp.status_code in (401, 403):
            return None
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data")
        if not isinstance(data, dict):
            data = body
        user_id = int(data["user_id"])
    except httpx.HTTPError as exc:  # 5xx/其他 4xx/超时/传输错误
        logfire.warning("billing auth unreachable", hash_prefix=h[:8], error=str(exc))
        raise BillingAuthUnavailable(str(exc)) from exc
    except Exception as exc:
        logfire.warning("billing auth bad payload", hash_prefix=h[:8], error=str(exc))
        raise BillingAuthUnavailable("billing balance payload unrecognized") from exc
    return TokenInfo(user_id=user_id, sk_hash=h,
                     group=str(data.get("group") or "default"), raw=raw)


async def store_user_sk(task_id: str, raw_sk: str, deadline_unix: int) -> None:
    """submit 成功时写入 ``sksess:{task_id}``（EX=任务 deadline+1h）。"""
    ttl = max(1, deadline_unix + SKSESS_GRACE_SECONDS - int(time.time()))
    await (await get_redis()).set(f"sksess:{task_id}", raw_sk, ex=ttl)


async def get_user_sk_for_task(task_id: str) -> str | None:
    """renewer/outbox/对账取回可透传的用户 sk；取不到返回 None（调用方告警）。"""
    try:
        sk = await (await get_redis()).get(f"sksess:{task_id}")
    except Exception:
        logfire.exception("sksess lookup failed", task_id=task_id)
        return None
    return str(sk) if sk else None


async def clear_user_sk(task_id: str) -> None:
    """终态 transition 时 DEL ``sksess:{task_id}``（敏感数据最小驻留）。"""
    try:
        await (await get_redis()).delete(f"sksess:{task_id}")
    except Exception:
        logfire.exception("sksess clear failed", task_id=task_id)


def _skip_auth_billing_applies(request: Request) -> bool:
    """跳过开关生效判定；未配置/令牌错误 → 忽略头按正常 sk 流程走（防探测）。"""
    if request.headers.get("X-Skip-Auth-Billing", "").lower() != "true":
        return False
    expected = settings.system_api_token
    if not expected:
        return False
    return hmac.compare_digest(request.headers.get("X-System-Token", ""), expected)


async def current_token(
    request: Request, authorization: Annotated[str | None, Header()] = None
) -> TokenInfo:
    """FastAPI 依赖：401/503/跳过开关 → system 身份（计费 no-op）。"""
    if _skip_auth_billing_applies(request):
        logfire.info("skip_auth_billing", event="skip_auth_billing",
                     client_ip=request.client.host if request.client else "",
                     path=request.url.path)
        return TokenInfo(user_id=SYSTEM_USER_ID, sk_hash="system", group="system",
                         is_system=True)
    if not authorization or not authorization.startswith("Bearer "):
        raise errors.unauthorized("missing bearer token")
    try:
        token = await verify_bearer(authorization.removeprefix("Bearer ").strip())
    except BillingAuthUnavailable as exc:
        logfire.warning("auth.deny", reason="billing_auth_unavailable")
        raise errors.backpressure(retry_after=5, message="auth delegation unavailable") from exc
    if token is None:
        logfire.info("auth.deny", reason="invalid_token")
        raise errors.unauthorized()
    return token


_TASK_SQL = text(
    "SELECT task_id, platform, user_id, status, progress, properties,"
    " private_data, data, fail_reason, created_at, updated_at, finish_time"
    r" FROM tasks WHERE task_id = :id AND platform LIKE 'gw\_%' LIMIT 1"
)


async def get_owned_task(task_id: str, token: TokenInfo) -> dict[str, Any]:
    """防 IDOR（§6.4）：不存在或不符 → 404；system 身份豁免归属校验。"""
    async with get_session_factory()() as session:
        row = (await session.execute(_TASK_SQL, {"id": task_id})).mappings().first()
    if not row or (row["user_id"] != token.user_id and not token.is_system):
        raise errors.not_found("task not found")
    return dict(row)
