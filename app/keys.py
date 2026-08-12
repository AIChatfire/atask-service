"""上游密钥轮询微服务客户端（KeyProviderClient，SPEC §3.12）。

【建议验证 V21】端点/字段为设计约定，需与 keys 微服务实际契约对齐：

- ``GET {KEYS_SERVICE_URL}/keys/{provider}/acquire``
  → 200 ``{"key_id": str, "credentials": {"ak"?, "sk"?, "api_key"?}, "ttl"?}``；
  进程内缓存 min(服务端 ttl, ``KEYS_CACHE_TTL_SECONDS``)（默认 300s）；
- ``POST {KEYS_SERVICE_URL}/keys/{key_id}/report``
  体 ``{"ok": bool, "status_code"?}``——上游 401/403 时由适配器触发回报
  （供轮询剔除坏 key）；report 尽力而为，失败仅告警。

降级：``KEYS_SERVICE_URL`` 未配置或服务 5xx/超时 → 返回 None，由调用方
（``app.tasks.manager.resolve_submit_secrets``）回退 env 静态密钥
（``{ref}_AK``/``{ref}_SK``、``os.environ[ref]`` 逻辑保留为 fallback）。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

import httpx
import logfire

from app.config import settings
from app.http_clients import keys_client


@dataclass
class KeyLease:
    """acquire 得到的密钥租约（credentials 键集与 SubmitContext.secrets 对齐）。"""

    key_id: str
    credentials: dict[str, str]
    expires_at: float = 0.0  # monotonic 到期点（0 = 不缓存）

    def fresh(self) -> bool:
        return time.monotonic() < self.expires_at


@dataclass
class KeyProviderClient:
    """keys 轮询微服务客户端；``KEYS_SERVICE_URL`` 未配置时整体禁用（acquire 恒
    None → env 兜底；report 恒 no-op）。"""

    _cache: dict[str, KeyLease] = field(default_factory=dict)

    @property
    def enabled(self) -> bool:
        return bool(settings.keys_service_url)

    async def acquire(self, provider: str) -> KeyLease | None:
        """取 provider 可用密钥；未配置/缓存外 5xx/超时/坏报文 → None（env 兜底）。"""
        if not self.enabled:
            return None
        if (cached := self._cache.get(provider)) and cached.fresh():
            return cached
        try:
            resp = await keys_client().get(f"/keys/{provider}/acquire")
            if resp.status_code >= 500:
                logfire.warning("keys acquire 5xx, env fallback",
                                provider=provider, status_code=resp.status_code)
                return None
            resp.raise_for_status()  # 其他 4xx 属契约异常，按不可用来兜底
            body = resp.json()
            key_id = str(body["key_id"])
            credentials = {str(k): str(v) for k, v in (body["credentials"] or {}).items()}
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            logfire.warning("keys acquire failed, env fallback",
                            provider=provider, error=str(exc))
            return None
        ttl = body.get("ttl")
        try:
            ttl_f = float(ttl) if ttl is not None else float("inf")
        except (TypeError, ValueError):
            ttl_f = float("inf")
        lease = KeyLease(
            key_id=key_id, credentials=credentials,
            expires_at=time.monotonic() + min(ttl_f, float(settings.keys_cache_ttl_seconds)),
        )
        self._cache[provider] = lease
        return lease

    async def report(self, key_id: str, *, ok: bool,
                     status_code: int | None = None) -> None:
        """上报 key 可用性（上游 401/403 供轮询剔除）；尽力而为，失败仅告警。"""
        if not self.enabled:
            return
        body: dict[str, object] = {"ok": ok}
        if status_code is not None:
            body["status_code"] = status_code
        try:
            resp = await keys_client().post(f"/keys/{key_id}/report", json=body)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logfire.warning("keys report failed", key_id=key_id, error=str(exc))

    def report_auth_failure(self, provider: str, status_code: int) -> None:
        """适配器同步上下文触发：对该 provider 当前租约异步 report(ok=False)。

        无租约（env 兜底路径）或无运行中事件循环 → no-op。
        """
        lease = self._cache.get(provider)
        if lease is None:
            return
        self._cache.pop(provider, None)  # 立即剔除，下个请求重新 acquire
        try:
            asyncio.get_running_loop().create_task(
                self.report(lease.key_id, ok=False, status_code=status_code)
            )
        except RuntimeError:
            pass  # 无事件循环（同步测试/装配期）：丢弃上报


key_provider = KeyProviderClient()
