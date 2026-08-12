"""keypool-service 实现（https://github.com/AIChatfire/keypool-service）
服务级 Bearer token 认证（GW_KEY_SVC_TOKEN），按 group+model 取 key。

POST {BASE}/v1/key:get     Authorization: Bearer <服务token>
  body {"group": "default", "model": "<model>", "retry": 0}
POST {BASE}/v1/key:report  Authorization: Bearer <服务token>  Idempotency-Key: <uuid>
  body {"channel_id": 7, "key_index": 0, "success": false, "status_code": 401, "error_message": "..."}

注意：key:get 响应字段名以实际服务为准（容忍 key/api_key、channel_id/id 两种命名）。
"""

import asyncio
import logging
import uuid

from app.config import settings
from app.schemas import KeyLease
from app.services import httpc
from app.services.providers import KeyLeaseError

log = logging.getLogger("gateway.provider.keypool")


class KeypoolProvider:
    def _headers(self, extra: dict | None = None) -> dict:
        return {"Authorization": f"Bearer {settings.key_svc_token}", **(extra or {})}

    async def lease(self, biz: str, model: str = "", key_id: int | None = None,
                    group: str = "default") -> KeyLease:
        async with httpc.new_client(timeout=settings.http_timeout) as client:
            resp = await client.post(
                f"{settings.key_svc_url}/v1/key:get",
                headers=self._headers(),
                json={"group": group, "model": model, "retry": 0},
            )
        if resp.status_code != 200:
            raise KeyLeaseError(f"keypool get failed: {resp.status_code} {resp.text[:200]}")
        data = resp.json()
        payload = data.get("data", data)
        key = payload.get("key") or payload.get("api_key") or ""
        if not key:
            raise KeyLeaseError(f"keypool returned no key: {str(payload)[:200]}")
        return KeyLease(
            key_id=int(payload.get("channel_id") or payload.get("id") or 0),
            key=key,
            key_index=int(payload.get("key_index") or 0),
            base_url=payload.get("base_url"),
        )

    async def report(self, key: KeyLease, ok: bool, status_code: int = 0,
                     latency_ms: int = 0, error: str = "") -> None:
        """fire-and-forget：上报失败不阻塞主链路"""

        async def _send() -> None:
            try:
                async with httpc.new_client(timeout=5) as client:
                    await client.post(
                        f"{settings.key_svc_url}/v1/key:report",
                        headers=self._headers({"Idempotency-Key": uuid.uuid4().hex}),
                        json={
                            "channel_id": key.key_id,
                            "key_index": key.key_index,
                            "success": ok,
                            "status_code": status_code,
                            "error_message": error[:200],
                        },
                    )
            except Exception:
                log.debug("key report failed", exc_info=True)

        asyncio.create_task(_send())
