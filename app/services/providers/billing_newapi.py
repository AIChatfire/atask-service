"""newapi-billing-service 实现（https://github.com/AIChatfire/newapi-billing-service）

POST {BASE}/api/v1/auth/inspect     Authorization: Bearer <用户token>
  → 200 {"valid": true, "user_id": 123, "token_id": 45} / 401
POST {BASE}/api/v1/billing/freeze   Authorization: Bearer <用户token>
  → 200 {..., "user_id": 123} / 401 非法 / 402 余额不足 / 409 request_id 已存在（幂等成功）
POST {BASE}/api/v1/billing/settle   Authorization: Bearer <服务账号token>
POST {BASE}/api/v1/billing/cancel   Authorization: Bearer <服务账号token>
"""

import logging

from app.config import settings
from app.schemas import UserIdentity
from app.services import httpc
from app.services.providers import BillingError

log = logging.getLogger("gateway.provider.billing")


class NewapiBillingProvider:
    def _client(self):
        return httpc.new_client(base_url=settings.billing_svc_url, timeout=settings.http_timeout)

    async def inspect(self, raw_token: str) -> UserIdentity | None:
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/auth/inspect",
                headers={"Authorization": f"Bearer {raw_token}"},
            )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data.get("valid"):
            return None
        return UserIdentity(user_id=data["user_id"], token_id=data.get("token_id", 0))

    async def freeze(self, *, raw_token: str, request_id: str, biz_type: str,
                     metric: str, amount: float, ttl_seconds: int, attrs: dict) -> dict:
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/billing/freeze",
                headers={"Authorization": f"Bearer {raw_token}"},
                json={
                    "request_id": request_id,
                    "biz_type": biz_type,
                    "metric": metric,
                    "amount": amount,
                    "ttl_seconds": ttl_seconds,
                    "attrs": attrs,
                },
            )
        if resp.status_code == 409:
            return {"duplicated": True}                    # 幂等重放，视为成功
        if resp.status_code != 200:
            raise BillingError(resp.status_code, resp.text[:200])
        data = resp.json()
        if data.get("error"):
            raise BillingError(502, str(data["error"]))
        return data

    async def settle(self, request_id: str, actual_amount: float) -> None:
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/billing/settle",
                headers={"Authorization": f"Bearer {settings.billing_admin_token}"},
                json={"request_id": request_id, "actual_amount": actual_amount},
            )
        if resp.status_code != 200:
            raise BillingError(resp.status_code, f"settle {request_id}: {resp.text[:200]}")

    async def cancel(self, request_id: str) -> None:
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/billing/cancel",
                headers={"Authorization": f"Bearer {settings.billing_admin_token}"},
                json={"request_id": request_id},
            )
        if resp.status_code != 200:
            raise BillingError(resp.status_code, f"cancel {request_id}: {resp.text[:200]}")
