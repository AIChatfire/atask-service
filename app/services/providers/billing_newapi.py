"""newapi-billing-service 实现（https://github.com/AIChatfire/newapi-billing-service）。

统一前缀 ``/api/v1``；鉴权恒为 ``Authorization: Bearer <终端用户 sk- 令牌>``，
user_id 由令牌解析（网关不指定、不缓存用户余额）。金额为 USD 十进制数。

- ``POST /auth/inspect`` → 200 扁平 ``{"valid": true, "user_id", "token_id"}`` / 401
- ``POST /billing/freeze``  body {request_id, biz_type, metric, amount, units?, attrs?, ttl_seconds?}
  → 200 / 401 非法 / 402 余额不足 / 409 锁竞争（可带相同 request_id 退避重试）
- ``POST /billing/settle``  body {request_id, actual_amount, units?, attrs?}（多退少补，幂等）
- ``POST /billing/cancel``  body {request_id}（全额解冻，幂等）

freeze 幂等：request_id 唯一索引，重复提交返回首次结果（不重复扣款）。
"""

from __future__ import annotations

from app.config import settings
from app.logging import log
from app.schemas import UserIdentity
from app.services import httpc
from app.services.providers import BillingError


def _err_body(resp) -> str:
    try:
        data = resp.json()
        return str(data.get("error") or data)[:200]
    except Exception:
        return resp.text[:200]


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
            log.debug("identity introspection rejected: status={}", resp.status_code)
            return None
        data = resp.json()
        if not data.get("valid"):
            log.debug("identity introspection invalid token")
            return None
        return UserIdentity(user_id=int(data["user_id"]), token_id=int(data.get("token_id", 0)))

    async def freeze(self, *, raw_token: str, request_id: str, biz_type: str,
                     metric: str, amount: float, ttl_seconds: int,
                     units: float | None = None, attrs: dict | None = None) -> dict:
        body: dict = {
            "request_id": request_id,
            "biz_type": biz_type,
            "metric": metric,
            "amount": round(amount, 6),
            "ttl_seconds": ttl_seconds,
            "attrs": attrs or {},
        }
        if units is not None:
            body["units"] = units
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/billing/freeze",
                headers={"Authorization": f"Bearer {raw_token}"},
                json=body,
            )
        if resp.status_code != 200:
            # 402 余额不足 / 409 锁竞争（可重试）/ 4xx 参数错误——状态码原样上抛
            log.warning("billing freeze rejected: request_id={} status={} body={}",
                        request_id, resp.status_code, _err_body(resp))
            raise BillingError(resp.status_code, _err_body(resp))
        log.info("billing freeze ok: request_id={} amount={} metric={}",
                 request_id, round(amount, 6), metric)
        data = resp.json()
        payload = data.get("data", data)
        if isinstance(payload, dict) and payload.get("error"):
            raise BillingError(502, str(payload["error"]))
        return payload if isinstance(payload, dict) else {"ok": True}

    async def settle(self, *, raw_token: str, request_id: str, actual_amount: float,
                     units: float | None = None, attrs: dict | None = None) -> None:
        body: dict = {"request_id": request_id, "actual_amount": round(actual_amount, 6)}
        if units is not None:
            body["units"] = units
        if attrs:
            body["attrs"] = attrs
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/billing/settle",
                headers={"Authorization": f"Bearer {raw_token}"},
                json=body,
            )
        if resp.status_code != 200:
            log.warning("billing settle rejected: request_id={} status={} body={}",
                        request_id, resp.status_code, _err_body(resp))
            raise BillingError(resp.status_code, f"settle {request_id}: {_err_body(resp)}")
        log.info("billing settle ok: request_id={} actual_amount={}",
                 request_id, round(actual_amount, 6))

    async def cancel(self, *, raw_token: str, request_id: str) -> None:
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/billing/cancel",
                headers={"Authorization": f"Bearer {raw_token}"},
                json={"request_id": request_id},
            )
        if resp.status_code != 200:
            log.warning("billing cancel rejected: request_id={} status={} body={}",
                        request_id, resp.status_code, _err_body(resp))
            raise BillingError(resp.status_code, f"cancel {request_id}: {_err_body(resp)}")
        log.info("billing cancel ok: request_id={}", request_id)
