"""计费服务客户端（SPEC §3.11.1；契约事实：简报 A §3，架构 §5.1/§13.3）。

对接既有 newapi-billing-service（Go），统一前缀 ``/api/v1/billing``：

- 鉴权：``Authorization: Bearer {user_sk}``——**透传终端用户 sk- 令牌**，
  user_id 一律由服务端从令牌解析（防 IDOR），绝不改用网关服务账号代扣；
- 幂等：``request_id`` 服务端唯一索引，重复调用返回首次结果；任务形态
  分片语义 ``{task_id}:{seq}``（SPEC §4.3），透传 charge 用 ``pt:`` 前缀；
- 金额：网关内部全程 ``Decimal`` 美元（≤6 位小数），**字符串序列化**；
  服务端按 ``quota = round(usd × 500000)``（half-up）转换；
- 错误：402 → :class:`InsufficientBalance`；409（同用户锁等待预算用尽，
  响应带 ``retry_after_ms``）→ :class:`BillingLockBusy`，带相同 request_id
  退避重试安全（tenacity ≤5 次，等待策略优先消费 retry_after_ms）；
  5xx/超时向上抛（调用方入 outbox，SPEC §3.11.4）；**绝不重试其他 4xx**；
- freeze 必传 ``ttl_seconds``（服务端 >86400 截断；billing sweeper 每 60s
  扫过期单自动解冻——webhook 永远不来也不锁死资金，最后兜底）。
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import logfire
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
)
from tenacity.wait import wait_base

from app.http_clients import billing_client

QUOTA_PER_UNIT = 500_000  # quota = USD × 500000（对齐 new-api，简报 A §2；勿改）

MAX_FREEZE_TTL_SECONDS = 86_400  # 计费服务端 freeze ttl 硬上限（超则截断）

# 409 重试纪律：同 request_id 退避重试安全，最多 5 次（SPEC §3.11.1）
_RETRY_MAX_ATTEMPTS = 5
_DEFAULT_RETRY_AFTER_MS = 500


class InsufficientBalance(Exception):
    """计费服务 402（余额不足）→ W1 转 HTTP 402；透传 charge 402 走欠费三连（§5.6）。"""


class BillingLockBusy(Exception):
    """计费服务 409：同用户锁等待预算用尽。

    响应携带 ``retry_after_ms``（服务端给出的精确锁等待预算）；带相同
    request_id 退避重试安全（服务端幂等）。
    """

    def __init__(self, retry_after_ms: int) -> None:
        self.retry_after_ms = retry_after_ms
        super().__init__(f"billing lock busy, retry after {retry_after_ms}ms")


class wait_retry_after_or_backoff(wait_base):
    """tenacity 等待策略：优先消费 409 响应中的 ``retry_after_ms``
    （比盲退避更准）；缺省退化为指数退避（0.2s 起、封顶 3s）。"""

    def __call__(self, retry_state: Any) -> float:
        if retry_state.outcome is not None:
            exc = retry_state.outcome.exception()
            if isinstance(exc, BillingLockBusy):
                return max(exc.retry_after_ms / 1000.0, 0.05)
        return min(0.2 * 2 ** (retry_state.attempt_number - 1), 3.0)


class BillingServiceClient:
    """freeze/settle/cancel/charge/balance（资金操作全部幂等安全重放）。"""

    def __init__(self) -> None:
        # 应用级 httpx 单例（超时四元组 connect3/read10/write5/pool2，§8.1）；
        # 禁止每请求新建 AsyncClient。
        self._client = billing_client()

    @retry(
        retry=retry_if_exception_type(BillingLockBusy),
        wait=wait_retry_after_or_backoff(),
        stop=stop_after_attempt(_RETRY_MAX_ATTEMPTS),
        reraise=True,
    )
    async def _post(self, path: str, body: dict[str, Any], user_sk: str) -> dict[str, Any]:
        resp = await self._client.post(
            f"/api/v1/billing{path}",
            json=body,
            headers={"Authorization": f"Bearer {user_sk}"},  # 透传用户 sk-，§5.1
        )
        if resp.status_code == 402:
            raise InsufficientBalance()
        if resp.status_code == 409:
            retry_after_ms = _DEFAULT_RETRY_AFTER_MS
            try:
                retry_after_ms = int(resp.json().get("retry_after_ms", _DEFAULT_RETRY_AFTER_MS))
            except Exception:  # 坏报文也要退避重试，按缺省预算
                logfire.warning("billing 409 without parseable retry_after_ms")
            raise BillingLockBusy(retry_after_ms)
        resp.raise_for_status()  # 5xx/其他 4xx 向上抛（5xx 由调用方入 outbox）
        return resp.json()["data"]

    async def freeze(
        self,
        *,
        request_id: str,
        biz_type: str,
        metric: str,
        amount_usd: Decimal,
        ttl_seconds: int,
        user_sk: str,
        attrs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """预冻结（幂等）。``ttl_seconds`` 必传且 ≤86400（sweeper 兜底纪律）。"""
        return await self._post(
            "/freeze",
            {
                "request_id": request_id,
                "biz_type": biz_type,
                "metric": metric,
                "amount": str(amount_usd),
                "ttl_seconds": min(ttl_seconds, MAX_FREEZE_TTL_SECONDS),
                "attrs": attrs or {},
            },
            user_sk,
        )

    async def settle(
        self,
        *,
        request_id: str,
        actual_usd: Decimal,
        user_sk: str,
        attrs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """结算多退少补（幂等）；返回含 settled/refunded/extra_charged/shortfall。"""
        return await self._post(
            "/settle",
            {
                "request_id": request_id,
                "actual_amount": str(actual_usd),
                "attrs": attrs or {},
            },
            user_sk,
        )

    async def cancel(self, *, request_id: str, user_sk: str) -> dict[str, Any]:
        """取消冻结、全额解冻（幂等；已过期/已解冻分片重放无副作用）。"""
        return await self._post("/cancel", {"request_id": request_id}, user_sk)

    async def charge(
        self,
        *,
        request_id: str,
        biz_type: str,
        metric: str,
        amount_usd: Decimal,
        user_sk: str,
        verify_only: bool = False,
    ) -> dict[str, Any]:
        """同步单次扣费（幂等）；``verify_only=True`` 只预判余额不动钱。"""
        return await self._post(
            "/charge",
            {
                "request_id": request_id,
                "biz_type": biz_type,
                "metric": metric,
                "amount": str(amount_usd),
                "verify_only": verify_only,
            },
            user_sk,
        )

    async def balance(self, *, user_sk: str) -> dict[str, Any]:
        """查询余额/冻结额（``{user_id, balance, frozen, *_usd}``；对账用）。"""
        resp = await self._client.get(
            "/api/v1/billing/balance",
            headers={"Authorization": f"Bearer {user_sk}"},
        )
        resp.raise_for_status()
        return resp.json()["data"]

    async def get_freeze(self, *, request_id: str, user_sk: str) -> dict[str, Any]:
        """查冻结单状态/金额/过期时间（对账轮询既定用法，架构 §5.5 第 1 项）。"""
        resp = await self._client.get(
            f"/api/v1/billing/freeze/{request_id}",
            headers={"Authorization": f"Bearer {user_sk}"},
        )
        resp.raise_for_status()
        return resp.json()["data"]

    async def get_billing_logs(
        self, *, request_id: str, user_sk: str
    ) -> list[dict[str, Any]]:
        """读计费服务资金流水（``GET /api/v1/billing/logs?request_id=``）。

        零自有表（决策 A-5）后网关不再落 ``gateway_billing_audit`` 表——计费
        审计以计费服务真实资金流水为准，对账三方核对读此接口。
        """
        resp = await self._client.get(
            "/api/v1/billing/logs",
            params={"request_id": request_id},
            headers={"Authorization": f"Bearer {user_sk}"},
        )
        resp.raise_for_status()
        return list(resp.json()["data"])
