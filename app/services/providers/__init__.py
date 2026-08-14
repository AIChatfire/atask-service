"""微服务适配层 —— 端口（Protocol）与实现分离。

调用方只依赖这里的两个端口对象：billing / keys。
切换实现只需改配置：GW_BILLING_PROVIDER / GW_KEY_PROVIDER。
新增 provider = 本目录新增实现类 + 在下方工厂注册一个名字，调用方零改动。

计费规则不走独立微服务：唯一事实源是 keypool 渠道元数据（gateway 块
``billing.rule``），随租约下发后由 ``app.services.pricing`` 本地沙箱求值。

契约来源（实现以真实服务为准，勿凭记忆改字段）：
- billing: https://github.com/AIChatfire/newapi-billing-service
- keys:    https://github.com/AIChatfire/keypool-service
"""

from typing import Any, Protocol

from app.config import settings
from app.schemas import KeyLease, UserIdentity

# ---------------- 异常 ----------------

class ProviderError(Exception):
    pass


class BillingError(ProviderError):
    """billing 服务错误。``status`` 为 HTTP 状态码（402 余额不足 / 409 锁竞争 /
    4xx 参数或状态错误 / 5xx 服务故障）。"""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message

    @property
    def retryable(self) -> bool:
        """5xx/网络类（599）可重试；4xx 为确定性失败（重试无意义）。"""
        return self.status >= 500


class PricingError(ProviderError):
    """渠道计费规则求值失败（500 语义：配置错误，绝不静默按 0 计费）。"""


class KeyLeaseError(ProviderError):
    """keypool 无可用 key / 服务故障（503 语义）。"""


# ---------------- 端口定义 ----------------

class BillingProvider(Protocol):
    """计费端口：身份内省 + 冻结/结算/取消（newapi-billing-service 语义）。

    所有操作的用户身份由**终端用户令牌**（sk-...）解析；settle/cancel 同样
    携带用户令牌（只能操作自己的冻结单，跨用户 403）。"""

    async def inspect(self, raw_token: str) -> UserIdentity | None: ...
    async def freeze(self, *, raw_token: str, request_id: str, biz_type: str,
                     metric: str, amount: float, ttl_seconds: int,
                     units: float | None = None, attrs: dict | None = None) -> dict: ...
    async def settle(self, *, raw_token: str, request_id: str, actual_amount: float,
                     units: float | None = None, attrs: dict | None = None) -> None: ...
    async def cancel(self, *, raw_token: str, request_id: str) -> None: ...


class KeyProvider(Protocol):
    """上游密钥端口：租约（含渠道全量覆盖配置与 billing 计费规则块）+ 用量/错误上报。"""

    async def lease(self, biz: str, model: str = "", key_id: int | None = None,
                    group: str = "") -> KeyLease: ...
    async def report(self, key: KeyLease, ok: bool, status_code: int = 0,
                     latency_ms: int = 0, error: str = "",
                     usage: dict[str, Any] | None = None) -> None: ...


# ---------------- 工厂（按配置装配） ----------------

def _build_billing() -> BillingProvider:
    if settings.billing_provider == "newapi-billing":
        from app.services.providers.billing_newapi import NewapiBillingProvider
        return NewapiBillingProvider()
    raise ProviderError(f"unknown billing provider: {settings.billing_provider}")


def _build_keys() -> KeyProvider:
    if settings.key_provider == "keypool":
        from app.services.providers.keypool import KeypoolProvider
        return KeypoolProvider()
    raise ProviderError(f"unknown key provider: {settings.key_provider}")


billing: BillingProvider = _build_billing()
keys: KeyProvider = _build_keys()
