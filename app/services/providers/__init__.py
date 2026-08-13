"""微服务适配层 —— 端口（Protocol）与实现分离。

调用方只依赖这里的三个端口对象：billing / pricing / keys。
切换实现只需改配置：GW_BILLING_PROVIDER / GW_PRICING_PROVIDER / GW_KEY_PROVIDER。
新增 provider = 本目录新增实现类 + 在下方工厂注册一个名字，调用方零改动。

契约来源（实现以真实服务为准，勿凭记忆改字段）：
- billing: https://github.com/AIChatfire/newapi-billing-service
- pricing: https://github.com/AIChatfire/pricing-service
- keys:    https://github.com/AIChatfire/keypool-service
"""

from typing import Any, Protocol

from app.config import settings
from app.schemas import KeyLease, Quote, UserIdentity

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
    """pricing 服务不可用/规则求值失败（503 语义）。"""


class ModelUnavailableError(PricingError):
    """模型在 pricing 服务中 status != 0（400 语义：模型不可用）。"""


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


class PricingProvider(Protocol):
    """定价端口：按模型报价（取规则 + 沙箱求值都在实现内完成）。

    ``request`` 为完整请求体（freeze 时为用户提交体；settle 重估时为
    合并了实际用量的请求体），规则函数 ``calulate(request)`` 自由取用。"""

    async def quote(self, model: str, request: dict) -> Quote: ...


class KeyProvider(Protocol):
    """上游密钥端口：租约（含渠道全量覆盖配置）+ 用量/错误上报。"""

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


def _build_pricing() -> PricingProvider:
    if settings.pricing_provider == "model-meta":
        from app.services.providers.modelmeta_pricing import ModelMetaPricingProvider
        return ModelMetaPricingProvider()
    raise ProviderError(f"unknown pricing provider: {settings.pricing_provider}")


def _build_keys() -> KeyProvider:
    if settings.key_provider == "keypool":
        from app.services.providers.keypool import KeypoolProvider
        return KeypoolProvider()
    raise ProviderError(f"unknown key provider: {settings.key_provider}")


billing: BillingProvider = _build_billing()
pricing: PricingProvider = _build_pricing()
keys: KeyProvider = _build_keys()
