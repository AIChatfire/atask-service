"""微服务适配层 —— 端口（Protocol）与实现分离。

调用方只依赖这里的三个端口对象：billing / pricing / keys。
切换实现只需改配置：GW_BILLING_PROVIDER / GW_PRICING_PROVIDER / GW_KEY_PROVIDER。
新增 provider = 本目录新增实现类 + 在下方工厂注册一个名字，调用方零改动。
"""

from typing import Protocol

from app.config import settings
from app.schemas import KeyLease, Quote, UserIdentity


# ---------------- 异常 ----------------

class ProviderError(Exception):
    pass


class BillingError(ProviderError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class PricingError(ProviderError):
    pass


class KeyLeaseError(ProviderError):
    pass


# ---------------- 端口定义 ----------------

class BillingProvider(Protocol):
    """计费端口：身份内省 + 冻结/结算/取消（newapi-billing-service 语义）"""

    async def inspect(self, raw_token: str) -> UserIdentity | None: ...
    async def freeze(self, *, raw_token: str, request_id: str, biz_type: str,
                     metric: str, amount: float, ttl_seconds: int, attrs: dict) -> dict: ...
    async def settle(self, request_id: str, actual_amount: float) -> None: ...
    async def cancel(self, request_id: str) -> None: ...


class PricingProvider(Protocol):
    """定价端口：按模型报价（取规则 + 沙箱求值都在实现内完成）"""

    async def quote(self, model: str, request: dict) -> Quote: ...


class KeyProvider(Protocol):
    """上游密钥端口：租约 + 用量/错误上报"""

    async def lease(self, biz: str, model: str = "", key_id: int | None = None,
                    group: str = "default") -> KeyLease: ...
    async def report(self, key: KeyLease, ok: bool, status_code: int = 0,
                     latency_ms: int = 0, error: str = "") -> None: ...


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
