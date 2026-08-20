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
        """5xx/网络类（599）可重试；409 锁竞争是瞬时状态（billing 契约明示
        可带相同 request_id 退避重试），同 5xx 处理；其余 4xx 为确定性失败
        （冻结已过期/已结算/跨用户，重试无意义）。"""
        return self.status >= 500 or self.status == 409


class PricingError(ProviderError):
    """渠道计费规则求值失败（500 语义：配置错误，绝不静默按 0 计费）。"""


#: keypool 错误包络 code（契约见 keypool-service README）
KP_NO_KEY = 40001            # 503 无可用 key（data.retry_after_ms 给建议退避）
KP_NO_CHANNEL = 40002        # 404 渠道不存在
KP_BAD_PARAM = 40010         # 400 参数错误（含 key_index 越界——永久性，重试无意义）


class KeyLeaseError(ProviderError):
    """keypool 无可用 key / 服务故障（503 语义）。

    ``retry_after_ms``：keypool 40001（无可用 key）给出的建议退避——探测重投
    与 503 响应的 ``Retry-After`` 头都以此为 hint（缺省 None = 无建议）。
    ``code``：keypool 错误包络 code（0 = 未解析出，如纯网络故障）。精确直达
    （``channel_id + key_index``）的失败分流依赖它：40010 越界为永久性错误
    （该 key 已不在渠道里），40001 为该 key 被禁用（渠道内换 key 仍可能有救），
    两者都触发"降级到渠道直达"；40002 渠道不存在则无从降级。
    """

    def __init__(self, message: str, *, retry_after_ms: int | None = None,
                 code: int = 0):
        super().__init__(message)
        self.retry_after_ms = retry_after_ms
        self.code = code

    @property
    def key_level(self) -> bool:
        """失败只针对"这一把 key"（渠道仍在）——精确直达可降级为渠道直达。"""
        return self.code in (KP_NO_KEY, KP_BAD_PARAM)


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
    async def renew(self, *, raw_token: str, request_id: str,
                    ttl_seconds: int) -> dict:
        """只推 expires_at、不动钱：冻结续期（HELD/长任务防 freeze 过期）。
        400 = 冻结已终态/超总量上限（非重试，响应体带当前 status）；409/5xx 可重试。"""
        ...


class KeyProvider(Protocol):
    """上游密钥端口：租约（含渠道全量覆盖配置与 billing 计费规则块）+ 用量/错误上报。

    三种定位形态（keypool select 契约）：
    ``group+model`` 加权选渠道 → ``key_id``（channel_id）渠道直达 →
    ``key_id + key_index`` **单 key 精确直达**（mode=direct，跳过调度算法与
    Redis）。精确直达是「同渠道多上游账号」场景的正解：任务查询/取消必须用
    创建时那把 key，否则 B 账号的 key 查不到 A 账号的任务。
    """

    async def lease(self, biz: str, model: str = "", key_id: int | None = None,
                    group: str = "", key_index: int | None = None) -> KeyLease: ...
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
