"""动态路由：上游配置唯一事实源 = **keypool 渠道元数据**，网关零路由文件。

核心约定（傻瓜式接入）：

- **统一分组**：全部渠道挂在 keypool 同一个 group（默认 ``keypool``，
  ``GW_KEY_GROUP`` 可配）下集中维护；选渠道 = ``select(group, model)``，
  model 决定渠道（abilities 表映射），与 URL 路径解耦。
- **biz 从渠道取**：``setting.gateway.biz`` 显式指定 → 渠道 ``name`` →
  URL 段兜底；URL ``/{biz}/`` 只是入口标签，内部记录（tasks.data、回调
  路径、计费维度）一律用渠道给出的权威 biz。
- **提取/推进配置也在渠道上**：渠道元数据里放一块网关配置——
  ``header_override.upstream`` / ``setting.gateway`` 两处等价任选
  （优先级从高到低；只支持当前同构配置位，不再兼容旧版
  ``other.gateway``），keypool 随租约下发（``include_channel=true``），
  网关据此提交/探测/提取结果。
- 进程内 TTL 缓存：每次租约解析后回填（remember）；callback 等无租约上下
  文先读缓存，未命中再按 channel_id 直达租约重建。
"""

from __future__ import annotations

import time
from typing import Any

from app.logging import log
from app.schemas import KeyLease, RouteConfig
from app.services.providers import KeyLeaseError, keys

#: 渠道 setting.gateway 缺省值——大多数 OpenAI-task 风格上游只需配 4 个路径
_GATEWAY_DEFAULTS: dict[str, Any] = {
    "submit_path": "",
    "probe_path": "",
    "task_id_path": "task_id",
    "probe_task_id_path": "",
    "status_path": "status",
    "result_path": "",
    "result_url_template": "",
    "error_path": "",
    "actual_amount_path": "",
    "settle_usage_map": {},
    "ok_check": None,
    "auth_type": "bearer",
    "timeout_sec": 60.0,
    "default_params": {},
    "body_allowlist": None,
    "supports_callback": False,
    "callback_param": "callback_url",
    "callback_secret": None,
    "callback_sig_header": "X-Signature",
    "failed_billing": "absorb",
    "cancel_path": "",
    "client_request_id_param": "",
    "pricing_biz_type": "",
    "billing_rule": "",
    "billing_type": "default",
    "discount_rate": 1.0,
    "status_map": {},
    "error_classify": {},
}


def route_from_channel(biz_hint: str, channel: dict[str, Any] | None) -> RouteConfig:
    """keypool 渠道元数据 → RouteConfig。

    网关配置块来源（优先级从高到低，两处等价任选其一；仅支持同构配置位，
    不再兼容旧版 ``other.gateway``）：

    1. ``channel.header_override.upstream``（嵌套对象；装配请求头时会剥离，
       不会作为 HTTP 头透出，见 providers/keypool.py 与 upstream.auth_headers）；
    2. ``channel.setting.gateway``。

    **biz 从渠道取**：配置块 ``biz`` 显式指定 → 渠道 ``name`` → ``biz_hint``
    （URL 段）兜底。``channel.base_url`` 作为 RouteConfig.upstream_base_url
    兜底（租约 ``base_url`` 字段优先，见 upstream.client_for）。
    """
    channel = channel or {}
    setting = channel.get("setting") or {}
    header_override = channel.get("header_override") or {}
    gw: dict[str, Any] = {}
    for candidate in (
        header_override.get("upstream"),
        setting.get("gateway"),
    ):
        if isinstance(candidate, dict) and candidate:
            gw = dict(candidate)
            break
    # 计费规则块：billing.rule / billing.type / billing.discount_rate（discountRate）
    # 摊平为 RouteConfig 字段；billing 为字符串时直接视为规则本体
    billing = gw.pop("billing", None)
    if isinstance(billing, str):
        gw.setdefault("billing_rule", billing)
    elif isinstance(billing, dict):
        gw.setdefault("billing_rule", str(billing.get("rule") or ""))
        gw.setdefault("billing_type", str(billing.get("type") or "default"))
        gw.setdefault("discount_rate",
                      float(billing.get("discount_rate", billing.get("discountRate", 1.0)) or 1.0))
    merged = {**_GATEWAY_DEFAULTS, **gw}
    # biz 从渠道取：gateway.biz → 渠道 name → URL 段兜底（pop 防与 RouteConfig.biz 冲突）
    biz = str(merged.pop("biz", "") or channel.get("name") or biz_hint)
    return RouteConfig(
        biz=biz,
        enabled=True,
        display_name=str(channel.get("name") or ""),
        channel_id=int(channel.get("id") or 0),
        upstream_base_url=str(channel.get("base_url") or ""),
        **merged,
    )


def route_from_lease(biz: str, lease: KeyLease) -> RouteConfig:
    """租约 → RouteConfig（租约已含渠道元数据时零额外 I/O）。"""
    return route_from_channel(biz, lease.channel)


class RouteRegistry:
    """RouteConfig 进程内缓存（TTL 秒）；事实源永远在 keypool 渠道。"""

    def __init__(self, ttl: float = 60.0) -> None:
        self._ttl = ttl
        self._cache: dict[str, tuple[RouteConfig, float]] = {}

    def remember(self, route: RouteConfig) -> RouteConfig:
        """租约解析后回填缓存（preflight/poller/proxy 每次拿到租约都调用）。"""
        self._cache[route.biz] = (route, time.monotonic())
        return route

    def get_cached(self, biz: str) -> RouteConfig | None:
        """同步读缓存（finalize 等已有租约的上下文兜底用）。"""
        hit = self._cache.get(biz)
        if hit and time.monotonic() - hit[1] < self._ttl:
            return hit[0]
        return None

    def channel_id_of(self, biz: str) -> int:
        """缓存里该 biz 最近使用的 channel_id（0 = 未知；免费透传钉渠道用）。"""
        cached = self.get_cached(biz)
        return cached.channel_id if cached else 0

    async def get(self, biz: str, *, model: str = "",
                  channel_id: int | None = None) -> RouteConfig | None:
        """取路由：缓存命中直接返回；否则向 keypool 租约重建并回填。

        keypool 无可用 key / 服务故障 → None（调用方按 404/503 语义处理）。
        """
        cached = self.get_cached(biz)
        if cached is not None:
            return cached
        try:
            lease = await keys.lease(biz, model=model, key_id=channel_id)
        except KeyLeaseError as exc:
            log.warning("route resolve via keypool failed: biz={} {}", biz, exc)
            return None
        return self.remember(route_from_lease(biz, lease))


registry = RouteRegistry()
