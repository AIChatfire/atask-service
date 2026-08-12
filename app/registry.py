"""biz 注册表：环境变量为唯一事实源 + 进程内 TTL 缓存（决策 B，原 SPEC §3.2）。

事实源（优先级从高到低）：

1. ``BIZ_CONFIGS_FILE``：JSON 文件路径（compose volume 挂载）——设置后
   registry 后台 loop 按 mtime watch，变更即重载（热更新）；
2. ``BIZ_CONFIGS``：内联单行 JSON 数组——改配置 = 改 .env + 重启 compose
   服务，多副本一致性由同一 env 注入保证（不再有 DB 回源与 pub-sub 广播）。

缓存：L1 进程内 ``dict[biz, BizConfig]``，TTL 30s（可配）扛高并发读；
``reload()`` 整体替换快照并清空 L1。

读路径：``registry.get(biz, session=None)``（路由/任务管理器共用；session
形参保留仅为调用方兼容，已不再使用）。未注册或 disabled 抛 404。

配置 JSON 字段与 :class:`BizConfig` 对齐（缺省值见 ``_DEFAULTS``）::

    [{"biz": "kling", "adapter": "kling",
      "upstream_base_url": "https://api-beijing.klingai.com",
      "auth_type": "aksk_jwt", "auth_secret_ref": "UPSTREAM_SECRET_KLING",
      "native_prefixes": ["v1/videos"], "enabled": true,
      "billing_keys": {"biz_type": "video_gen", "metric": "call"},
      "default_freeze_amount_usd": "1.000000",
      "rate_limit": {"user_rpm": 60, "biz_rpm": 600, "upstream_concurrency": 32},
      "newapi_channel_id": null, "display_name": "Kling 视频"}]
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import logfire

from app.config import settings
from app.errors import not_found


@dataclass
class BizConfig:
    """biz 配置（与 BIZ_CONFIGS/文件中的 JSON 对象一一对应，SPEC §3.2 字段契约）。

    ``default_freeze_amount_usd`` 用字符串承载（Decimal JSON 序列化安全，
    金额纪律：字符串序列化避免浮点误差，§5.1）。
    """

    biz: str
    adapter: str                             # 'kling' / 'seedance'（适配器注册名）
    upstream_base_url: str
    auth_type: str                           # 'aksk_jwt' | 'bearer_key'
    auth_secret_ref: str                     # 密钥引用名（明文走 Secret/ENV 注入）
    native_prefixes: list[str]
    enabled: bool
    billing_keys: dict[str, Any]             # {biz_type, metric, billing_mode, charge_on_get,
                                             #  verify_only_precheck, allow_user_direct_callback...}
    default_freeze_amount_usd: str | None
    rate_limit: dict[str, Any]               # {user_rpm, biz_rpm, upstream_concurrency}
    newapi_channel_id: int | None            # → tasks.channel_id（对账口径一致，§4.2）
    version: int
    display_name: str = ""
    loaded_at: float = field(default_factory=time.monotonic)

    @property
    def billing_mode(self) -> str:
        """'prepaid'（预扣+结算，默认）| 'postpaid'（charge 后扣，透传默认）。"""
        return str(self.billing_keys.get("billing_mode", "prepaid"))


# JSON 配置可省略字段的缺省值（其余为必填）
_DEFAULTS: dict[str, Any] = {
    "native_prefixes": [],
    "enabled": True,
    "billing_keys": {},
    "default_freeze_amount_usd": None,
    "rate_limit": {},
    "newapi_channel_id": None,
    "version": 1,
    "display_name": "",
}


def _row_to_config(rec: dict[str, Any]) -> BizConfig:
    """JSON 对象 → BizConfig；类型归一与校验（非法配置 ValueError）。"""
    d = {**_DEFAULTS, **rec}
    for key in ("native_prefixes", "billing_keys", "rate_limit"):
        v = d.get(key)
        if isinstance(v, str):
            d[key] = json.loads(v)
    amount = d.get("default_freeze_amount_usd")
    if amount is not None and not isinstance(amount, str):
        amount = str(Decimal(str(amount)))
    if not d.get("biz") or not d.get("adapter"):
        raise ValueError(f"biz config missing required keys: {rec!r}")
    return BizConfig(
        biz=str(d["biz"]),
        display_name=str(d.get("display_name") or ""),
        adapter=str(d["adapter"]),
        upstream_base_url=str(d.get("upstream_base_url") or ""),
        auth_type=str(d.get("auth_type") or ""),
        auth_secret_ref=str(d.get("auth_secret_ref") or ""),
        native_prefixes=list(d.get("native_prefixes") or []),
        enabled=bool(d["enabled"]),
        billing_keys=dict(d.get("billing_keys") or {}),
        default_freeze_amount_usd=amount,
        rate_limit=dict(d.get("rate_limit") or {}),
        newapi_channel_id=d.get("newapi_channel_id"),
        version=int(d.get("version") or 1),
        loaded_at=time.monotonic(),
    )


def parse_biz_configs(text: str) -> dict[str, BizConfig]:
    """解析 biz 配置 JSON（数组或单个对象）→ ``{biz: BizConfig}``。"""
    raw = json.loads(text)
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        raise ValueError("biz configs must be a JSON array")
    configs: dict[str, BizConfig] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError(f"biz config entry must be an object: {item!r}")
        cfg = _row_to_config(item)
        configs[cfg.biz] = cfg
    return configs


class BizRegistry:
    """配置快照 + 进程内 L1 缓存 + 文件 mtime watch。进程内单例（模块底部）。"""

    def __init__(self, l1_ttl: float | None = None) -> None:
        self._configs: dict[str, BizConfig] = {}
        self._l1: dict[str, BizConfig] = {}
        self._l1_ttl = l1_ttl if l1_ttl is not None else settings.biz_l1_ttl_seconds
        self._file_mtime: float | None = None

    async def get(self, biz: str, session: Any = None) -> BizConfig:
        """取 biz 配置；未注册或 disabled 抛 404（OpenAI 风格 error）。

        ``session`` 形参仅为历史调用方签名兼容（DB 回源已删除），不使用。
        """
        del session
        cfg = self._l1.get(biz)
        if cfg is not None and time.monotonic() - cfg.loaded_at < self._l1_ttl:
            return cfg
        cfg = self._configs.get(biz)
        if cfg is None or not cfg.enabled:
            raise not_found(f"unknown biz: {biz}")
        self._l1[biz] = cfg
        return cfg

    def known_biz(self) -> list[str]:
        """当前快照中的 biz 清单（观测/冒烟用）。"""
        return sorted(self._configs)

    # ---------- 加载与热更新 ----------

    def reload(self) -> int:
        """从 settings 重载配置快照（FILE 优先于内联 JSON），清空 L1。

        解析失败：保持旧快照 + logfire.error（绝不因坏配置清空注册表）。
        返回生效 biz 数。
        """
        source = "inline BIZ_CONFIGS"
        text: str | None = None
        mtime: float | None = None
        if settings.biz_configs_file:
            source = f"file {settings.biz_configs_file}"
            try:
                mtime = os.path.getmtime(settings.biz_configs_file)
                with open(settings.biz_configs_file, encoding="utf-8") as fh:
                    text = fh.read()
            except OSError as exc:
                logfire.error("biz configs file unreadable, keeping previous snapshot",
                              file=settings.biz_configs_file, error=str(exc))
                return len(self._configs)
        elif settings.biz_configs:
            text = settings.biz_configs
        if text is None or not text.strip():
            if self._configs:
                logfire.warning("biz configs emptied in settings, keeping previous snapshot")
            else:
                logfire.warning("no biz configs loaded (BIZ_CONFIGS/BIZ_CONFIGS_FILE unset)")
            return len(self._configs)
        try:
            configs = parse_biz_configs(text)
        except Exception as exc:
            logfire.error("biz configs parse failed, keeping previous snapshot",
                          source=source, error=str(exc))
            return len(self._configs)
        self._configs = configs
        self._l1.clear()
        self._file_mtime = mtime
        logfire.info("biz configs loaded", source=source, biz=sorted(configs))
        return len(configs)

    def _file_changed(self) -> bool:
        if not settings.biz_configs_file:
            return False
        try:
            mtime = os.path.getmtime(settings.biz_configs_file)
        except OSError:
            return False
        return self._file_mtime is None or mtime != self._file_mtime

    async def invalidate_loop(self) -> None:
        """热更新 watch（原 pub-sub 失效 loop 骨架）：``BIZ_CONFIGS_FILE``
        设置时按 mtime 轮询重载；未设置时空转（改配置走滚动重启路径）。"""
        logfire.info("biz registry watch loop started",
                     file=settings.biz_configs_file)
        while True:
            await asyncio.sleep(settings.biz_watch_interval_seconds)
            if self._file_changed():
                logfire.info("biz configs file changed, reloading",
                             file=settings.biz_configs_file)
                self.reload()

    def invalidate_local(self, biz: str) -> None:
        """清单个 biz 的 L1（测试与运维入口保留）。"""
        self._l1.pop(biz, None)


registry = BizRegistry()
