"""运行时热配置（dynconf）：只开放「运营调参」子集，安全项永不可改。

## 为什么是白名单，不是黑名单

`MUTABLE` 显式登记**可以**热改的配置项。新增配置项默认**不可**热改，要开必须
显式登记并给出类型 / 值域 / 校验器。反过来（黑名单）时，新增一个敏感项而忘了
把它加进黑名单，就直接暴露了——漏一个等于开一个后门，且没有任何机械手段能
证明「黑名单是完备的」。白名单则天然完备：不在表里就改不了。

## 读取优先级

Redis 覆盖值 > env / `.env` > 代码默认值。

读取带 5 秒进程内缓存：热路径（每次并发占用、每轮探测）不该被 Redis RTT 拖慢。
Redis 不可用时**返回空覆盖并静默回落 env**——动态配置是增强能力，绝不能成为
可用性单点（观测链路/配置链路都不得成为单点）。

## 写入纪律

- **整批校验**：任一项非法则整批拒绝，一个值都不改。半套配置比旧配置更危险
  ——运维以为改成了 A，实际生效的是 A 与 B 的混合体，排障时无从判断。
- 写后立即清本进程缓存；多副本之间最多 5s 偏差（有意接受，与读缓存同源）。

## 键规范

覆盖值存在**单个 Redis Hash** `gw:dynconf`（一次 HGETALL 拿全量），键前缀与
本项目 ``gw:`` 规范一致。规范本应集中写在 ``app/redis.py`` 的 ``K_`` 常量区，
但本模块的改动范围内没有 ``app/redis.py`` 的修改权，故键常量先就地定义；
**后续应上移到 app/redis.py 并在此处改为 import**（已登记为本模块 TODO）。
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any, Literal

from app.config import settings
from app.logging import log
from app.redis import r

#: Redis 里存放覆盖值的键（单个 Hash，一次 HGETALL 拿全量）。
#: TODO: 上移到 app/redis.py 常量区（见模块 docstring）。
_KEY = "gw:dynconf"

#: 进程内缓存 TTL（秒）。
_CACHE_TTL = 5.0

Kind = Literal["int", "float", "bool", "str", "json"]


class Spec:
    """一个可热改配置项的元数据（看板据此渲染表单并做前端校验）。"""

    __slots__ = ("group", "key", "kind", "label", "maximum", "minimum", "note",
                 "validator")

    def __init__(self, key: str, kind: Kind, label: str, group: str, *,
                 minimum: float | None = None, maximum: float | None = None,
                 note: str = "",
                 validator: Callable[[Any], Any] | None = None) -> None:
        self.key = key
        self.kind = kind
        self.label = label
        self.group = group
        self.minimum = minimum
        self.maximum = maximum
        self.note = note
        self.validator = validator

    def coerce(self, raw: Any) -> Any:
        """字符串/原始值 → 目标类型，并做区间钳制。非法值抛 ``ValueError``。

        ``json`` 型（退避阶梯）：接受 JSON 数组、逗号串、单值（管理面 JSON 体
        里可能是 list）；解析后交给 ``validator``。空串/None 也交给 validator
        ——是否允许空由具体项决定（阶梯必须非空）。
        """
        if self.kind == "json":
            parsed: Any = raw
            if isinstance(raw, str):
                text = raw.strip()
                if text == "":
                    parsed = None
                else:
                    try:
                        parsed = json.loads(text)
                    except ValueError:
                        # 非 JSON 文本（如逗号分隔的阶梯）交给 validator 解析；
                        # 无 validator 的 json 项则明确报错，不静默存字符串
                        parsed = text
            if self.validator is not None:
                return self.validator(parsed)
            if isinstance(parsed, str):
                raise ValueError(f"{self.key}: not valid JSON")
            return parsed
        if self.kind == "bool":
            if isinstance(raw, bool):
                return raw
            text = str(raw).strip().lower()
            if text in ("1", "true", "yes", "on"):
                return True
            if text in ("0", "false", "no", "off"):
                return False
            raise ValueError(f"{self.key}: expected boolean, got {raw!r}")
        if self.kind == "str":
            return str(raw)
        number = float(raw) if self.kind == "float" else int(float(raw))
        if self.minimum is not None and number < self.minimum:
            raise ValueError(f"{self.key}: below minimum {self.minimum}")
        if self.maximum is not None and number > self.maximum:
            raise ValueError(f"{self.key}: above maximum {self.maximum}")
        return number

    def to_dict(self, current: Any, overridden: bool) -> dict:
        value = current
        if self.kind == "json":
            value = json.dumps(list(current or []), ensure_ascii=False)
        return {
            "key": self.key, "kind": self.kind, "label": self.label,
            "group": self.group, "value": value, "overridden": overridden,
            "min": self.minimum, "max": self.maximum, "note": self.note,
        }


#: **白名单**：只有登记在此的配置项可以运行时热改。
#: 只开放「运营调参」——不含任何安全项（密钥/白名单/连接串永不可改）。
MUTABLE: dict[str, Spec] = {
    s.key: s for s in (
        Spec("max_concurrent_tasks", "int", "每用户并发任务上限", "并发",
             minimum=1, maximum=10000,
             note="纯并发保护，不参与任何资金判定；改小不回收已在途任务"),
        Spec("upstream_breaker_threshold", "int", "上游熔断阈值 (次)", "上游",
             minimum=1, maximum=100000,
             note="窗口内失败达到该次数即打开熔断 (窗口见 UPSTREAM_BREAKER_WINDOW_SECONDS)"),
    )
}

#: 显式登记**永不可热改**的关键项 + 原因，仅供看板展示与审计说明。
#: 代码不做 blacklist 判定（判定只看 MUTABLE 白名单），这张表是「为什么这些
#: 不能开放」的书面答案，防止下一个人以为只是忘了加。
IMMUTABLE_REASONS: dict[str, str] = {
    "database_url": "启动项/连接串：改了等于换一个服务，且动态配置自身也活不了",
    "redis_url": "启动项：动态配置本身存在 Redis，改它等于自断存储",
    "admin_token": "安全项：管理面自身的密钥，可写等于能在线关掉管理面鉴权",
    "callback_sign_secret": "密钥：用户回调 HMAC 签名密钥，泄露即可伪造回调",
    "gateway_platform": "启动项：tasks.platform 划分依据，改了会与共享表其他行混行",
    "rate_limit_per_minute": "限流基线：与并发上限不同，改它影响免费 GET 风控面，暂不开放",
    "upstream_allowlist": "安全项：上游 host 白名单，改它等于放开防 SSRF 的第二道防线",
}

_cache: dict[str, Any] = {}
_cache_at: float = 0.0


async def _load() -> dict[str, Any]:
    """读取 Redis 覆盖值（带 5s 本地缓存）。Redis 不可用返回空 dict（回落 env）。"""
    global _cache, _cache_at
    if time.monotonic() - _cache_at < _CACHE_TTL:
        return _cache
    try:
        raw = await r.hgetall(_KEY)
    except Exception:
        log.opt(exception=True).debug("dynconf read failed, falling back to env")
        _cache = {}
        _cache_at = time.monotonic()
        return _cache
    parsed: dict[str, Any] = {}
    for key, value in (raw or {}).items():
        spec = MUTABLE.get(str(key))
        if spec is None:
            continue                     # 白名单外的残留键直接忽略
        try:
            parsed[str(key)] = spec.coerce(json.loads(value))
        except Exception:
            log.warning("dynconf value invalid, ignoring: key={}", key)
    _cache = parsed
    _cache_at = time.monotonic()
    return _cache


async def get(key: str) -> Any:
    """取配置值。优先 Redis 覆盖，回落 ``settings``。未登记项直接回落 env。"""
    if key not in MUTABLE:
        return getattr(settings, key)
    overrides = await _load()
    if key in overrides:
        return overrides[key]
    return getattr(settings, key)


async def get_int(key: str) -> int:
    return int(await get(key))


async def set_many(updates: dict[str, Any]) -> dict[str, Any]:
    """批量写覆盖值。返回生效后的全量视图。

    校验失败**整批拒绝**——先全部 coerce，全部通过才落盘；任何一项非法都不写。
    """
    if not updates:
        return await snapshot()

    payload: dict[str, str] = {}
    for key, raw in updates.items():
        spec = MUTABLE.get(key)
        if spec is None:
            reason = IMMUTABLE_REASONS.get(key, "not in mutable allowlist")
            raise ValueError(f"{key} cannot be changed at runtime ({reason})")
        payload[key] = json.dumps(spec.coerce(raw), ensure_ascii=False)

    await r.hset(_KEY, mapping=payload)
    _invalidate()
    log.info("dynconf updated: keys={}", sorted(payload))
    return await snapshot()


async def reset(keys: list[str] | None = None) -> dict[str, Any]:
    """删除覆盖值，回落 env。``keys=None`` 清空全部覆盖。"""
    if keys:
        unknown = [k for k in keys if k not in MUTABLE]
        if unknown:
            raise ValueError(f"unknown keys: {unknown}")
        await r.hdel(_KEY, *keys)
        log.info("dynconf reset: keys={}", sorted(keys))
    else:
        await r.delete(_KEY)
        log.info("dynconf reset: all")
    _invalidate()
    return await snapshot()


async def snapshot() -> dict[str, Any]:
    """看板用的全量视图：可改项（含当前值与是否被覆盖）+ 只读项说明。"""
    overrides = await _load()
    groups: dict[str, list[dict]] = {}
    for key, spec in MUTABLE.items():
        current = overrides.get(key, getattr(settings, key))
        groups.setdefault(spec.group, []).append(spec.to_dict(current, key in overrides))
    return {
        "groups": [{"name": name, "items": items} for name, items in groups.items()],
        "override_count": len(overrides),
        "immutable": [{"key": k, "reason": v} for k, v in IMMUTABLE_REASONS.items()],
    }


def _invalidate() -> None:
    """写后立即失效本地缓存（只清本进程；多副本最多再用 5s 旧值，有意取舍）。"""
    global _cache, _cache_at
    _cache = {}
    _cache_at = 0.0
