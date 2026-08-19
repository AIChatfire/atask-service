"""上游错误分类表：判定失败归属层级，决定止损动作（多项方案的共同前置）。

五级语义（误判原则：**拿不准一律按任务级**——错杀账户级/key 级代价远
大于错放，账户级只认渠道显式配置的名单）：

- ``TASK_LEVEL`` 任务级失败：任务本身被拒（4xx 业务错误/信封业务错）
  → FAILURE + cancel（现状纪律）；
- ``KEY_LEVEL`` key 级失效：渠道内某个 key 失效（默认 401/403）
  → ``providers.keys.report(ok=False)`` 驱动 keypool 禁用，下轮换 key
  （探测钉回 channel_id 不变，keypool 返回同渠道健康 key）；
- ``ACCOUNT_LEVEL`` 账户级故障：欠费/封禁——**只在渠道显式配置名单内
  才判定**（状态码名单 + body 子串），HELD 挂起（[6]）的前置；
- ``RATE_LIMITED`` 限流：429（+ Retry-After）→ 不上报 keypool，与账户级同走
  HELD 挂起 + 金丝雀重提交（固定 5m 退避、1h 判死，见 held.py）；
- ``AMBIGUOUS`` 模糊失败：超时/5xx/连接中断 → 重试探测；submit 绝不重试。

渠道覆盖位（gateway 块 ``error_classify``，缺省走内置默认表）::

    "error_classify": {
        "key_level": [401],                          # HTTP 状态码名单
        "account_level": [403],                      # 如 403=欠费 的渠道
        "account_level_messages": ["insufficient balance", "overdue"],
        "task_level": [400, 404, 422]
    }
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # 避免循环 import（upstream ← errclass 只在运行期需要类型）
    from app.schemas import RouteConfig
    from app.services.upstream import UpstreamError

TASK_LEVEL = "task_level"
KEY_LEVEL = "key_level"
ACCOUNT_LEVEL = "account_level"
RATE_LIMITED = "rate_limited"
AMBIGUOUS = "ambiguous"

#: 内置默认表（HTTP 状态码 → 层级）；账户级故意留空——只认渠道显式配置
_DEFAULT_STATUS: dict[int, str] = {
    401: KEY_LEVEL,
    403: KEY_LEVEL,
    429: RATE_LIMITED,
}


def _codes(value: Any) -> set[int]:
    """渠道名单容忍 int/str 混写，非法项静默丢弃。"""
    out: set[int] = set()
    for item in value or []:
        try:
            out.add(int(item))
        except (TypeError, ValueError):
            continue
    return out


def classify(route: RouteConfig | None, exc: UpstreamError) -> str:
    """上游错误 → 失败层级。渠道 error_classify 名单优先于内置默认表。"""
    if exc.envelope:
        return TASK_LEVEL                    # 信封业务错（HTTP 200 + 业务码不匹配）
    status = exc.status
    if status >= 500:
        return AMBIGUOUS                     # 含 599（网络/超时封装）：重试探测
    cfg = (route.error_classify if route else None) or {}
    # 渠道显式名单（账户级 > key 级 > 限流 > 任务级，先配置先生效）
    for category in (ACCOUNT_LEVEL, KEY_LEVEL, RATE_LIMITED, TASK_LEVEL):
        if status in _codes(cfg.get(category)):
            return category
    # 账户级 body 子串（欠费/封禁措辞因厂商而异，必须显式配置才生效）
    body = (exc.body or "").lower()
    for needle in cfg.get("account_level_messages") or []:
        if str(needle).lower() in body:
            return ACCOUNT_LEVEL
    if status in _DEFAULT_STATUS:
        return _DEFAULT_STATUS[status]
    if 400 <= status < 500:
        return TASK_LEVEL                    # 拿不准的 4xx 一律按任务级
    return AMBIGUOUS
