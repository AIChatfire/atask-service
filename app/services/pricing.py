"""计费规则求值：规则唯一事实源 = **keypool 渠道元数据**。

渠道 gateway 配置块（``header_override.upstream`` / ``setting.gateway``，
两处等价）携带 ``billing`` 子块::

    "billing": {
        "rule": "def calulate(request):\\n    return float(request.get('duration') or 5) * 0.026",
        "type": "second",
        "discount_rate": 1.0
    }

随 keypool 租约（include_channel=true）下发 → ``registry.route_from_channel``
摊平为 ``RouteConfig.billing_rule / billing_type / discount_rate``，网关在
本地沙箱求值，**零额外远程调用**（preflight 报价与 settle 重估同一份规则）。

- ``rule`` 是完整 Python 函数定义（asteval 沙箱执行），约定函数名 ``calulate``
  （历史拼写，兼容 calculate/calc/compute/price）；入参为完整请求体，返回值
  为**计费金额（USD）**；纯表达式形态（如 ``duration * 0.026``）自动兜底。
- 实际金额 = 规则返回值 × ``discount_rate``（缺省 1，折扣必乘）。
- 渠道未配 ``billing`` → 报价 0（免费渠道，不产生冻结）。
"""

from __future__ import annotations

import asteval

from app.schemas import Quote, RouteConfig
from app.services.providers import PricingError

_FN_NAMES = ("calulate", "calculate", "calc", "compute", "price")


def eval_rule(logic: str, request: dict) -> float:
    """asteval 沙箱执行计费规则。每调用独立 Interpreter —— 共享 symtable 会并发串账。

    符号表：request（完整请求体，函数形态用）+ 数值字段平铺 + units 兜底
    （表达式形态用）。
    """
    syms = {"request": request, "units": 1}
    syms.update({k: v for k, v in request.items() if isinstance(v, int | float)})
    aeval = asteval.Interpreter(usersyms=syms, use_numpy=False)
    result = aeval.eval(logic, show_errors=False, raise_errors=False)
    if aeval.error:
        raise PricingError(f"rule exec failed: {[str(e) for e in aeval.error][:2]}")
    fn = next((aeval.symtable[n] for n in _FN_NAMES if callable(aeval.symtable.get(n))), None)
    if fn is not None:
        try:
            result = fn(request)
        except Exception as exc:
            raise PricingError(f"rule function raised: {exc}") from exc
    if not isinstance(result, int | float):
        raise PricingError(f"rule returned non-numeric: {result!r}")
    return float(result)


def quote_from_route(route: RouteConfig | None, request: dict) -> Quote:
    """从路由（渠道元数据）携带的计费规则报价；未配规则 → 0（免费渠道）。

    规则求值失败抛 :class:`PricingError`——绝不静默按 0 计费（调用方映射为
    5xx / 回退冻结金额并告警）。
    """
    if route is None or not route.billing_rule:
        return Quote(amount=0.0, metric="default", logic="")
    amount = round(eval_rule(route.billing_rule, request) * route.discount_rate, 6)
    return Quote(amount=amount, metric=route.billing_type or "default",
                 logic=route.billing_rule)
