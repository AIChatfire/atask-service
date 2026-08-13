"""pricing-service 实现（https://github.com/AIChatfire/pricing-service）。

``GET {BASE}/v1/models/{model}`` → 模型元数据 + ``billing.rule`` / ``billing.type``
/ ``discountRate`` / ``status``。

- ``rule`` 是完整 Python 函数定义（asteval 沙箱执行），约定函数名 ``calulate``
  （历史拼写，兼容 calculate/calc/compute/price）；入参为完整请求体，返回值
  为**计费金额（USD）**；纯表达式形态（如 ``duration * 0.026``）自动兜底。
- 实际冻结金额 = 规则返回值 × ``discountRate``（缺省 1，pricing 文档：折扣必乘）。
- ``status != 0`` → :class:`ModelUnavailableError`（模型不可用，pricing 文档：
  仅 status == 0 放行）。
- 缓存：Redis ``pricing_cache_ttl``（默认 300s）+ 无 TTL stale 兜底
  （pricing 抖动时用最后一份规则，绝不因 pricing 故障放大为提交故障）。
"""

from __future__ import annotations

import json
import logging

import asteval

from app.config import settings
from app.redis import K_PRICING, r
from app.schemas import Quote
from app.services import httpc
from app.services.providers import ModelUnavailableError, PricingError

log = logging.getLogger("gateway.provider.pricing")

_FN_NAMES = ("calulate", "calculate", "calc", "compute", "price")


def _eval_rule(logic: str, request: dict) -> float:
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


class ModelMetaPricingProvider:
    async def quote(self, model: str, request: dict) -> Quote:
        info = await self._get_model(model)
        if info.get("status", 0) != 0:
            raise ModelUnavailableError(f"model {model} unavailable (status={info.get('status')})")
        billing = info.get("billing") or {}
        logic = billing.get("rule") or "0"
        discount = float(info.get("discountRate") or 1)
        amount = round(_eval_rule(logic, request) * discount, 6)
        return Quote(amount=amount, metric=billing.get("type") or "default", logic=logic)

    async def _get_model(self, model: str) -> dict:
        key = K_PRICING.format(biz=model, metric="model")
        cached = await r.get(key)
        if cached:
            return json.loads(cached)
        try:
            async with httpc.new_client(timeout=settings.http_timeout) as client:
                resp = await client.get(f"{settings.pricing_svc_url}/v1/models/{model}")
                resp.raise_for_status()
                info = resp.json()
            await r.set(key, json.dumps(info, ensure_ascii=False), ex=settings.pricing_cache_ttl)
            await r.set(key + ":stale", json.dumps(info, ensure_ascii=False))
            return info
        except Exception as exc:
            stale = await r.get(key + ":stale")
            if stale:
                log.warning("pricing svc down, use stale model info for %s: %s", model, exc)
                return json.loads(stale)
            raise PricingError(f"pricing unavailable for model {model}") from exc
