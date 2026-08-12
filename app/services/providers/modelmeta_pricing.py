"""模型元数据定价实现：GET {BASE}/v1/models/{model} → billing.rule / billing.type

rule 是完整 Python 函数定义（asteval 沙箱执行），例如：
    def calulate(request):
        ...
        return 1            # 返回冻结金额
兼容：函数名拼写以 calulate/calculate/calc/compute/price 自动识别；
     纯表达式形态（如 "duration * 0.004"）自动兜底。
缓存：Redis 60s + 无 TTL stale 兜底（pricing 抖动时用最后一份规则）。
"""

import json
import logging

import asteval

from app.config import settings
from app.redis import K_PRICING, r
from app.schemas import Quote
from app.services import httpc
from app.services.providers import PricingError

log = logging.getLogger("gateway.provider.pricing")

_FN_NAMES = ("calulate", "calculate", "calc", "compute", "price")


def _eval_rule(logic: str, request: dict) -> float:
    """asteval 沙箱执行计费规则。每调用独立 Interpreter —— 共享 symtable 会并发串账。
    符号表：request（完整请求体，函数形态用）+ 数值字段平铺 + units 兜底（表达式形态用）。"""
    syms = {"request": request, "units": 1}
    syms.update({k: v for k, v in request.items() if isinstance(v, (int, float))})
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
    if not isinstance(result, (int, float)):
        raise PricingError(f"rule returned non-numeric: {result!r}")
    return round(float(result), 6)


class ModelMetaPricingProvider:
    async def quote(self, model: str, request: dict) -> Quote:
        info = await self._get_model(model)
        billing = info.get("billing") or {}
        logic = billing.get("rule") or "0"
        amount = _eval_rule(logic, request)
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
