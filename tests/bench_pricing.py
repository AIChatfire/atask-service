"""微基准：pricing 报价沙箱优化前后，单条 rule 反复求值耗时对比。

手动运行（非 pytest 用例，文件名不匹配 test_* 不会被收集）::

    .venv/bin/python tests/bench_pricing.py [次数]

对照组 = 优化前实现的原样复刻（每次新建 Interpreter + 传字符串求值）；
实验组 = app.services.pricing.eval_rule（AST 缓存 + 符号表模板浅拷贝）。
"""

from __future__ import annotations

import sys
import timeit

import asteval

from app.services.pricing import eval_rule
from app.services.providers import PricingError

RULE_FN = "def calulate(request):\n    return float(request.get('duration') or 5) * 0.026"
RULE_EXPR = "duration * 0.026"
REQUEST = {"duration": 10}


def reference_eval(logic: str, request: dict) -> float:
    """优化前实现（见 git 历史 / tests/test_pricing.py::_reference_eval）。"""
    syms = {"request": request, "units": 1}
    syms.update({k: v for k, v in request.items() if isinstance(v, int | float)})
    aeval = asteval.Interpreter(usersyms=syms, use_numpy=False)
    result = aeval.eval(logic, show_errors=False, raise_errors=False)
    if aeval.error:
        raise PricingError(f"rule exec failed: {[str(e) for e in aeval.error][:2]}")
    fn = next((aeval.symtable[n] for n in ("calulate", "calculate", "calc", "compute", "price")
               if callable(aeval.symtable.get(n))), None)
    if fn is not None:
        result = fn(request)
    return float(result)


def _bench(label: str, fn, n: int) -> float:
    total = timeit.timeit(fn, number=n)
    per_call_us = total / n * 1e6
    print(f"  {label:<12} {n:>6} 次  共 {total * 1e3:8.1f} ms   单次 {per_call_us:7.2f} µs")
    return per_call_us


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    # 预热：让缓存进入稳态（真实服务中同渠道 rule 会被反复求值）
    eval_rule(RULE_FN, REQUEST)
    eval_rule(RULE_EXPR, REQUEST)

    for name, rule in (("函数形态", RULE_FN), ("表达式形态", RULE_EXPR)):
        print(f"[{name}] {rule!r}")
        old = _bench("优化前", lambda r=rule: reference_eval(r, REQUEST), n)
        new = _bench("优化后", lambda r=rule: eval_rule(r, REQUEST), n)
        print(f"  提速 {old / new:.2f}x（单次省 {old - new:.2f} µs）\n")


if __name__ == "__main__":
    main()
