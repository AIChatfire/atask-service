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

性能（报价热路径：preflight / settle 重估每次都走这里）：

- **解析缓存**：同一条 rule 字符串的 ``ast.parse`` 产物按 LRU 缓存
  （:class:`ParsedRuleCache`，上限 256）。AST 节点解析后只读（asteval 求值
  只遍历不改写），可跨协程/线程安全共享；缓存有界，防异常渠道配置撑爆内存。
- **符号表模板**：asteval 内建符号表（``use_numpy=False``，123 个符号）模块级
  构建一次、只读共享；每次求值 ``dict()`` 浅拷贝后再注入 request 符号，
  绝不原地修改（gunicorn ``preload_app`` fork 后各 worker 亦只读共享）。
- **不缓存 Interpreter/函数实例**：symtable 在 eval 时可变，共享实例并发
  eval 会串账；每规则共享闭包还会让「模块级可变全局」类规则跨调用残留状态，
  改变语义。因此每次求值仍新建 Interpreter——计费纪律红线：语义与优化前
  完全一致，报价金额一个分都不能变。
"""

from __future__ import annotations

import ast
import threading
from collections import OrderedDict

import asteval
from asteval.astutils import make_symbol_table

from app.schemas import Quote, RouteConfig
from app.services.providers import PricingError

_FN_NAMES = ("calulate", "calculate", "calc", "compute", "price")

# 与 asteval.Interpreter 默认 max_statement_length 对齐：超限规则视为解析失败，
# 回落到原始完整求值路径，错误语义与优化前逐字节一致。
_MAX_STATEMENT_LENGTH = 50000

# 计费规则种类 = keypool 渠道配置数（量级十~百）；LRU 上限有界，防恶意/异常
# 配置制造大量不同 rule 字符串撑爆内存。
_RULE_CACHE_MAXSIZE = 256

# asteval 内建符号表（use_numpy=False 与原求值路径一致）。只读共享模板：
# 每次求值浅拷贝后使用，本模块保证绝不原地修改它。
_BASE_SYMTABLE: dict = make_symbol_table(use_numpy=False)


class ParsedRuleCache:
    """按 rule 字符串缓存 ``ast.parse`` 产物（LRU 有界 + threading.Lock）。

    并发安全设计：

    - 缓存值是不可变 AST，求值不持锁、天然可并发共享；
    - ``threading.Lock`` 仅保护 OrderedDict 的读/改写（微秒级临界区），
      对事件循环与 taskiq 线程池模型都正确（``threading.local`` 无法做到
      全局有界，per-rule 锁则不必——这里没有可变的共享解释器状态）；
    - ``hits`` / ``misses`` 计数供缓存命中率观测与测试断言。
    """

    def __init__(self, maxsize: int = _RULE_CACHE_MAXSIZE) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self._maxsize = maxsize
        self._lock = threading.Lock()
        self._nodes: OrderedDict[str, ast.Module] = OrderedDict()
        self.hits = 0
        self.misses = 0

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def __len__(self) -> int:
        with self._lock:
            return len(self._nodes)

    def __contains__(self, logic: str) -> bool:
        with self._lock:
            return logic in self._nodes

    def get_node(self, logic: str) -> ast.Module | None:
        """取 rule 的缓存解析树；未命中则解析并缓存。

        解析失败（语法错误/超长/非法字节等）返回 ``None``——调用方回落到
        原始完整求值路径，让 asteval 自己产生与优化前一致的错误信息。
        """
        with self._lock:
            node = self._nodes.get(logic)
            if node is not None:
                self._nodes.move_to_end(logic)
                self.hits += 1
                return node
            self.misses += 1
        # 解析不持锁：ast.parse 是纯函数，并发重复解析同一 rule 结果等价，
        # 后到者覆盖同值，无害。
        node = self._parse(logic)
        if node is None:
            return None
        with self._lock:
            self._nodes[logic] = node
            self._nodes.move_to_end(logic)
            while len(self._nodes) > self._maxsize:
                self._nodes.popitem(last=False)
        return node

    @staticmethod
    def _parse(logic: str) -> ast.Module | None:
        if len(logic) > _MAX_STATEMENT_LENGTH:
            return None
        try:
            return ast.parse(logic)
        except (SyntaxError, ValueError, RuntimeError, MemoryError):
            # RuntimeError 覆盖 RecursionError（病态嵌套）；全部按「无效规则」
            # 处理，回落慢速路径复现原始错误语义。
            return None


_RULE_CACHE = ParsedRuleCache()


def _fresh_interpreter(request: dict) -> asteval.Interpreter:
    """每次求值新建 Interpreter：symtable 在 eval 时可变，共享实例会并发串账。

    符号表 = 内建模板浅拷贝 + request（完整请求体，函数形态用）+ 数值字段
    平铺 + units 兜底（表达式形态用）；注入顺序与优化前一致（用户符号覆盖
    内建同名符号，数值字段覆盖 request/units 键）。
    """
    syms = dict(_BASE_SYMTABLE)
    syms["request"] = request
    syms["units"] = 1
    syms.update({k: v for k, v in request.items() if isinstance(v, int | float)})
    return asteval.Interpreter(symtable=syms, use_numpy=False)


def eval_rule(logic: str, request: dict) -> float:
    """asteval 沙箱执行计费规则。每调用独立 Interpreter —— 共享 symtable 会并发串账。

    快路径复用缓存的解析树（``eval`` 直接接受 AST 节点，与传字符串走同一段
    求值/错误记录代码）；解析失败的无效规则回落原始完整路径。两条路径的
    结果与错误消息均与优化前一致（含异常类型 :class:`PricingError`）。
    """
    node = _RULE_CACHE.get_node(logic)
    aeval = _fresh_interpreter(request)
    if node is not None:
        result = aeval.eval(node, show_errors=False, raise_errors=False)
    else:
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
