"""asteval 安全求值沙箱（SPEC §3.11.3；四层防御，架构 §5.3，简报 B §4）。

四层防御：

1. **版本与配置层**：asteval ≥1.0.6（GHSA-vp47-9734-prjw 修复版，pin 1.0.9
   订阅 GHSA）；``Interpreter(minimal=True, no_while/no_for/no_functiondef/
   no_print, use_numpy=False, max_time=10, builtins_readonly=True,
   readonly_symbols=[...])``——锁定内置符号，防表达式覆写内建名。
2. **AST 预检层**：求值前 ``ast.parse`` 静态扫描——长度 >1KB、节点 >200、
   ``**`` 右操作数非 ≤4 小字面量（``9**9**9`` 幂塔 DoS）、字符串字面量
   >256 一律拒收。**沙箱管不了资源消耗，必须在求值前拦截。**
3. **进程外资源限制层**：求值放入独立子进程池（``ProcessPoolExecutor``），
   worker 进程 ``resource.setrlimit`` 限制 CPU/地址空间；``max_time`` 只是
   循环间检查不可靠，真正的超时/内存限制在沙箱外做。
4. **输入上下文只读层**：每次求值新建 symtable，只注入计费变量
   （SPEC §3.11.2 契约键，float/str 标量）。

结果约束：必须为非负有限数值（bool/str/NaN/inf 一律拒收）。
``python_func`` 类型 **不实现 exec 路径**（V5 未定），由调用方按求值失败处理。
"""

from __future__ import annotations

import ast
import math
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from app.config import settings

MAX_EXPR_LEN = 1024  # 表达式长度上限（入库与拉取双重校验，§5.2/§10.7）
_MAX_AST_NODES = 200  # 节点总数上限（复杂度炸弹）
_MAX_POW_EXPONENT = 4  # 幂运算右操作数允许的最大字面量绝对值
_MAX_STRING_LITERAL = 256  # 字符串字面量长度上限
_ASTEVAL_MAX_TIME = 10  # asteval 循环间软超时（不可靠，仅纵深一层）

# 第 4 层（输入上下文只读层）：minimal symtable 仍残留 open/dir/type/print 等
# 内建（asteval 1.0.9 实测），按「只注入计费变量」契约收敛为白名单——
# readonly_symbols 六内建 + 三个字面量常量 + 当次上下文变量，其余一律摘除。
_SAFE_SYMTABLE_SYMBOLS = frozenset(
    {"abs", "min", "max", "round", "float", "int", "True", "False", "None"}
)

_eval_pool: ProcessPoolExecutor | None = None


def ast_precheck(expr: str) -> None:
    """AST 静态预检（架构 §13.3 ``_ast_precheck`` 语义）。

    拒绝：长度 >1024 / AST 节点 >200 / ``**`` 右操作数非 ≤4 小字面量 /
    字符串字面量 >256。违规抛 ``ValueError``。
    """
    if len(expr) > MAX_EXPR_LEN:
        raise ValueError("expr too long")
    tree = ast.parse(expr, mode="eval")
    if len(list(ast.walk(tree))) > _MAX_AST_NODES:
        raise ValueError("expr too complex")
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Pow):
            # 拒绝 9**9**9 幂塔：指数端只允许 <=4 的小字面量
            if not (
                isinstance(node.right, ast.Constant)
                and isinstance(node.right.value, int | float)
                and not isinstance(node.right.value, bool)
                and abs(node.right.value) <= _MAX_POW_EXPONENT
            ):
                raise ValueError("pow with large/non-literal exponent rejected")
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and len(node.value) > _MAX_STRING_LITERAL
        ):
            raise ValueError("long string literal rejected")
        if (
            isinstance(node, ast.List | ast.Tuple | ast.Set)
            and len(node.elts) > _MAX_AST_NODES
        ):
            raise ValueError("long sequence literal rejected")


def _sandbox_worker_init() -> None:
    """进程池 worker 初始化钩子（ProcessPoolExecutor ``initializer``）。

    RLIMIT_CPU/AS 只对沙箱 worker 子进程生效。集成修复：原实现在
    ``eval_expr_subprocess`` 函数体内 setrlimit——被测试在主进程直接调用时
    会污染调用方（主进程累计 CPU 达上限后被内核 SIGKILL、地址空间被锁死），
    而资源限制的防御对象本就只是池化子进程。
    """
    import resource

    cpu = settings.asteval_cpu_limit_seconds
    mem = settings.asteval_mem_limit_mb
    resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))
    resource.setrlimit(resource.RLIMIT_AS, (mem << 20, mem << 20))


def eval_expr_subprocess(expr: str, context: dict[str, float | str]) -> float:
    """子进程入口（ProcessPoolExecutor target，必须模块级可 pickle）。

    ast_precheck → asteval minimal 求值 → 结果校验（CPU/AS 资源限制由
    ``get_eval_pool`` 的 worker initializer 施加）。
    任何违规/异常向上抛（池化调用方按求值失败处理）。
    """
    ast_precheck(expr)

    from asteval import Interpreter  # >=1.0.6（GHSA-vp47-9734-prjw 修复）

    aeval = Interpreter(
        minimal=True,
        no_while=True,
        no_for=True,
        no_functiondef=True,
        no_print=True,
        use_numpy=False,
        max_time=_ASTEVAL_MAX_TIME,
        builtins_readonly=True,
        # readonly_symbols（简报 B §4 加固清单）：锁定内置符号，
        # 防表达式覆写 symtable 中的函数/内建名制造副作用
        readonly_symbols=["abs", "min", "max", "round", "float", "int"],
    )
    # 只读计费变量：白名单收敛 symtable 后仅注入契约上下文（float/str 标量）
    allowed = _SAFE_SYMTABLE_SYMBOLS | set(context)
    for key in list(aeval.symtable):
        if key not in allowed:
            del aeval.symtable[key]
    aeval.symtable.update(dict(context))
    result: Any = aeval(expr, show_errors=False, raise_errors=True)
    if (
        isinstance(result, bool)
        or not isinstance(result, int | float)
        or not math.isfinite(result)
        or result < 0
    ):
        raise ValueError(f"pricing expr must yield non-negative finite number, got {result!r}")
    return float(result)


def get_eval_pool() -> ProcessPoolExecutor:
    """模块级惰性子进程池（大小 settings.asteval_pool_size）。

    惰性创建：Gunicorn post-fork 安全（fork 后首次使用才建池）。
    """
    global _eval_pool
    if _eval_pool is None or _eval_pool._broken:  # type: ignore[attr-defined]
        _eval_pool = ProcessPoolExecutor(
            max_workers=settings.asteval_pool_size,
            initializer=_sandbox_worker_init,
        )
    return _eval_pool


def shutdown_eval_pool() -> None:
    """lifespan 退出时关闭池（worker.py/main.py 优雅停机钩子用）。"""
    global _eval_pool
    if _eval_pool is not None:
        _eval_pool.shutdown(wait=False, cancel_futures=True)
        _eval_pool = None
