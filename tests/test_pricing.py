"""计费规则测试：规则唯一事实源 = keypool 渠道 gateway 块 billing。

- registry.route_from_channel：billing 子块摊平为 RouteConfig 字段；
- pricing.eval_rule：asteval 沙箱求值（函数形态 / 表达式兜底 / 错误不静默）；
- pricing.quote_from_route：规则 × discount_rate → Quote（未配规则 = 免费）。
"""

from __future__ import annotations

import ast
import threading
from concurrent.futures import ThreadPoolExecutor

import asteval
import pytest

import app.services.pricing as pricing
from app.services.pricing import eval_rule, quote_from_route
from app.services.providers import PricingError
from app.services.registry import route_from_channel

RULE_FN = "def calulate(request):\n    return float(request.get('duration') or 5) * 0.026"


def _channel_with_billing(billing) -> dict:
    return {"id": 7, "name": "minimax-a", "base_url": "https://api.minimaxi.com",
            "setting": {"gateway": {"submit_path": "/v2/video_generation",
                                    "billing": billing}}}


# ---------------------------------------------------------------------------
# registry：billing 块解析
# ---------------------------------------------------------------------------


def test_billing_block_flattened_to_route():
    route = route_from_channel("minimax", _channel_with_billing(
        {"rule": RULE_FN, "type": "second", "discount_rate": 0.9}))
    assert route.billing_rule == RULE_FN
    assert route.billing_type == "second"
    assert route.discount_rate == pytest.approx(0.9)


def test_billing_discount_rate_alias():
    """discountRate（驼峰）同样识别。"""
    route = route_from_channel("minimax", _channel_with_billing(
        {"rule": RULE_FN, "type": "second", "discountRate": 0.8}))
    assert route.discount_rate == pytest.approx(0.8)


def test_billing_string_shorthand():
    """billing 直接给字符串 = 规则本体（表达式形态）。"""
    route = route_from_channel("minimax", _channel_with_billing("duration * 0.026"))
    assert route.billing_rule == "duration * 0.026"
    assert route.billing_type == "default"
    assert route.discount_rate == 1.0


def test_billing_absent_defaults():
    route = route_from_channel("minimax", {
        "id": 7, "setting": {"gateway": {"submit_path": "/x"}}})
    assert route.billing_rule == ""
    assert route.billing_type == "default"
    assert route.discount_rate == 1.0


def test_billing_flat_keys_override_nested():
    """摊平键（billing_rule 等）显式给出时优先于 billing 子块。"""
    route = route_from_channel("minimax", {
        "id": 7,
        "setting": {"gateway": {"billing": {"rule": "duration * 1", "type": "second"},
                                "billing_rule": "duration * 2"}},
    })
    assert route.billing_rule == "duration * 2"


# ---------------------------------------------------------------------------
# pricing：沙箱求值与报价
# ---------------------------------------------------------------------------


def test_eval_rule_function_form():
    assert eval_rule(RULE_FN, {"duration": 5}) == pytest.approx(0.13)


def test_eval_rule_expression_fallback():
    assert eval_rule("duration * 0.5", {"duration": 4}) == pytest.approx(2.0)


def test_eval_rule_syntax_error_raises():
    with pytest.raises(PricingError):
        eval_rule("duration *", {"duration": 5})


def test_eval_rule_non_numeric_raises():
    with pytest.raises(PricingError):
        eval_rule("'not-a-number'", {"duration": 5})


def test_quote_from_route(route_factory):
    route = route_factory(billing_rule=RULE_FN, billing_type="second",
                          discount_rate=0.9)
    quote = quote_from_route(route, {"duration": 10})
    assert quote.amount == pytest.approx(0.26 * 0.9)
    assert quote.metric == "second"
    assert quote.logic == RULE_FN


def test_quote_no_rule_is_free(route_factory):
    """渠道未配 billing → 报价 0（免费渠道，不产生冻结）。"""
    quote = quote_from_route(route_factory(), {"duration": 5})
    assert quote.amount == 0.0 and quote.metric == "default"
    assert quote_from_route(None, {}).amount == 0.0


# ---------------------------------------------------------------------------
# pricing：解析缓存（性能优化）——语义与优化前逐字节一致
# ---------------------------------------------------------------------------


def _reference_eval(logic: str, request: dict) -> float:
    """优化前实现的原样复刻（每次新建 Interpreter + 传字符串求值），作为语义基准。"""
    syms = {"request": request, "units": 1}
    syms.update({k: v for k, v in request.items() if isinstance(v, int | float)})
    aeval = asteval.Interpreter(usersyms=syms, use_numpy=False)
    result = aeval.eval(logic, show_errors=False, raise_errors=False)
    if aeval.error:
        raise PricingError(f"rule exec failed: {[str(e) for e in aeval.error][:2]}")
    fn = next((aeval.symtable[n] for n in ("calulate", "calculate", "calc", "compute", "price")
               if callable(aeval.symtable.get(n))), None)
    if fn is not None:
        try:
            result = fn(request)
        except Exception as exc:
            raise PricingError(f"rule function raised: {exc}") from exc
    if not isinstance(result, int | float):
        raise PricingError(f"rule returned non-numeric: {result!r}")
    return float(result)


@pytest.fixture
def fresh_cache(monkeypatch):
    """每个用例独立的规则缓存，隔离模块级单例避免相互污染。"""
    cache = pricing.ParsedRuleCache()
    monkeypatch.setattr(pricing, "_RULE_CACHE", cache)
    return cache


def test_cache_hit_second_eval_same_rule(fresh_cache):
    """同 rule 二次求值命中缓存：结果一致、只解析一次。"""
    rule = RULE_FN + "\n# cache-hit-case"
    assert eval_rule(rule, {"duration": 5}) == pytest.approx(0.13)
    assert fresh_cache.misses == 1 and fresh_cache.hits == 0
    # 命中缓存后结果仍按当次 request 求值（symtable 每次新建）
    assert eval_rule(rule, {"duration": 10}) == pytest.approx(0.26)
    assert fresh_cache.misses == 1 and fresh_cache.hits == 1
    assert len(fresh_cache) == 1 and rule in fresh_cache


def test_cache_bounded_lru_eviction():
    """缓存有界：超出 maxsize 逐出最久未用条目，被逐出后重新解析。"""
    cache = pricing.ParsedRuleCache(maxsize=4)
    for i in range(6):
        assert cache.get_node(f"duration * {i}") is not None
    assert len(cache) == 4
    assert "duration * 0" not in cache and "duration * 1" not in cache  # 最旧两条被逐出
    assert "duration * 5" in cache
    misses_before = cache.misses
    assert cache.get_node("duration * 0") is not None  # 重新解析并再次入缓存
    assert cache.misses == misses_before + 1
    assert len(cache) == 4 and "duration * 2" not in cache  # 挤掉下一条最旧的


def test_cache_rejects_invalid_maxsize():
    with pytest.raises(ValueError):
        pricing.ParsedRuleCache(maxsize=0)


async def test_cache_concurrent_coroutines_no_crosstalk(fresh_cache):
    """多协程并发求值同一条 rule：结果各自独立、无串扰。"""
    import asyncio

    rule = RULE_FN + "\n# concurrent-case"
    durations = list(range(1, 65))
    results = await asyncio.gather(
        *(asyncio.to_thread(eval_rule, rule, {"duration": d}) for d in durations)
    )
    for d, got in zip(durations, results, strict=True):
        assert got == pytest.approx(d * 0.026)
    assert len(fresh_cache) == 1  # 全部协程共享同一条缓存 AST


def test_cache_concurrent_threads_no_crosstalk(fresh_cache):
    """多线程并发求值同一条 rule（taskiq/to_thread 模型）：结果无串扰。"""
    rule = RULE_FN + "\n# thread-case"
    durations = list(range(1, 65))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda d: eval_rule(rule, {"duration": d}), durations))
    for d, got in zip(durations, results, strict=True):
        assert got == pytest.approx(d * 0.026)
    assert len(fresh_cache) == 1


def test_invalid_rule_error_semantics_unchanged(fresh_cache):
    """无效 rule：异常类型与消息和优化前逐字节一致；不污染缓存、每次一致报错。"""
    with pytest.raises(PricingError) as ref_exc:
        _reference_eval("duration *", {"duration": 5})
    for _ in range(2):
        with pytest.raises(PricingError) as exc:
            eval_rule("duration *", {"duration": 5})
        assert str(exc.value) == str(ref_exc.value)
    assert len(fresh_cache) == 0  # 解析失败的规则不入缓存
    assert fresh_cache.misses == 2 and fresh_cache.hits == 0


def test_oversize_rule_error_semantics_unchanged(fresh_cache):
    """超过 asteval max_statement_length 的 rule：与优化前同样抛 PricingError。"""
    rule = "1 + " * 20000 + "1"  # 长度 > 50000
    assert len(rule) > 50000
    with pytest.raises(PricingError) as ref_exc:
        _reference_eval(rule, {})
    with pytest.raises(PricingError) as exc:
        eval_rule(rule, {})
    assert str(exc.value) == str(ref_exc.value)
    assert len(fresh_cache) == 0


def test_runtime_error_messages_match_reference(fresh_cache):
    """运行期求值错误（未定义变量/除零/函数内异常）消息与优化前一致。"""
    cases = [
        ("nosuchvar * 2", {}),
        ("duration / 0", {"duration": 5}),
        ("def calulate(request):\n    return 1 / 0", {}),
        ("'not-a-number'", {}),
    ]
    for rule, req in cases:
        with pytest.raises(PricingError) as ref_exc:
            _reference_eval(rule, req)
        with pytest.raises(PricingError) as exc:
            eval_rule(rule, req)
        assert str(exc.value) == str(ref_exc.value), rule


def test_fresh_symtable_per_eval_no_state_leak(fresh_cache):
    """模块级可变状态的规则：每次求值都是全新符号表（缓存 AST ≠ 缓存解释器）。"""
    rule = ("acc = []\n"
            "def calulate(request):\n"
            "    acc.append(1)\n"
            "    return float(len(acc))\n"
            "# stateful-case")
    for _ in range(3):
        assert eval_rule(rule, {}) == 1.0  # 若共享解释器，会随调用次数递增
        assert _reference_eval(rule, {}) == 1.0


def test_user_symbol_shadowing_matches_reference(fresh_cache):
    """请求数值字段与内建符号同名时，覆盖优先级与优化前一致。"""
    rule = "float(request.get('duration')) * 1\n# shadow-case"
    req = {"duration": 7}
    assert eval_rule(rule, req) == _reference_eval(rule, req)
    req_shadow = {"duration": 7, "float": 3}  # 数值字段覆盖内建 float → 两边一致报错
    with pytest.raises(PricingError) as ref_exc:
        _reference_eval(rule, req_shadow)
    with pytest.raises(PricingError) as exc:
        eval_rule(rule, req_shadow)
    assert str(exc.value) == str(ref_exc.value)


def test_money_consistency_miss_hit_reference(fresh_cache):
    """金额红线：多种形态规则，miss / hit / 优化前 reference 三路径 repr 逐位相等。"""
    cases = [
        # 纯表达式浮点
        ("duration * 0.026", [{"duration": 3}, {"duration": 7.5}, {"duration": 0.1}]),
        # 函数 + 条件分支阶梯价
        ("def calulate(request):\n"
         "    d = float(request.get('duration') or 0)\n"
         "    if d <= 10:\n"
         "        return d * 0.05\n"
         "    elif d <= 60:\n"
         "        return 0.5 + (d - 10) * 0.03\n"
         "    return 2.0 + (d - 60) * 0.01",
         [{"duration": 5}, {"duration": 30}, {"duration": 120}, {}]),
        # 函数 + 内建函数组合
        ("def calculate(request):\n"
         "    return max(0.01, min(float(request.get('tokens') or 1) * 0.000002, 9.99))",
         [{"tokens": 100}, {"tokens": 5_000_000}, {"tokens": 1}]),
        # 表达式 + 整数除法取余
        ("seconds // 60 * 0.1 + (seconds % 60) * 0.002",
         [{"seconds": 95}, {"seconds": 61}, {"seconds": 3600}]),
        # 浮点精度敏感（0.1+0.2 伪影必须逐位一致）
        ("def calulate(request):\n"
         "    return 0.1 + 0.2 + float(request.get('x') or 0)",
         [{"x": 0.3}, {"x": 1e-9}, {}]),
    ]
    for rule, requests in cases:
        for req in requests:
            ref = _reference_eval(rule, req)
            miss = eval_rule(rule, req)  # 首次：miss 路径（也走 eval(AST)）
            hit = eval_rule(rule, req)   # 二次：hit 路径
            assert repr(miss) == repr(ref), (rule, req, miss, ref)
            assert repr(hit) == repr(ref), (rule, req, hit, ref)
    assert fresh_cache.misses == len(cases)  # 每条 rule 恰好解析一次


def test_runtime_error_on_hit_path_matches_reference(fresh_cache):
    """规则先成功求值（入缓存），hit 路径触发运行期错误：消息一致、缓存不污染。"""
    rule = "def calulate(request):\n    return 10 / int(request.get('n'))\n# hit-err-case"
    assert eval_rule(rule, {"n": 2}) == 5.0
    assert fresh_cache.misses == 1 and fresh_cache.hits == 0
    with pytest.raises(PricingError) as hit_exc:
        eval_rule(rule, {"n": 0})  # hit 路径除零
    assert fresh_cache.hits == 1
    with pytest.raises(PricingError) as ref_exc:
        _reference_eval(rule, {"n": 0})
    assert str(hit_exc.value) == str(ref_exc.value)
    assert eval_rule(rule, {"n": 4}) == 2.5  # 缓存未被错误污染
    assert len(fresh_cache) == 1


def test_cached_ast_not_mutated_by_eval(fresh_cache):
    """缓存 AST 共享只读：多次求值+函数调用后 ast.dump 逐字节不变、同一对象。"""
    rule = RULE_FN + "\n# ast-readonly-case"
    node_before = fresh_cache.get_node(rule)
    dump_before = ast.dump(node_before)
    for d in (1, 500, 2000, 99999):
        eval_rule(rule, {"duration": d})
    node_after = fresh_cache.get_node(rule)
    assert node_after is node_before
    assert ast.dump(node_after) == dump_before


def test_concurrent_two_rules_alternating_no_crosstalk(fresh_cache):
    """两条不同规则 × 不同变量集交替高并发：结果各自正确、缓存恰两条。"""
    rule_a = RULE_FN + "\n# alt-a"
    rule_b = ("def calulate(request):\n"
              "    t = int(request.get('tokens') or 1)\n"
              "    return t * 0.000002 if t > 1000 else 0.002\n# alt-b")
    tasks = []
    for i in range(1000):
        if i % 2 == 0:
            tasks.append((rule_a, {"duration": i % 97 + 1}, (i % 97 + 1) * 0.026))
        else:
            t = i % 5000 + 1
            tasks.append((rule_b, {"tokens": t}, t * 0.000002 if t > 1000 else 0.002))
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda a: eval_rule(a[0], a[1]), tasks))
    for (rule, req, expected), got in zip(tasks, results, strict=True):
        assert repr(got) == repr(float(expected)), (rule, req, got, expected)
    assert len(fresh_cache) == 2


def test_lru_bounded_under_concurrent_inserts():
    """并发写入大量不同 rule：最终长度不超过 maxsize、计数守恒。"""
    cache = pricing.ParsedRuleCache(maxsize=16)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: cache.get_node(f"duration * {i} + 1"), range(800)))
    assert len(cache) <= 16
    assert cache.misses == 800 and cache.hits == 0


def test_cache_key_is_exact_rule_string(fresh_cache):
    """缓存键 = rule 原文：空白/注释差异即不同键、无碰撞，相近规则互不误命中。"""
    for v in ["duration * 0.5", "duration*0.5", "duration * 0.5 ", "duration * 0.5\n# c"]:
        assert eval_rule(v, {"duration": 4}) == 2.0
    assert len(fresh_cache) == 4
    assert eval_rule("duration * 1", {"duration": 5}) == 5.0
    assert eval_rule("duration * 2", {"duration": 5}) == 10.0
    assert eval_rule("duration * 1", {"duration": 5}) == 5.0  # hit 仍按各自 rule 求值


def test_eval_does_not_hold_cache_lock(fresh_cache, monkeypatch):
    """锁粒度：Interpreter.eval 求值期间当前线程不得持有缓存锁（不退化串行）。"""
    owner = {"tid": None}
    real_lock = fresh_cache._lock

    class SpyLock:
        def acquire(self, *a, **kw):
            real_lock.acquire(*a, **kw)
            owner["tid"] = threading.get_ident()
            return True

        def release(self):
            owner["tid"] = None
            real_lock.release()

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *a):
            self.release()
            return False

    monkeypatch.setattr(fresh_cache, "_lock", SpyLock())
    violations = []
    real_eval = asteval.Interpreter.eval

    def spy_eval(self, *a, **kw):
        if owner["tid"] is not None and owner["tid"] == threading.get_ident():
            violations.append(threading.get_ident())
        return real_eval(self, *a, **kw)

    monkeypatch.setattr(asteval.Interpreter, "eval", spy_eval)
    rule = RULE_FN + "\n# lock-case"
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda d: eval_rule(rule, {"duration": d}), range(200)))
    assert violations == []
    assert len(fresh_cache) == 1
