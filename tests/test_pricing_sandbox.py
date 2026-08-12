"""W3 沙箱攻防 + 计费逻辑缓存/求值测试（SPEC §3.11.2/§3.11.3）。

覆盖：AST 预检（幂塔/非字面量指数/超长表达式/超长字符串/节点数）、
asteval 逃逸尝试（__class__/import/open/dir/type）、禁循环/函数定义、
结果校验（负数/字符串/bool/inf）、子进程池求值、超时降级、
evaluate 两相位（freeze fail-closed 顶格 vs settle 抛 PricingEvalError）、
get_logic 三级缓存（L1/L2/L3/全 miss fail-closed）。HTTP/Redis/DB 全 mock。
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest
import respx

from app.billing.pricing import (
    PricingEvalError,
    PricingEvaluator,
    PricingLogic,
)
from app.billing.sandbox import (
    MAX_EXPR_LEN,
    ast_precheck,
    eval_expr_subprocess,
    get_eval_pool,
    shutdown_eval_pool,
)
from app.config import settings
from app.registry import BizConfig
from tests.w3_fakes import FakeRedis, FakeSession, FakeSessionFactory

CTX: dict[str, float | str] = {
    "duration": 5.0,
    "resolution": "1080p",
    "mode": "pro",
    "quantity": 2.0,
    "usage_tokens": 100.0,
    "generate_audio": 1.0,
    "has_image_input": 0.0,
    "service_tier": "default",
}


@pytest.fixture(scope="module", autouse=True)
def _close_pool() -> None:
    yield
    shutdown_eval_pool()


def _biz_cfg(default_amount: str | None = "2.5") -> BizConfig:
    return BizConfig(
        biz="kling", adapter="kling", upstream_base_url="http://up",
        auth_type="aksk_jwt", auth_secret_ref="X", native_prefixes=["v1"],
        enabled=True, billing_keys={"biz_type": "kling_video", "metric": "call"},
        default_freeze_amount_usd=default_amount, rate_limit={},
        newapi_channel_id=None, version=1,
    )


# ---------- AST 预检（架构 §13.3 _ast_precheck 语义） ----------


def test_precheck_accepts_normal_expr() -> None:
    ast_precheck("duration * 0.08 * quantity + usage_tokens * 0.001")
    ast_precheck("2 ** 4")  # 小字面量指数放行


def test_precheck_rejects_pow_tower() -> None:
    with pytest.raises(ValueError):
        ast_precheck("9**9**9")  # 幂塔 DoS


def test_precheck_rejects_non_literal_exponent() -> None:
    with pytest.raises(ValueError):
        ast_precheck("duration ** quantity")
    with pytest.raises(ValueError):
        ast_precheck("2 ** 100")


def test_precheck_rejects_overlong_expr() -> None:
    with pytest.raises(ValueError):
        ast_precheck("1+" * (MAX_EXPR_LEN // 2) + "1" * MAX_EXPR_LEN)


def test_precheck_rejects_long_string_literal() -> None:
    with pytest.raises(ValueError):
        ast_precheck("'" + "a" * 300 + "'")


def test_precheck_rejects_too_many_nodes() -> None:
    with pytest.raises(ValueError):
        ast_precheck("+".join(["1"] * 210))


# ---------- 子进程沙箱攻防 ----------


def test_subprocess_eval_normal() -> None:
    value = eval_expr_subprocess(
        "duration * 0.08 * quantity * (1 + 0.2 * generate_audio)", CTX
    )
    assert value == pytest.approx(0.96)


@pytest.mark.parametrize(
    "attack",
    [
        "().__class__.__mro__",  # 类层级逃逸
        "(1).__class__.__bases__[0].__subclasses__()",
        "__import__('os')",
        "import os",
        "open('/etc/passwd').read()",  # symtable 白名单摘除 open
        "dir()",
        "type(1)",
        "while True: pass",  # no_while（语法即拒）
        "for i in range(10): pass",  # no_for
        "def f(): return 1",  # no_functiondef
        "[i for i in range(10)]",  # minimal 禁列表推导
        "print('x')",
        "abs = 1",  # builtins_readonly / readonly_symbols
        "duration.__class__",  # 注入对象dunder属性
    ],
)
def test_subprocess_blocks_attacks(attack: str) -> None:
    with pytest.raises((ValueError, SyntaxError, AttributeError, NameError,
                        NotImplementedError)):
        eval_expr_subprocess(attack, CTX)


@pytest.mark.parametrize(
    "bad",
    ["-5", "'a string'", "True", "float('inf')", "0 - duration"],
)
def test_subprocess_rejects_bad_result_types(bad: str) -> None:
    """结果必须非负有限数值：负数/字符串/bool/inf 一律拒收。"""
    with pytest.raises(ValueError):
        eval_expr_subprocess(bad, CTX)


async def test_pool_round_trip() -> None:
    """模块级进程池真实求值（可 pickle + rlimit 子进程）。"""
    loop = asyncio.get_running_loop()
    value = await loop.run_in_executor(
        get_eval_pool(), eval_expr_subprocess, "duration * 2", dict(CTX)
    )
    assert value == pytest.approx(10.0)


# ---------- evaluate 两相位失败处置 ----------

def _logic(expr: str = "duration * 0.08", expr_type: str = "asteval") -> PricingLogic:
    return PricingLogic(expr=expr, expr_type=expr_type, version=7,
                        fallback_amount_usd=Decimal("2.000000"))


def _evaluator() -> PricingEvaluator:
    return PricingEvaluator(FakeSessionFactory([FakeSession()]))


async def test_evaluate_asteval_quantized() -> None:
    out = await _evaluator().evaluate(_logic(), dict(CTX), phase="freeze")
    assert out == Decimal("0.400000")  # 6 位小数 ROUND_HALF_UP


async def test_evaluate_freeze_fail_closed_fallback() -> None:
    """freeze 相位求值失败 → 顶格兜底 + 不放行免费请求。"""
    out = await _evaluator().evaluate(_logic("9**9**9"), dict(CTX), phase="freeze")
    assert out == Decimal("2.000000")


async def test_evaluate_settle_raises_no_silent_fallback() -> None:
    """settle 相位求值失败 → PricingEvalError（绝不静默顶格多扣）。"""
    with pytest.raises(PricingEvalError):
        await _evaluator().evaluate(_logic("9**9**9"), dict(CTX), phase="settle")


async def test_evaluate_python_func_not_implemented() -> None:
    """python_func 不实现 exec 路径：freeze 降级 / settle 抛错。"""
    logic = _logic("def quote(ctx): return 1", expr_type="python_func")
    assert await _evaluator().evaluate(logic, dict(CTX), phase="freeze") == Decimal("2.000000")
    with pytest.raises(PricingEvalError):
        await _evaluator().evaluate(logic, dict(CTX), phase="settle")


async def test_evaluate_json_logic() -> None:
    rule = json.dumps({"*": [{"var": "duration"}, {"var": "quantity"}, 0.08]})
    out = await _evaluator().evaluate(_logic(rule, "json_logic"), dict(CTX), phase="freeze")
    assert out == Decimal("0.800000")


async def test_evaluate_json_logic_unknown_op_fails() -> None:
    out = await _evaluator().evaluate(
        _logic(json.dumps({"exec": ["rm -rf /"]}), "json_logic"), dict(CTX), phase="freeze"
    )
    assert out == Decimal("2.000000")


async def test_evaluate_timeout_is_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """子进程求值总超时（pricing_eval_timeout_seconds）→ 按相位失败处置。"""
    monkeypatch.setattr(settings, "pricing_eval_timeout_seconds", 0.001)
    out = await _evaluator().evaluate(_logic(), dict(CTX), phase="freeze")
    assert out == Decimal("2.000000")
    with pytest.raises(PricingEvalError):
        await _evaluator().evaluate(_logic(), dict(CTX), phase="settle")


# ---------- get_logic 三级缓存 + fail-closed ----------

_PRICING_BASE = "http://127.0.0.1:8081"  # settings.pricing_service_url 默认值


def _patch_redis(monkeypatch: pytest.MonkeyPatch, redis: FakeRedis) -> None:
    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr("app.billing.pricing.get_redis", _get_redis)


@respx.mock
async def test_get_logic_l3_then_l1_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_redis(monkeypatch, FakeRedis())
    route = respx.get(f"{_PRICING_BASE}/api/v1/pricing/logic").mock(
        return_value=httpx.Response(200, json={"data": {
            "expr": "duration * 0.08", "expr_type": "asteval",
            "version": 3, "fallback_amount": "1.5"}})
    )
    ev = _evaluator()
    logic = await ev.get_logic("kling", "kling-v3", "text2video")
    assert logic.expr == "duration * 0.08" and logic.version == 3
    assert logic.fallback_amount_usd == Decimal("1.5")
    again = await ev.get_logic("kling", "kling-v3", "text2video")  # L1 命中
    assert again is logic
    assert len(route.calls) == 1  # 第二次不再回源


async def test_get_logic_l2_redis_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()
    redis.strings["pricing:kling:kling-v3:text2video"] = json.dumps({
        "expr": "quantity * 0.5", "expr_type": "asteval", "version": 9,
        "fallback_amount": "3.0", "updated_at": 1})
    _patch_redis(monkeypatch, redis)
    ev = _evaluator()
    logic = await ev.get_logic("kling", "kling-v3", "text2video")
    assert logic.expr == "quantity * 0.5" and logic.version == 9


@respx.mock
async def test_get_logic_l3_backfills_l2(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()
    _patch_redis(monkeypatch, redis)
    respx.get(f"{_PRICING_BASE}/api/v1/pricing/logic").mock(
        return_value=httpx.Response(200, json={
            "expr": "1.0", "expr_type": "asteval", "version": 1,
            "fallback_amount": "1.0"})
    )  # 响应无 data 信封也兼容
    ev = _evaluator()
    await ev.get_logic("kling", "m", "a")
    blob = json.loads(redis.strings["pricing:kling:m:a"])
    assert blob["expr"] == "1.0" and blob["fallback_amount"] == "1.0"


@respx.mock
async def test_get_logic_all_miss_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """L1/L2/L3 全 miss → biz 注册表 default_freeze_amount_usd 固定金额逻辑。"""
    _patch_redis(monkeypatch, FakeRedis())
    respx.get(f"{_PRICING_BASE}/api/v1/pricing/logic").mock(
        return_value=httpx.Response(500, json={"error": "down"})
    )

    async def _fake_get(biz: str, session: object) -> BizConfig:
        return _biz_cfg("2.5")

    monkeypatch.setattr("app.billing.pricing.registry.get", _fake_get)
    ev = PricingEvaluator(FakeSessionFactory([FakeSession()]))
    logic = await ev.get_logic("kling", "m", "a")
    assert logic.expr == "2.5" and logic.expr_type == "asteval"
    # 固定金额逻辑可直接求值，顶格预冻
    assert await ev.evaluate(logic, dict(CTX), phase="freeze") == Decimal("2.500000")


@respx.mock
async def test_get_logic_no_default_amount_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """连兜底价都没有 → PricingEvalError（fail-closed，绝不免费放行）。"""
    _patch_redis(monkeypatch, FakeRedis())
    respx.get(f"{_PRICING_BASE}/api/v1/pricing/logic").mock(
        return_value=httpx.Response(500, json={"error": "down"})
    )

    async def _fake_get(biz: str, session: object) -> BizConfig:
        return _biz_cfg(None)

    monkeypatch.setattr("app.billing.pricing.registry.get", _fake_get)
    ev = PricingEvaluator(FakeSessionFactory([FakeSession()]))
    with pytest.raises(PricingEvalError):
        await ev.get_logic("kling", "m", "a")


@respx.mock
async def test_get_logic_rejects_overlong_expr(monkeypatch: pytest.MonkeyPatch) -> None:
    """L3 返回 expr >1024 → 拒收 → fail-closed 兜底。"""
    _patch_redis(monkeypatch, FakeRedis())
    respx.get(f"{_PRICING_BASE}/api/v1/pricing/logic").mock(
        return_value=httpx.Response(200, json={"data": {
            "expr": "1" * 2000, "expr_type": "asteval", "version": 1,
            "fallback_amount": "1.0"}})
    )

    async def _fake_get(biz: str, session: object) -> BizConfig:
        return _biz_cfg("4.0")

    monkeypatch.setattr("app.billing.pricing.registry.get", _fake_get)
    ev = PricingEvaluator(FakeSessionFactory([FakeSession()]))
    logic = await ev.get_logic("kling", "m", "a")
    assert logic.expr == "4.0"


async def test_get_logic_for_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """settle 阶段从 tasks 行 request_snapshot 取回 (biz, model, action)。"""
    pdata = {"gateway": {"biz": "kling",
                         "request_snapshot": {"model": "kling-v3", "action": "text2video"}}}
    session = FakeSession([[{"private_data": json.dumps(pdata)}]])
    ev = PricingEvaluator(FakeSessionFactory([]))
    seen: dict[str, str] = {}

    async def _fake_get_logic(biz: str, model: str, action: str) -> PricingLogic:
        seen.update(biz=biz, model=model, action=action)
        return _logic()

    monkeypatch.setattr(ev, "get_logic", _fake_get_logic)
    logic = await ev.get_logic_for_task(session, "task_x")
    assert logic.expr == "duration * 0.08"
    assert seen == {"biz": "kling", "model": "kling-v3", "action": "text2video"}
    # tasks 查询带 platform 前缀条件（§4.5 纪律）
    assert "platform LIKE" in session.executed[0][0]


async def test_get_logic_for_task_missing_dims() -> None:
    session = FakeSession([[{"private_data": json.dumps({"gateway": {}})}]])
    ev = PricingEvaluator(FakeSessionFactory([]))
    with pytest.raises(PricingEvalError):
        await ev.get_logic_for_task(session, "task_x")
