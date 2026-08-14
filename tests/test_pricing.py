"""计费规则测试：规则唯一事实源 = keypool 渠道 gateway 块 billing。

- registry.route_from_channel：billing 子块摊平为 RouteConfig 字段；
- pricing.eval_rule：asteval 沙箱求值（函数形态 / 表达式兜底 / 错误不静默）；
- pricing.quote_from_route：规则 × discount_rate → Quote（未配规则 = 免费）。
"""

from __future__ import annotations

import pytest

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
