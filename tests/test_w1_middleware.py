"""W1 中间件测试：熔断器状态机、限流三层、幂等回放/冲突、欠费熔断。"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request
from w1_helpers import FakeRedis, make_biz, make_token

from app import errors, middleware
from app.middleware import (
    CircuitBreaker,
    IdempotentReplay,
    check_biz_rate_limit,
    check_debt_block,
    check_user_rate_limit,
    circuit_breaker,
    idempotency_complete,
    idempotency_guard,
    idempotency_release,
)


@pytest.fixture(autouse=True)
def _redis(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    fake = FakeRedis()
    monkeypatch.setattr(middleware, "get_redis", AsyncMock(return_value=fake))
    return fake


def _request(headers: dict[str, str] | None = None, body: bytes = b"{}",
             method: str = "POST", path: str = "/kling/v1/videos") -> Request:
    scope = {
        "type": "http", "method": method, "path": path, "scheme": "http",
        "server": ("testserver", 80), "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
    }
    req = Request(scope)
    req._body = body
    return req


# ---------------------------------------------------------------------------
# 熔断器（§8.2：fail_count>=5 → open(30s) → half-open 单探针 → closed）
# ---------------------------------------------------------------------------


async def test_circuit_closed_then_open_after_5_failures(_redis: FakeRedis) -> None:
    cb = CircuitBreaker()
    assert await cb.allow("upstream:kling")
    for _ in range(4):
        await cb.on_failure("upstream:kling")
        assert await cb.allow("upstream:kling")      # 未到阈值仍放行
    await cb.on_failure("upstream:kling")            # 第 5 次 → open
    assert not await cb.allow("upstream:kling")
    assert await cb.state("upstream:kling") == "open"


async def test_circuit_half_open_single_probe(_redis: FakeRedis) -> None:
    cb = CircuitBreaker()
    for _ in range(5):
        await cb.on_failure("upstream:kling")
    # 冷却期满（回拨 opened_at）
    _redis.hashes["circuit:upstream:kling"]["opened_at"] = "0"
    assert await cb.allow("upstream:kling")          # 抢到探针权
    assert not await cb.allow("upstream:kling")      # 第二家副本抢不到（NX）
    # 探针失败 → 立即重新 open
    await cb.on_failure("upstream:kling")
    assert not await cb.allow("upstream:kling")
    # 再冷却 + 探针成功 → closed
    _redis.hashes["circuit:upstream:kling"]["opened_at"] = "0"
    assert await cb.allow("upstream:kling")
    await cb.on_success("upstream:kling")
    assert await cb.state("upstream:kling") == "closed"
    assert await cb.allow("upstream:kling")


async def test_circuit_success_resets_fail_count(_redis: FakeRedis) -> None:
    cb = CircuitBreaker()
    for _ in range(4):
        await cb.on_failure("upstream:kling")
    await cb.on_success("upstream:kling")
    for _ in range(4):
        await cb.on_failure("upstream:kling")
    assert await cb.allow("upstream:kling")          # 计数已清零，未开闸


# ---------------------------------------------------------------------------
# 限流（§8.3）
# ---------------------------------------------------------------------------


async def test_user_rate_limit_sliding_window(_redis: FakeRedis) -> None:
    token = make_token()
    cfg = make_biz(rate_limit={"user_rpm": 2})
    await check_user_rate_limit(token, cfg)
    await check_user_rate_limit(token, cfg)
    with pytest.raises(errors.GatewayError) as ei:
        await check_user_rate_limit(token, cfg)
    assert ei.value.status_code == 429
    assert "Retry-After" in (ei.value.headers or {})


async def test_biz_rate_limit_token_bucket(_redis: FakeRedis) -> None:
    cfg = make_biz(rate_limit={"biz_rpm": 2})
    await check_biz_rate_limit(cfg)
    await check_biz_rate_limit(cfg)
    with pytest.raises(errors.GatewayError) as ei:
        await check_biz_rate_limit(cfg)
    assert ei.value.status_code == 429
    # 未配置 biz_rpm → 不限
    await check_biz_rate_limit(make_biz())


async def test_upstream_slot_acquire_release(_redis: FakeRedis) -> None:
    from app.middleware import acquire_upstream_slot, release_upstream_slot

    cfg = make_biz(rate_limit={"upstream_concurrency": 1})
    assert await acquire_upstream_slot(cfg)
    assert not await acquire_upstream_slot(cfg)      # 已满 → 调用方 503 背压
    await release_upstream_slot(cfg.biz)
    assert await acquire_upstream_slot(cfg)
    # 未配置并发上限 → 恒放行
    assert await acquire_upstream_slot(make_biz())


# ---------------------------------------------------------------------------
# 幂等键（§8.4）
# ---------------------------------------------------------------------------


async def test_idempotency_replay_same_payload(_redis: FakeRedis) -> None:
    token = make_token()
    body = b'{"model":"kling-v2","prompt":"x"}'
    key = await idempotency_guard(
        _request({"Idempotency-Key": "idem-1"}, body), token)
    assert key == "idem-1"
    first_body = {"id": "task_1", "task_id": "task_1", "status": "queued"}
    await idempotency_complete(token, key, status_code=201, body=first_body)
    # 同 key 同 payload → 回放首个响应
    with pytest.raises(IdempotentReplay) as ei:
        await idempotency_guard(_request({"Idempotency-Key": "idem-1"}, body), token)
    assert ei.value.status_code == 201
    assert ei.value.body["task_id"] == "task_1"


async def test_idempotency_conflict_different_payload(_redis: FakeRedis) -> None:
    token = make_token()
    await idempotency_guard(_request({"Idempotency-Key": "k"}, b'{"a":1}'), token)
    with pytest.raises(errors.GatewayError) as ei:
        await idempotency_guard(_request({"Idempotency-Key": "k"}, b'{"a":2}'), token)
    assert ei.value.status_code == 409
    assert ei.value.error_type == "idempotency_error"


async def test_idempotency_in_progress_conflict(_redis: FakeRedis) -> None:
    """首请求仍在途（pending）→ 同 key 并发请求 409 拒绝。"""
    token = make_token()
    body = b'{"a":1}'
    await idempotency_guard(_request({"Idempotency-Key": "k"}, body), token)
    with pytest.raises(errors.GatewayError) as ei:
        await idempotency_guard(_request({"Idempotency-Key": "k"}, body), token)
    assert ei.value.status_code == 409


async def test_idempotency_no_header_returns_none(_redis: FakeRedis) -> None:
    assert await idempotency_guard(_request(), make_token()) is None


async def test_idempotency_release_allows_retry(_redis: FakeRedis) -> None:
    """提交失败释放占位后，同 key 可重新占位（402 不落库场景）。"""
    token = make_token()
    body = b'{"a":1}'
    key = await idempotency_guard(_request({"Idempotency-Key": "k"}, body), token)
    assert key is not None
    await idempotency_release(token, key)
    assert await idempotency_guard(
        _request({"Idempotency-Key": "k"}, body), token) == "k"
    stored = json.loads(_redis.strings[f"idem:{token.user_id}:k"])
    assert stored["state"] == "pending"


# ---------------------------------------------------------------------------
# 欠费熔断名单（§5.6）
# ---------------------------------------------------------------------------


async def test_debt_block(_redis: FakeRedis) -> None:
    token = make_token()
    await check_debt_block(token)                    # 无欠费：放行
    _redis.strings[f"debt:{token.user_id}"] = "pt_x"
    with pytest.raises(errors.GatewayError) as ei:
        await check_debt_block(token)
    assert ei.value.status_code == 402
    assert ei.value.error_type == "billing_error"


async def test_module_singleton_exists() -> None:
    assert isinstance(circuit_breaker, CircuitBreaker)
