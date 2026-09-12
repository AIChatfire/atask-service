"""``app/services/upstream`` 熔断件：**拦截**（不只是记账）与恢复。

为什么单独成篇：``test_upstream.py`` 随旧链路删除时被一并删掉，之后全仓再无用例
触发熔断——而 ``relay.call_upstream`` 每次出站前都走 ``breaker_guard``。只验证
「计数器 +1」不叫熔断；必须验证「达阈值后出站被真的拦下、一个包都不发」。

覆盖：
1. ``breaker_report`` 失败累加 / 成功清零（含窗口 TTL）；
2. ``breaker_guard`` 达阈值即抛 ``BreakerOpenError``；
3. **行为级**：熔断打开时 ``call_upstream`` 被拦截且零出站（respx 计数不增）；
4. 连续 5xx 达阈值 → 后续调用被拦（不是继续打靶）；
5. 阈值以下的一次成功会清零计数，后续调用恢复；
6. 传输错误也算失败（累加计数）。
"""

from __future__ import annotations

import httpx
import pytest

from app.redis import K_BREAKER
from app.services import relay, upstream

UP_BASE = "http://upstream.test"
UPSTREAM_HOST = "upstream.test"   # 熔断键取上游 host:port（relay._breaker_key）
KEY = K_BREAKER.format(host=UPSTREAM_HOST)


@pytest.fixture
def breaker_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings
    from app.services import dynconf

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    monkeypatch.setattr(settings, "upstream_breaker_threshold", 3)
    monkeypatch.setattr(settings, "upstream_breaker_window_seconds", 30)
    dynconf._invalidate()                  # 防上一用例的 5s 进程内缓存串味
    return settings


# ---------------------------------------------------------------------------
# 1. 记账：失败累加、成功清零、窗口 TTL
# ---------------------------------------------------------------------------


async def test_report_accumulates_failures_with_window_ttl(patch_redis, breaker_settings):
    breaker_settings.upstream_breaker_window_seconds = 17
    await upstream.breaker_report(UPSTREAM_HOST, ok=False)
    await upstream.breaker_report(UPSTREAM_HOST, ok=False)
    await upstream.breaker_report(UPSTREAM_HOST, ok=False)
    assert await patch_redis.get(KEY) == "3"
    ttl = await patch_redis.ttl(KEY)
    assert 0 < ttl <= 17                  # 窗口 TTL 落上，计数不会永久残留


async def test_report_success_clears_counter(patch_redis, breaker_settings):
    await upstream.breaker_report(UPSTREAM_HOST, ok=False)
    assert await patch_redis.get(KEY) == "1"
    await upstream.breaker_report(UPSTREAM_HOST, ok=True)
    assert await patch_redis.get(KEY) is None


# ---------------------------------------------------------------------------
# 2. 护栏：达阈值即打开
# ---------------------------------------------------------------------------


async def test_guard_opens_only_at_threshold(patch_redis, breaker_settings):
    breaker_settings.upstream_breaker_threshold = 3
    await patch_redis.set(KEY, 2)
    await upstream.breaker_guard(UPSTREAM_HOST)                     # 2 < 3：放行

    await patch_redis.set(KEY, 3)
    with pytest.raises(upstream.BreakerOpenError):
        await upstream.breaker_guard(UPSTREAM_HOST)                 # 3 >= 3：打开


async def test_guard_passes_when_no_counter(patch_redis, breaker_settings):
    await upstream.breaker_guard(UPSTREAM_HOST)                     # 无键 → 放行，不抛


# ---------------------------------------------------------------------------
# 3. 行为级：熔断打开时出站被真的拦下
# ---------------------------------------------------------------------------


async def test_call_upstream_blocked_without_any_outbound(
    patch_redis, breaker_settings, respx_router,
):
    """熔断打开 → ``BreakerOpenError``，且**一个出站包都不发**（不是先打靶再记一笔）。"""
    breaker_settings.upstream_breaker_threshold = 2
    await patch_redis.set(KEY, 2)

    with pytest.raises(upstream.BreakerOpenError):
        await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")

    assert len(respx_router.calls) == 0


async def test_consecutive_5xx_open_breaker_and_stop_targeting(
    patch_redis, breaker_settings, respx_router,
):
    """连续 5xx 达阈值：前两次真的打上游，第三次起被拦（不再继续打靶）。"""
    breaker_settings.upstream_breaker_threshold = 2
    route = respx_router.get(f"{UP_BASE}/v1/x").mock(
        return_value=httpx.Response(500, json={"error": "boom"})
    )

    status1, _, _ = await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")
    status2, _, _ = await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")
    assert (status1, status2) == (500, 500)
    assert len(route.calls) == 2

    with pytest.raises(upstream.BreakerOpenError):
        await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")
    assert len(route.calls) == 2                          # 第三次没发出去


# ---------------------------------------------------------------------------
# 4. 恢复
# ---------------------------------------------------------------------------


async def test_success_below_threshold_resets_and_recovers(
    patch_redis, breaker_settings, respx_router,
):
    breaker_settings.upstream_breaker_threshold = 5
    route = respx_router.get(f"{UP_BASE}/v1/x").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, json={"ok": True}),
            httpx.Response(200, json={"ok": True}),
        ]
    )

    s1, _, _ = await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")
    assert s1 == 500
    assert await patch_redis.get(KEY) == "1"

    s2, _, _ = await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")
    assert s2 == 200
    assert await patch_redis.get(KEY) is None             # 成功清零

    s3, _, _ = await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")
    assert s3 == 200                                      # 恢复后照常出站
    assert len(route.calls) == 3


async def test_transport_error_counts_toward_breaker(
    patch_redis, breaker_settings, respx_router,
):
    respx_router.get(f"{UP_BASE}/v1/x").mock(side_effect=httpx.ConnectError("boom"))
    with pytest.raises(relay.RelayError) as exc:
        await relay.call_upstream("GET", UP_BASE, "/v1/x", token="t")
    assert exc.value.status == 599
    assert await patch_redis.get(KEY) == "1"              # 传输错误也计入失败
