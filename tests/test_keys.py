"""keys 轮询微服务客户端测试（SPEC §3.12；契约 V21 建议验证）。

覆盖：acquire 200 租约、进程内缓存命中（第二次无 HTTP）、5xx/超时/坏报文/
字段不齐 → 返回 None 并由 ``resolve_submit_secrets`` 降级 env 静态密钥、
``KEYS_SERVICE_URL`` 未配置整体禁用、上游 401/403 由适配器触发
report(ok=False) 且缓存立即剔除。

纪律：一律使用 conftest 的 ``respx_router`` fixture（裸 ``@respx.mock``
带参会建独立 router，拦不到 app.http_clients 单例）。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import respx

import app.http_clients as http_clients
from app.adapters.base import CanonicalTaskRequest, SubmitContext, UpstreamBizError
from app.adapters.kling import KlingAdapter
from app.config import settings
from app.keys import KeyLease, key_provider
from app.registry import BizConfig
from app.tasks.manager import resolve_submit_secrets

KEYS_BASE = "http://keys.test"
UPSTREAM_BASE = "https://upstream.example.com"


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """隔离：清租约缓存、重置 keys httpx 单例（base_url 创建期烘焙）。"""
    key_provider._cache.clear()
    monkeypatch.setattr(settings, "keys_service_url", KEYS_BASE)
    http_clients._clients.pop("keys", None)
    yield
    key_provider._cache.clear()
    http_clients._clients.pop("keys", None)


def _acquire_ok(**over: Any) -> httpx.Response:
    body: dict[str, Any] = {
        "key_id": "key-1",
        "credentials": {"ak": "ak-lease", "sk": "sk-lease"},
        "ttl": 60,
    }
    body.update(over)
    return httpx.Response(200, json=body)


def _biz_cfg() -> BizConfig:
    return BizConfig(
        biz="kling-biz",
        adapter="kling",
        upstream_base_url=UPSTREAM_BASE,
        auth_type="aksk_jwt",
        auth_secret_ref="UPSTREAM_SECRET_KLING",
        native_prefixes=["v1/videos"],
        enabled=True,
        billing_keys={"biz_type": "video", "metric": "call"},
        default_freeze_amount_usd="1.0",
        rate_limit={},
        newapi_channel_id=50,
        version=1,
    )


# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------


async def test_acquire_200_returns_lease(respx_router: respx.MockRouter) -> None:
    """acquire 200 → KeyLease（key_id/credentials/缓存 TTL 取 min(服务端, 配置)）。"""
    respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=_acquire_ok())
    lease = await key_provider.acquire("kling")
    assert lease is not None
    assert lease.key_id == "key-1"
    assert lease.credentials == {"ak": "ak-lease", "sk": "sk-lease"}
    assert lease.fresh()


async def test_acquire_cache_hit_no_second_http(
        respx_router: respx.MockRouter) -> None:
    """缓存命中：第二次 acquire 复用租约，不再发 HTTP。"""
    route = respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=_acquire_ok())
    first = await key_provider.acquire("kling")
    second = await key_provider.acquire("kling")
    assert first is second
    assert len(route.calls) == 1


async def test_acquire_expired_cache_refetches(
        respx_router: respx.MockRouter) -> None:
    """租约过期（expires_at 已过）→ 重新 acquire。"""
    route = respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=_acquire_ok())
    await key_provider.acquire("kling")
    key_provider._cache["kling"].expires_at = 0.0  # 强制过期
    await key_provider.acquire("kling")
    assert len(route.calls) == 2


async def test_acquire_5xx_env_fallback(
        monkeypatch: pytest.MonkeyPatch, respx_router: respx.MockRouter) -> None:
    """acquire 5xx → None；manager 层降级 env 静态密钥。"""
    respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=httpx.Response(503))
    assert await key_provider.acquire("kling") is None
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-env")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-env")
    assert await resolve_submit_secrets(_biz_cfg()) == {"ak": "ak-env", "sk": "sk-env"}


async def test_acquire_timeout_env_fallback(
        monkeypatch: pytest.MonkeyPatch, respx_router: respx.MockRouter) -> None:
    """acquire 传输异常/超时 → None → env 兜底。"""
    respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        side_effect=httpx.ConnectTimeout("boom"))
    assert await key_provider.acquire("kling") is None
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-env")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-env")
    assert await resolve_submit_secrets(_biz_cfg()) == {"ak": "ak-env", "sk": "sk-env"}


async def test_keys_url_unset_disables_and_env_fallback(
        monkeypatch: pytest.MonkeyPatch, respx_router: respx.MockRouter) -> None:
    """KEYS_SERVICE_URL 未配置 → acquire 恒 None（零 HTTP）→ env 兜底。"""
    monkeypatch.setattr(settings, "keys_service_url", None)
    route = respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=_acquire_ok())
    assert key_provider.enabled is False
    assert await key_provider.acquire("kling") is None
    assert len(route.calls) == 0
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-env")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-env")
    assert await resolve_submit_secrets(_biz_cfg()) == {"ak": "ak-env", "sk": "sk-env"}


async def test_acquire_bad_payload_env_fallback(
        monkeypatch: pytest.MonkeyPatch, respx_router: respx.MockRouter) -> None:
    """200 但字段不齐（缺 credentials）→ 契约异常 None → env 兜底。"""
    respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=httpx.Response(200, json={"key_id": "key-1"}))
    assert await key_provider.acquire("kling") is None
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-env")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-env")
    assert await resolve_submit_secrets(_biz_cfg()) == {"ak": "ak-env", "sk": "sk-env"}


async def test_resolve_incomplete_credentials_env_fallback(
        monkeypatch: pytest.MonkeyPatch, respx_router: respx.MockRouter) -> None:
    """租约凭证与 auth_type 不匹配（aksk_jwt 缺 sk）→ manager 判定无效回退 env。"""
    respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=_acquire_ok(credentials={"ak": "ak-only"}))
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-env")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-env")
    assert await resolve_submit_secrets(_biz_cfg()) == {"ak": "ak-env", "sk": "sk-env"}


async def test_resolve_uses_lease_credentials(
        respx_router: respx.MockRouter) -> None:
    """keys 凭证齐全时优先于 env（轮询微服务是主来源）。"""
    respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=_acquire_ok())
    assert await resolve_submit_secrets(_biz_cfg()) == {
        "ak": "ak-lease", "sk": "sk-lease"}


# ---------------------------------------------------------------------------
# report：适配器 401/403 触发，缓存立即剔除
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403])
async def test_adapter_auth_failure_reports_and_evicts(
        status: int, respx_router: respx.MockRouter) -> None:
    """上游 401/403 → 适配器触发 report(ok=False) + 租约缓存立即剔除。"""
    respx_router.get(f"{KEYS_BASE}/keys/kling/acquire").mock(
        return_value=_acquire_ok())
    report_route = respx_router.post(f"{KEYS_BASE}/keys/key-1/report").mock(
        return_value=httpx.Response(200))
    respx_router.post(f"{UPSTREAM_BASE}/v1/videos/text2video").mock(
        return_value=httpx.Response(status, json={"message": "bad key"}))

    lease = await key_provider.acquire("kling")
    assert lease is not None

    KlingAdapter._jwt_cache.clear()
    adapter = KlingAdapter()
    ctx = SubmitContext(
        biz="kling-biz", task_id="task_gw1",
        gateway_callback_url="https://gw.test/callbacks/kling-biz/kling/tok",
        upstream_base_url=UPSTREAM_BASE,
        secrets={"ak": lease.credentials["ak"], "sk": lease.credentials["sk"]},
    )
    req = CanonicalTaskRequest(model="kling-v2", prompt="p", action="text2video")
    with pytest.raises(UpstreamBizError):
        await adapter.submit(req, ctx)

    # 缓存同步剔除（下个请求重新 acquire）
    assert "kling" not in key_provider._cache
    # 上报任务已调度；让出事件循环直至出站请求完成
    for _ in range(20):
        await asyncio.sleep(0)
        if report_route.calls:
            break
    assert report_route.calls, "401/403 必须触发 POST /keys/{key_id}/report"
    sent = json.loads(report_route.calls.last.request.content)
    assert sent == {"ok": False, "status_code": status}


async def test_report_no_lease_is_noop(respx_router: respx.MockRouter) -> None:
    """无租约（env 兜底路径）→ report_auth_failure no-op，零 HTTP。"""
    route = respx_router.post(f"{KEYS_BASE}/keys/key-1/report").mock(
        return_value=httpx.Response(200))
    key_provider.report_auth_failure("kling", 401)
    for _ in range(5):
        await asyncio.sleep(0)
    assert len(route.calls) == 0


async def test_report_failure_only_warns(respx_router: respx.MockRouter) -> None:
    """report 尽力而为：keys 服务 5xx 不抛异常。"""
    key_provider._cache["kling"] = KeyLease(
        key_id="key-9", credentials={"ak": "a", "sk": "s"}, expires_at=1e18)
    respx_router.post(f"{KEYS_BASE}/keys/key-9/report").mock(
        return_value=httpx.Response(500))
    await key_provider.report("key-9", ok=False, status_code=401)  # 不抛
