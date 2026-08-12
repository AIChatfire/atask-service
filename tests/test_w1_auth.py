"""W1 认证测试：委托计费服务鉴权 + 委托结论缓存 + 系统跳过开关 + sksess。

覆盖：KEY_RE 格式门禁、委托 200/401/403/5xx（fail-closed 503）、L1/L2 缓存
命中与回填、skip 头三态（未配置系统令牌忽略/令牌错误按正常流程/正确生效）、
sksess 写入/取回/终态清除、防 IDOR。
"""

from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import respx
from starlette.requests import Request
from w1_helpers import FakeRedis, make_session, make_session_factory, make_token

from app import auth, errors
from app.auth import (
    BillingAuthUnavailable,
    clear_user_sk,
    current_token,
    get_owned_task,
    get_user_sk_for_task,
    local_cache,
    store_user_sk,
    token_hash,
    verify_bearer,
)
from app.config import settings

RAW = "sk-" + "b" * 48
H = token_hash(RAW)
BASE = "http://127.0.0.1:8080"  # settings.billing_service_url 默认值
BALANCE_URL = f"{BASE}/api/v1/billing/balance"


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    local_cache.clear()
    fake = FakeRedis()
    monkeypatch.setattr(auth, "get_redis", AsyncMock(return_value=fake))
    monkeypatch.setattr(settings, "system_api_token", None)
    return fake


def _request(headers: dict[str, str] | None = None) -> Request:
    raw_headers = [
        (k.lower().encode(), v.encode()) for k, v in (headers or {}).items()
    ]
    return Request({
        "type": "http", "method": "POST", "path": "/kling/v1/videos",
        "headers": raw_headers, "client": ("203.0.113.7", 52000),
        "query_string": b"", "server": ("test", 80), "scheme": "http",
    })


def _balance_ok(user_id: int = 7, group: str = "vip") -> httpx.Response:
    return httpx.Response(200, json={"data": {"user_id": user_id,
                                              "group": group, "balance": 100}})


# ---------------------------------------------------------------------------
# 委托管线
# ---------------------------------------------------------------------------


async def test_format_gate_rejects_without_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """① 格式门禁：畸形 key 无 I/O 快拒。"""
    spy = AsyncMock(side_effect=AssertionError("must not touch redis"))
    monkeypatch.setattr(auth, "get_redis", spy)
    assert await verify_bearer("not-a-key") is None
    assert await verify_bearer("sk-tooshort") is None
    assert await verify_bearer("sk-" + "x" * 65 + "!") is None
    spy.assert_not_awaited()


async def test_delegate_success(respx_router: respx.MockRouter) -> None:
    """④ 委托 200 → user_id/group；请求透传用户 sk。"""
    route = respx_router.get(BALANCE_URL).mock(return_value=_balance_ok())
    info = await verify_bearer(RAW)
    assert info is not None
    assert info.user_id == 7 and info.group == "vip" and info.raw == RAW
    assert info.sk_hash == H
    req = route.calls[0].request
    assert req.headers["Authorization"] == f"Bearer {RAW}"


async def test_delegate_flat_payload_and_default_group(
        respx_router: respx.MockRouter) -> None:
    """响应体兼容平铺形态；group 缺省回退 default。"""
    respx_router.get(BALANCE_URL).mock(
        return_value=httpx.Response(200, json={"user_id": 9}))
    info = await verify_bearer(RAW)
    assert info is not None and info.user_id == 9 and info.group == "default"


@pytest.mark.parametrize("status", [401, 403])
async def test_delegate_invalid_token(status: int, respx_router: respx.MockRouter) -> None:
    """委托 401/403 → 无效 token（None，且不回填缓存）。"""
    respx_router.get(BALANCE_URL).mock(return_value=httpx.Response(status))
    assert await verify_bearer(RAW) is None
    assert local_cache.get(H) is None


async def test_delegate_5xx_fails_closed(respx_router: respx.MockRouter) -> None:
    """计费服务 5xx → BillingAuthUnavailable（fail-closed 503，绝不放行）。"""
    respx_router.get(BALANCE_URL).mock(return_value=httpx.Response(500))
    with pytest.raises(BillingAuthUnavailable):
        await verify_bearer(RAW)


async def test_delegate_timeout_fails_closed(respx_router: respx.MockRouter) -> None:
    """计费服务超时/传输异常 → fail-closed。"""
    respx_router.get(BALANCE_URL).mock(side_effect=httpx.ConnectTimeout("boom"))
    with pytest.raises(BillingAuthUnavailable):
        await verify_bearer(RAW)


async def test_delegate_bad_payload_fails_closed(
        respx_router: respx.MockRouter) -> None:
    """200 但报文无 user_id → 契约异常 fail-closed（不当作无效 token）。"""
    respx_router.get(BALANCE_URL).mock(
        return_value=httpx.Response(200, json={"data": {}}))
    with pytest.raises(BillingAuthUnavailable):
        await verify_bearer(RAW)


# ---------------------------------------------------------------------------
# 委托结论缓存（L1/L2）
# ---------------------------------------------------------------------------


async def test_l1_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    """③a L1 进程内缓存命中：不触 Redis、不发起委托。"""
    local_cache.set(H, make_token(user_id=7, raw=""))
    spy = AsyncMock(side_effect=AssertionError("L1 hit must not touch redis"))
    monkeypatch.setattr(auth, "get_redis", spy)
    info = await verify_bearer(RAW)
    assert info is not None and info.user_id == 7 and info.raw == RAW
    spy.assert_not_awaited()


async def test_l2_hit_backfills_l1(_clean: FakeRedis) -> None:
    """③b L2 Redis 命中：raw 由请求头补回、L1 回填、不发起委托。"""
    _clean.strings[f"apikey:{H}"] = make_token(user_id=7, raw="").dump()
    info = await verify_bearer(RAW)
    assert info is not None and info.raw == RAW and info.user_id == 7
    assert local_cache.get(H) is not None


async def test_delegate_success_writes_l1_l2(
        _clean: FakeRedis, respx_router: respx.MockRouter) -> None:
    """委托 200 → 回填 L2(apikey:{h}, 不落 raw)/L1；二次调用命中 L1 不再委托。"""
    route = respx_router.get(BALANCE_URL).mock(return_value=_balance_ok())
    info = await verify_bearer(RAW)
    assert info is not None
    blob = _clean.strings.get(f"apikey:{H}")
    assert blob is not None
    assert json.loads(blob)["sk_hash"] == H and "raw" not in json.loads(blob)
    assert local_cache.get(H) is not None
    again = await verify_bearer(RAW)
    assert again is not None and len(route.calls) == 1  # L1 命中，未再委托


# ---------------------------------------------------------------------------
# current_token / 系统跳过开关三态
# ---------------------------------------------------------------------------


async def test_current_token_missing_header() -> None:
    with pytest.raises(errors.GatewayError) as ei:
        await current_token(_request(), None)
    assert ei.value.status_code == 401
    assert ei.value.error_type == "authentication_error"


async def test_current_token_invalid(respx_router: respx.MockRouter) -> None:
    respx_router.get(BALANCE_URL).mock(return_value=httpx.Response(401))
    with pytest.raises(errors.GatewayError) as ei:
        await current_token(_request(), f"Bearer {RAW}")
    assert ei.value.status_code == 401


async def test_current_token_ok(respx_router: respx.MockRouter) -> None:
    respx_router.get(BALANCE_URL).mock(return_value=_balance_ok())
    token = await current_token(_request(), f"Bearer {RAW}")
    assert token.user_id == 7 and not token.is_system


async def test_current_token_billing_down_is_503(
        respx_router: respx.MockRouter) -> None:
    """计费服务不可达 → 503 背压（fail-closed），非 401 非放行。"""
    respx_router.get(BALANCE_URL).mock(return_value=httpx.Response(503))
    with pytest.raises(errors.GatewayError) as ei:
        await current_token(_request(), f"Bearer {RAW}")
    assert ei.value.status_code == 503


async def test_skip_header_ignored_without_system_token_configured(
        respx_router: respx.MockRouter) -> None:
    """三态①：SYSTEM_API_TOKEN 未配置 → 头一律忽略，按正常 sk 流程走。"""
    respx_router.get(BALANCE_URL).mock(return_value=_balance_ok())
    headers = {"X-Skip-Auth-Billing": "true", "X-System-Token": "anything"}
    with pytest.raises(errors.GatewayError) as ei:
        await current_token(_request(headers), None)  # 无 Bearer → 401
    assert ei.value.status_code == 401
    token = await current_token(_request(headers), f"Bearer {RAW}")
    assert not token.is_system and token.user_id == 7  # 正常委托结果


async def test_skip_header_wrong_system_token_falls_back(
        monkeypatch: pytest.MonkeyPatch, respx_router: respx.MockRouter) -> None:
    """三态②：系统令牌错误 → 忽略头按正常 sk 流程走（不报错，防探测）。"""
    monkeypatch.setattr(settings, "system_api_token", "s3cret")
    respx_router.get(BALANCE_URL).mock(return_value=_balance_ok())
    headers = {"X-Skip-Auth-Billing": "true", "X-System-Token": "wrong"}
    token = await current_token(_request(headers), f"Bearer {RAW}")
    assert not token.is_system and token.user_id == 7


async def test_skip_header_valid_system_token(
        monkeypatch: pytest.MonkeyPatch, respx_router: respx.MockRouter) -> None:
    """三态③：正确系统令牌 → system 身份（user_id=0），无需 Bearer、不委托。"""
    monkeypatch.setattr(settings, "system_api_token", "s3cret")
    route = respx_router.get(BALANCE_URL).mock(
        side_effect=AssertionError("skip path must not delegate"))
    headers = {"X-Skip-Auth-Billing": "true", "X-System-Token": "s3cret"}
    token = await current_token(_request(headers), None)
    assert token.is_system and token.user_id == 0 and token.raw == ""
    assert len(route.calls) == 0


# ---------------------------------------------------------------------------
# sksess（后台流程 user_sk）
# ---------------------------------------------------------------------------


async def test_sksess_store_get_clear(_clean: FakeRedis) -> None:
    """sksess 写入（EX=deadline+1h）→ 取回 → 终态清除。"""
    deadline = int(time.time()) + 3600
    captured: dict[str, Any] = {}

    async def set_spy(key: str, value: Any, **kw: Any) -> bool:
        captured.update(key=key, value=value, **kw)
        return await FakeRedis.set(_clean, key, value, **kw)

    monkey_redis = _clean
    monkey_redis.set = set_spy  # type: ignore[method-assign]
    await store_user_sk("task_x", RAW, deadline)
    assert captured["key"] == "sksess:task_x" and captured["value"] == RAW
    assert abs(int(captured["ex"]) - (3600 + 3600)) <= 2  # deadline+1h 宽限
    assert await get_user_sk_for_task("task_x") == RAW
    await clear_user_sk("task_x")
    assert await get_user_sk_for_task("task_x") is None


async def test_sksess_missing_returns_none() -> None:
    assert await get_user_sk_for_task("task_nope") is None


# ---------------------------------------------------------------------------
# get_owned_task（防 IDOR；system 豁免）
# ---------------------------------------------------------------------------


def _patch_db(monkeypatch: pytest.MonkeyPatch, row: dict | None) -> AsyncMock:
    session = make_session(row)
    monkeypatch.setattr(auth, "get_session_factory",
                        lambda: make_session_factory(session))
    return session


async def test_get_owned_task_idor_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    """防 IDOR：非属主与不存在都返回 404（防存在性探测）。"""
    row = {"task_id": "task_x", "user_id": 999, "platform": "gw_kling",
           "status": "SUCCESS"}
    _patch_db(monkeypatch, row)
    with pytest.raises(errors.GatewayError) as ei:
        await get_owned_task("task_x", make_token())      # user_id=7 ≠ 999
    assert ei.value.status_code == 404

    _patch_db(monkeypatch, None)
    with pytest.raises(errors.GatewayError) as ei2:
        await get_owned_task("task_x", make_token())
    assert ei2.value.status_code == 404


async def test_get_owned_task_ok_and_platform_scope(
        monkeypatch: pytest.MonkeyPatch) -> None:
    row = {"task_id": "task_x", "user_id": 7, "platform": "gw_kling",
           "status": "SUCCESS", "progress": "100%", "properties": "{}",
           "private_data": "{}", "data": "{}", "fail_reason": "",
           "created_at": 1, "updated_at": 2, "finish_time": 3}
    session = _patch_db(monkeypatch, row)
    out = await get_owned_task("task_x", make_token())
    assert out["task_id"] == "task_x"
    sql = session.execute.await_args.args[0].text
    assert r"platform LIKE 'gw\_%'" in sql      # 只读自有行（§4.1 纪律）


async def test_get_owned_task_system_bypasses_ownership(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """system 身份豁免归属校验（运维排障）；不存在的任务仍 404。"""
    row = {"task_id": "task_x", "user_id": 999, "platform": "gw_kling",
           "status": "SUCCESS", "progress": "100%", "properties": "{}",
           "private_data": "{}", "data": "{}", "fail_reason": "",
           "created_at": 1, "updated_at": 2, "finish_time": 3}
    _patch_db(monkeypatch, row)
    sys_token = make_token(user_id=0, sk_hash="system", raw="", is_system=True)
    out = await get_owned_task("task_x", sys_token)
    assert out["task_id"] == "task_x"

    _patch_db(monkeypatch, None)
    with pytest.raises(errors.GatewayError) as ei:
        await get_owned_task("task_x", sys_token)
    assert ei.value.status_code == 404
