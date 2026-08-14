"""两微服务 provider 契约测试（respx 拦截，对齐真实服务 API）。

- keypool: POST /v1/keys/select（统一包络 + include_channel 渠道全量元数据，
           含 gateway 块 billing 计费规则）
           POST /v1/keys/report（Idempotency-Key，fire-and-forget）
- billing: /api/v1/auth/inspect、/billing/freeze、/billing/settle、/billing/cancel
           （settle/cancel 携带**用户令牌**——billing 只认令牌身份）

计费规则求值（asteval 沙箱）见 tests/test_pricing.py。
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from app.services.providers import (
    BillingError,
    KeyLeaseError,
)

# ---------------------------------------------------------------------------
# keypool
# ---------------------------------------------------------------------------

_SELECT_PAYLOAD = {
    "code": 0,
    "message": "ok",
    "data": {
        "channel_id": 7,
        "key_index": 2,
        "key": "sk-real-upstream-key",
        "base_url": "https://api.upstream.example",
        "epoch": "a1b2c3d4",
        "lease_id": "0123cdef",
        "channel": {
            "id": 7,
            "name": "upstream-a",
            "base_url": "https://api.upstream.example",
            "model_mapping": {"gpt-4o": "gpt-4o-2024-08-06"},
            "status_code_mapping": {"503": "500"},
            "header_override": {"X-Custom-Header": "v"},
            "param_override": {"temperature": 0.5},
            "openai_organization": "org-xxx",
            "setting": {
                "proxy": "http://127.0.0.1:7890",
                "gateway": {"submit_path": "/v2/video_generation"},
            },
        },
    },
}


async def test_keypool_lease_parses_full_channel(respx_router, test_settings):
    from app.services.providers.keypool import KeypoolProvider

    route = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT_PAYLOAD)
    )
    lease = await KeypoolProvider().lease("minimax", model="gpt-4o")

    # 请求契约：统一分组 keypool（GW_KEY_GROUP 默认）+ model + include_channel
    req_body = json.loads(route.calls.last.request.content)
    assert req_body["group"] == "keypool" and req_body["model"] == "gpt-4o"
    assert req_body["include_channel"] is True
    assert route.calls.last.request.headers["Authorization"] == "Bearer kp-token"

    # 响应契约：key/索引/指纹/租约 + 渠道全量覆盖字段
    assert lease.key == "sk-real-upstream-key"
    assert lease.key_id == 7 and lease.key_index == 2
    assert lease.base_url == "https://api.upstream.example"
    assert lease.epoch == "a1b2c3d4" and lease.lease_id == "0123cdef"
    assert lease.model_mapping == {"gpt-4o": "gpt-4o-2024-08-06"}
    assert lease.status_code_mapping == {"503": "500"}
    assert lease.header_override == {"X-Custom-Header": "v"}
    assert lease.param_override == {"temperature": 0.5}
    assert lease.proxy == "http://127.0.0.1:7890"
    assert lease.openai_organization == "org-xxx"
    assert lease.channel["setting"]["gateway"]["submit_path"] == "/v2/video_generation"


async def test_keypool_lease_channel_id_direct(respx_router, test_settings):
    from app.services.providers.keypool import KeypoolProvider

    route = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT_PAYLOAD)
    )
    await KeypoolProvider().lease("minimax", key_id=7)
    req_body = json.loads(route.calls.last.request.content)
    assert req_body["channel_id"] == 7
    assert "group" not in req_body           # channel_id 直达不带 group/model


async def test_keypool_no_available_key_40001(respx_router, test_settings):
    from app.services.providers.keypool import KeypoolProvider

    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(
            503, json={"code": 40001, "message": "no available key",
                       "data": {"retry_after_ms": 1000}})
    )
    with pytest.raises(KeyLeaseError, match="no available key"):
        await KeypoolProvider().lease("minimax", model="x")


async def test_keypool_auth_failure(respx_router, test_settings):
    from app.services.providers.keypool import KeypoolProvider

    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(401, json={"code": 40100, "message": "unauthorized"})
    )
    with pytest.raises(KeyLeaseError, match="auth failed"):
        await KeypoolProvider().lease("minimax", model="x")


async def test_keypool_report_body(respx_router, test_settings, key_lease_factory):
    from app.services.providers.keypool import KeypoolProvider

    route = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    lease = key_lease_factory(epoch="a1b2c3d4")
    await KeypoolProvider().report(lease, ok=False, status_code=401, error="bad key")
    await asyncio.sleep(0)                   # fire-and-forget：让背景任务发出请求
    await asyncio.sleep(0)

    assert route.calls, "report 必须以 fire-and-forget 方式发出"
    req = route.calls.last.request
    body = json.loads(req.content)
    assert body["channel_id"] == 7 and body["key_index"] == 0
    assert body["epoch"] == "a1b2c3d4"
    assert body["success"] is False and body["status_code"] == 401
    assert body["error_message"] == "bad key"
    assert req.headers["Idempotency-Key"]    # 幂等头存在


async def test_keypool_header_override_strips_nested_upstream(respx_router, test_settings):
    """header_override 嵌套 upstream 配置块：剥离后才是纯 HTTP 头（防 pydantic
    校验失败 / 防配置块被当成请求头透出）；原始 channel 保留供路由构建。"""
    from app.services.providers.keypool import KeypoolProvider
    from app.services.registry import route_from_lease

    payload = {
        "code": 0, "message": "ok",
        "data": {
            "channel_id": 7, "key_index": 0, "key": "sk-x",
            "base_url": "http://upstream.test", "epoch": "e1",
            "channel": {
                "id": 7, "name": "minimax-main",
                "header_override": {
                    "X-Channel-Tag": "paid",
                    "upstream": {"biz": "minimax", "submit_path": "/v2/video_generation"},
                },
            },
        },
    }
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=payload)
    )
    lease = await KeypoolProvider().lease("minimax", model="MiniMax-H3")

    # 剥离：KeyLease.header_override 只剩纯标量头，无嵌套配置块
    assert lease.header_override == {"X-Channel-Tag": "paid"}
    # 原始 channel 保留：路由构建仍能从 upstream 块读出网关配置
    route = route_from_lease("minimax", lease)
    assert route.biz == "minimax"
    assert route.submit_path == "/v2/video_generation"


# ---------------------------------------------------------------------------
# billing
# ---------------------------------------------------------------------------


async def test_billing_inspect(respx_router, test_settings):
    from app.services.providers.billing_newapi import NewapiBillingProvider

    respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 123, "token_id": 45})
    )
    identity = await NewapiBillingProvider().inspect("sk-user-token")
    assert identity and identity.user_id == 123 and identity.token_id == 45

    respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(401)
    )
    assert await NewapiBillingProvider().inspect("sk-bad") is None


async def test_billing_freeze_ok_and_402(respx_router, test_settings):
    from app.services.providers.billing_newapi import NewapiBillingProvider

    route = respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"request_id": "r-1"}})
    )
    provider = NewapiBillingProvider()
    out = await provider.freeze(
        raw_token="sk-u", request_id="r-1", biz_type="video_generation",
        metric="second", amount=0.13, ttl_seconds=1800, attrs={"model": "MiniMax-H3"},
    )
    assert out["request_id"] == "r-1"
    req = route.calls.last.request
    assert req.headers["Authorization"] == "Bearer sk-u"
    body = json.loads(req.content)
    assert body["amount"] == 0.13 and body["ttl_seconds"] == 1800

    respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(402, json={"error": "insufficient balance"})
    )
    with pytest.raises(BillingError) as exc_info:
        await provider.freeze(
            raw_token="sk-u", request_id="r-2", biz_type="b", metric="m",
            amount=1, ttl_seconds=60,
        )
    assert exc_info.value.status == 402 and not exc_info.value.retryable


async def test_billing_settle_and_cancel_use_user_token(respx_router, test_settings):
    from app.services.providers.billing_newapi import NewapiBillingProvider

    settle = respx_router.post("http://billing.test/api/v1/billing/settle").mock(
        return_value=httpx.Response(200, json={"data": {"settled_amount": 5200}})
    )
    cancel = respx_router.post("http://billing.test/api/v1/billing/cancel").mock(
        return_value=httpx.Response(200, json={"data": {"status": "cancelled"}})
    )
    provider = NewapiBillingProvider()
    await provider.settle(raw_token="sk-user-1", request_id="r-1",
                          actual_amount=0.0104, units=4, attrs={"duration": 4})
    await provider.cancel(raw_token="sk-user-2", request_id="r-2")

    # 关键契约：settle/cancel 用**用户令牌**（跨用户 403），不是服务账号
    assert settle.calls.last.request.headers["Authorization"] == "Bearer sk-user-1"
    assert cancel.calls.last.request.headers["Authorization"] == "Bearer sk-user-2"
    body = json.loads(settle.calls.last.request.content)
    assert body["actual_amount"] == 0.0104 and body["units"] == 4
