"""W3 计费服务客户端测试（SPEC §3.11.1；respx 拦 httpx 单例，无真实 HTTP）。

覆盖：freeze/settle/cancel/charge/balance/get_freeze 请求形制（路径、透传
Bearer、金额字符串、ttl 截断）、402→InsufficientBalance、409→BillingLockBusy
退避重试（retry_after_ms 消费、≤5 次）、5xx 不重试向上抛。
"""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
import respx

from app.billing.client import (
    BillingLockBusy,
    BillingServiceClient,
    InsufficientBalance,
)

BASE = "http://127.0.0.1:8080"  # settings.billing_service_url 默认值
SK = "sk-user-token-0123456789abcdef"


@pytest.fixture
def client() -> BillingServiceClient:
    return BillingServiceClient()


@respx.mock
async def test_freeze_request_shape_and_ttl_truncation(client: BillingServiceClient) -> None:
    route = respx.post(f"{BASE}/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}})
    )
    out = await client.freeze(
        request_id="task_abc:0", biz_type="kling_video", metric="call",
        amount_usd=Decimal("1.234567"), ttl_seconds=999_999, user_sk=SK,
        attrs={"model": "kling-v3"},
    )
    assert out == {"status": "frozen"}
    req = route.calls[0].request
    assert req.headers["Authorization"] == f"Bearer {SK}"  # 透传用户 sk-
    body = __import__("json").loads(req.content)
    assert body["request_id"] == "task_abc:0"  # 分片语义 {task_id}:{seq}
    assert body["amount"] == "1.234567"  # 金额字符串序列化
    assert body["ttl_seconds"] == 86400  # >86400 截断
    assert body["biz_type"] == "kling_video" and body["metric"] == "call"
    assert body["attrs"] == {"model": "kling-v3"}


@respx.mock
async def test_settle_cancel_charge_shapes(client: BillingServiceClient) -> None:
    settle = respx.post(f"{BASE}/api/v1/billing/settle").mock(
        return_value=httpx.Response(200, json={"data": {"settled_amount": 100}})
    )
    cancel = respx.post(f"{BASE}/api/v1/billing/cancel").mock(
        return_value=httpx.Response(200, json={"data": {"status": "cancelled"}})
    )
    charge = respx.post(f"{BASE}/api/v1/billing/charge").mock(
        return_value=httpx.Response(200, json={"data": {"would_succeed": True}})
    )
    import json

    await client.settle(request_id="task_a:2", actual_usd=Decimal("0.5"), user_sk=SK)
    assert json.loads(settle.calls[0].request.content) == {
        "request_id": "task_a:2", "actual_amount": "0.5", "attrs": {}}

    await client.cancel(request_id="task_a:1", user_sk=SK)
    assert json.loads(cancel.calls[0].request.content) == {"request_id": "task_a:1"}

    out = await client.charge(request_id="pt:7:abc", biz_type="kling_video",
                              metric="call", amount_usd=Decimal("0.01"),
                              user_sk=SK, verify_only=True)
    assert out == {"would_succeed": True}
    assert json.loads(charge.calls[0].request.content)["verify_only"] is True



@respx.mock
async def test_balance_and_get_freeze(client: BillingServiceClient) -> None:
    bal = respx.get(f"{BASE}/api/v1/billing/balance").mock(
        return_value=httpx.Response(200, json={"data": {"balance_usd": "9.5"}})
    )
    frz = respx.get(f"{BASE}/api/v1/billing/freeze/task_a:0").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}})
    )
    assert await client.balance(user_sk=SK) == {"balance_usd": "9.5"}
    assert await client.get_freeze(request_id="task_a:0", user_sk=SK) == {"status": "frozen"}
    assert bal.calls[0].request.headers["Authorization"] == f"Bearer {SK}"
    assert frz.calls[0].request.headers["Authorization"] == f"Bearer {SK}"


@respx.mock
async def test_402_raises_insufficient_balance(client: BillingServiceClient) -> None:
    respx.post(f"{BASE}/api/v1/billing/freeze").mock(
        return_value=httpx.Response(402, json={"error": "insufficient"})
    )
    with pytest.raises(InsufficientBalance):
        await client.freeze(request_id="task_a:0", biz_type="b", metric="call",
                            amount_usd=Decimal("1"), ttl_seconds=100, user_sk=SK)


@respx.mock
async def test_409_retry_consumes_retry_after_ms(client: BillingServiceClient) -> None:
    route = respx.post(f"{BASE}/api/v1/billing/settle").mock(
        side_effect=[
            httpx.Response(409, json={"retry_after_ms": 1}),
            httpx.Response(200, json={"data": {"settled": True}}),
        ]
    )
    out = await client.settle(request_id="task_a:0", actual_usd=Decimal("1"), user_sk=SK)
    assert out == {"settled": True}
    assert len(route.calls) == 2  # 同 request_id 退避重试后成功


@respx.mock
async def test_409_exhausts_five_attempts(client: BillingServiceClient) -> None:
    route = respx.post(f"{BASE}/api/v1/billing/cancel").mock(
        return_value=httpx.Response(409, json={"retry_after_ms": 1})
    )
    with pytest.raises(BillingLockBusy) as exc_info:
        await client.cancel(request_id="task_a:0", user_sk=SK)
    assert exc_info.value.retry_after_ms == 1
    assert len(route.calls) == 5  # tenacity ≤5 次


@respx.mock
async def test_409_without_retry_after_ms_uses_default(client: BillingServiceClient) -> None:
    route = respx.post(f"{BASE}/api/v1/billing/cancel").mock(
        side_effect=[
            httpx.Response(409, json={}),
            httpx.Response(200, json={"data": {"ok": 1}}),
        ]
    )
    out = await client.cancel(request_id="task_a:0", user_sk=SK)
    assert out == {"ok": 1}
    assert len(route.calls) == 2


@respx.mock
async def test_5xx_propagates_without_retry(client: BillingServiceClient) -> None:
    route = respx.post(f"{BASE}/api/v1/billing/settle").mock(
        return_value=httpx.Response(500, json={"error": "boom"})
    )
    with pytest.raises(httpx.HTTPStatusError):
        await client.settle(request_id="task_a:0", actual_usd=Decimal("1"), user_sk=SK)
    assert len(route.calls) == 1  # 5xx 不重试（调用方入 outbox）


@respx.mock
async def test_other_4xx_not_retried(client: BillingServiceClient) -> None:
    route = respx.post(f"{BASE}/api/v1/billing/cancel").mock(
        return_value=httpx.Response(400, json={"error": "bad state"})
    )
    with pytest.raises(httpx.HTTPStatusError):
        await client.cancel(request_id="task_a:0", user_sk=SK)
    assert len(route.calls) == 1  # 绝不重试其他 4xx
