"""W1 透传测试：鉴权改写、白名单 404、callback_url 改写、超时 504、
计费闭环（charge/欠费三连/GET 不计费/verify_only 预检）、passthrough_tracked
挂钩。零自有表（决策 A-4/A-9）：欠费单与 outbox 入 Redis。"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx
from w1_helpers import (
    FakeRedis,
    build_test_app,
    make_biz,
    make_orm_session,
    make_token,
)

import app.billing.outbox as obx
import app.middleware as mw
from app.adapters.base import register
from app.routing import dynamic_router as dr
from app.routing import videos

TOKEN = make_token()
BIZ_CFG = make_biz()


class FakeAdapter:
    """测试适配器（SPEC §3.2 协议最小实现）。"""

    name = "fakeadp"
    callback_capability = True
    echoes_external_task_id = True

    def auth_headers(self, cfg: Any) -> dict[str, str]:
        return {"Authorization": "Bearer upstream-secret-key"}

    def rewrite_callback_url(self, raw_body: bytes, cfg: Any) -> bytes:
        try:
            payload = json.loads(raw_body)
        except Exception:
            return raw_body
        payload["callback_url"] = "https://gw.example.com/callbacks/rewritten"
        return json.dumps(payload).encode()


register(FakeAdapter())  # 进程内注册表；本测试模块唯一使用者


@pytest.fixture(autouse=True)
def _deps(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    fake = FakeRedis()
    monkeypatch.setattr(mw, "get_redis", AsyncMock(return_value=fake))
    monkeypatch.setattr(dr, "get_redis", AsyncMock(return_value=fake),
                        raising=False)
    monkeypatch.setattr(dr.registry, "get", AsyncMock(return_value=BIZ_CFG))
    # _charge_request_id / _record_debt 内部延迟 import 的 get_redis
    import app.redis_client as rc
    monkeypatch.setattr(rc, "get_redis", AsyncMock(return_value=fake))
    # outbox/debt 入 Redis（决策 A-4/A-9）：同一 fake
    monkeypatch.setattr(obx, "get_redis", AsyncMock(return_value=fake))
    return fake


@pytest.fixture
def billing() -> tuple[MagicMock, MagicMock]:
    pricing = MagicMock()
    pricing.get_logic = AsyncMock(return_value="logic")
    pricing.evaluate = AsyncMock(return_value=Decimal("0.5"))
    billing = MagicMock()
    billing.charge = AsyncMock(return_value={"ok": True})
    dr.set_passthrough_billing(pricing, billing)
    yield pricing, billing
    dr.set_passthrough_billing(None, None)


@pytest.fixture
def tm() -> MagicMock:
    fake = MagicMock()
    fake.track_passthrough_task = AsyncMock(return_value="task_pt")
    videos.set_task_manager(fake)
    yield fake
    videos.set_task_manager(None)  # type: ignore[arg-type]


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test")


def _app(session: Any = None) -> Any:
    return build_test_app(dr.router, token=TOKEN,
                          session=session if session is not None else make_orm_session())


# ---------------------------------------------------------------------------
# 鉴权改写 + 计费闭环
# ---------------------------------------------------------------------------


@respx.mock
async def test_passthrough_auth_rewrite_and_charge(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock) -> None:
    """摘用户 Bearer/注入上游凭证、callback_url 改写、X-Gateway-Biz、charge+tracked。"""
    route = respx.post("http://upstream.test/v1/videos/text2video").respond(
        200, json={"task_id": "up-1", "usage": {"completion_tokens": 10}})
    session = make_orm_session()
    async with _client(_app(session)) as c:
        resp = await c.post(
            "/kling/v1/videos/text2video",
            headers={"Authorization": "Bearer sk-user-token",
                     "Idempotency-Key": "cli-key-1"},
            json={"model_name": "kling-v2", "prompt": "p",
                  "callback_url": "https://evil.example.com/hook"},
        )
    assert resp.status_code == 200
    assert resp.headers["X-Gateway-Biz"] == "kling"
    assert resp.json()["task_id"] == "up-1"              # 响应原样回传

    upstream_req = route.calls.last.request
    # 鉴权改写：用户 sk- 被摘除，注入上游凭证（§3.4①）
    assert upstream_req.headers["Authorization"] == "Bearer upstream-secret-key"
    # callback_url 默认改写回网关（用户回调由 §7.2 透传）
    sent = json.loads(upstream_req.content)
    assert sent["callback_url"] == "https://gw.example.com/callbacks/rewritten"

    # 计费闭环：POST 2xx → charge（带 Idempotency-Key 的 request_id 规则）
    _, billing_client = billing
    expected = f"pt:{TOKEN.user_id}:{hashlib.sha256(b'cli-key-1').hexdigest()}"
    # 两次调用：①verify_only 预检（决策 A-9，上游调用前验余额）②正式 charge
    assert billing_client.charge.await_count == 2
    pre = billing_client.charge.await_args_list[0].kwargs
    assert pre["request_id"] == f"{expected}:vo"
    assert pre["verify_only"] is True
    kwargs = billing_client.charge.await_args_list[1].kwargs
    assert kwargs["request_id"] == expected
    assert kwargs["amount_usd"] == Decimal("0.5")
    assert kwargs["user_sk"] == TOKEN.raw                # 透传用户令牌（§5.1）

    # 响应含上游 task_id → passthrough_tracked 挂钩（W2 提供实现）
    tm.track_passthrough_task.assert_awaited_once()
    assert tm.track_passthrough_task.await_args.kwargs["upstream_task_id"] == "up-1"


@respx.mock
async def test_passthrough_generated_request_id_reused(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock,
        _deps: FakeRedis) -> None:
    """未带 Idempotency-Key：pt_{ulid} 并写 idem:{user}:{req_hash} 复用。"""
    respx.post("http://upstream.test/v1/videos/text2video").respond(
        200, json={"task_id": "up-1"})
    payload = {"model_name": "kling-v2", "prompt": "p"}
    _, billing_client = billing
    async with _client(_app()) as c:
        await c.post("/kling/v1/videos/text2video", json=payload)
        await c.post("/kling/v1/videos/text2video", json=payload)
    ids = [call.kwargs["request_id"] for call in billing_client.charge.await_args_list
           if not call.kwargs.get("verify_only")]          # 排除 verify_only 预检调用
    assert ids[0] == ids[1] and ids[0].startswith("pt_")   # 重试命中复用


@respx.mock
async def test_passthrough_get_not_charged(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock) -> None:
    """GET 查询类默认不计费（§5.6）；debt 名单不阻断查询。"""
    respx.get("http://upstream.test/v1/videos/up-1").respond(200, json={"id": "up-1"})
    _, billing_client = billing
    async with _client(_app()) as c:
        resp = await c.get("/kling/v1/videos/up-1")
    assert resp.status_code == 200
    billing_client.charge.assert_not_awaited()
    tm.track_passthrough_task.assert_not_awaited()       # GET 不落 tracked 行


@respx.mock
async def test_passthrough_get_charge_on_get(
        monkeypatch: pytest.MonkeyPatch,
        billing: tuple[MagicMock, MagicMock], tm: MagicMock) -> None:
    """billing_keys.charge_on_get=true 时 GET 也计费（biz 级覆盖）。"""
    monkeypatch.setattr(dr.registry, "get", AsyncMock(return_value=make_biz(
        billing_keys={"biz_type": "video", "charge_on_get": True})))
    respx.get("http://upstream.test/v1/videos/up-1").respond(200, json={"id": "up-1"})
    _, billing_client = billing
    async with _client(_app()) as c:
        await c.get("/kling/v1/videos/up-1")
    billing_client.charge.assert_awaited_once()


# ---------------------------------------------------------------------------
# 白名单 / 超时 / 欠费
# ---------------------------------------------------------------------------


async def test_passthrough_whitelist_404(billing: tuple[MagicMock, MagicMock],
                                         tm: MagicMock) -> None:
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/other/path", json={})
    assert resp.status_code == 404
    assert resp.json()["error"]["type"] == "invalid_request_error"


@respx.mock
async def test_passthrough_timeout_504(billing: tuple[MagicMock, MagicMock],
                                       tm: MagicMock, _deps: FakeRedis) -> None:
    respx.post("http://upstream.test/v1/videos/text2video").mock(
        side_effect=httpx.ConnectTimeout("boom"))
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert resp.status_code == 504
    assert resp.json()["error"]["type"] == "upstream_error"
    # 超时计入熔断失败（429 才不计，§8.2）
    h = _deps.hashes.get("circuit:upstream:kling", {})
    assert int(h.get("fail_count", 0)) == 1


@respx.mock
async def test_passthrough_debt_flow(billing: tuple[MagicMock, MagicMock],
                                     tm: MagicMock, _deps: FakeRedis) -> None:
    """charge 402 → 欠费三连（欠费单+outbox+debt 名单）；响应原样回传 + debt 头。"""
    respx.post("http://upstream.test/v1/videos/text2video").respond(
        200, json={"task_id": "up-1"})
    _, billing_client = billing

    async def _charge(**kw):  # type: ignore[no-untyped-def]
        if not kw.get("verify_only"):  # 预检放行，正式 charge 402
            raise dr.InsufficientBalance("no balance")
        return {"ok": True}

    billing_client.charge.side_effect = _charge
    session = make_orm_session()
    async with _client(_app(session)) as c:
        resp = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert resp.status_code == 200                       # 上游已执行：原样回传
    assert resp.headers["X-Gateway-Billing"] == "debt"
    assert f"debt:{TOKEN.user_id}" in _deps.strings      # ② 熔断名单

    # ① 欠费单（Redis debt:order:{request_id} HASH + debt:orders SET，决策 A-9）
    debt_keys = [k for k in _deps.hashes if k.startswith("debt:order:")]
    assert len(debt_keys) == 1
    debt = _deps.hashes[debt_keys[0]]
    assert debt["user_id"] == str(TOKEN.user_id)
    assert debt["status"] == "open"
    rid = debt_keys[0].removeprefix("debt:order:")
    assert rid in _deps.sets["debt:orders"]
    # ③ outbox charge 条（Redis obx 队列，debt=True 持续追扣）
    obx_items = [h for k, h in _deps.hashes.items() if k.startswith("obx:obx_")]
    assert len(obx_items) == 1
    assert obx_items[0]["op"] == "charge"
    payload = json.loads(obx_items[0]["payload"])
    assert payload["debt"] is True
    assert payload["request_id"] == rid
    assert not session.add.called                          # 零自有表：无 ORM 写入


@respx.mock
async def test_passthrough_charge_failure_to_outbox(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock,
        _deps: FakeRedis) -> None:
    """计费服务不可用 → charge 异步入 Redis outbox，响应不受影响。"""
    respx.post("http://upstream.test/v1/videos/text2video").respond(
        200, json={"task_id": "up-1"})
    _, billing_client = billing
    billing_client.charge.side_effect = httpx.ConnectError("billing down")
    session = make_orm_session()
    async with _client(_app(session)) as c:
        resp = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert resp.status_code == 200
    assert "X-Gateway-Billing" not in resp.headers
    obx_items = [h for k, h in _deps.hashes.items() if k.startswith("obx:obx_")]
    assert len(obx_items) == 1
    payload = json.loads(obx_items[0]["payload"])
    assert payload["debt"] is False
    assert payload["amount"] == "0.5"


@respx.mock
async def test_passthrough_upstream_5xx_passthrough_body(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock,
        _deps: FakeRedis) -> None:
    """上游 5xx：原样回传（不计费），计入熔断。"""
    respx.post("http://upstream.test/v1/videos/text2video").respond(
        500, json={"error": "upstream broken"})
    _, billing_client = billing
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert resp.status_code == 500
    # 上游 5xx 不计费：仅 verify_only 预检调用，无正式 charge
    for call in billing_client.charge.await_args_list:
        assert call.kwargs.get("verify_only") is True
    assert int(_deps.hashes["circuit:upstream:kling"]["fail_count"]) == 1


# ---------------------------------------------------------------------------
# native_path `..` 穿越防护（httpx 出站 dot-segment 归一绕过前缀校验）
# ---------------------------------------------------------------------------


def test_native_path_dotdot_rejected_unit() -> None:
    """含 `..` 段的 native_path 直接拒绝（归一化后可逃出白名单前缀）。"""
    import pytest as _pytest

    from app.errors import GatewayError
    with _pytest.raises(GatewayError) as exc_info:
        dr._check_native_path_allowed(BIZ_CFG, "v1/videos/../../admin")
    assert exc_info.value.status_code == 404
    with _pytest.raises(GatewayError):
        dr._check_native_path_allowed(BIZ_CFG, "v1/../v1/videos")
    with _pytest.raises(GatewayError):
        dr._check_native_path_allowed(BIZ_CFG, "..")
    # 单 `.` 段同样拒绝（保守口径：归一化路径与原始路径不一致的一律不放行）
    with _pytest.raises(GatewayError):
        dr._check_native_path_allowed(BIZ_CFG, "v1/./videos")
    # 正常路径不受影响
    dr._check_native_path_allowed(BIZ_CFG, "v1/videos")
    dr._check_native_path_allowed(BIZ_CFG, "v1/videos/text2video")


async def _raw_asgi_request(app: Any, path: str, body: bytes) -> dict[str, Any]:
    """不经 httpx（客户端会做 dot-segment 归一），手工构造 ASGI 请求，
    模拟 curl/原始 HTTP 客户端送达未归一化的恶意路径。"""
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "POST", "scheme": "http", "path": path, "raw_path": path.encode(),
        "query_string": b"", "root_path": "",
        "headers": [(b"content-type", b"application/json"),
                    (b"authorization", b"Bearer sk-user")],
        "client": ("127.0.0.1", 12345), "server": ("test", 80),
    }
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    chunks = b"".join(
        m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return {"status": start["status"], "body": chunks,
            "headers": dict(start.get("headers") or [])}


@respx.mock
async def test_passthrough_dotdot_traversal_rejected(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock) -> None:
    """攻击用例：`v1/videos/../../admin` 过朴素前缀校验但归一化后逃逸——
    必须 404，且绝不转发上游（上游凭证不打到白名单外路径）。"""
    upstream = respx.route(host="upstream.test").respond(200, json={})
    app = _app()
    resp = await _raw_asgi_request(
        app, "/kling/v1/videos/../../admin", b'{"prompt":"p"}')
    assert resp["status"] == 404
    assert b"invalid_request_error" in resp["body"]
    assert upstream.calls == []                      # 未转发，凭证未泄漏
    _, billing_client = billing
    billing_client.charge.assert_not_awaited()

    # 合法路径对照：仍正常透传（命中上方面向 host 的兜底盘路由）
    async with _client(app) as c:
        ok = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert ok.status_code == 200
    assert len(upstream.calls) == 1
    assert upstream.calls.last.request.url.path == "/v1/videos/text2video"


# ---------------------------------------------------------------------------
# verify_only 预检（决策 A-9）：上游调用前验余额，402 直接拒绝零上游成本
# ---------------------------------------------------------------------------


@respx.mock
async def test_verify_only_precheck_rejects_402(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock,
        _deps: FakeRedis) -> None:
    """预检 402 → 直接拒绝，不打上游（零成本）、不落 tracked、不正式 charge。"""
    upstream = respx.post("http://upstream.test/v1/videos/text2video").respond(
        200, json={"task_id": "up-1"})
    _, billing_client = billing
    billing_client.charge.side_effect = dr.InsufficientBalance("no balance")
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert resp.status_code == 402
    assert resp.json()["error"]["code"] == "insufficient_quota"
    assert upstream.calls == []                            # 对上游零成本
    tm.track_passthrough_task.assert_not_awaited()
    assert billing_client.charge.await_count == 1          # 仅预检一次
    assert billing_client.charge.await_args.kwargs["verify_only"] is True
    assert billing_client.charge.await_args.kwargs["request_id"].endswith(":vo")


@respx.mock
async def test_verify_only_precheck_disabled_by_biz_config(
        monkeypatch: pytest.MonkeyPatch,
        billing: tuple[MagicMock, MagicMock], tm: MagicMock) -> None:
    """biz 配置可关：verify_only_precheck=false → 无预检调用，仅正式 charge。"""
    monkeypatch.setattr(dr.registry, "get", AsyncMock(return_value=make_biz(
        billing_keys={"biz_type": "video", "verify_only_precheck": False})))
    respx.post("http://upstream.test/v1/videos/text2video").respond(
        200, json={"task_id": "up-1"})
    _, billing_client = billing
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert resp.status_code == 200
    billing_client.charge.assert_awaited_once()
    assert "verify_only" not in billing_client.charge.await_args.kwargs


@respx.mock
async def test_verify_only_precheck_billing_down_proceeds(
        billing: tuple[MagicMock, MagicMock], tm: MagicMock) -> None:
    """预检时计费服务不可用 → 放行（best-effort，正式 charge 闭环照常）。"""
    respx.post("http://upstream.test/v1/videos/text2video").respond(
        200, json={"task_id": "up-1"})
    _, billing_client = billing

    async def _charge(**kw):  # type: ignore[no-untyped-def]
        if kw.get("verify_only"):
            raise httpx.ConnectError("billing down")       # 预检失败放行
        return {"ok": True}

    billing_client.charge.side_effect = _charge
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos/text2video", json={"prompt": "p"})
    assert resp.status_code == 200
    assert billing_client.charge.await_count == 2          # 预检 + 正式 charge
