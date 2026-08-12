"""终审修复的跨模块集成测试（不手工构造关键 payload/不 mock 关键链路）。

- H1：dynamic_router 真实入队 Redis obx 的 charge 条 → OutboxWorker 端到端消费
  （payload 键契约 amount/biz_type/metric/request_id 必须对齐，防 KeyError 死信）；
- H3：欠费清偿链路——402 落欠费单（``debt:order:{request_id}`` HASH +
  ``debt:orders`` SET）+ debt outbox 条（含 user_id/biz）→ 用户充值后
  worker charge 成功 → 欠费单 cleared + ``debt:{user_id}`` 熔断名单解除；
- H2：SubmitContext.secrets 由 auth_secret_ref 真实解析并到达真实适配器
  （kling JWT 可用 SK 验签 / seedance Bearer 头原值；不 mock auth_headers）。
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import jwt
import pytest
import respx
from test_task_manager import (
    FakeBilling,
    FakePricing,
    make_biz_cfg,
    make_req,
    make_token,
)
from test_task_manager import (
    FakeRedis as TmFakeRedis,
)
from test_task_manager import (
    FakeSession as TmFakeSession,
)
from w1_helpers import (
    FakeRedis,
    build_test_app,
    make_biz,
    make_orm_session,
)
from w1_helpers import (
    make_token as w1_make_token,
)

import app.billing.outbox as obx
import app.middleware as mw
import app.tasks.manager as mgr
from app.adapters.base import register
from app.billing.outbox import OutboxWorker
from app.routing import dynamic_router as dr
from app.routing import videos
from app.tasks.manager import TaskManager
from tests.w3_fakes import FakeSession, FakeSessionFactory

W1_TOKEN = w1_make_token()
W1_BIZ_CFG = make_biz()


class PassthroughFakeAdapter:
    """透传形态测试适配器（同 test_w1_passthrough 的最小协议实现）。"""

    name = "fakeadp"
    callback_capability = True
    echoes_external_task_id = True

    def auth_headers(self, cfg: Any) -> dict[str, str]:
        return {"Authorization": "Bearer upstream-secret-key"}

    def rewrite_callback_url(self, raw_body: bytes, cfg: Any) -> bytes:
        return raw_body


register(PassthroughFakeAdapter())


@pytest.fixture
def _w1_deps(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    fake = FakeRedis()
    monkeypatch.setattr(mw, "get_redis", AsyncMock(return_value=fake))
    monkeypatch.setattr(dr, "get_redis", AsyncMock(return_value=fake),
                        raising=False)
    monkeypatch.setattr(dr.registry, "get", AsyncMock(return_value=W1_BIZ_CFG))
    import app.redis_client as rc
    monkeypatch.setattr(rc, "get_redis", AsyncMock(return_value=fake))
    # outbox 入队/欠费单/熔断名单与 worker 消费共用同一 fake（模块级 get_redis）
    monkeypatch.setattr(obx, "get_redis", AsyncMock(return_value=fake))
    return fake


@pytest.fixture
def _tm_mock() -> MagicMock:
    fake = MagicMock()
    fake.track_passthrough_task = AsyncMock(return_value="task_pt")
    videos.set_task_manager(fake)
    yield fake
    videos.set_task_manager(None)  # type: ignore[arg-type]


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test")


async def _do_passthrough_charge(
    fake: FakeRedis, charge_side_effect: Exception,
) -> tuple[str, dict[str, Any]]:
    """真实走一遍透传 POST：上游 2xx + 正式 charge 失败 → 返回真实入队的
    Redis obx 条目（item_id, payload）。

    计费服务不可用（ConnectError）→ outbox 补偿条（debt=False）；
    余额不足（InsufficientBalance）→ 欠费三连（debt=True 持续追扣条）。
    """
    pricing = MagicMock()
    pricing.get_logic = AsyncMock(return_value="logic")
    pricing.evaluate = AsyncMock(return_value=Decimal("0.5"))
    billing = MagicMock()

    async def _charge(**kw: Any) -> dict[str, Any]:
        # verify_only 预检（决策 A-9，上游调用前第一次调用）放行：
        # side_effect 只对正式 charge（verify_only=False）生效
        if kw.get("verify_only"):
            return {"ok": True}
        raise charge_side_effect

    billing.charge = AsyncMock(side_effect=_charge)
    dr.set_passthrough_billing(pricing, billing)
    try:
        respx.post("http://upstream.test/v1/videos/text2video").respond(
            200, json={"task_id": "up-1"})
        session = make_orm_session()
        app = build_test_app(dr.router, token=W1_TOKEN, session=session)
        async with _client(app) as c:
            resp = await c.post("/kling/v1/videos/text2video",
                                json={"prompt": "p"})
        assert resp.status_code == 200  # 上游已执行：响应原样回传
    finally:
        dr.set_passthrough_billing(None, None)
    # 零自有表：从 w1_helpers FakeRedis 的 obx: 结构取真实入队条目
    obx_ids = [k.removeprefix("obx:") for k in fake.hashes
               if k.startswith("obx:obx_")]
    assert len(obx_ids) == 1
    item = fake.hashes[f"obx:{obx_ids[0]}"]
    assert item["op"] == "charge"
    assert not session.add.called                   # 无任何 ORM 落库
    return obx_ids[0], json.loads(item["payload"])


# ---------------------------------------------------------------------------
# H1：透传 charge outbox 行 → OutboxWorker 端到端消费
# ---------------------------------------------------------------------------


@respx.mock
async def test_passthrough_outbox_row_consumed_end_to_end(
    _w1_deps: FakeRedis, _tm_mock: MagicMock,
) -> None:
    """dynamic_router 真实入队（写入方）→ OutboxWorker 原样重放（消费方）。

    回归保护：写入方 payload 键与消费方契约不一致（amount_usd vs amount）
    会在消费端 KeyError → 死信；本测试不手工构造 payload，键错位必失败。
    """
    outbox_id, payload = await _do_passthrough_charge(
        _w1_deps, httpx.ConnectError("billing down"))
    # 写入方契约断言（与 outbox.py 消费端逐键对齐）
    assert payload["amount"] == "0.5"               # 金额键必须是 amount
    assert payload["biz_type"] == "video" and payload["metric"] == "call"
    assert payload["user_id"] == W1_TOKEN.user_id and payload["biz"] == "kling"
    assert payload["debt"] is False
    assert _w1_deps.hashes[f"obx:{outbox_id}"]["state"] == "pending"

    # 消费方端到端：同一 FakeRedis 原样 sweep，charge 重放成功
    worker_billing = AsyncMock()
    worker_billing.charge.return_value = {"charged": True}
    session = FakeSession()
    worker = OutboxWorker(worker_billing, AsyncMock(),
                          FakeSessionFactory([session]))
    processed = await worker._sweep_once()

    assert processed == 1
    worker_billing.charge.assert_awaited_once()     # 无 KeyError（未进死信）
    kw = worker_billing.charge.await_args.kwargs
    assert kw["request_id"] == payload["request_id"]   # request_id 原样（幂等）
    assert kw["amount_usd"] == Decimal("0.5")
    assert kw["biz_type"] == "video" and kw["metric"] == "call"
    assert kw["user_sk"] == W1_TOKEN.raw
    # 成功收口：条目 HASH 删除 + due/lease 摘除，无死信
    assert await _w1_deps.hgetall(f"obx:{outbox_id}") == {}
    assert await _w1_deps.zcard("obx:due") == 0
    assert await _w1_deps.zcard("obx:lease") == 0
    assert await _w1_deps.zcard("obx:dead") == 0
    # billing_state 回写：platform 前缀条件（§4.5），不校验 status（§4.7）
    state = session.statements_containing("$.gateway.billing_state")
    assert state and state[0][1]["state"] == "charged"
    assert "platform LIKE" in state[0][0]
    assert session.commits == 1


# ---------------------------------------------------------------------------
# H3：欠费清偿链路（402 落 debt 行 → 充值后扣回 → 熔断解除）
# ---------------------------------------------------------------------------


@respx.mock
async def test_debt_clear_chain_after_topup(
    _w1_deps: FakeRedis, _tm_mock: MagicMock,
) -> None:
    """charge 402 → 欠费三连（Redis 欠费单 + debt outbox 条含 user_id + 熔断名单）；
    用户充值后 worker 重放成功 → 欠费单 cleared + clear_debt_block 解除熔断。"""
    from app.billing.client import InsufficientBalance

    outbox_id, payload = await _do_passthrough_charge(
        _w1_deps, InsufficientBalance("no balance"))
    assert payload["debt"] is True
    assert payload["user_id"] == W1_TOKEN.user_id   # 清偿解除熔断的必要键
    assert payload["biz"] == "kling"
    rid = str(payload["request_id"])
    # ① 欠费单（Redis debt:order:{request_id} HASH + debt:orders SET，决策 A-9）
    debt = _w1_deps.hashes[f"debt:order:{rid}"]
    assert debt["status"] == "open"
    assert debt["user_id"] == str(W1_TOKEN.user_id) and debt["biz"] == "kling"
    assert rid in _w1_deps.sets["debt:orders"]
    # ② 熔断名单
    assert f"debt:{W1_TOKEN.user_id}" in _w1_deps.strings

    # 消费方复用同一 FakeRedis（debt 名单在其中），模拟充值后 charge 成功
    worker_billing = AsyncMock()
    worker_billing.charge.return_value = {"charged": True}
    session = FakeSession()
    worker = OutboxWorker(worker_billing, AsyncMock(),
                          FakeSessionFactory([session]))
    processed = await worker._sweep_once()

    assert processed == 1
    worker_billing.charge.assert_awaited_once()
    # 清偿成功 → 欠费单 cleared + open 清单摘除 + 熔断名单解除 + billing_state 回写
    assert _w1_deps.hashes[f"debt:order:{rid}"]["status"] == "cleared"
    assert rid not in _w1_deps.sets.get("debt:orders", set())
    assert f"debt:{W1_TOKEN.user_id}" not in _w1_deps.strings  # clear_debt_block
    state = session.statements_containing("$.gateway.billing_state")
    assert state and state[0][1]["state"] == "charged"
    assert "platform LIKE" in state[0][0]
    # 持续追扣条成功收口（无死信）
    assert await _w1_deps.hgetall(f"obx:{outbox_id}") == {}
    assert await _w1_deps.zcard("obx:dead") == 0


# ---------------------------------------------------------------------------
# H2：secrets 真实解析并到达真实适配器（不 mock auth_headers）
# ---------------------------------------------------------------------------


@respx.mock
async def test_submit_reaches_real_kling_adapter_with_jwt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """submit_task → 真实 KlingAdapter：secrets 来自 {ref}_AK/{ref}_SK，
    Authorization 为用 SK 可验签的 HS256 JWT（iss=AK）。"""
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-e2e")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-e2e")
    redis = TmFakeRedis()

    async def _get_redis() -> TmFakeRedis:
        return redis

    monkeypatch.setattr(mgr, "get_redis", _get_redis)
    monkeypatch.setattr("app.auth.get_redis", _get_redis)
    route = respx.post(
        "https://upstream.example.com/v1/videos/text2video").respond(
        200, json={"code": 0, "message": "ok",
                   "data": {"task_id": "up-real-1", "task_status": "submitted"}})
    tm = TaskManager(billing=FakeBilling(), pricing=FakePricing())
    session = TmFakeSession()
    out = await tm.submit_task(
        session,
        biz_cfg=make_biz_cfg(),          # adapter="kling"（真实适配器，不 mock）
        req=make_req(model="kling-v2"),  # 旧版 v1 形态
        token=make_token(), form="videos", idem_key=None,
    )
    assert out["status"] == "queued"
    assert route.calls, "真实适配器必须发出上游请求"
    auth = route.calls.last.request.headers["Authorization"]
    payload = jwt.decode(auth.removeprefix("Bearer "), "sk-e2e",
                         algorithms=["HS256"])     # SK 验签通过 = SK 真实到达
    assert payload["iss"] == "ak-e2e"              # AK 真实到达


@respx.mock
async def test_submit_reaches_real_seedance_adapter_with_bearer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """submit_task → 真实 SeedanceAdapter：secrets 来自 os.environ[ref]，
    Authorization 原值 Bearer key（缺失必 401 的回归保护）。"""
    monkeypatch.setenv("UPSTREAM_KEY_ARK", "ark-e2e-key")
    redis = TmFakeRedis()

    async def _get_redis() -> TmFakeRedis:
        return redis

    monkeypatch.setattr(mgr, "get_redis", _get_redis)
    monkeypatch.setattr("app.auth.get_redis", _get_redis)
    cfg = make_biz_cfg()
    cfg.adapter = "seedance"
    cfg.auth_type = "bearer_key"
    cfg.auth_secret_ref = "UPSTREAM_KEY_ARK"
    route = respx.post(
        "https://upstream.example.com/api/v3/contents/generations/tasks").respond(
        200, json={"id": "cgt-e2e-1"})
    tm = TaskManager(billing=FakeBilling(), pricing=FakePricing())
    out = await tm.submit_task(
        TmFakeSession(), biz_cfg=cfg, req=make_req(model="doubao-seedance-1-0"),
        token=make_token(), form="videos", idem_key=None,
    )
    assert out["status"] == "queued"
    assert route.calls
    auth = route.calls.last.request.headers["Authorization"]
    assert auth == "Bearer ark-e2e-key"


@respx.mock
async def test_poller_reaches_real_kling_adapter_with_jwt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PollWorker._poll_one → 真实 KlingAdapter.poll：secrets 同样按
    auth_secret_ref 解析（poller 构造点回归保护）。"""
    import test_poller as tp

    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-poll")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-poll")
    session = TmFakeSession()
    session.on("FOR UPDATE SKIP LOCKED", tp.in_flight_row())
    session.on("SELECT task_id, action, private_data",
               tp.full_row(deadline_unix=9999999999))
    monkeypatch.setattr(
        "app.tasks.poller.registry.get", AsyncMock(return_value=make_biz_cfg()))
    route = respx.get(
        "https://upstream.example.com/v1/videos/text2video/up-123").respond(
        200, json={"code": 0, "message": "ok",
                   "data": {"task_id": "up-123", "task_status": "processing"}})
    transition = AsyncMock(return_value=True)
    tm = MagicMock()
    tm.transition = transition
    worker = tp.PollWorker(task_manager=tm,
                           session_factory=tp.FakeSessionFactory(session))
    claimed = await worker._poll_once()

    assert claimed == 1
    transition.assert_awaited_once()
    assert route.calls
    auth = route.calls.last.request.headers["Authorization"]
    payload = jwt.decode(auth.removeprefix("Bearer "), "sk-poll",
                         algorithms=["HS256"])
    assert payload["iss"] == "ak-poll"
