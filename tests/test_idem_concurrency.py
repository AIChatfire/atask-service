"""幂等键原子占位（KI3 根治）测试。

原并发窗：preflight 先查重放、落库后才回填幂等键——同 Idempotency-Key
的两个真并发请求都查不到 → 双建任务、双冻结。根治：preflight SET NX
原子占位（pending，短 TTL），同键并发只有占位者继续创建链路，其余短
轮询等占位在同一键上回填为 task_id 后回放；超时/过期按 409 冲突
（不放行重建）；创建链路失败 CAS 归还占位。

边界：billing/keypool 走 respx，taskstore 内存实现，queue 发布门面
记录器，Redis FakeRedis（幂等占位也落在 FakeRedis 上）。
"""

from __future__ import annotations

import asyncio
import hashlib
import json

import httpx
import pytest
from fastapi import HTTPException, Request

from app.config import settings
from app.deps.preflight import preflight
from app.main import app
from app.redis import K_IDEM
from app.services import flow, idem, providers

BODY = {"model": "MiniMax-H3", "duration": 5}
TOKEN = "sk-user-42"
TOKEN_HASH = hashlib.sha256(TOKEN.encode()).hexdigest()

_CHANNEL = {
    "code": 0, "message": "ok",
    "data": {
        "channel_id": 7, "key_index": 0, "key": "sk-upstream-key",
        "base_url": "http://upstream.test", "epoch": "e1",
        "channel": {
            "id": 7, "name": "ch-7", "base_url": "http://upstream.test",
            "setting": {"gateway": {
                "biz": "minimax",
                "submit_path": "/v2/video_generation",
                "probe_path": "/v2/query/video_generation/{upstream_task_id}",
                "status_path": "task.status",
                "billing": {"rule": "def calulate(request):\n    return 0.13"},
            }},
        },
    },
}


@pytest.fixture
def mocks(respx_router):
    m = type("Mocks", (), {})()
    m.inspect = respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 42, "token_id": 9})
    )
    m.freeze = respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}})
    )
    m.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_CHANNEL)
    )
    return m


def _make_request(body: dict) -> Request:
    """构造最小 starlette Request（preflight 只读 headers/json/url.path）。"""
    payload = json.dumps(body).encode()
    sent = False

    async def _receive() -> dict:
        nonlocal sent
        chunk, sent = (b"" if sent else payload), True
        return {"type": "http.request", "body": chunk, "more_body": False}

    return Request({
        "type": "http", "http_version": "1.1", "method": "POST",
        "scheme": "http", "server": ("gw.test", 80),
        "path": "/minimax/v1/tasks", "raw_path": b"/minimax/v1/tasks",
        "query_string": b"", "root_path": "", "path_params": {},
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload)).encode()),
        ],
        "client": ("127.0.0.1", 12345), "app": app,
    }, _receive)


async def _create_once(idem_key: str) -> dict:
    """一次完整创建链路：preflight（含原子占位）→ flow.create_task。"""
    pf = await preflight("minimax", _make_request(BODY),
                         authorization=f"Bearer {TOKEN}",
                         idempotency_key=idem_key)
    return await flow.create_task("minimax", pf.body, pf, action="task", source="tasks")


# ---------------------------------------------------------------------------
# KI3 主场景：同键真并发 → 单任务、单冻结、回放同一 task_id
# ---------------------------------------------------------------------------


async def test_concurrent_same_idem_key_creates_single_task(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
    monkeypatch,
):
    """两个协程同 Idempotency-Key 真并发 create_task：只建一个任务、只冻结
    一次、只入队一次提交事件；另一请求等占位回填后回放同一 task_id。"""
    freeze_calls: list[dict] = []

    async def _slow_freeze(**kwargs):
        await asyncio.sleep(0.1)       # 拉宽占位窗口，确保并发方进入等待路径
        freeze_calls.append(kwargs)
        return {}

    monkeypatch.setattr(providers.billing, "freeze", _slow_freeze)

    view1, view2 = await asyncio.gather(
        _create_once("idem-conc-1"), _create_once("idem-conc-1"))

    assert view1["task_id"] == view2["task_id"]           # 回放同一任务
    assert len(task_store.rows) == 1                      # 只建一个任务
    assert len(freeze_calls) == 1                         # 只冻结一次
    assert queue_events["submit"] == [view1["task_id"]]   # 只入队一次提交事件
    # 幂等键已回填为真实 task_id（同键 pending → task_id 状态流转完成）
    assert await idem.get_task_id(TOKEN_HASH, "idem-conc-1") == view1["task_id"]


async def test_idem_conflict_wait_timeout_returns_409(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
    monkeypatch,
):
    """他方占位在飞却迟迟不回填（创建方卡死）：等待超时按 409 冲突处理——
    不放行重建（重建会双建双冻结），客户端原键重试即可；零冻结零落库。"""
    monkeypatch.setattr(settings, "idem_replay_wait_seconds", 0.2)
    await patch_redis.set(K_IDEM.format(token_hash=TOKEN_HASH, key="idem-stuck"),
                          idem.PENDING, ex=30)

    with pytest.raises(HTTPException) as exc_info:
        await preflight("minimax", _make_request(BODY),
                        authorization=f"Bearer {TOKEN}",
                        idempotency_key="idem-stuck")

    assert exc_info.value.status_code == 409
    assert len(task_store.rows) == 0
    assert len(mocks.freeze.calls) == 0


async def test_placeholder_released_when_create_path_fails(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """占位者创建链路失败（freeze 拒）→ CAS 归还占位：无 pending 残留，
    同键重试立即重建（不必干等占位 TTL）。"""
    mocks.freeze = respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(402, json={"error": "insufficient quota"})
    )

    with pytest.raises(HTTPException) as exc_info:
        await preflight("minimax", _make_request(BODY),
                        authorization=f"Bearer {TOKEN}",
                        idempotency_key="idem-fail")

    assert exc_info.value.status_code == 402
    # 占位已归还（键不存在）：后续同键请求可立即重新占位创建
    assert await patch_redis.get(
        K_IDEM.format(token_hash=TOKEN_HASH, key="idem-fail")) is None


# ---------------------------------------------------------------------------
# 占位状态机单测（idem 模块语义）
# ---------------------------------------------------------------------------


async def test_placeholder_lifecycle(patch_redis):
    """占位状态机：acquire 占位 → 并发方见 pending（无回放目标）→
    set_task_id 同键回填 → 并发方 acquire 直接回放；release CAS 不误删
    已回填的 task_id。"""
    owned, replay = await idem.acquire(TOKEN_HASH, "k")
    assert (owned, replay) == (True, None)

    owned2, replay2 = await idem.acquire(TOKEN_HASH, "k")
    assert (owned2, replay2) == (False, None)             # 占位中，无回放目标
    assert await idem.get_task_id(TOKEN_HASH, "k") is None  # pending 不是 task_id

    await idem.set_task_id(TOKEN_HASH, "k", "minimax_abc")  # pending → task_id
    owned3, replay3 = await idem.acquire(TOKEN_HASH, "k")
    assert (owned3, replay3) == (False, "minimax_abc")

    await idem.release(TOKEN_HASH, "k")                   # CAS：已回填绝不误删
    assert await idem.get_task_id(TOKEN_HASH, "k") == "minimax_abc"


async def test_release_deletes_pending_placeholder(patch_redis):
    """占位失败归还：pending 被 CAS 删除，键消失（等待方立即走 409 语义）。"""
    assert (await idem.acquire(TOKEN_HASH, "k"))[0] is True
    await idem.release(TOKEN_HASH, "k")
    assert await patch_redis.get(K_IDEM.format(token_hash=TOKEN_HASH, key="k")) is None


async def test_wait_task_id_returns_none_when_placeholder_released(
    patch_redis, monkeypatch,
):
    """占位被释放（创建方失败）→ 等待方拿到 None（preflight 按 409 处理）。"""
    monkeypatch.setattr(settings, "idem_replay_wait_seconds", 5.0)
    assert (await idem.acquire(TOKEN_HASH, "k"))[0] is True

    async def _release_soon():
        await asyncio.sleep(0.05)
        await idem.release(TOKEN_HASH, "k")

    releaser = asyncio.create_task(_release_soon())
    assert await idem.wait_task_id(TOKEN_HASH, "k") is None
    await releaser


# ---------------------------------------------------------------------------
# proxy 透传形态：幂等占位/回填/归还完整性（KI3 补齐回填的回归）
# ---------------------------------------------------------------------------


async def test_proxy_billable_idem_placeholder_backfill_and_replay(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """计费透传带 Idempotency-Key：落库后占位回填为 task_id；同键二次请求
    直接 202 回放首个任务——上游只透传一次、只冻结一次、只落一行。"""
    upstream_call = respx_router.post(
        "http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "up-1"})
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://gw.test") as client:
        resp1 = await client.post(
            "/minimax/v2/video_generation", json=BODY,
            headers={"authorization": f"Bearer {TOKEN}",
                     "idempotency-key": "idem-proxy-1"})
        resp2 = await client.post(
            "/minimax/v2/video_generation", json=BODY,
            headers={"authorization": f"Bearer {TOKEN}",
                     "idempotency-key": "idem-proxy-1"})

    assert resp1.status_code == 200
    assert resp2.status_code == 202                        # 重放短路
    assert len(upstream_call.calls) == 1                   # 上游只透传一次
    assert len(mocks.freeze.calls) == 1                    # 只冻结一次
    assert len(task_store.rows) == 1
    task_id = next(iter(task_store.rows))
    assert resp2.json()["task_id"] == task_id
    # 占位已同键回填为真实 task_id（pending → task_id 流转完成）
    assert await idem.get_task_id(TOKEN_HASH, "idem-proxy-1") == task_id


async def test_proxy_billable_releases_placeholder_when_store_fails(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
    monkeypatch,
):
    """透传落库失败 → CAS 归还占位：无 pending 残留，同键重试立即可重建。"""
    import app.services.taskstore as ts

    async def _boom(**kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(ts, "create", _boom)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://gw.test") as client:
        resp = await client.post(
            "/minimax/v2/video_generation", json=BODY,
            headers={"authorization": f"Bearer {TOKEN}",
                     "idempotency-key": "idem-proxy-fail"})

    assert resp.status_code == 500
    assert await patch_redis.get(
        K_IDEM.format(token_hash=TOKEN_HASH, key="idem-proxy-fail")) is None


# ---------------------------------------------------------------------------
# 创建链路提前出局分支的归还纪律（QA 回归：占位/并发槽/预冻结三件套）
# ---------------------------------------------------------------------------


async def test_concurrency_limit_releases_placeholder_and_unfreezes(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
    monkeypatch,
):
    """并发槽超限（conc_acquire 429）= 创建链路失败：幂等占位必须 CAS 归还
    （同键重试立即可重建，不干等占位 TTL），预冻结必须取消（资金不无任务
    挂账至 freeze TTL）；并发槽未占用无需归还（LUA 未抢到已自减）。"""
    from app.redis import K_CONC, K_IDEM

    monkeypatch.setattr(settings, "max_concurrent_tasks", 0)   # 必撞并发上限

    pf = await preflight("minimax", _make_request(BODY),
                         authorization=f"Bearer {TOKEN}",
                         idempotency_key="idem-conc-429")
    assert len(mocks.freeze.calls) == 1                    # 预冻结已发生

    with pytest.raises(HTTPException) as exc_info:
        await flow.create_task("minimax", pf.body, pf, action="task", source="tasks")
    assert exc_info.value.status_code == 429

    # 幂等占位已归还（键不存在）：同键重试立即重建
    assert await patch_redis.get(
        K_IDEM.format(token_hash=TOKEN_HASH, key="idem-conc-429")) is None
    # 预冻结已取消（billing cancel 按 request_id=task_id 幂等解冻）
    assert [c["request_id"] for c in queue_events["cancel"]] == [pf.task_id]
    # 并发槽未泄漏（LUA 未抢到已自减回 0 / 键不存在）
    assert await patch_redis.get(K_CONC.format(token_hash=TOKEN_HASH)) in (None, "0")
    assert len(task_store.rows) == 0


async def test_submit_path_missing_releases_slot_placeholder_and_unfreezes(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """渠道配置改坏（submit_path 缺失，502 提前出局）：幂等占位归还 +
    并发槽归还（K_CONC 无 TTL，泄漏即永久丢槽）+ 预冻结取消。"""
    from app.redis import K_CONC, K_IDEM

    broken = json.loads(json.dumps(_CHANNEL))
    broken["data"]["channel"]["setting"]["gateway"]["submit_path"] = ""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=broken)
    )

    pf = await preflight("minimax", _make_request(BODY),
                         authorization=f"Bearer {TOKEN}",
                         idempotency_key="idem-502")
    assert len(mocks.freeze.calls) == 1

    with pytest.raises(HTTPException) as exc_info:
        await flow.create_task("minimax", pf.body, pf, action="task", source="tasks")
    assert exc_info.value.status_code == 502

    assert await patch_redis.get(
        K_IDEM.format(token_hash=TOKEN_HASH, key="idem-502")) is None
    assert [c["request_id"] for c in queue_events["cancel"]] == [pf.task_id]
    assert await patch_redis.get(K_CONC.format(token_hash=TOKEN_HASH)) in (None, "0")
    assert len(task_store.rows) == 0
