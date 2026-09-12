"""``app/services/idem``：幂等原子占位（ADR-010 新链路核心不变量）。

为什么单独成篇：``relayflow.create_batch_task`` 靠它防「同 Idempotency-Key 并发
双建任务 + 双投递」。原 ``test_idem_concurrency.py`` 随旧链路删除时被一并删掉，
这里是按新链路现状重建的独立断言（不复用旧用例，避免把旧语义带回来）。

覆盖：
1. 占位者继续（``acquire`` → True）、回填后回放（同键第二次 → 拿到同一 task_id）；
2. **同键真并发**：只有占位者拿到推进权，其余等回填 / 超时 409，绝不放行重建；
3. 创建链路失败归还占位（CAS 删除只删 pending，不误删已回填的 task_id）；
4. 路由层：同键并发只有一个 202、其余 409；回放不落新行、不再入队。
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

from app.main import app
from app.services import idem

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Upstream-Base-Url": UP_BASE, **extra}


@pytest.fixture
def idem_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    monkeypatch.setattr(settings, "batch_deny_prefixes", "/api/,/console/")
    monkeypatch.setattr(settings, "idem_pending_ttl_seconds", 30)
    monkeypatch.setattr(settings, "idem_ttl", 86400)
    return settings


@pytest.fixture
def submit_events(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    import app.queue as q

    events: list[str] = []

    async def _publish(task_id: str) -> None:
        events.append(task_id)

    monkeypatch.setattr(q, "publish_batch_submit", AsyncMock(side_effect=_publish))
    return events


# ---------------------------------------------------------------------------
# 1. 基本状态流转
# ---------------------------------------------------------------------------


async def test_placeholder_owner_continues_then_replays(patch_redis, idem_settings):
    owned, replay = await idem.acquire("h1", "k1")
    assert (owned, replay) == (True, None)          # 占位者继续
    assert await idem.get_task_id("h1", "k1") is None   # pending 不算已回填

    await idem.set_task_id("h1", "k1", "batch_abc")
    assert await idem.get_task_id("h1", "k1") == "batch_abc"

    owned2, replay2 = await idem.acquire("h1", "k1")
    assert (owned2, replay2) == (False, "batch_abc")    # 回填后回放


async def test_distinct_keys_are_independent(patch_redis, idem_settings):
    assert (await idem.acquire("h1", "a"))[0] is True
    assert (await idem.acquire("h1", "b"))[0] is True
    assert (await idem.acquire("h2", "a"))[0] is True       # token_hash 也参与分组


async def test_pending_ttl_is_applied(patch_redis, idem_settings):
    """占位键必须带 TTL——否则创建方崩溃会让同键**永久** 409（死锁）。"""
    idem_settings.idem_pending_ttl_seconds = 7
    await idem.acquire("h1", "k1")
    ttl = await patch_redis.ttl(idem._key("h1", "k1"))
    assert 0 < ttl <= 7


# ---------------------------------------------------------------------------
# 2. 同键真并发：原子占位（SET NX）
# ---------------------------------------------------------------------------


async def test_concurrent_acquire_has_exactly_one_owner(patch_redis, idem_settings):
    """8 个并发 acquire 同一键：恰好 1 个 owner，其余都拿到 (False, None)。"""
    results = await asyncio.gather(*(idem.acquire("h1", "race") for _ in range(8)))
    owners = [r for r in results if r[0]]
    assert len(owners) == 1
    assert all(r == (True, None) for r in owners)
    assert all(r == (False, None) for r in results if not r[0])


async def test_wait_returns_filled_id(patch_redis, idem_settings):
    """他方占位回填后，等待方拿到真实 task_id（不是 409）。"""
    idem_settings.idem_replay_wait_seconds = 2.0
    await idem.acquire("h1", "k1")                   # 他方占位

    async def _fill() -> None:
        await asyncio.sleep(0.05)
        await idem.set_task_id("h1", "k1", "batch_filled")

    filler = asyncio.create_task(_fill())
    got = await idem.wait_task_id("h1", "k1")
    await filler
    assert got == "batch_filled"


async def test_wait_returns_none_when_placeholder_released(patch_redis, idem_settings):
    """创建方失败归还占位 → 等待方立刻放弃（不空转到超时）。"""
    idem_settings.idem_replay_wait_seconds = 5.0     # 刻意大于下面的等待耗时
    await idem.acquire("h1", "k1")
    await idem.release("h1", "k1")

    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await idem.wait_task_id("h1", "k1") is None
    assert loop.time() - started < 1.0               # 是「立刻返回」而非等满超时


async def test_wait_times_out_on_pending(patch_redis, idem_settings):
    """占位一直不回填 → 超时返回 None（调用方按 409 处理，绝不放行重建）。"""
    idem_settings.idem_replay_wait_seconds = 0.08
    await idem.acquire("h1", "k1")
    assert await idem.wait_task_id("h1", "k1") is None


# ---------------------------------------------------------------------------
# 3. 归还占位：CAS 只删 pending
# ---------------------------------------------------------------------------


async def test_release_never_deletes_filled_task_id(patch_redis, idem_settings):
    """已回填的键绝不误删（否则并发等待方会误判「创建方失败」）。"""
    await idem.acquire("h1", "k1")
    await idem.set_task_id("h1", "k1", "batch_keep")
    await idem.release("h1", "k1")
    assert await idem.get_task_id("h1", "k1") == "batch_keep"


async def test_release_deletes_pending(patch_redis, idem_settings):
    await idem.acquire("h1", "k1")
    await idem.release("h1", "k1")
    assert await idem.get_task_id("h1", "k1") is None


# ---------------------------------------------------------------------------
# 4. 路由层：同键并发 409 / 回放
# ---------------------------------------------------------------------------


async def test_same_key_concurrent_requests_only_one_creates(
    idem_settings, patch_redis, task_store, submit_events, monkeypatch,
):
    """同 Idempotency-Key 真并发：占位者 202，其余在占位期间等到超时 → 409。

    让 ``set_task_id`` 变慢，把「占位在飞」窗口拉长到超过等待上限，从而确定性地
    触发等待方超时（不靠调度巧合）。
    """
    idem_settings.idem_replay_wait_seconds = 0.05
    real_set = idem.set_task_id

    async def slow_set(token_hash: str, idem_key: str, task_id: str) -> None:
        await asyncio.sleep(0.25)
        await real_set(token_hash, idem_key, task_id)

    monkeypatch.setattr(idem, "set_task_id", slow_set)

    async with _client() as client:
        a, b = await asyncio.gather(
            client.post("/batch/v1/tasks", json={"model": "m"},
                        headers=_headers(**{"Idempotency-Key": "same-key"})),
            client.post("/batch/v1/tasks", json={"model": "m"},
                        headers=_headers(**{"Idempotency-Key": "same-key"})),
        )

    codes = sorted([a.status_code, b.status_code])
    assert codes == [202, 409], (a.status_code, a.text, b.status_code, b.text)
    loser = a if a.status_code == 409 else b
    assert "conflict" in loser.text.lower()
    assert len(task_store.rows) == 1                  # 只建了一个任务
    assert len(submit_events) == 1                    # 只投了一次上游提交


async def test_replay_returns_same_task_without_side_effects(
    idem_settings, patch_redis, task_store, submit_events, respx_router,
):
    async with _client() as client:
        first = await client.post("/batch/v1/tasks", json={"model": "m"},
                                  headers=_headers(**{"Idempotency-Key": "k-replay"}))
        second = await client.post("/batch/v1/tasks", json={"model": "m"},
                                   headers=_headers(**{"Idempotency-Key": "k-replay"}))

    assert first.status_code == 202 and second.status_code == 202
    assert first.json()["task_id"] == second.json()["task_id"]
    assert len(task_store.rows) == 1                  # 回放不落新行
    assert submit_events == [first.json()["task_id"]]  # 回放不再入队
    assert len(respx_router.calls) == 0


async def test_placeholder_released_when_create_chain_fails(
    idem_settings, patch_redis, task_store, submit_events,
):
    """创建链路中途失败（403 路径准入）必须归还占位，同键重试仍可成功。"""
    async with _client() as client:
        denied = await client.post("/batch/api/models", json={"model": "m"},
                                   headers=_headers(**{"Idempotency-Key": "k-retry"}))
        assert denied.status_code == 403
        assert not task_store.rows
        # 占位已归还：同键不是 409，而是真的重新走创建链路
        retried = await client.post("/batch/v1/tasks", json={"model": "m"},
                                    headers=_headers(**{"Idempotency-Key": "k-retry"}))

    assert retried.status_code == 202, retried.text
    assert len(task_store.rows) == 1
    assert submit_events == [retried.json()["task_id"]]
