"""``app/services/tokensession``：用户令牌会话的 store / get / clear 与 TTL。

为什么单独成篇：新链路拿用户 token 去探测上游的唯一来源就是这里（``relay`` 出站
必须携带用户本人 sk-）。原 ``test_ops_tokensession.py`` 随旧链路删除后被并删，
现仅剩 ``test_batch_route`` 顺带碰一下。这里独立钉住会话的读写/过期/诊断与
「明文令牌只进 Redis、绝不外发」的红线。

TTL 用 FakeRedis 的时间语义验证（直接操纵过期时刻），**不真等时钟**。
"""

from __future__ import annotations

import time

import httpx
import pytest
from loguru import logger

from app.main import app
from app.services import tokensession

AUTH = {"Authorization": "Bearer sk-secret-token"}
UP_BASE = "http://upstream.test"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


@pytest.fixture
def session_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "sk_session_ttl_seconds", 172800)
    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    return settings


def _key(task_id: str) -> str:
    return tokensession._K.format(task_id=task_id)


# ---------------------------------------------------------------------------
# store / get / clear
# ---------------------------------------------------------------------------


async def test_store_then_get_roundtrip(patch_redis, session_settings):
    await tokensession.store("t1", "sk-secret-token")
    assert await tokensession.get("t1") == "sk-secret-token"


async def test_clear_makes_token_unavailable(patch_redis, session_settings):
    await tokensession.store("t1", "sk-secret-token")
    await tokensession.clear("t1")
    assert await tokensession.get("t1") is None


async def test_get_missing_returns_none_and_warns(patch_redis, session_settings):
    records: list = []
    sink = logger.add(lambda m: records.append(m.record), level="WARNING")
    try:
        assert await tokensession.get("nope") is None
    finally:
        logger.remove(sink)
    assert any(r["level"].name == "WARNING" and "token session missing" in r["message"]
               for r in records)


# ---------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------


async def test_ttl_follows_configuration(patch_redis, session_settings):
    session_settings.sk_session_ttl_seconds = 1234
    await tokensession.store("t1", "sk-secret-token")
    ttl = await patch_redis.ttl(_key("t1"))
    assert 0 < ttl <= 1234


async def test_session_expires_with_fake_redis_clock(patch_redis, session_settings):
    """过期后取不到令牌，且不再刷 WARNING（会话过期是常态，由 redis 语义兜底）。"""
    session_settings.sk_session_ttl_seconds = 1
    await tokensession.store("t1", "sk-secret-token")

    # 用 FakeRedis 的时间语义把过期时刻推到过去（不真等 1 秒）
    patch_redis._expires[_key("t1")] = time.time() - 1
    assert await tokensession.get("t1") is None


# ---------------------------------------------------------------------------
# 诊断视图：只暴露存在性与 TTL
# ---------------------------------------------------------------------------


async def test_session_info_reports_existence_and_ttl_without_token(
    patch_redis, session_settings,
):
    session_settings.sk_session_ttl_seconds = 900
    await tokensession.store("t1", "sk-secret-token")

    info = await tokensession.session_info("t1")
    assert info["exists"] is True
    assert 0 < info["ttl_seconds"] <= 900
    assert "sk-secret-token" not in repr(info)        # 诊断视图绝不回令牌本体


async def test_session_info_absent(patch_redis, session_settings):
    info = await tokensession.session_info("nope")
    assert info == {"exists": False, "ttl_seconds": -2}


# ---------------------------------------------------------------------------
# 新链路集成：受理落会话，终态清会话
# ---------------------------------------------------------------------------


async def test_route_stores_session_and_terminal_clears_it(
    session_settings, patch_redis, task_store, respx_router, monkeypatch,
):
    """受理 → 会话可查；探测到终态 → 会话被清（明文 token 不留 Redis）。"""
    import app.queue as q

    async def _noop(*_a, **_k) -> None:
        return None

    monkeypatch.setattr(q, "publish_batch_submit", _noop)

    async with _client() as client:
        created = await client.post(
            "/batch/v1/tasks", json={"model": "m"},
            headers={**AUTH, "X-Upstream-Base-Url": UP_BASE},
        )
        assert created.status_code == 202, created.text
        task_id = created.json()["task_id"]
        assert await tokensession.get(task_id) == "sk-secret-token"

        await task_store.patch_data(task_id, {"upstream_task_id": "up-1"}, status="QUEUED")
        respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
            return_value=httpx.Response(200, json={"id": "up-1", "status": "succeeded"})
        )
        got = await client.get(f"/batch/v1/tasks/{task_id}")

    assert got.status_code == 200
    assert task_store.rows[task_id]["status"] == "SUCCESS"
    assert await tokensession.get(task_id) is None       # 终态即清
