"""W1 健康检查测试：live 恒 200；ready 按 Redis/DB 状态 200/503（不抛异常）。"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from w1_helpers import FakeRedis, make_session, make_session_factory

import app.healthz as healthz
from app.healthz import healthz_live, healthz_ready


async def test_live_always_ok() -> None:
    assert await healthz_live() == {"status": "ok"}


async def test_ready_all_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(healthz, "get_redis", AsyncMock(return_value=FakeRedis()))
    monkeypatch.setattr(healthz, "get_session_factory",
                        lambda: make_session_factory(make_session()))
    resp = await healthz_ready()
    assert resp.status_code == 200


async def test_ready_redis_down_503(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(healthz, "get_redis",
                        AsyncMock(side_effect=ConnectionError("redis down")))
    monkeypatch.setattr(healthz, "get_session_factory",
                        lambda: make_session_factory(make_session()))
    resp = await healthz_ready()
    assert resp.status_code == 503
    assert b'"redis":"fail: ConnectionError"' in resp.body


async def test_ready_db_down_503(monkeypatch: pytest.MonkeyPatch) -> None:
    session = make_session()
    session.execute = AsyncMock(side_effect=RuntimeError("db down"))
    monkeypatch.setattr(healthz, "get_redis", AsyncMock(return_value=FakeRedis()))
    monkeypatch.setattr(healthz, "get_session_factory",
                        lambda: make_session_factory(session))
    resp = await healthz_ready()
    assert resp.status_code == 503
