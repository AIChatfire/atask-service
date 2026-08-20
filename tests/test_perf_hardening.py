"""长期运行稳定性三件套的专项回归：
- 并发槽 TTL + 校准（conc_recalibrate：泄漏收回 / 少计补齐 / 归零删键）
- sweep 重入锁（慢轮时 cron 叠加轮直接跳过；异常也释放锁）
- tidx 上游 id 反查索引（命中直达 / 脏索引回落 SQL / SQL 命中回写索引）
"""

from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.deps import ratelimit
from app.redis import K_CONC, K_SWEEP_LOCK, K_TIDX
from app.services import reconcile

# ---------------------------------------------------------------------------
# 并发槽校准
# ---------------------------------------------------------------------------


def _seed_active(task_store, task_id: str, token_hash: str, status: str = "SUBMITTED"):
    now = int(time.time())
    task_store.rows[task_id] = {
        "task_id": task_id, "platform": "gateway", "action": "video",
        "status": status, "progress": "0%", "fail_reason": "",
        "data": {"biz": "minimax", "token_hash": token_hash, "settled": False},
        "user_id": 1, "channel_id": 7,
        "submit_time": now, "created_at": now, "updated_at": now, "finish_time": 0,
    }


async def test_recalibrate_reclaims_leaked_slots(test_settings, patch_redis, task_store):
    """泄漏（Redis > 实际）：崩溃丢 release 后计数虚高 → 回写为事实源。"""
    _seed_active(task_store, "t1", "h-leak")
    await patch_redis.set(K_CONC.format(token_hash="h-leak"), "3")

    fixed = await ratelimit.conc_recalibrate()

    assert fixed == 1
    assert await patch_redis.get(K_CONC.format(token_hash="h-leak")) == "1"


async def test_recalibrate_deletes_key_when_no_active(test_settings, patch_redis, task_store):
    """实际为 0：键直接删除（彻底归还并发余额）。"""
    await patch_redis.set(K_CONC.format(token_hash="h-gone"), "2")

    fixed = await ratelimit.conc_recalibrate()

    assert fixed == 1
    assert await patch_redis.get(K_CONC.format(token_hash="h-gone")) is None


async def test_recalibrate_restores_undercount(test_settings, patch_redis, task_store):
    """少计（Redis < 实际，如 TTL 过期重建）：补齐计数防止放行超限并发。"""
    _seed_active(task_store, "t1", "h-under")
    _seed_active(task_store, "t2", "h-under", status="QUEUED")

    fixed = await ratelimit.conc_recalibrate()

    assert fixed == 1
    assert await patch_redis.get(K_CONC.format(token_hash="h-under")) == "2"


async def test_recalibrate_skips_consistent_and_held(test_settings, patch_redis, task_store):
    """一致不动；HELD 不计（挂起时槽已释放，口径同 acquire/release）。"""
    _seed_active(task_store, "t1", "h-ok")
    _seed_active(task_store, "t2", "h-ok", status="HELD")   # 不计入
    await patch_redis.set(K_CONC.format(token_hash="h-ok"), "1")

    fixed = await ratelimit.conc_recalibrate()

    assert fixed == 0
    assert await patch_redis.get(K_CONC.format(token_hash="h-ok")) == "1"


async def test_conc_acquire_sets_ttl(test_settings, patch_redis):
    """占槽时挂 TTL 兜底：进程崩溃后槽不再永久泄漏。"""
    assert await ratelimit.conc_try_acquire("h-ttl")
    key = K_CONC.format(token_hash="h-ttl")
    assert key in patch_redis._expires   # TTL 已挂
    assert await patch_redis.get(key) == "1"


# ---------------------------------------------------------------------------
# sweep 重入锁
# ---------------------------------------------------------------------------


async def test_sweep_skips_when_lock_held(test_settings, patch_redis, monkeypatch):
    """上一轮未结束（锁被占）：本轮直接跳过，不叠加并发轮。"""
    inner = AsyncMock()
    monkeypatch.setattr(reconcile, "_sweep_once_locked", inner)
    await patch_redis.set(K_SWEEP_LOCK, "1")

    await reconcile.sweep_once()

    inner.assert_not_awaited()
    assert await patch_redis.get(K_SWEEP_LOCK) == "1"   # 不是本轮的锁，不许误删


async def test_sweep_runs_and_releases_lock(test_settings, patch_redis, monkeypatch):
    """正常轮：拿锁 → 执行 → 释放（下一轮可继续）。"""
    inner = AsyncMock()
    monkeypatch.setattr(reconcile, "_sweep_once_locked", inner)

    await reconcile.sweep_once()

    inner.assert_awaited_once()
    assert await patch_redis.get(K_SWEEP_LOCK) is None


async def test_sweep_releases_lock_on_failure(test_settings, patch_redis, monkeypatch):
    """轮内异常：锁也要释放，不永久卡死巡检。"""
    inner = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(reconcile, "_sweep_once_locked", inner)

    with pytest.raises(RuntimeError):
        await reconcile.sweep_once()

    assert await patch_redis.get(K_SWEEP_LOCK) is None


# ---------------------------------------------------------------------------
# tidx 上游 id 反查索引（真实 taskstore 实现；SQL 边界用假 session 工厂钉住）
# ---------------------------------------------------------------------------


class _FakeDB:
    """get_session_factory()() 的替身：execute 返回预置行（None = 无兜底命中）。"""

    def __init__(self, row: dict | None = None):
        self.row = row
        self.sql_calls = 0

    async def __aenter__(self) -> _FakeDB:
        return self

    async def __aexit__(self, *_: Any) -> bool:
        return False

    async def execute(self, *_: Any, **__: Any) -> Any:
        self.sql_calls += 1
        row = self.row

        class _Res:
            def mappings(self) -> Any:
                class _M:
                    @staticmethod
                    def first() -> dict | None:
                        return row
                return _M()
        return _Res()

    async def commit(self) -> None:
        pass


def _sql_row(task_id: str, upstream_task_id: str) -> dict:
    return {
        "task_id": task_id, "platform": "gateway", "status": "SUBMITTED",
        "data": json.dumps({"upstream_task_id": upstream_task_id}),
    }


async def test_tidx_hit_skips_sql(test_settings, patch_redis, monkeypatch):
    """索引命中且校验一致：直达返回，SQL 全表扫描零次。"""
    import app.services.taskstore as ts

    db = _FakeDB(row=None)
    monkeypatch.setattr(ts, "get_session_factory", lambda: lambda: db)

    async def fake_get(task_id: str) -> dict | None:
        assert task_id == "local-1"
        return {"task_id": "local-1", "data": {"upstream_task_id": "mm-9"}}

    monkeypatch.setattr(ts, "get", fake_get)
    await patch_redis.set(K_TIDX.format(upstream_task_id="mm-9"), "local-1")

    task = await ts.get_by_upstream_id("mm-9")

    assert task and task["task_id"] == "local-1"
    assert db.sql_calls == 0


async def test_tidx_stale_pointer_falls_back_to_sql(test_settings, patch_redis, monkeypatch):
    """索引指向脏数据（校验不一致）：回落 SQL，命中后索引被纠正。"""
    import app.services.taskstore as ts

    db = _FakeDB(row=_sql_row("local-2", "mm-9"))
    monkeypatch.setattr(ts, "get_session_factory", lambda: lambda: db)

    async def fake_get(task_id: str) -> dict | None:   # 索引指向的任务已被改写
        return {"task_id": "local-stale", "data": {"upstream_task_id": "mm-other"}}

    monkeypatch.setattr(ts, "get", fake_get)
    await patch_redis.set(K_TIDX.format(upstream_task_id="mm-9"), "local-stale")

    task = await ts.get_by_upstream_id("mm-9")

    assert task and task["task_id"] == "local-2"
    assert db.sql_calls == 1
    assert await patch_redis.get(K_TIDX.format(upstream_task_id="mm-9")) == "local-2"


async def test_tidx_sql_hit_backfills_index(test_settings, patch_redis, monkeypatch):
    """无索引：SQL 兜底命中后回写索引（下次直达）。"""
    import app.services.taskstore as ts

    db = _FakeDB(row=_sql_row("local-3", "mm-3"))
    monkeypatch.setattr(ts, "get_session_factory", lambda: lambda: db)

    task = await ts.get_by_upstream_id("mm-3")

    assert task and task["task_id"] == "local-3"
    assert await patch_redis.get(K_TIDX.format(upstream_task_id="mm-3")) == "local-3"


async def test_patch_data_writes_tidx(test_settings, patch_redis, monkeypatch):
    """patch_data 回填 upstream_task_id 时顺带写索引（submit/held 恢复/proxy 三点共用）。"""
    import app.services.taskstore as ts

    db = _FakeDB(row=None)
    monkeypatch.setattr(ts, "get_session_factory", lambda: lambda: db)

    await ts.patch_data("local-4", {"upstream_task_id": "mm-4", "other": 1})

    assert await patch_redis.get(K_TIDX.format(upstream_task_id="mm-4")) == "local-4"


async def test_patch_data_without_upstream_id_skips_tidx(test_settings, patch_redis, monkeypatch):
    """补丁不含 upstream_task_id：不碰索引。"""
    import app.services.taskstore as ts

    db = _FakeDB(row=None)
    monkeypatch.setattr(ts, "get_session_factory", lambda: lambda: db)

    await ts.patch_data("local-5", {"progress": "50%"})

    assert not [k for k in patch_redis._data if k.startswith("gw:tidx:")]
