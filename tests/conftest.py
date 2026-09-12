"""统一测试基建：内存版 Redis / 内存 taskstore / respx 出站拦截 / 受控 settings。

原则：单测不依赖真实 MySQL/Redis/上游；外部边界只有两处——
- HTTP 出站（relay 出站）：respx 拦截；
- Redis：FakeRedis（decode_responses=True 语义，覆盖网关用到的命令子集）。
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
import respx

# ---------------------------------------------------------------------------
# FakeRedis
# ---------------------------------------------------------------------------


class FakePipeline:
    """收集 incr/expire/delete 等命令，execute 时顺序执行（breaker 计数用）。"""

    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._ops: list[tuple[str, tuple]] = []

    def incr(self, key: str) -> FakePipeline:
        self._ops.append(("incr", (key,)))
        return self

    def expire(self, key: str, seconds: int) -> FakePipeline:
        self._ops.append(("expire", (key, seconds)))
        return self

    async def execute(self) -> list[Any]:
        out = []
        for op, args in self._ops:
            out.append(await getattr(self._redis, op)(*args))
        self._ops.clear()
        return out


class FakeRedis:
    """内存版异步 Redis（单测用）。值一律按 str 存储/返回。"""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._expires: dict[str, float] = {}

    def _expired(self, key: str) -> bool:
        exp = self._expires.get(key)
        return exp is not None and exp <= time.time()

    def _alive(self, key: str) -> bool:
        if self._expired(key):
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return False
        return True

    @staticmethod
    def _s(value: Any) -> str:
        return value if isinstance(value, str) else str(value)

    # ---- 通用 ----

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None

    def pipeline(self) -> FakePipeline:
        return FakePipeline(self)

    async def delete(self, *keys: str) -> int:
        n = 0
        for key in keys:
            if self._alive(key) and key in self._data:
                n += 1
            self._data.pop(key, None)
            self._expires.pop(key, None)
        return n

    async def expire(self, key: str, seconds: int) -> bool:
        if not (self._alive(key) and key in self._data):
            return False
        self._expires[key] = time.time() + seconds
        return True

    async def ttl(self, key: str) -> int:
        """语义对齐 Redis TTL：不存在 -2；无过期 -1；否则剩余秒数。"""
        if not self._alive(key):
            return -2
        exp = self._expires.get(key)
        return -1 if exp is None else max(0, int(exp - time.time()))

    # ---- STRING ----

    async def get(self, key: str) -> str | None:
        if not self._alive(key):
            return None
        value = self._data.get(key)
        return value if isinstance(value, str) else None

    async def set(self, key: str, value: Any, ex: int | None = None,
                  nx: bool = False, **_: Any) -> Any:
        if nx and self._alive(key) and key in self._data:
            return None
        self._data[key] = self._s(value)
        if ex is not None:
            self._expires[key] = time.time() + ex
        return True

    async def incr(self, key: str) -> int:
        cur = await self.get(key)
        new = (int(cur) if cur is not None else 0) + 1
        self._data[key] = str(new)
        return new

    # ---- LIST / STREAM / SCAN（queue_stats 用）----

    async def llen(self, key: str) -> int:
        value = self._data.get(key) if self._alive(key) else None
        return len(value) if isinstance(value, list) else 0

    async def xlen(self, key: str) -> int:
        value = self._data.get(key) if self._alive(key) else None
        return len(value) if isinstance(value, list) else 0

    async def scan_iter(self, pattern: str, count: int | None = None):
        import fnmatch

        for key in list(self._data):
            if self._alive(key) and fnmatch.fnmatch(key, pattern):
                yield key

    # ---- Lua（app.redis 脚本按常量等价实现）----

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        from app.redis import (
            LUA_CAS_DELETE,
            LUA_CONC_ACQUIRE,
            LUA_CONC_RELEASE,
            LUA_RATE_LIMIT,
        )

        key = args[0]
        argv = [str(a) for a in args[numkeys:]]
        if script == LUA_CAS_DELETE:
            if self._alive(key) and self._data.get(key) == argv[0]:
                self._data.pop(key, None)
                self._expires.pop(key, None)
                return 1
            return 0
        if script == LUA_RATE_LIMIT:
            now_ms, window_ms, limit = int(argv[0]), int(argv[1]), int(argv[2])
            entries: list[int] = self._data.get(key) if self._alive(key) else []
            entries = [t for t in (entries or []) if t > now_ms - window_ms]
            if len(entries) >= limit:
                self._data[key] = entries
                return 0
            entries.append(now_ms)
            self._data[key] = entries
            self._expires[key] = time.time() + window_ms / 1000
            return 1
        if script == LUA_CONC_ACQUIRE:
            limit = int(argv[0])
            cur = int(self._data.get(key, "0")) if self._alive(key) else 0
            if cur + 1 > limit:
                return 0
            self._data[key] = str(cur + 1)
            if len(argv) > 1:                      # TTL 兜底（防占槽崩溃永久泄漏）
                self._expires[key] = time.time() + int(argv[1])
            return 1
        if script == LUA_CONC_RELEASE:
            cur = int(self._data.get(key, "0")) if self._alive(key) else 0
            self._data[key] = str(max(0, cur - 1))
            return 1
        raise AssertionError(f"unexpected Lua script: {script[:60]}")

    # ---- 测试辅助 ----

    def dump(self) -> dict[str, Any]:
        return {k: v for k, v in self._data.items() if self._alive(k)}


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def patch_redis(monkeypatch: pytest.MonkeyPatch, fake_redis: FakeRedis) -> FakeRedis:
    """把网关所有 ``from app.redis import r`` 的消费方统一切换到 FakeRedis。

    **清单必须与「import ``app.redis`` 的模块全集」一致**——这是手工维护的清单，
    历史上已漂移过（``app.main`` / ``services.dynconf`` / ``services.relayflow``
    被漏掉，导致未挂本夹具的用例静默打到真 Redis）。现已由静态门禁
    ``tests/test_static_gates.py::test_redis_patch_list_covers_importers`` 机械保证，
    新增模块若 import 了 ``app.redis``，忘加这里会直接让门禁转红。
    """
    # 注意 app.main **不在此列**：它在 lifespan() 里**嵌套**导入 r，模块顶层没有
    # 这个属性，`monkeypatch.setattr` 会直接 AttributeError。门禁
    # test_redis_patch_list_covers_importers 只要求覆盖「顶层导入」的模块，正是
    # 为了把这种形态区别对待——按名字强塞会炸掉整套测试。
    import app.deps.ratelimit
    import app.healthz
    import app.queue
    import app.services.dynconf
    import app.services.idem
    import app.services.relayflow
    import app.services.tokensession
    import app.services.upstream

    for module in (
        app.deps.ratelimit,
        app.healthz,
        app.queue,
        app.services.dynconf,
        app.services.idem,
        app.services.relayflow,
        app.services.tokensession,
        app.services.upstream,
    ):
        monkeypatch.setattr(module, "r", fake_redis)
    return fake_redis


# ---------------------------------------------------------------------------
# respx 出站拦截
# ---------------------------------------------------------------------------


@pytest.fixture
def respx_router() -> Iterator[respx.MockRouter]:
    """respx 拦截器（assert_all_mocked=True：任何未声明的出站请求立即失败）。"""
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        yield router


# ---------------------------------------------------------------------------
# 受控 settings（进程单例属性级替换；模块共享同一对象，改属性即生效）
# ---------------------------------------------------------------------------


@pytest.fixture
def test_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "logfire_enabled", False)
    return settings


# ---------------------------------------------------------------------------
# 内存 taskstore（替换 app.services.taskstore 全部函数，保持 CAS 语义）
# ---------------------------------------------------------------------------


class InMemoryTaskStore:
    """tasks 表内存实现：create/get/cas/patch_data/counts_by_status 语义对齐。"""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    async def create(self, task_id: str, user_id: int, channel_id: int,
                     action: str, data: dict) -> None:
        from app.config import settings   # 惰性导入：与真实 taskstore 同一取值点

        now = int(time.time())
        self.rows[task_id] = {
            "task_id": task_id, "platform": settings.gateway_platform, "action": action,
            "status": "SUBMITTED", "progress": "0%", "fail_reason": "",
            "data": json.loads(json.dumps(data, ensure_ascii=False)),
            "user_id": user_id, "channel_id": channel_id,
            "submit_time": now, "start_time": now, "created_at": now,
            "updated_at": now, "finish_time": 0,
        }

    async def get(self, task_id: str) -> dict | None:
        row = self.rows.get(task_id)
        return dict(row) if row else None

    async def cas(self, task_id: str, from_statuses: tuple[str, ...], to_status: str,
                  patch: dict | None = None, fail_reason: str = "") -> bool:
        row = self.rows.get(task_id)
        if not row or row["status"] not in from_statuses:
            return False
        row["status"] = to_status
        row["updated_at"] = int(time.time())
        if to_status in ("SUCCESS", "FAILURE", "CANCELED"):
            row["finish_time"] = int(time.time())     # 秒（与真实实现同口径）
            row["progress"] = "100%"                  # 终态一律 100%，不只 SUCCESS
        if fail_reason:
            row["fail_reason"] = fail_reason
        if patch:
            row["data"].update(patch)
        return True

    async def patch_data(self, task_id: str, patch: dict,
                         status: str | None = None) -> None:
        row = self.rows.get(task_id)
        if not row:
            return
        if status:
            row["status"] = status
        row["data"].update(patch)
        row["updated_at"] = int(time.time())

    async def counts_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for t in self.rows.values():
            counts[t["status"]] = counts.get(t["status"], 0) + 1
        return counts


@pytest.fixture
def task_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryTaskStore:
    """把 app.services.taskstore 模块函数替换为内存实现（relayflow 共用）。"""
    import app.services.taskstore as ts

    store = InMemoryTaskStore()
    for name in ("create", "get", "cas", "patch_data", "counts_by_status"):
        monkeypatch.setattr(ts, name, getattr(store, name))
    return store


# ---------------------------------------------------------------------------
# 队列边界（taskiq 发布门面 → AsyncMock 记录器）
# ---------------------------------------------------------------------------


@pytest.fixture
def queue_events(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """拦截 app.queue 发布门面，记录调用参数（batch submit / notify）。"""
    from unittest.mock import AsyncMock

    import app.queue as q

    events: dict[str, list] = {"batch_submit": [], "notify": []}

    async def _batch_submit(task_id: str) -> None:
        events["batch_submit"].append(task_id)

    async def _notify(task_id: str, url: str, payload: dict) -> None:
        events["notify"].append({"task_id": task_id, "url": url, "payload": payload})

    monkeypatch.setattr(q, "publish_batch_submit", AsyncMock(side_effect=_batch_submit))
    monkeypatch.setattr(q, "publish_notify", AsyncMock(side_effect=_notify))
    return events
