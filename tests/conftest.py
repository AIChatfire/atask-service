"""统一测试基建：内存版 Redis / 内存 taskstore / respx 出站拦截 / 受控 settings。

原则：单测不依赖真实 MySQL/Redis/上游/三微服务；外部边界只有两处——
- HTTP 出站（providers 与 upstream 引擎）：respx 拦截；
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

    async def scan_iter(self, pattern: str):
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
    """把网关所有 ``from app.redis import r`` 的消费方统一切换到 FakeRedis。"""
    import app.deps.auth
    import app.deps.ratelimit
    import app.healthz
    import app.queue
    import app.routers.callback
    import app.services.idem
    import app.services.reconcile
    import app.services.statelog
    import app.services.submit
    import app.services.tokensession
    import app.services.upstream

    for module in (
        app.deps.auth,
        app.deps.ratelimit,
        app.healthz,
        app.queue,
        app.routers.callback,
        app.services.idem,
        app.services.reconcile,
        app.services.statelog,
        app.services.submit,
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

    monkeypatch.setattr(settings, "key_svc_url", "http://keypool.test")
    monkeypatch.setattr(settings, "key_svc_token", "kp-token")
    monkeypatch.setattr(settings, "billing_svc_url", "http://billing.test")
    monkeypatch.setattr(settings, "gateway_public_base_url", "https://gw.test")
    monkeypatch.setattr(settings, "logfire_enabled", False)
    return settings


# ---------------------------------------------------------------------------
# 内存 taskstore（替换 app.services.taskstore 全部函数，保持 CAS 语义）
# ---------------------------------------------------------------------------


class InMemoryTaskStore:
    """tasks 表内存实现：create/get/cas/patch_data/mark_settled 语义对齐。"""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    async def create(self, task_id: str, user_id: int, channel_id: int,
                     action: str, data: dict) -> None:
        now = int(time.time())
        self.rows[task_id] = {
            "task_id": task_id, "platform": "gateway", "action": action,
            "status": "SUBMITTED", "progress": "0%", "fail_reason": "",
            "data": json.loads(json.dumps(data, ensure_ascii=False)),
            "user_id": user_id, "channel_id": channel_id,
            "submit_time": now, "start_time": now, "created_at": now,
            "updated_at": now, "finish_time": 0,
        }

    async def get(self, task_id: str) -> dict | None:
        row = self.rows.get(task_id)
        return dict(row) if row else None

    async def get_by_upstream_id(self, upstream_task_id: str) -> dict | None:
        for row in self.rows.values():
            if (row.get("data") or {}).get("upstream_task_id") == upstream_task_id:
                return dict(row)
        return None

    async def cas(self, task_id: str, from_statuses: tuple[str, ...], to_status: str,
                  patch: dict | None = None, fail_reason: str = "") -> bool:
        row = self.rows.get(task_id)
        if not row or row["status"] not in from_statuses:
            return False
        row["status"] = to_status
        row["updated_at"] = int(time.time())
        if to_status in ("SUCCESS", "FAILURE", "CANCELED"):
            row["finish_time"] = int(time.time())
        if fail_reason:
            row["fail_reason"] = fail_reason
        if to_status == "SUCCESS":
            row["progress"] = "100%"
        if patch:
            row["data"].update(patch)
        return True

    async def patch_data(self, task_id: str, patch: dict,
                         status: str | None = None,
                         channel_id: int | None = None) -> None:
        row = self.rows.get(task_id)
        if not row:
            return
        if status:
            row["status"] = status
        if channel_id:
            row["channel_id"] = channel_id
        row["data"].update(patch)
        row["updated_at"] = int(time.time())

    async def mark_settled(self, task_id: str, amount: float) -> None:
        await self.patch_data(task_id, {"settled": True, "settled_amount": amount})

    # ---- sweeper 查询（语义对齐 app.services.taskstore）----

    async def stale_active(self, stale_seconds: int, limit: int = 200) -> list[str]:
        cutoff = int(time.time()) - stale_seconds
        return [t["task_id"] for t in self.rows.values()
                if t["status"] in ("SUBMITTED", "QUEUED", "IN_PROGRESS")
                and t["updated_at"] < cutoff][:limit]

    async def terminal_unsettled(self, limit: int = 200) -> list[dict]:
        out = []
        for t in self.rows.values():
            if t["status"] in ("SUCCESS", "FAILURE", "CANCELED") \
                    and not t["data"].get("settled"):
                out.append({"task_id": t["task_id"], "status": t["status"],
                            "data": dict(t["data"])})
        return out[:limit]

    async def counts_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for t in self.rows.values():
            counts[t["status"]] = counts.get(t["status"], 0) + 1
        return counts

    async def orphan_active(self, older_than_seconds: int, limit: int = 50) -> list[str]:
        cutoff = int(time.time()) - older_than_seconds
        return [t["task_id"] for t in self.rows.values()
                if t["status"] in ("SUBMITTED", "QUEUED", "IN_PROGRESS")
                and not t["data"].get("upstream_task_id")
                and t["created_at"] < cutoff][:limit]

    async def expiring_freezes(self, margin_seconds: int, limit: int = 100) -> list[dict]:
        deadline = int(time.time()) + margin_seconds
        out = []
        for t in self.rows.values():
            d = t["data"]
            exp = int(d.get("freeze_expires_at") or 0)
            if (t["status"] in ("SUBMITTED", "QUEUED", "IN_PROGRESS", "HELD")
                    and 0 < exp <= deadline and not d.get("settled")):
                out.append({"task_id": t["task_id"], "data": dict(d)})
        return out[:limit]

    async def oldest_held(self) -> str | None:
        held = [t for t in self.rows.values() if t["status"] == "HELD"]
        if not held:
            return None
        return min(held, key=lambda t: t["created_at"])["task_id"]

    async def held_expired(self, max_age_seconds: int,
                           rate_limited_max_age_seconds: int = 3600,
                           limit: int = 100) -> list[str]:
        now = int(time.time())
        cutoff, cutoff_rl = now - max_age_seconds, now - rate_limited_max_age_seconds
        out = []
        for t in self.rows.values():
            if t["status"] != "HELD":
                continue
            rl = t["data"].get("held_reason") == "rate_limited"
            if t["updated_at"] < (cutoff_rl if rl else cutoff):
                out.append(t["task_id"])
        return out[:limit]

    async def reconcile_candidates(self, window_seconds: int, recheck_seconds: int,
                                   limit: int = 20) -> list[dict]:
        now = int(time.time())
        out = []
        for t in self.rows.values():
            d = t["data"]
            if (t["status"] == "FAILURE" and d.get("settled")
                    and not d.get("reconciled") and d.get("upstream_task_id")
                    and (t.get("finish_time") or 0) > now - window_seconds
                    and int(d.get("reconcile_checked_at") or 0) < now - recheck_seconds):
                out.append({"task_id": t["task_id"], "data": dict(d)})
        return out[:limit]


@pytest.fixture
def task_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryTaskStore:
    """把 app.services.taskstore 模块函数替换为内存实现（flow/polling 共用）。"""
    import app.services.taskstore as ts

    store = InMemoryTaskStore()
    for name in ("create", "get", "get_by_upstream_id", "cas", "patch_data",
                 "mark_settled", "stale_active", "terminal_unsettled",
                 "counts_by_status", "orphan_active", "expiring_freezes",
                 "reconcile_candidates", "oldest_held", "held_expired"):
        monkeypatch.setattr(ts, name, getattr(store, name))
    return store


# ---------------------------------------------------------------------------
# 队列边界（taskiq 发布门面 → AsyncMock 记录器）
# ---------------------------------------------------------------------------


@pytest.fixture
def queue_events(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """拦截 app.queue 发布门面，记录调用参数（submit/settle/cancel/notify/poll）。"""
    from unittest.mock import AsyncMock

    import app.queue as q

    events: dict[str, list] = {"submit": [], "settle": [], "cancel": [], "notify": [],
                               "poll": [], "resume_held": []}

    async def _submit(task_id):
        events["submit"].append(task_id)

    async def _settle(request_id, actual_amount, user_sk, units=None, attrs=None):
        events["settle"].append({
            "request_id": request_id, "actual_amount": actual_amount,
            "user_sk": user_sk, "units": units, "attrs": attrs,
        })

    async def _cancel(request_id, user_sk):
        events["cancel"].append({"request_id": request_id, "user_sk": user_sk})

    async def _notify(task_id, url, payload):
        events["notify"].append({"task_id": task_id, "url": url, "payload": payload})

    async def _poll(task_id, delay):
        events["poll"].append({"task_id": task_id, "delay": delay})

    async def _resume_held(delay):
        events["resume_held"].append({"delay": delay})

    monkeypatch.setattr(q, "publish_submit", AsyncMock(side_effect=_submit))
    monkeypatch.setattr(q, "publish_settle", AsyncMock(side_effect=_settle))
    monkeypatch.setattr(q, "publish_cancel", AsyncMock(side_effect=_cancel))
    monkeypatch.setattr(q, "publish_notify", AsyncMock(side_effect=_notify))
    monkeypatch.setattr(q, "schedule_poll", AsyncMock(side_effect=_poll))
    monkeypatch.setattr(q, "schedule_resume_held", AsyncMock(side_effect=_resume_held))
    # polling.py / held.py 是 from-import 直接绑定名字，需同步打补丁
    import app.services.held as held_mod
    import app.services.polling as polling_mod

    monkeypatch.setattr(polling_mod, "schedule_poll", AsyncMock(side_effect=_poll))
    monkeypatch.setattr(held_mod, "schedule_poll", AsyncMock(side_effect=_poll))
    monkeypatch.setattr(held_mod, "schedule_resume_held", AsyncMock(side_effect=_resume_held))
    return events


# ---------------------------------------------------------------------------
# 工厂 fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def key_lease_factory():
    """KeyLease 工厂（含渠道覆盖与 setting.gateway 提取配置）。"""
    from app.schemas import KeyLease

    def _make(**overrides: Any) -> KeyLease:
        kwargs: dict[str, Any] = {
            "key_id": 7, "key_index": 0, "key": "sk-upstream-key",
            "base_url": "http://upstream.test", "epoch": "a1b2c3d4",
            "channel": {
                "id": 7, "name": "upstream-a", "base_url": "http://upstream.test",
                "setting": {},
            },
        }
        kwargs.update(overrides)
        return KeyLease(**kwargs)

    return _make


@pytest.fixture
def route_factory():
    """RouteConfig 工厂（MiniMax-H3 提取配置为默认形态）。"""
    from app.schemas import RouteConfig

    def _make(**overrides: Any) -> RouteConfig:
        kwargs: dict[str, Any] = {
            "biz": "minimax",
            "upstream_base_url": "http://upstream.test",
            "submit_path": "/v2/video_generation",
            "probe_path": "/v2/query/video_generation/{upstream_task_id}",
            "task_id_path": "task_id",
            "status_path": "task.status",
            "result_path": "task.content.url",
            "error_path": "task.error",
            "settle_usage_map": {"duration": "task.usage.output_seconds"},
        }
        kwargs.update(overrides)
        return RouteConfig(**kwargs)

    return _make
