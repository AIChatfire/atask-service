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
            LUA_BATCH_CLAIM,
            LUA_BATCH_JOIN,
            LUA_BATCH_LEAVE,
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
        if script == LUA_BATCH_JOIN:
            # KEYS = [成员 ZSET, 到期 ZSET]；ARGV = [task_id, now, due_at, key, ttl]
            # 返回顺序与 Lua 一致：**可能为 nil 的 ZSCORE 排最后**（表在 nil 处截断）。
            members_key, due_key = args[0], args[1]
            task_id, now, due_at, group, ttl = argv[:5]
            await self.zadd(members_key, {task_id: float(now)})
            added = await self.zadd(due_key, {group: float(due_at)}, nx=True)
            await self.expire(members_key, int(ttl))
            count = await self.zcard(members_key)
            score = await self.zscore(due_key, group)
            return [count, added] if score is None else [count, added, score]
        if script == LUA_BATCH_CLAIM:
            # 摘取即互斥：摘成员与清到期索引在「同一次 EVAL」里完成
            members_key, due_key = args[0], args[1]
            out = await self.zrange(members_key, 0, -1)
            await self.delete(members_key)
            await self.zrem(due_key, argv[0])
            return out
        if script == LUA_BATCH_LEAVE:
            members_key, due_key = args[0], args[1]
            await self.zrem(members_key, argv[0])
            if await self.zcard(members_key) == 0:
                await self.delete(members_key)
                await self.zrem(due_key, argv[1])
            return 1
        raise AssertionError(f"unexpected Lua script: {script[:60]}")

    # ---- ZSET（攒批的成员索引 / 到期索引）----

    def _zset(self, key: str) -> dict[str, float]:
        """返回底层 dict 本体（不是副本）：zrem/zadd 必须落到同一份存储上。"""
        if not self._alive(key):
            return {}
        value = self._data.get(key)
        return value if isinstance(value, dict) else {}

    async def zadd(self, key: str, mapping: dict, nx: bool = False, **_: Any) -> int:
        if not self._alive(key):
            pass                                   # 过期即清空，下面重建
        cur = self._data.get(key)
        if not isinstance(cur, dict):
            cur = {}
            self._data[key] = cur
        added = 0
        for member, score in mapping.items():
            member = self._s(member)
            if nx and member in cur:
                continue
            if member not in cur:
                added += 1
            cur[member] = float(score)
        return added

    async def zcard(self, key: str) -> int:
        return len(self._zset(key))

    async def zscore(self, key: str, member: str) -> float | None:
        return self._zset(key).get(self._s(member))

    async def zrem(self, key: str, *members: str) -> int:
        cur = self._zset(key)
        removed = 0
        for member in members:
            if cur.pop(self._s(member), None) is not None:
                removed += 1
        return removed

    async def zrange(self, key: str, start: int = 0, stop: int = -1) -> list[str]:
        members = [m for m, _ in self._sorted(key)]
        return members[start:] if stop == -1 else members[start:stop + 1]

    async def zrangebyscore(self, key: str, min: Any, max: Any,
                            start: int | None = None,
                            num: int | None = None) -> list[str]:
        lo = float("-inf") if min in ("-inf", float("-inf")) else float(min)
        hi = float("inf") if max in ("+inf", float("inf")) else float(max)
        out = [m for m, score in self._sorted(key) if lo <= score <= hi]
        if start is not None:
            out = out[start:]
        if num is not None:
            out = out[:num]
        return out

    def _sorted(self, key: str) -> list[tuple[str, float]]:
        # 真 Redis 的 ZSET 同分按成员字典序，批次成员的 score 可能相同（同一秒入批）
        return sorted(self._zset(key).items(), key=lambda kv: (kv[1], kv[0]))

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
    #
    # 攒批上线后新增 services.batching（顶层 import r）：它是**放行链路的唯一入口**，
    # 漏登记的后果不是「用例打不到桩」而是「用例静默操作真 Redis 里的真实批次索引」
    # ——本地恰好有 Redis 时全绿、CI 里莫名红，是测试虚假绿灯的典型形态。
    import app.deps.ratelimit
    import app.healthz
    import app.queue
    import app.services.batching
    import app.services.dynconf
    import app.services.idem
    import app.services.relayflow
    import app.services.tokensession
    import app.services.upstream

    for module in (
        app.deps.ratelimit,
        app.healthz,
        app.queue,
        app.services.batching,
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

    # ---- 攒批（batching）相关的放行权 / 还槽权 / 兜底查询 ----
    #
    # 这几个必须与原实现**同语义**，否则攒批用例会「绿在假实现上」：
    #   - claim_for_release 是条件更新（起点不符即 False），不是无条件赋值；
    #   - claim_slot_release 的**缺键视为已占槽**（COALESCE(...,1)）是刻意语义，
    #     不是随手写的默认值（本特性上线前的在途任务没有 slot_flags 键）。

    async def get_batch_meta(self, task_id: str) -> dict | None:
        row = self.rows.get(task_id)
        if not row:
            return None
        data = row["data"]
        return {
            "task_id": task_id,
            "status": row["status"],
            "token_hash": data.get("token_hash"),
            "batch_state": data.get("batch_state"),
            "slot_flags": data.get("slot_flags", 0),
            "batch_key": data.get("batch_key"),
            "requeue_attempts": data.get("requeue_attempts", 0),
        }

    async def claim_for_release(self, task_id: str) -> bool:
        from app.services.taskstore import BATCH_WAITING_STATES

        row = self.rows.get(task_id)
        if not row or row["status"] != "SUBMITTED":
            return False
        if row["data"].get("batch_state") not in BATCH_WAITING_STATES:
            return False
        row["data"]["batch_state"] = "releasing"
        row["updated_at"] = int(time.time())
        return True

    async def unclaim_for_release(self, task_id: str, restore: str = "waiting") -> None:
        row = self.rows.get(task_id)
        if not row or row["status"] != "SUBMITTED":
            return
        if row["data"].get("batch_state") != "releasing":
            return
        row["data"]["batch_state"] = restore
        row["updated_at"] = int(time.time())

    async def claim_slot_release(self, task_id: str) -> bool:
        row = self.rows.get(task_id)
        if not row:
            return False
        if int(row["data"].get("slot_flags", 1) or 0) <= 0:
            return False
        row["data"]["slot_flags"] = 0
        row["updated_at"] = int(time.time())
        return True

    async def stale_batch_waiting(self, stale_seconds: int,
                                  limit: int = 200) -> list[dict]:
        from app.services.taskstore import BATCH_WAITING_STATES

        cutoff = int(time.time()) - max(0, int(stale_seconds))
        out: list[dict] = []
        for task_id, row in self.rows.items():
            data = row["data"]
            if data.get("source") != "queue" or row["status"] != "SUBMITTED":
                continue
            state = data.get("batch_state")
            due = int(data.get("batch_due_at") or 0)
            waiting_due = state in BATCH_WAITING_STATES and 0 < due <= cutoff
            # 卡在 releasing 的判定用 updated_at（抢权时刷新），**不能用 batch_due_at**：
            # 放行正是由到期触发的，它的 due 必然已是过去时刻，拿它判会把正在飞的放行
            # 也捞出来（真 SQL 里有同样的注释）。
            stuck_releasing = (state == "releasing"
                               and int(row.get("updated_at") or 0) <= cutoff)
            if not (waiting_due or stuck_releasing):
                continue
            out.append({
                "task_id": task_id, "status": row["status"],
                "token_hash": data.get("token_hash"),
                "batch_state": state, "batch_due_at": due,
            })
        out.sort(key=lambda item: item["batch_due_at"])
        return out[:max(1, min(int(limit), 500))]


@pytest.fixture
def task_store(monkeypatch: pytest.MonkeyPatch) -> InMemoryTaskStore:
    """把 app.services.taskstore 模块函数替换为内存实现（relayflow / batching 共用）。

    **清单必须与原实现的消费面同步**：漏登记的函数会打到真 MySQL——本地可能恰好有库
    而绿，CI 里红，更糟的是「跑在真实状态上却报绿」。新增放行相关函数时记得加。
    """
    import app.services.taskstore as ts

    store = InMemoryTaskStore()
    for name in ("create", "get", "cas", "patch_data", "counts_by_status",
                 "get_batch_meta", "claim_for_release", "unclaim_for_release",
                 "claim_slot_release", "stale_batch_waiting"):
        monkeypatch.setattr(ts, name, getattr(store, name))
    return store


# ---------------------------------------------------------------------------
# 队列边界（taskiq 发布门面 → AsyncMock 记录器）
# ---------------------------------------------------------------------------


@pytest.fixture
def queue_events(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """拦截 app.queue 发布门面，记录调用参数（queue submit / notify）。"""
    from unittest.mock import AsyncMock

    import app.queue as q

    events: dict[str, list] = {"queue_submit": [], "notify": []}

    async def _queue_submit(task_id: str) -> None:
        events["queue_submit"].append(task_id)

    async def _notify(task_id: str, url: str, payload: dict) -> None:
        events["notify"].append({"task_id": task_id, "url": url, "payload": payload})

    monkeypatch.setattr(q, "publish_queue_submit", AsyncMock(side_effect=_queue_submit))
    monkeypatch.setattr(q, "publish_notify", AsyncMock(side_effect=_notify))
    return events
