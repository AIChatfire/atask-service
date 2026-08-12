"""W1 测试共享替身：FakeRedis（含 Lua 语义等价实现）/ FakeSession / 工厂函数。

不引入新依赖（SPEC §7.1 允许 fakeredis 或 AsyncMock——pyproject 未登记
fakeredis，故用最小自研 fake；只覆盖 W1 代码路径用到的命令子集）。
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from app import middleware
from app.auth import TokenInfo, token_hash
from app.registry import BizConfig


class FakePipeline:
    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._ops: list[tuple] = []

    def hset(self, key: str, mapping: dict | None = None, **kw: Any) -> FakePipeline:
        self._ops.append(("hset", key, mapping or kw))
        return self

    def delete(self, *keys: str) -> FakePipeline:
        self._ops.append(("delete", keys))
        return self

    async def execute(self) -> list[Any]:
        out = []
        for op in self._ops:
            if op[0] == "hset":
                out.append(await self._redis.hset(op[1], mapping=op[2]))
            else:
                out.append(await self._redis.delete(*op[1]))
        self._ops.clear()
        return out


class FakeRedis:
    """字符串/HASH/ZSET 子集 + W1 四个 Lua 脚本的等价 Python 实现。"""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.sets: dict[str, set[str]] = {}

    # ---- string ----
    async def get(self, key: str) -> str | None:
        return self.strings.get(key)

    async def set(self, key: str, value: Any, *, ex: Any = None, px: Any = None,
                  nx: bool = False, **kw: Any) -> bool:
        if nx and key in self.strings:
            return False
        self.strings[key] = str(value)
        return True

    async def delete(self, *keys: str) -> int:
        n = 0
        for store in (self.strings, self.hashes, self.zsets):
            for key in keys:
                if store.pop(key, None) is not None:
                    n += 1
        return n

    async def pttl(self, key: str) -> int:
        return 86_400_000 if key in self.strings else -2

    async def exists(self, key: str) -> int:
        return int(
            key in self.strings or key in self.hashes
            or key in self.zsets or key in self.sets
        )

    # ---- zset ----
    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        z = self.zsets.setdefault(key, {})
        n = 0
        for member, score in mapping.items():
            if member not in z:
                n += 1
            z[str(member)] = float(score)
        return n

    async def zrem(self, key: str, *members: str) -> int:
        z = self.zsets.get(key, {})
        n = 0
        for m in members:
            if m in z:
                del z[m]
                n += 1
        return n

    async def zrangebyscore(
        self, key: str, min_score: Any, max_score: Any,
        start: int | None = None, num: int | None = None,
    ) -> list[str]:
        z = self.zsets.get(key, {})
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score in ("+inf", "inf") else float(max_score)
        items = sorted(
            ((m, s) for m, s in z.items() if lo <= s <= hi),
            key=lambda kv: (kv[1], kv[0]),
        )
        if start is not None or num is not None:
            items = items[start or 0: (start or 0) + (num or len(items))]
        return [m for m, _ in items]

    async def zcard(self, key: str) -> int:
        return len(self.zsets.get(key, {}))

    # ---- set ----
    async def sadd(self, key: str, *members: str) -> int:
        s = self.sets.setdefault(key, set())
        n = 0
        for m in members:
            if m not in s:
                s.add(str(m))
                n += 1
        return n

    async def srem(self, key: str, *members: str) -> int:
        s = self.sets.get(key, set())
        n = 0
        for m in members:
            if m in s:
                s.discard(m)
                n += 1
        return n

    async def sismember(self, key: str, member: str) -> bool:
        return member in self.sets.get(key, set())

    async def incr(self, key: str) -> int:
        v = int(self.strings.get(key, "0")) + 1
        self.strings[key] = str(v)
        return v

    async def ping(self) -> bool:
        return True

    # ---- hash ----
    async def hset(self, key: str, mapping: dict | None = None, **kw: Any) -> int:
        h = self.hashes.setdefault(key, {})
        items = dict(mapping or {}, **kw)
        for k, v in items.items():
            h[str(k)] = str(v)
        return len(items)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    # ---- pipeline ----
    def pipeline(self) -> FakePipeline:
        return FakePipeline(self)

    # ---- Lua 等价实现（按脚本常量分发） ----
    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        from app import redis_queue as rq

        # gwqueue 通用队列原语（app/redis_queue.py；语义照抄 conftest.FakeRedis）
        if script == rq.LUA_CLAIM:
            due, lease = args[0], args[1]
            now, limit, lease_sec, prefix = (
                float(args[2]), int(args[3]), float(args[4]), str(args[5]))
            ids = await self.zrangebyscore(due, "-inf", now, start=0, num=limit)
            claimed: list[str] = []
            for item_id in ids:
                if await self.zrem(due, item_id) == 1:
                    deadline = now + lease_sec
                    await self.hset(
                        f"{prefix}{item_id}",
                        mapping={"state": "delivering", "lease_until": str(deadline)},
                    )
                    await self.zadd(lease, {item_id: deadline})
                    claimed.append(item_id)
            return claimed
        if script == rq.LUA_RECLAIM:
            lease, due = args[0], args[1]
            now, limit, prefix = float(args[2]), int(args[3]), str(args[4])
            ids = await self.zrangebyscore(lease, "-inf", now, start=0, num=limit)
            reclaimed: list[str] = []
            for item_id in ids:
                if await self.zrem(lease, item_id) == 1:
                    await self.hset(
                        f"{prefix}{item_id}",
                        mapping={"state": "pending", "lease_until": ""},
                    )
                    await self.zadd(due, {item_id: now})
                    reclaimed.append(item_id)
            return reclaimed
        if script == rq.LUA_REPLAY:
            dead, due = args[0], args[1]
            item_id, now, prefix = str(args[2]), float(args[3]), str(args[4])
            h = self.hashes.get(f"{prefix}{item_id}", {})
            if h.get("state") != "dead":
                return 0
            await self.hset(
                f"{prefix}{item_id}",
                mapping={"state": "pending", "attempts": "0",
                         "dead_reason": "", "last_error": ""},
            )
            await self.zrem(dead, item_id)
            await self.zadd(due, {item_id: now})
            return 1
        key = args[0]
        if script == middleware._LUA_USER_WINDOW:
            now_ms, window_ms, limit, member = (
                int(args[1]), int(args[2]), int(args[3]), str(args[4]))
            z = self.zsets.setdefault(key, {})
            for m in [m for m, s in z.items() if s <= now_ms - window_ms]:
                del z[m]
            if len(z) < limit:
                z[member] = float(now_ms)
                return 0
            oldest = min(z.values())
            return max(1, int(oldest + window_ms - now_ms))
        if script == middleware._LUA_BIZ_BUCKET:
            now_ms, capacity, refill = int(args[1]), int(args[2]), float(args[3])
            h = self.hashes.setdefault(key, {})
            tokens = float(h.get("tokens", capacity))
            ts = float(h.get("ts_ms", now_ms))
            tokens = min(float(capacity), tokens + (now_ms - ts) * refill)
            h["ts_ms"] = str(now_ms)
            if tokens >= 1:
                h["tokens"] = str(tokens - 1)
                return 1
            h["tokens"] = str(tokens)
            return 0
        if script == middleware._LUA_UPSTREAM_ACQUIRE:
            limit = int(args[1])
            c = int(self.strings.get(key, "0")) + 1
            self.strings[key] = str(c)
            if c > limit:
                self.strings[key] = str(c - 1)
                return 0
            return 1
        if script == middleware._LUA_UPSTREAM_RELEASE:
            c = int(self.strings.get(key, "0"))
            if c > 0:
                self.strings[key] = str(c - 1)
            return 1
        raise AssertionError(f"unexpected Lua script: {script[:60]}")

    async def publish(self, channel: str, message: str) -> int:
        return 0

    async def aclose(self) -> None:
        return None


class FakeSessionCM:
    """``get_session_factory()()`` 替身：async with 语义。"""

    def __init__(self, session: Any) -> None:
        self.session = session

    async def __aenter__(self) -> Any:
        return self.session

    async def __aexit__(self, *exc: Any) -> bool:
        return False


def make_session(first_row: dict | None = None) -> AsyncMock:
    """execute(...).mappings().first() → first_row 的 session 替身。"""
    result = MagicMock()
    result.mappings.return_value.first.return_value = first_row
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result)
    return session


def make_session_factory(session: Any) -> Any:
    return lambda: FakeSessionCM(session)


def make_orm_session() -> AsyncMock:
    """ORM 写入路径替身：add 为同步 MagicMock（AsyncMock 默认会把 add 变协程）。"""
    session = AsyncMock()
    session.add = MagicMock()
    return session


def make_biz(**overrides: Any) -> BizConfig:
    """BizConfig 工厂（字段与 SPEC §3.5.1 契约一致）。"""
    defaults: dict[str, Any] = dict(
        biz="kling",
        adapter="fakeadp",
        upstream_base_url="http://upstream.test",
        auth_type="bearer_key",
        auth_secret_ref="TEST_UPSTREAM_KEY",
        native_prefixes=["v1/videos"],
        enabled=True,
        billing_keys={"biz_type": "video", "metric": "call"},
        default_freeze_amount_usd=None,
        rate_limit={},
        newapi_channel_id=None,
        version=1,
    )
    defaults.update(overrides)
    return BizConfig(**defaults)


def make_token(**overrides: Any) -> TokenInfo:
    raw = overrides.pop("raw", "sk-" + "a" * 48)
    defaults: dict[str, Any] = dict(
        user_id=7, sk_hash=token_hash(raw), raw=raw, group="default",
    )
    defaults.update(overrides)
    return TokenInfo(**defaults)


def build_test_app(router: Any, *, token: TokenInfo | None = None,
                   session: Any = None) -> Any:
    """最小测试应用：单 router + 错误处理器 + 依赖覆盖（不经 create_app/DB/Redis）。"""
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse

    from app.auth import current_token
    from app.db import get_session
    from app.errors import GatewayError, gateway_exception_handler
    from app.middleware import IdempotentReplay

    app = FastAPI()
    app.add_exception_handler(GatewayError, gateway_exception_handler)
    app.add_exception_handler(HTTPException, gateway_exception_handler)

    async def _replay_handler(_req: Any, exc: Exception) -> JSONResponse:
        assert isinstance(exc, IdempotentReplay)
        return JSONResponse(status_code=exc.status_code, content=exc.body)

    app.add_exception_handler(IdempotentReplay, _replay_handler)
    if token is not None:
        app.dependency_overrides[current_token] = lambda: token
    if session is not None:
        async def _get_session() -> Any:
            yield session

        app.dependency_overrides[get_session] = _get_session
    app.include_router(router)
    return app
