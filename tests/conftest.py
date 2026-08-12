"""全项目统一测试基建（SPEC §7.2；W6 交付）。

统一 fixtures：

- ``mock_session``      —— AsyncMock 版 AsyncSession（fake_db；execute 返回值可预置）
- ``mock_session_factory`` —— 返回 ``mock_session`` 的会话工厂（worker 类构造注入用）
- ``fake_redis``        —— 内存版异步 Redis fake（decode_responses=True 语义）
- ``respx_router``      —— respx 路由（fake_httpx；拦截 httpx 单例出站请求）
- ``test_settings``     —— 受控 Settings 测试实例（清 lru_cache 重载，进程外依赖指假地址）
- ``biz_cfg_factory``   —— ``app.registry.BizConfig`` 工厂
- ``token_factory``     —— ``app.auth.TokenInfo`` 工厂（W1 交付后可用；惰性导入）
- ``task_row_factory``  —— tasks 行 dict 工厂（字段与 SPEC §5.1 映射一致）

原则：单测不依赖真实 MySQL/Redis/上游；本文件只 import 骨架模块，
对其他 W 模块一律在 fixture 内惰性导入（并行开发期可安全收集）。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from typing import Any
from unittest.mock import AsyncMock

import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession

# ---------------------------------------------------------------------------
# fake_db：AsyncMock session（SPEC §7.1：execute 返回预置 mappings/rowcount）
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_session() -> AsyncMock:
    """AsyncMock 版 AsyncSession。

    预置查询结果示例::

        result = MagicMock()
        result.mappings.return_value.first.return_value = {"task_id": "task_x"}
        mock_session.execute.return_value = result
        # 或按 rowcount 断言 CAS 胜负：result.rowcount = 0

    ``commit``/``rollback``/``flush``/``close`` 均为 AsyncMock，可断言调用次数。
    """
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest.fixture
def mock_session_factory(mock_session: AsyncMock) -> Callable[[], AsyncMock]:
    """返回 ``mock_session`` 的工厂（PolllWorker/OutboxWorker 等构造注入用）。

    支持 ``async with factory() as session:`` 形态（__aenter__ 返回自身）。
    """
    mock_session.__aenter__.return_value = mock_session
    mock_session.__aexit__.return_value = None
    return lambda: mock_session


# ---------------------------------------------------------------------------
# fake_redis：内存版异步 Redis（redis.asyncio 语义，decode_responses=True）
# ---------------------------------------------------------------------------


class FakeRedis:
    """内存版异步 Redis fake（单测用；SPEC §7.1 Redis mock 条款）。

    覆盖网关用到的主要命令子集：STRING（get/set 带 nx/xx/ex/px/get、setex、
    setnx、getdel、incr/incrby、delete/unlink、exists、expire/ttl）、
    HASH（hset/hget/hgetall/hdel）、LIST（rpush/lpush/lrange/lrem、
    blpop/brpoplpush 立即返回形态）、publish、ping、close/aclose。

    值一律按 str 存储/返回（decode_responses=True 语义；int 入参自动 str()）。
    未实现的命令访问时抛 ``NotImplementedError``（不做静默通过的假阳性）。
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._expires: dict[str, float] = {}  # key -> 过期 unix 秒
        self.published: list[tuple[str, str]] = []  # (channel, message) 发布记录

    # ---- 内部 ----

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

    async def close(self) -> None:
        return None

    async def aclose(self) -> None:
        return None

    async def delete(self, *keys: str) -> int:
        n = 0
        for key in keys:
            if self._alive(key) and key in self._data:
                n += 1
            self._data.pop(key, None)
            self._expires.pop(key, None)
        return n

    unlink = delete

    async def exists(self, *keys: str) -> int:
        return sum(1 for k in keys if self._alive(k) and k in self._data)

    async def expire(self, key: str, seconds: int) -> bool:
        if not (self._alive(key) and key in self._data):
            return False
        self._expires[key] = time.time() + seconds
        return True

    async def ttl(self, key: str) -> int:
        if not (self._alive(key) and key in self._data):
            return -2
        if key not in self._expires:
            return -1
        return max(0, int(self._expires[key] - time.time()))

    # ---- STRING ----

    async def get(self, key: str) -> str | None:
        if not self._alive(key):
            return None
        value = self._data.get(key)
        return value if isinstance(value, str) else None

    async def set(
        self,
        key: str,
        value: Any,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> Any:
        exists = self._alive(key) and key in self._data
        old = self._data.get(key) if exists else None
        if nx and exists:
            return old if get else None
        if xx and not exists:
            return old if get else None
        self._data[key] = self._s(value)
        if ex is not None:
            self._expires[key] = time.time() + ex
        elif px is not None:
            self._expires[key] = time.time() + px / 1000
        else:
            self._expires.pop(key, None)
        return old if get else True

    async def setex(self, key: str, seconds: int, value: Any) -> bool:
        return bool(await self.set(key, value, ex=seconds))

    async def setnx(self, key: str, value: Any) -> bool:
        return bool(await self.set(key, value, nx=True))

    async def getdel(self, key: str) -> str | None:
        value = await self.get(key)
        if value is not None:
            await self.delete(key)
        return value

    async def incr(self, key: str) -> int:
        return await self.incrby(key, 1)

    async def incrby(self, key: str, amount: int = 1) -> int:
        cur = await self.get(key)
        new = (int(cur) if cur is not None else 0) + amount
        exp = self._expires.get(key)
        self._data[key] = str(new)
        if exp is not None:
            self._expires[key] = exp
        return new

    # ---- HASH ----

    async def hset(
        self, key: str, field: str | None = None, value: Any = None, mapping: dict | None = None
    ) -> int:
        if not self._alive(key) or not isinstance(self._data.get(key), dict):
            self._data[key] = {}
        h: dict[str, str] = self._data[key]
        items: dict[str, Any] = dict(mapping or {})
        if field is not None:
            items[field] = value
        n = 0
        for f, v in items.items():
            if f not in h:
                n += 1
            h[f] = self._s(v)
        return n

    async def hget(self, key: str, field: str) -> str | None:
        if not self._alive(key):
            return None
        h = self._data.get(key)
        return h.get(field) if isinstance(h, dict) else None

    async def hgetall(self, key: str) -> dict[str, str]:
        if not self._alive(key):
            return {}
        h = self._data.get(key)
        return dict(h) if isinstance(h, dict) else {}

    async def hdel(self, key: str, *fields: str) -> int:
        h = self._data.get(key)
        if not (self._alive(key) and isinstance(h, dict)):
            return 0
        n = 0
        for f in fields:
            if f in h:
                del h[f]
                n += 1
        return n

    # ---- LIST（回调队列等） ----

    async def rpush(self, key: str, *values: Any) -> int:
        if not self._alive(key) or not isinstance(self._data.get(key), list):
            self._data[key] = []
        lst: list[str] = self._data[key]
        lst.extend(self._s(v) for v in values)
        return len(lst)

    async def lpush(self, key: str, *values: Any) -> int:
        if not self._alive(key) or not isinstance(self._data.get(key), list):
            self._data[key] = []
        lst: list[str] = self._data[key]
        for v in values:
            lst.insert(0, self._s(v))
        return len(lst)

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        lst = self._data.get(key)
        if not (self._alive(key) and isinstance(lst, list)):
            return []
        end = len(lst) - 1 if end == -1 else end
        return lst[start : end + 1]

    async def lrem(self, key: str, count: int, value: Any) -> int:
        lst = self._data.get(key)
        if not (self._alive(key) and isinstance(lst, list)):
            return 0
        target = self._s(value)
        removed = 0
        keep: list[str] = []
        for item in lst:
            if item == target and (count == 0 or removed < count):
                removed += 1
            else:
                keep.append(item)
        self._data[key] = keep
        return removed

    async def blpop(
        self, keys: str | list[str], timeout: int = 0  # noqa: ASYNC109 - redis-py 同名签名（fake 不阻塞）
    ) -> tuple[str, str] | None:
        for key in [keys] if isinstance(keys, str) else keys:
            lst = self._data.get(key)
            if self._alive(key) and isinstance(lst, list) and lst:
                return key, lst.pop(0)
        return None  # 立即返回形态：单测用，不模拟阻塞

    async def brpoplpush(
        self, source: str, destination: str, timeout: int = 0  # noqa: ASYNC109 - redis-py 同名签名（fake 不阻塞）
    ) -> str | None:
        lst = self._data.get(source)
        if not (self._alive(source) and isinstance(lst, list) and lst):
            return None
        value = lst.pop()
        await self.lpush(destination, value)
        return value

    # ---- ZSET（延迟队列 dlv/obx 等） ----

    def _zset(self, key: str) -> dict[str, float]:
        if not self._alive(key) or not isinstance(self._data.get(key), dict):
            self._data[key] = {}
        return self._data[key]

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        z = self._zset(key)
        n = 0
        for member, score in mapping.items():
            if member not in z:
                n += 1
            z[str(member)] = float(score)
        return n

    async def zrem(self, key: str, *members: str) -> int:
        z = self._data.get(key)
        if not (self._alive(key) and isinstance(z, dict)):
            return 0
        n = 0
        for m in members:
            if m in z:
                del z[m]
                n += 1
        return n

    async def zrangebyscore(
        self, key: str, min_score: Any, max_score: Any,
        start: int | None = None, num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        z = self._data.get(key)
        if not (self._alive(key) and isinstance(z, dict)):
            return []
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score in ("+inf", "inf") else float(max_score)
        items = sorted(
            ((m, s) for m, s in z.items() if lo <= s <= hi),
            key=lambda kv: (kv[1], kv[0]),
        )
        if start is not None or num is not None:
            items = items[start or 0 : (start or 0) + (num or len(items))]
        if withscores:
            return items
        return [m for m, _ in items]

    async def zrange(self, key: str, start: int, end: int, withscores: bool = False) -> list[Any]:
        z = self._data.get(key)
        if not (self._alive(key) and isinstance(z, dict)):
            return []
        items = sorted(z.items(), key=lambda kv: (kv[1], kv[0]))
        end = len(items) - 1 if end == -1 else end
        items = items[start : end + 1]
        return items if withscores else [m for m, _ in items]

    async def zcard(self, key: str) -> int:
        z = self._data.get(key)
        if not (self._alive(key) and isinstance(z, dict)):
            return 0
        return len(z)

    # ---- SET（debt:orders 等） ----

    def _set(self, key: str) -> set[str]:
        if not self._alive(key) or not isinstance(self._data.get(key), set):
            self._data[key] = set()
        return self._data[key]

    async def sadd(self, key: str, *members: str) -> int:
        s = self._set(key)
        n = 0
        for m in members:
            if m not in s:
                s.add(str(m))
                n += 1
        return n

    async def srem(self, key: str, *members: str) -> int:
        s = self._data.get(key)
        if not (self._alive(key) and isinstance(s, set)):
            return 0
        n = 0
        for m in members:
            if m in s:
                s.discard(m)
                n += 1
        return n

    async def smembers(self, key: str) -> set[str]:
        s = self._data.get(key)
        if not (self._alive(key) and isinstance(s, set)):
            return set()
        return set(s)

    async def sismember(self, key: str, member: str) -> bool:
        s = self._data.get(key)
        return bool(self._alive(key) and isinstance(s, set) and member in s)

    # ---- PUB/SUB ----

    async def publish(self, channel: str, message: Any) -> int:
        self.published.append((channel, self._s(message)))
        return 1

    # ---- Lua（app.redis_queue 脚本等价 Python 实现，按脚本常量分发） ----

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        from app import redis_queue as rq

        keys = list(args[:numkeys])
        argv = [str(a) for a in args[numkeys:]]
        if script == rq.LUA_CLAIM:
            due, lease = keys
            now, limit, lease_sec, prefix = (
                float(argv[0]), int(argv[1]), float(argv[2]), argv[3])
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
            lease, due = keys
            now, limit, prefix = float(argv[0]), int(argv[1]), argv[2]
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
            dead, due = keys
            item_id, now, prefix = argv[0], float(argv[1]), argv[2]
            h = await self.hgetall(f"{prefix}{item_id}")
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
        raise AssertionError(f"unexpected Lua script: {script[:60]}")

    # ---- 测试辅助 ----

    def dump(self) -> dict[str, Any]:
        """当前存活键值快照（断言用）。"""
        return {k: v for k, v in self._data.items() if self._alive(k)}


@pytest.fixture
def fake_redis() -> FakeRedis:
    """内存版异步 Redis（SPEC §7.2 统一 fake_redis fixture）。"""
    return FakeRedis()


# ---------------------------------------------------------------------------
# fake_httpx：respx 路由（拦截 app.http_clients 单例的出站请求）
# ---------------------------------------------------------------------------


@pytest.fixture
def respx_router() -> Iterator[respx.MockRouter]:
    """respx 拦截器（``assert_all_called=False``：允许声明多用少）。

    用法::

        def test_x(respx_router):
            respx_router.post("https://upstream/api").mock(
                return_value=httpx.Response(200, json={...})
            )
    """
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        yield router


# ---------------------------------------------------------------------------
# settings 测试实例（受控环境变量重载单例）
# ---------------------------------------------------------------------------


@pytest.fixture
def test_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """受控 ``Settings`` 实例：清 ``get_settings`` lru_cache 后按测试环境重载。

    所有外置依赖指向不可达的假地址（保证误触真实 I/O 时立刻失败而非连上本地）。
    注意：已 ``from app.config import settings`` 的模块持有旧引用不受影响——
    需要注入配置的代码请优先走依赖注入/工厂参数。
    """
    from app.config import get_settings

    monkeypatch.setenv("DATABASE_URL", "mysql+asyncmy://test:test@127.0.0.1:3399/test_gw")
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:6399/15")
    monkeypatch.setenv("BILLING_SERVICE_URL", "http://127.0.0.1:18099")
    monkeypatch.setenv("PRICING_SERVICE_URL", "http://127.0.0.1:18098")
    monkeypatch.setenv("GATEWAY_PUBLIC_BASE_URL", "https://gw.test")
    monkeypatch.setenv("CALLBACK_SIGNING_SECRET_CURRENT", "test-signing-secret")
    monkeypatch.setenv("APP_ENV", "test")
    get_settings.cache_clear()
    try:
        yield get_settings()
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# 工厂 fixtures（SPEC §7.2）
# ---------------------------------------------------------------------------


@pytest.fixture
def biz_cfg_factory() -> Callable[..., Any]:
    """``app.registry.BizConfig`` 工厂（骨架模块，字段默认值对齐 SPEC §3.5.1）。"""
    from app.registry import BizConfig

    def _make(**overrides: Any) -> Any:
        kwargs: dict[str, Any] = {
            "biz": "kling",
            "adapter": "kling",
            "upstream_base_url": "https://api-beijing.klingai.com",
            "auth_type": "aksk_jwt",
            "auth_secret_ref": "UPSTREAM_SECRET_KLING",
            "native_prefixes": ["v1/videos"],
            "enabled": True,
            "billing_keys": {
                "biz_type": "video_gen",
                "metric": "call",
                "billing_mode": "prepaid",
            },
            "default_freeze_amount_usd": "1.000000",
            "rate_limit": {"user_rpm": 60, "biz_rpm": 600, "upstream_concurrency": 32},
            "newapi_channel_id": None,
            "version": 1,
            "display_name": "Kling 视频",
        }
        kwargs.update(overrides)
        return BizConfig(**kwargs)

    return _make


@pytest.fixture
def token_factory() -> Callable[..., Any]:
    """``app.auth.TokenInfo`` 工厂（W1 交付后可用；并行期惰性导入）。"""
    try:
        from app.auth import TokenInfo  # type: ignore[attr-defined]  # W1 交付后可用
    except (ImportError, AttributeError) as exc:  # pragma: no cover - 并行开发期
        pytest.skip(f"app.auth.TokenInfo 尚未交付（W1）：{exc}")

    def _make(**overrides: Any) -> Any:
        from app.auth import token_hash

        raw = overrides.pop("raw", "sk-" + "a" * 48)
        kwargs: dict[str, Any] = {
            "user_id": 1001,
            "sk_hash": token_hash(raw),
            "raw": raw,
            "group": "default",
        }
        kwargs.update(overrides)
        return TokenInfo(**kwargs)

    return _make


@pytest.fixture
def task_row_factory() -> Callable[..., dict[str, Any]]:
    """tasks 行 dict 工厂（19 列，字段与 SPEC §5.1 映射一致）。

    默认值即「网关在途行」：platform=gw_kling、quota=0、SUBMITTED/10%、
    bigint 时间列非 NULL、fail_reason=''、private_data 含 gateway 子对象。
    """

    def _make(**overrides: Any) -> dict[str, Any]:
        now = int(time.time())
        row: dict[str, Any] = {
            "id": 1,
            "created_at": now,
            "updated_at": now,
            "task_id": "task_" + "0" * 32,
            "platform": "gw_kling",
            "user_id": 1001,
            "group": "default",
            "channel_id": 0,
            "quota": 0,                       # 三件套：恒 0（SPEC §4.1）
            "action": "generate",
            "status": "SUBMITTED",
            "fail_reason": "",
            "submit_time": now,
            "start_time": 0,                  # 绝不 NULL（SPEC §4.1）
            "finish_time": 0,
            "progress": "10%",
            "properties": {
                "input": "a cat",
                "upstream_model_name": "kling-v3",
                "origin_model_name": "kling-v3",
            },
            "private_data": {
                "upstream_task_id": "up_123",
                "gateway": {
                    "biz": "kling",
                    "form": "videos",
                    "sk_hash": "c" * 64,
                    "idempotency_key": None,
                    "callback_url": None,
                    "billing_state": "frozen",
                    "deadline_unix": now + 86400,
                    "freeze_shard_seq": 0,
                    "next_poll_at": 0,
                    "usage_actual": None,
                    "request_snapshot": {},
                },
            },
            "data": None,
        }
        row.update(overrides)
        return row

    return _make
