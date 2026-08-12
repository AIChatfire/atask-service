"""W3 测试共用内存 fake：DB session / Redis（不依赖真实 MySQL/Redis，SPEC §7.1）。

各 W3 测试文件专用（w3_ 前缀避免与其他代理的测试基建冲突；W6 conftest.py
交付后可迁移到统一 fixtures）。
"""

from __future__ import annotations

from typing import Any


class FakeResult:
    """模拟 sqlalchemy Result 的 mappings() 链式读取；rowcount 模拟 DML 影响行数。"""

    def __init__(self, rows: list[dict[str, Any]], rowcount: int = 1) -> None:
        self._rows = rows
        self.rowcount = rowcount

    def mappings(self) -> FakeResult:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def all(self) -> list[dict[str, Any]]:
        return list(self._rows)


class FakeSession:
    """AsyncMock 风格的内存 session：execute 按预置结果队列出队，全量记录 SQL。"""

    def __init__(self, result_queues: list[list[dict[str, Any]]] | None = None) -> None:
        self._results = list(result_queues or [])
        self.executed: list[tuple[str, dict[str, Any] | None]] = []
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *args: Any) -> bool:
        return False

    async def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> FakeResult:
        sql = str(stmt)
        self.executed.append((sql, params))
        rows = self._results.pop(0) if self._results else []
        return FakeResult(rows)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    def statements_containing(self, fragment: str) -> list[tuple[str, dict[str, Any] | None]]:
        return [(sql, p) for sql, p in self.executed if fragment in sql]


class FakeSessionFactory:
    """async_sessionmaker 替身：每次调用弹出下一个预置 session。"""

    def __init__(self, sessions: list[FakeSession]) -> None:
        self._sessions = list(sessions)

    def __call__(self) -> FakeSession:
        assert self._sessions, "FakeSessionFactory: no session left"
        return self._sessions.pop(0)


class FakeRedis:
    """redis-py asyncio 内存替身（W3 用到的最小命令集；nx 语义与 redis 一致）。"""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    async def get(self, key: str) -> str | None:
        return self.strings.get(key)

    async def set(
        self, key: str, value: Any, nx: bool = False, ex: int | None = None
    ) -> bool | None:
        if nx and key in self.strings:
            return None
        self.strings[key] = str(value)
        return True

    async def exists(self, key: str) -> int:
        return int(key in self.strings)

    async def delete(self, key: str) -> int:
        existed = key in self.strings or key in self.hashes
        self.strings.pop(key, None)
        self.hashes.pop(key, None)
        return int(existed)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        mapping: dict[str, Any] | None = None,
    ) -> int:
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update({str(k): str(v) for k, v in mapping.items()})
        elif field is not None:
            h[str(field)] = str(value)
        return 1

    async def hget(self, key: str, field: str) -> str | None:
        return self.hashes.get(key, {}).get(field)

    async def expire(self, key: str, ttl: int) -> bool:
        return True
