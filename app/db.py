"""MySQL 引擎/会话工厂（SPEC §3.5.4）。

**零自有表（决策 A）**：网关无任何 MySQL 自有表/建表职责，本模块只连与
new-api 共享的实例（读写 ``tasks`` 自有行）。``create_gateway_tables()``
已删除——tasks 表由 new-api AutoMigrate 维护，网关绝不 create/alter。

引擎惰性单例（post-fork 安全）：模块导入（gunicorn preload）时不建连接，
首个使用者（worker 进程事件循环内）触发创建。

事务纪律：调用方显式 ``commit``；``get_session`` 依赖在异常时自动
rollback。W2 终态迁移按「先 DB commit 后 Redis 入队」次序（SPEC §4.7）。

连接数纪律：进程数 × (pool_size + max_overflow) ≤ MySQL max_connections × 0.8
（与 new-api 共享实例，SPEC §3.7 默认值即按此预算）。
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """惰性引擎单例（post-fork 安全）：首次调用才创建连接池。"""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_recycle=settings.db_pool_recycle,
            pool_pre_ping=settings.db_pool_pre_ping,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """会话工厂单例（worker 类构造注入用）。"""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI 依赖：请求级会话；异常自动 rollback（提交由调用方显式执行）。"""
    async with get_session_factory()() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def close_db() -> None:
    """lifespan/worker 退出时释放连接池（幂等）。"""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
    _session_factory = None
