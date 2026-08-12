from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import settings

# 连接数纪律：进程数 × (pool_size + max_overflow) ≤ MySQL max_connections × 0.8（与 NewAPI 共享）
engine = create_async_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_recycle=300,      # 必须小于 MySQL wait_timeout
    pool_pre_ping=True,
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
