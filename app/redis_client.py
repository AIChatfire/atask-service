"""Redis 异步客户端单例（redis-py asyncio，SPEC §2/§3.6）。

Key 命名空间见 SPEC §3.6 清单；本模块只提供连接，不定义 key 语义。
worker 事件循环内惰性创建（Gunicorn post-fork 安全）。
"""

from __future__ import annotations

import redis.asyncio as aioredis

from app.config import settings

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    """进程内连接池单例。decode_responses=True：网关 key 全为文本协议。"""
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=100,
            socket_connect_timeout=3.0,
            socket_timeout=5.0,
        )
    return _redis


async def close_redis() -> None:
    """lifespan 退出时关闭连接池。"""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
