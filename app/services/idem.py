"""幂等键：客户端重试不产生重复任务。

原子占位（KI3 根治）：原「先查重放、落库后回填」在两请求真并发时都查
不到 → 双建任务。现受理链路先 SET NX 写占位标记（pending，短
TTL），把「先查后写」变原子——同 Idempotency-Key 的并发请求只有占位者
继续创建链路；其余短轮询等占位在同一键上回填为真实 task_id 后回放，
超时/占位过期按 409 冲突处理（不放行重建：重建会双建任务，409 让
客户端原键重试）。

状态流转全部在同一 Redis 键上完成：``pending`` → ``task_id``。
"""

from __future__ import annotations

import asyncio
import time

from app.config import settings
from app.redis import K_IDEM, LUA_CAS_DELETE, r

#: 占位标记：创建链路在飞（受理 → 落库 → 回填）期间的键值；
#: task_id 形态为 ``queue_{uuid4hex}``（``ids.new_task_id("queue")``），
#: 绝不与本标记碰撞
PENDING = "pending"

#: 短轮询间隔（秒）：等待占位回填的并发请求按此节奏读键
_WAIT_INTERVAL_SECONDS = 0.05


def _key(token_hash: str, idem_key: str) -> str:
    return K_IDEM.format(token_hash=token_hash, key=idem_key)


async def get_task_id(token_hash: str, idem_key: str) -> str | None:
    """已回填的 task_id；占位中（pending）/键不存在 → None。"""
    value = await r.get(_key(token_hash, idem_key))
    if not value or value == PENDING:
        return None
    return value


async def acquire(token_hash: str, idem_key: str) -> tuple[bool, str | None]:
    """原子占位（SET NX，先查后写变原子）。

    返回 ``(owned, replay_task_id)``：
    - ``(True, None)``：抢到占位，调用方负责走创建链路并在落库后
      ``set_task_id`` 回填（失败须 ``release`` 归还）；
    - ``(False, task_id)``：键已回填，直接回放该任务；
    - ``(False, None)``：他方占位中（同键真并发），调用方 ``wait_task_id``
      短轮询等回填。
    """
    if await r.set(_key(token_hash, idem_key), PENDING,
                   ex=settings.idem_pending_ttl_seconds, nx=True):
        return True, None
    return False, await get_task_id(token_hash, idem_key)


async def wait_task_id(token_hash: str, idem_key: str) -> str | None:
    """短轮询等他方占位回填为真实 task_id。

    超时（``IDEM_REPLAY_WAIT_SECONDS``）或占位消失（创建方失败已归还/
    占位 TTL 过期）→ None，调用方按 409 冲突处理，绝不放行重建。
    """
    deadline = time.monotonic() + settings.idem_replay_wait_seconds
    while time.monotonic() < deadline:
        value = await r.get(_key(token_hash, idem_key))
        if value is None:            # 占位已消失：创建方失败，不再等
            return None
        if value != PENDING:
            return value
        await asyncio.sleep(_WAIT_INTERVAL_SECONDS)
    return None


async def set_task_id(token_hash: str, idem_key: str, task_id: str) -> None:
    """占位回填：同一键上 ``pending`` → ``task_id``（覆盖写，TTL 换全量；
    占位保证单写者，无需 NX）。"""
    await r.set(_key(token_hash, idem_key), task_id, ex=settings.idem_ttl)


async def release(token_hash: str, idem_key: str) -> None:
    """创建链路失败时归还占位（CAS：仅当值仍是 pending 才删，
    已回填的 task_id 绝不误删）。"""
    await r.eval(LUA_CAS_DELETE, 1, _key(token_hash, idem_key), PENDING)
