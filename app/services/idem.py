"""幂等键：客户端重试不产生重复任务/重复扣费。
Redis 丢失的极端情况由 billing 的 request_id 唯一约束兜底。
"""

from app.config import settings
from app.redis import K_IDEM, r


async def get_task_id(token_hash: str, idem_key: str) -> str | None:
    return await r.get(K_IDEM.format(token_hash=token_hash, key=idem_key))


async def set_task_id(token_hash: str, idem_key: str, task_id: str) -> None:
    await r.set(K_IDEM.format(token_hash=token_hash, key=idem_key), task_id, ex=settings.idem_ttl, nx=True)
