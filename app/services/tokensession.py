"""用户令牌会话：终态 settle/cancel 需要**用户本人的 sk- 令牌**（billing 服务
只认令牌身份，跨用户 403），而任务终态由探测/回调异步到达，请求上下文早已
结束。冻结成功后按 task_id 暂存令牌到 Redis（TTL 48h），终态结算/解冻取用
后立即清除。

安全口径：令牌只放 Redis（AOF everysec），不落 tasks 表、不进日志；
Redis 丢失的最坏后果是 settle 无法执行——billing 侧 freeze 有 TTL，
过期由 billing sweeper 自动解冻，资金不会锁死（对账以 billing 台账为准）。
"""

from __future__ import annotations

from app.config import settings
from app.redis import r

_K = "gw:sk:{task_id}"


async def store(task_id: str, raw_token: str) -> None:
    await r.set(_K.format(task_id=task_id), raw_token, ex=settings.sk_session_ttl_seconds)


async def get(task_id: str) -> str | None:
    return await r.get(_K.format(task_id=task_id))


async def clear(task_id: str) -> None:
    await r.delete(_K.format(task_id=task_id))
