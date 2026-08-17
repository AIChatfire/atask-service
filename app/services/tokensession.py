"""用户令牌会话：**task_id → 用户 sk- 令牌**的唯一查询处。

为什么需要它：任务状态存在两条免鉴权观察路径——对外 GET 查询（task_id 即
凭证）与 new-api 渠道侧轮询（不带用户 sk）——终态都在请求上下文之外异步
到达，而终态 settle/cancel 必须携带**用户本人的 sk- 令牌**（billing 服务
只认令牌身份，跨用户 403）。因此冻结成功后按 task_id 把令牌暂存进 Redis
（TTL 48h），终态结算/解冻按 task_id 查询取用后立即清除。

安全口径：令牌只放 Redis（AOF everysec），不落 tasks 表、不进日志、
不出任何 HTTP 响应（ops 诊断端点只暴露存在性与 TTL，不见
``app.routers.ops``）；Redis 丢失的最坏后果是 settle 无法执行——billing
侧 freeze 有 TTL，过期由 billing sweeper 自动解冻，资金不会锁死
（对账以 billing 台账为准）。
"""

from __future__ import annotations

from app.config import settings
from app.logging import log
from app.redis import r

_K = "gw:sk:{task_id}"


async def store(task_id: str, raw_token: str) -> None:
    await r.set(_K.format(task_id=task_id), raw_token, ex=settings.sk_session_ttl_seconds)
    log.debug("token session stored: task_id={} ttl={}s", task_id, settings.sk_session_ttl_seconds)


async def get(task_id: str) -> str | None:
    """终态结算/解冻取用（内部用，绝不外发）。"""
    token = await r.get(_K.format(task_id=task_id))
    if token is None:
        log.warning("token session missing: task_id={}", task_id)
    return token


async def session_info(task_id: str) -> dict:
    """诊断视图（ops 端点用）：只暴露存在性与剩余 TTL，绝不返回令牌本体。"""
    key = _K.format(task_id=task_id)
    exists = await r.get(key) is not None
    ttl = await r.ttl(key) if exists else -2
    return {"exists": exists, "ttl_seconds": ttl}


async def clear(task_id: str) -> None:
    await r.delete(_K.format(task_id=task_id))
    log.debug("token session cleared: task_id={}", task_id)
