"""用户令牌会话：**task_id → 用户 sk- 令牌**的唯一查询处。

为什么需要它：中继链路的出站（提交 / 探测 / 取消）都发生在**请求上下文之外**——
提交由 worker 执行、探测由后台 sweep 或后续 GET 触发——而每次出站都必须携带
**用户本人的 sk- 令牌**（网关不做鉴权内省，令牌的有效性判定在上游）。所以受理
成功后按 task_id 把令牌暂存进 Redis（TTL 48h），出站前按 task_id 取用，终态收口
时立即清除。ADR-010 后网关零资金动作，本模块**不再服务于任何 billing 结算**。

安全口径：令牌只放 Redis（AOF everysec），不落 tasks 表、不进日志、
不出任何 HTTP 响应（``session_info`` 诊断视图只暴露存在性与 TTL，见
``app.routers.ops``）。Redis 丢失的最坏后果是「该任务再也无法探测/取消」——
任务由客户端轮询或后台 sweep 在会话有效期内收敛；会话过期后任务停在原状态、
不自愈（ADR-010 已登记为已知限制），绝不误判终态。
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
