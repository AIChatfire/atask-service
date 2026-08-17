"""限流（滑动窗口）与并发占用。计数键：计费接口按 token_hash，免费 GET 按 IP。"""

import time

from fastapi import HTTPException, Request

from app.config import settings
from app.logging import log
from app.redis import K_CONC, K_RL, LUA_CONC_ACQUIRE, LUA_CONC_RELEASE, LUA_RATE_LIMIT, r


async def check_rate(subject: str) -> None:
    now_ms = int(time.time() * 1000)
    ok = await r.eval(
        LUA_RATE_LIMIT, 1, K_RL.format(subject=subject),
        now_ms, 60_000, settings.rate_limit_per_minute,
    )
    if not ok:
        raise HTTPException(429, "rate limit exceeded", headers={"Retry-After": "10"})


def client_ip(request: Request) -> str:
    fwd = request.headers.get("X-Forwarded-For")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def ip_rate_limit(request: Request) -> None:
    await check_rate(f"ip:{client_ip(request)}")


async def conc_try_acquire(token_hash: str) -> bool:
    """非抛出式并发占用（HELD 恢复重提交等异步路径用）：拿到 True，超限 False。"""
    return bool(await r.eval(
        LUA_CONC_ACQUIRE, 1, K_CONC.format(token_hash=token_hash),
        settings.max_concurrent_tasks,
    ))


async def conc_acquire(token_hash: str) -> None:
    if not await conc_try_acquire(token_hash):
        raise HTTPException(429, "too many concurrent tasks", headers={"Retry-After": "30"})


async def conc_release(token_hash: str | None) -> None:
    if not token_hash:
        return
    try:
        await r.eval(LUA_CONC_RELEASE, 1, K_CONC.format(token_hash=token_hash))
    except Exception:
        log.opt(exception=True).warning("conc release failed for {}", token_hash)
