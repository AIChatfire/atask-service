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
    """非抛出式并发占用（HELD 恢复重提交等异步路径用）：拿到 True，超限 False。

    键带 TTL 兜底（``GW_CONC_TTL_SECONDS``，每次 acquire 刷新）：进程在
    「占槽后、落库前」崩溃时槽不再永久泄漏；精确校准由 sweep 按 tasks 表
    事实源回写（conc_recalibrate）。"""
    return bool(await r.eval(
        LUA_CONC_ACQUIRE, 1, K_CONC.format(token_hash=token_hash),
        settings.max_concurrent_tasks, settings.conc_ttl_seconds,
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


async def conc_recalibrate() -> int:
    """并发槽校准（sweep 每轮调用）：按 tasks 表事实源回写 Redis 计数。

    覆盖两类漂移，均不可自愈：
    - **泄漏**（Redis > 实际）：占槽后进程崩溃、release 调用丢失——用户并发
      余额被吃掉，累积到上限后永远 429；
    - **少计**（Redis < 实际，如键 TTL 过期重建）：放行超上限的并发。

    实现：SCAN 全部 ``gw:conc:*`` 键 ∪ 表里有活跃任务的 token_hash，逐个
    与实际活跃数（HELD 除外，口径同 acquire/release）对齐；一致则跳过，
    实际为 0 直接删键。返回修正的键数。
    """
    from app.services import taskstore   # 局部导入：deps 层不反向依赖 services

    actual = await taskstore.active_counts_by_token()
    prefix = K_CONC.format(token_hash="")
    subjects = set(actual)
    async for redis_key in r.scan_iter(f"{prefix}*", count=200):
        subjects.add(redis_key[len(prefix):])

    fixed = 0
    for token_hash in list(subjects)[: settings.conc_recalibrate_batch]:
        key = K_CONC.format(token_hash=token_hash)
        want = actual.get(token_hash, 0)
        raw = await r.get(key)
        have = int(raw) if raw and str(raw).lstrip("-").isdigit() else 0
        if have == want:
            continue
        if want <= 0:
            await r.delete(key)
        else:
            await r.set(key, want, ex=settings.conc_ttl_seconds)
        log.warning("conc slot recalibrated: token={} {} -> {}", token_hash, have, want)
        fixed += 1
    return fixed
