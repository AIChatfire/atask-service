"""上游出站熔断（中性件）：Redis 失败计数 + 熔断护栏。

``/batch`` 中继链路的出站统一走 ``app.services.relay``（共享连接池），
本模块只保留它复用的熔断件：:func:`breaker_guard` / :func:`breaker_report`
与 :class:`BreakerOpenError`。旧链路基于 RouteConfig / KeyLease 的
submit / probe / cancel 与连接池已随 ADR-010 删除。
"""

from __future__ import annotations

from app.config import settings
from app.logging import log
from app.redis import K_BREAKER, r
from app.services import dynconf


class BreakerOpenError(Exception):
    pass


async def breaker_guard(biz: str) -> None:
    """熔断护栏：窗口内失败达到阈值即打开（中继链路以目标 host 做分组键）。"""
    failures = await r.get(K_BREAKER.format(biz=biz))
    threshold = await dynconf.get_int("upstream_breaker_threshold")
    if failures and int(failures) >= threshold:
        log.warning("upstream circuit open: biz={} failures={}", biz, failures)
        raise BreakerOpenError(f"upstream {biz} circuit open")


async def breaker_report(biz: str, ok: bool) -> None:
    """上报一次出站结果：成功清计数，失败在窗口内累加。"""
    key = K_BREAKER.format(biz=biz)
    if ok:
        await r.delete(key)
    else:
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, settings.upstream_breaker_window_seconds)
        await pipe.execute()
