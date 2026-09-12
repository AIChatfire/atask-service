"""上游出站熔断（中性件）：Redis 失败计数 + 熔断护栏。

``/queue`` 中继链路的出站统一走 ``app.services.relay``（共享连接池），
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


async def breaker_guard(host: str) -> None:
    """熔断护栏：窗口内失败达到阈值即打开（分组键 = 目标 host）。

    ``host`` 而非旧名 ``biz``：换向后渠道分组概念随 keypool 退场，出站分组键
    一直是「打到哪个上游地址」，名字与事实对齐（Redis 键形态未变）。
    """
    failures = await r.get(K_BREAKER.format(host=host))
    threshold = await dynconf.get_int("upstream_breaker_threshold")
    if failures and int(failures) >= threshold:
        log.warning("upstream circuit open: host={} failures={}", host, failures)
        raise BreakerOpenError(f"upstream {host} circuit open")


async def breaker_report(host: str, ok: bool) -> None:
    """上报一次出站结果：成功清计数，失败在窗口内累加。"""
    key = K_BREAKER.format(host=host)
    if ok:
        await r.delete(key)
    else:
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, settings.upstream_breaker_window_seconds)
        await pipe.execute()
