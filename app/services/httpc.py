"""HTTP 客户端工厂：控制面（billing/keypool 两微服务）客户端按需挂 Logfire 埋点。
数据面（上游提交/探测）客户端不走这里 —— 探测调用高频，全量 trace 无意义，
状态变化由 statelog 负责记录。

控制面客户端一律走 ``shared_client``（进程级连接池复用）：每次调用新建
AsyncClient 会让每个请求都付出完整 TCP + TLS 握手（无 keep-alive），
preflight 一次提交就有 inspect/lease/freeze 三个控制面调用，握手开销会
叠加成提交链路的主要延迟来源之一。
"""

import httpx

from app.config import settings
from app.logging import log


def new_client(**kwargs) -> httpx.AsyncClient:
    client = httpx.AsyncClient(**kwargs)
    if settings.logfire_enabled:
        try:
            import logfire

            logfire.instrument_httpx(client)
        except Exception:
            pass
    return client


# 进程级共享客户端池：key = 构造参数组合（同参数返回同一实例，连接池 keep-alive 复用）
_shared: dict[tuple, httpx.AsyncClient] = {}


def shared_client(**kwargs) -> httpx.AsyncClient:
    """按构造参数缓存的共享 AsyncClient（连接池复用，免去逐请求握手）。

    调用方不得 aclose() 返回值；进程退出由 ``close_all()``（lifespan）统一释放。
    """
    key = tuple(sorted((name, repr(value)) for name, value in kwargs.items()))
    client = _shared.get(key)
    if client is None or client.is_closed:
        client = new_client(**kwargs)
        _shared[key] = client
    return client


async def close_all() -> None:
    """释放全部共享客户端（web lifespan / worker 退出时调用，幂等）。

    先清表再逐个关闭：关闭期间新进的调用拿到的是新建实例（不会复用
    正在关闭的 client）；单个 aclose 失败只告警，不阻塞其余客户端释放
    （lifespan 里 close_all 之后还有 close_db，不能因一个坏 client 中断）。
    """
    clients = list(_shared.values())
    _shared.clear()
    for client in clients:
        try:
            await client.aclose()
        except Exception:
            log.opt(exception=True).warning("shared http client close failed")
