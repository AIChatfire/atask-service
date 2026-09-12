"""HTTP 客户端工厂：按构造参数缓存进程级共享客户端（连接池 keep-alive 复用）。

中继链路出站（``app.services.relay``）走 ``shared_client``：每次请求新建
``AsyncClient`` 都要重付一轮 TCP + TLS 握手（无 keep-alive），数据面热路径上
握手开销会直接叠加到提交延迟。调用方不得 aclose() 返回值；进程退出由
``close_all()``（lifespan）统一释放。新建客户端默认挂 Logfire 埋点
（``app.observability`` 单点）。
"""

import httpx

from app import observability
from app.logging import log


def new_client(**kwargs) -> httpx.AsyncClient:
    """新建客户端并按需挂 Logfire 埋点（委托 ``app.observability`` 单点）。"""
    client = httpx.AsyncClient(**kwargs)
    observability.instrument_httpx(client)
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
