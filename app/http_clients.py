"""出站 httpx 客户端单例工厂（SPEC §2 / 架构 §8.1）。

纪律（架构 §8.1/§13.1）：所有出站调用复用本模块的应用级单例，
**绝不每请求新建 AsyncClient**（新建不关闭会泄漏连接池/文件描述符，
且每请求重建 TLS 会话）。按目标分组四个客户端，超时四元组各自收紧：
- upstream：connect 紧（5s 快速失败）、read 匹配上游提交/查询快接口（60s 余量）
- billing：资金链路更紧（read 10s、pool 2s 排队饿死保护）
- pricing：逻辑服务超时 3s（§5.2）
- delivery：用户回调投递 10s（§7.3）
"""

from __future__ import annotations

import httpx

from app.config import settings

_clients: dict[str, httpx.AsyncClient] = {}


def upstream_client() -> httpx.AsyncClient:
    """上游（kling/seedance/…）调用客户端。"""
    return _get_or_create(
        "upstream",
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20,
                            keepalive_expiry=30.0),
        timeout=httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0),
        http2=True,
    )


def billing_client() -> httpx.AsyncClient:
    """计费服务客户端（freeze/settle/cancel/charge）。"""
    return _get_or_create(
        "billing",
        base_url=settings.billing_service_url,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
        timeout=httpx.Timeout(connect=3.0, read=10.0, write=5.0, pool=2.0),
    )


def pricing_client() -> httpx.AsyncClient:
    """计费逻辑服务客户端（GET /pricing/logic）。"""
    t = settings.pricing_http_timeout
    return _get_or_create(
        "pricing",
        base_url=settings.pricing_service_url,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
        timeout=httpx.Timeout(connect=2.0, read=t, write=t, pool=2.0),
    )


def keys_client() -> httpx.AsyncClient:
    """keys 轮询微服务客户端（仅 ``keys_service_url`` 配置后调用，§3.12）。"""
    assert settings.keys_service_url, "keys_client() requires KEYS_SERVICE_URL"
    return _get_or_create(
        "keys",
        base_url=settings.keys_service_url,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
        timeout=httpx.Timeout(connect=2.0, read=5.0, write=3.0, pool=2.0),
    )


def delivery_client() -> httpx.AsyncClient:
    """用户 callback 投递客户端（§7.3，超时 10s，跟随重定向由接收方决定）。"""
    return _get_or_create(
        "delivery",
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=10),
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=3.0),
        follow_redirects=False,
    )


def _get_or_create(name: str, **kwargs: object) -> httpx.AsyncClient:
    client = _clients.get(name)
    if client is None or client.is_closed:
        client = httpx.AsyncClient(**kwargs)  # type: ignore[arg-type]
        _clients[name] = client
    return client


async def close_http_clients() -> None:
    """lifespan 退出时统一关闭。"""
    for client in _clients.values():
        if not client.is_closed:
            await client.aclose()
    _clients.clear()
