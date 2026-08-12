"""上游调用：每 biz 独立 httpx 连接池（舱壁），Redis 计数熔断，JSON 路径提取。"""

import logging
from typing import Any

import httpx

from app.config import settings
from app.redis import K_BREAKER, r
from app.schemas import KeyLease, RouteConfig

log = logging.getLogger("gateway.upstream")

BREAKER_THRESHOLD = 10        # 30s 内失败 10 次熔断
BREAKER_WINDOW = 30

_clients: dict[str, httpx.AsyncClient] = {}


class UpstreamError(Exception):
    def __init__(self, biz: str, status: int, body: str):
        super().__init__(f"{biz} upstream {status}: {body[:200]}")
        self.status = status
        self.body = body


class BreakerOpenError(Exception):
    pass


def client_for(route: RouteConfig) -> httpx.AsyncClient:
    client = _clients.get(route.biz)
    if client is None:
        client = httpx.AsyncClient(
            base_url=route.upstream_base_url,
            timeout=httpx.Timeout(route.timeout_sec, connect=10.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        _clients[route.biz] = client
    return client


async def close_all() -> None:
    for client in _clients.values():
        await client.aclose()
    _clients.clear()


def auth_headers(route: RouteConfig, key: KeyLease) -> dict:
    if route.auth_type == "bearer":
        return {"Authorization": f"Bearer {key.key}"}
    if route.auth_type == "x-api-key":
        return {"X-Api-Key": key.key}
    return {}


async def breaker_guard(biz: str) -> None:
    failures = await r.get(K_BREAKER.format(biz=biz))
    if failures and int(failures) >= BREAKER_THRESHOLD:
        raise BreakerOpenError(f"upstream {biz} circuit open")


async def breaker_report(biz: str, ok: bool) -> None:
    key = K_BREAKER.format(biz=biz)
    if ok:
        await r.delete(key)
    else:
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, BREAKER_WINDOW)
        await pipe.execute()


async def submit(route: RouteConfig, key: KeyLease, payload: dict) -> dict:
    """提交任务到上游，返回解析后的 JSON。非 2xx 抛 UpstreamError。"""
    await breaker_guard(route.biz)
    client = client_for(route)
    try:
        resp = await client.post(route.submit_path, json=payload, headers=auth_headers(route, key))
    except httpx.HTTPError as exc:
        await breaker_report(route.biz, ok=False)
        raise UpstreamError(route.biz, 599, str(exc)) from exc
    await breaker_report(route.biz, ok=resp.status_code < 500)
    if resp.status_code >= 400:
        raise UpstreamError(route.biz, resp.status_code, resp.text)
    return resp.json()


async def probe(route: RouteConfig, key: KeyLease, upstream_task_id: str) -> dict:
    """查询上游任务状态"""
    await breaker_guard(route.biz)
    client = client_for(route)
    path = route.probe_path.format(upstream_task_id=upstream_task_id)
    try:
        resp = await client.get(path, headers=auth_headers(route, key))
    except httpx.HTTPError as exc:
        await breaker_report(route.biz, ok=False)
        raise UpstreamError(route.biz, 599, str(exc)) from exc
    await breaker_report(route.biz, ok=resp.status_code < 500)
    if resp.status_code >= 400:
        raise UpstreamError(route.biz, resp.status_code, resp.text)
    return resp.json()


def extract_path(obj: Any, dotted: str) -> Any:
    """按点分路径提取 JSON 字段，如 data.task_id"""
    if not dotted:
        return None
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur
