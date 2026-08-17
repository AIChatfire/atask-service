"""上游调用引擎：声明式 RouteConfig + keypool 渠道覆盖驱动的通用实现。

新增模型不改本文件——所有差异收敛到三层配置：

1. **RouteConfig**（路由侧）：submit/probe 路径、响应提取路径、默认参数、
   信封校验、回调注入、状态映射；
2. **KeyLease 渠道覆盖**（上游侧，keypool 渠道元数据）：base_url /
   model_mapping / param_override / header_override / status_code_mapping /
   setting.proxy / openai_organization；
3. **请求体以用户提交为基底**，叠加顺序：``route.default_params`` <
   用户 body < ``channel.param_override``（最高优先，渠道运营侧兜底）。

提交体组装::

    body = {**default_params, **用户body(过 allowlist), **param_override}
    body["model"] = model_mapping.get(body["model"], body["model"])   # 渠道模型映射
    body[callback_param] = 网关回调地址                                # supports_callback 时

每个 (biz, base_url, proxy) 组合独立 httpx 连接池（舱壁），Redis 计数熔断。
"""

from __future__ import annotations

from typing import Any

import httpx

from app.logging import log
from app.redis import K_BREAKER, r
from app.schemas import KeyLease, RouteConfig

BREAKER_THRESHOLD = 10        # 30s 内失败 10 次熔断
BREAKER_WINDOW = 30

_clients: dict[tuple[str, str, str], httpx.AsyncClient] = {}


class UpstreamError(Exception):
    """上游调用失败。``status`` 为（经渠道 status_code_mapping 重写后的）HTTP
    状态码；信封业务错误为 200 + ``envelope=True``（上报 keypool 时不按 HTTP
    失败分类，仅记录消息）。"""

    def __init__(self, biz: str, status: int, body: str, *, envelope: bool = False):
        super().__init__(f"{biz} upstream {status}: {body[:200]}")
        self.status = status
        self.body = body
        self.envelope = envelope


class BreakerOpenError(Exception):
    pass


# ---------------------------------------------------------------------------
# 客户端（舱壁 + 渠道代理）
# ---------------------------------------------------------------------------


def client_for(route: RouteConfig, key: KeyLease | None = None) -> httpx.AsyncClient:
    """按 (biz, base_url, proxy) 缓存连接池：同一 biz 不同渠道 base_url/代理
    各自独立，渠道差异不需要新建配置。"""
    base_url = ((key.base_url if key else None) or route.upstream_base_url).rstrip("/")
    proxy = (key.proxy if key else None) or ""
    cache_key = (route.biz, base_url, proxy)
    client = _clients.get(cache_key)
    if client is None:
        client = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(route.timeout_sec, connect=10.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
            proxy=proxy or None,
        )
        _clients[cache_key] = client
    return client


async def close_all() -> None:
    for client in _clients.values():
        await client.aclose()
    _clients.clear()


# ---------------------------------------------------------------------------
# 鉴权头 + 渠道头覆盖
# ---------------------------------------------------------------------------


def auth_headers(route: RouteConfig, key: KeyLease) -> dict:
    headers: dict[str, str] = {}
    if route.auth_type == "bearer":
        headers["Authorization"] = f"Bearer {key.key}"
    elif route.auth_type == "x-api-key":
        headers["X-Api-Key"] = key.key
    if key.openai_organization:
        headers["OpenAI-Organization"] = key.openai_organization
    # 渠道自定义头（可覆盖鉴权头形态）；防御性过滤嵌套配置值
    # （header_override.upstream 是网关配置块，已在 provider 层剥离，这里再兜底）
    headers.update({k: v for k, v in key.header_override.items() if isinstance(v, str)})
    return headers


# ---------------------------------------------------------------------------
# 提交体组装（傻瓜式适配核心）
# ---------------------------------------------------------------------------


def build_submit_body(route: RouteConfig, key: KeyLease, body: dict,
                      callback_url: str | None = None) -> dict:
    """用户 body 为基底，按路由/渠道配置塑形（见模块 docstring 叠加顺序）。

    用户自带的 ``callback_url``/``webhook`` 一律摘除——用户回调由网关在终态
    经 notify 签名投递，绝不直接透给上游（上游回调地址只能由网关注入）。
    """
    payload = dict(body)
    payload.pop("callback_url", None)
    payload.pop("webhook", None)
    if route.body_allowlist is not None:
        allow = set(route.body_allowlist) | {route.callback_param}
        payload = {k: v for k, v in payload.items() if k in allow}
    merged = {**route.default_params, **payload, **key.param_override}
    model = merged.get("model")
    if model and model in key.model_mapping:
        merged["model"] = key.model_mapping[model]   # 渠道模型名映射（如 gpt-4o → gpt-4o-2024-08-06）
    if callback_url and route.supports_callback:
        merged[route.callback_param] = callback_url  # 网关注入回调，用户回调由 notify 透传
    return merged


# ---------------------------------------------------------------------------
# 熔断
# ---------------------------------------------------------------------------


async def breaker_guard(biz: str) -> None:
    failures = await r.get(K_BREAKER.format(biz=biz))
    if failures and int(failures) >= BREAKER_THRESHOLD:
        log.warning("upstream circuit open: biz={} failures={}", biz, failures)
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


# ---------------------------------------------------------------------------
# 提交 / 探测
# ---------------------------------------------------------------------------


def _mapped_status(key: KeyLease, status_code: int) -> int:
    """渠道 status_code_mapping（如 {"503": "500"}）重写上游状态码。"""
    mapped = key.status_code_mapping.get(str(status_code))
    if mapped is None:
        return status_code
    try:
        return int(mapped)
    except (TypeError, ValueError):
        return status_code


def _check_envelope(route: RouteConfig, data: Any) -> None:
    """可选信封校验：HTTP 2xx 但业务码不匹配 → UpstreamError(envelope=True)。"""
    check = route.ok_check
    if not check or not isinstance(data, dict):
        return
    actual = extract_path(data, str(check.get("path") or ""))
    expected = check.get("equals")
    if actual != expected:
        message = extract_path(data, str(check.get("message_path") or "")) if check.get("message_path") else None
        raise UpstreamError(
            route.biz, 200,
            f"envelope {check.get('path')}={actual!r} (expect {expected!r}): {message or data}",
            envelope=True,
        )


async def submit(route: RouteConfig, key: KeyLease, payload: dict) -> dict:
    """提交任务到上游，返回解析后的 JSON。非 2xx / 信封业务错抛 UpstreamError。

    ``payload`` 必须经 :func:`build_submit_body` 组装（渠道覆盖已应用）。
    """
    await breaker_guard(route.biz)
    client = client_for(route, key)
    try:
        resp = await client.post(route.submit_path, json=payload,
                                 headers=auth_headers(route, key))
    except httpx.HTTPError as exc:
        await breaker_report(route.biz, ok=False)
        raise UpstreamError(route.biz, 599, str(exc)) from exc
    status = _mapped_status(key, resp.status_code)
    await breaker_report(route.biz, ok=status < 500)
    if status >= 400:
        raise UpstreamError(route.biz, status, resp.text)
    data = resp.json()
    _check_envelope(route, data)
    return data


async def probe(route: RouteConfig, key: KeyLease, upstream_task_id: str) -> dict:
    """查询上游任务状态（路径含 ``{upstream_task_id}`` 占位，可为路径段或查询参数）。"""
    await breaker_guard(route.biz)
    client = client_for(route, key)
    path = route.probe_path.format(upstream_task_id=upstream_task_id)
    try:
        resp = await client.get(path, headers=auth_headers(route, key))
    except httpx.HTTPError as exc:
        await breaker_report(route.biz, ok=False)
        raise UpstreamError(route.biz, 599, str(exc)) from exc
    status = _mapped_status(key, resp.status_code)
    await breaker_report(route.biz, ok=status < 500)
    if status >= 400:
        raise UpstreamError(route.biz, status, resp.text)
    data = resp.json()
    _check_envelope(route, data)
    return data


# ---------------------------------------------------------------------------
# JSON 路径提取
# ---------------------------------------------------------------------------


def extract_path(obj: Any, dotted: str) -> Any:
    """按点分路径提取 JSON 字段：``task.content.url``、``data.0.task_id``
    （数字段按数组下标）。路径为空或任一段不命中 → None。"""
    if not dotted:
        return None
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit():
            idx = int(part)
            cur = cur[idx] if 0 <= idx < len(cur) else None
        else:
            return None
    return cur
