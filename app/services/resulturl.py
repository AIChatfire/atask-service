"""结果直链改写（转存/镜像）——渠道配置驱动的纯函数，零 I/O。

上游产物直链（``result_path`` 提取出的 URL）常常不能直接给终端用户：有效期短、
跨境慢、域名不可控、要过 CDN / 对象存储。渠道上配一行模板即可让网关对外统一
改写成自己的地址（**网关不搬运字节**，转存/回源由模板指向的服务负责）::

    "setting": {"gateway": {
        "result_path": "task.content.url",
        "result_url_template": "https://myhost.com/{upstream_result_url}"
    }}

可用占位符（按需组合，模板里出现几个填几个）：

===========================================  ==================================
``{upstream_result_url}``                    上游原始直链，原样
``{upstream_result_url_encoded}``            上游直链，百分号编码（当作查询参数值时用）
``{upstream_result_url_no_scheme}``          去掉 ``https://`` 前缀（拼成路径段时用）
``{upstream_result_host}`` / ``{upstream_result_path}``  直链的 host / path（含查询串）
``{task_id}``                                网关本地 task_id
===========================================  ==================================

生效范围（全部入口一致，客户端永远看不到上游直链）：

- ``flow.finalize_task``：终态落库时改写 ``data.result``，原始直链另存
  ``data.upstream_result``（对账/回源用）→ ``/v1/tasks`` GET、videos 视图、
  用户回调载荷自动跟随；
- 原生查询透传拦截：上游报文里的直链**字节级替换**为改写后的地址（报文其余
  部分 100% 同构），终态快照回放同理。

模板为空 = 不改写（默认行为，零影响）。
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote, urlsplit

from app.logging import log
from app.schemas import RouteConfig

#: 模板里的占位符前缀（用于"模板是否需要 URL"的零成本判断）
_URL_TOKEN = "{upstream_result_"


def _tokens(url: str, task_id: str) -> dict[str, str]:
    parts = urlsplit(url)
    no_scheme = url.split("://", 1)[-1] if "://" in url else url
    path = parts.path + (f"?{parts.query}" if parts.query else "")
    return {
        "{upstream_result_url}": url,
        "{upstream_result_url_encoded}": quote(url, safe=""),
        "{upstream_result_url_no_scheme}": no_scheme,
        "{upstream_result_host}": parts.netloc,
        "{upstream_result_path}": path.lstrip("/"),
        "{task_id}": task_id,
    }


def render(template: str, url: str, task_id: str = "") -> str:
    """按模板渲染改写后的直链。模板/URL 为空 → 原样返回 ``url``。

    未知占位符原样留在结果里（响亮暴露配置错误，而不是静默产出坏链接）。
    """
    if not template or not url:
        return url
    out = template
    for token, value in _tokens(url, task_id).items():
        out = out.replace(token, value)
    return out


def transform(route: RouteConfig, value: Any, task_id: str = "") -> Any:
    """改写 ``result_path`` 提取出的值：字符串直接渲染，列表逐项渲染
    （多产物上游），其余类型原样返回。渠道未配模板 → 原样返回。"""
    template = route.result_url_template
    if not template or value is None:
        return value
    if isinstance(value, str):
        return render(template, value, task_id)
    if isinstance(value, list):
        return [render(template, item, task_id) if isinstance(item, str) else item
                for item in value]
    log.debug("result_url_template skipped for non-url result: type={}", type(value).__name__)
    return value


def pairs(route: RouteConfig, value: Any, task_id: str = "") -> list[tuple[str, str]]:
    """``[(上游直链, 改写后直链), ...]``——供报文字节级替换用（只收真正
    发生变化且非空的对）。"""
    if not route.result_url_template or value is None:
        return []
    raw_items = [value] if isinstance(value, str) else (
        [item for item in value if isinstance(item, str)] if isinstance(value, list) else []
    )
    out: list[tuple[str, str]] = []
    for raw in raw_items:
        mirrored = render(route.result_url_template, raw, task_id)
        if raw and mirrored and mirrored != raw:
            out.append((raw, mirrored))
    return out


def _json_fragment(value: str) -> bytes:
    """字符串在 JSON 文本里的字节形态（不含包裹引号）——URL 里的 ``/`` 不转义，
    但非 ASCII 与特殊字符会，直接用 ``json.dumps`` 保证与报文字节一致。"""
    return json.dumps(value, ensure_ascii=False)[1:-1].encode()


def rewrite_bytes(body: bytes, replacements: list[tuple[str, str]]) -> bytes:
    """把报文字节里的上游直链替换为改写后的直链（**不重新序列化**，报文其余
    部分 100% 同构）。同时尝试原文与 JSON 转义两种字节形态。"""
    if not body or not replacements:
        return body
    for raw, mirrored in replacements:
        body = body.replace(raw.encode(), mirrored.encode())
        raw_frag, mirrored_frag = _json_fragment(raw), _json_fragment(mirrored)
        if raw_frag != raw.encode():
            body = body.replace(raw_frag, mirrored_frag)
    return body


__all__ = ["pairs", "render", "rewrite_bytes", "transform"]
