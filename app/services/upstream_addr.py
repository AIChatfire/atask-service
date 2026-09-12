"""上游寻址：``X-Upstream-Base-Url`` 头优先，回退 ``settings.upstream_base_url``。

本模块是 ADR-010「鉴权与计费全部下沉上游」后的**唯一**上游地址入口：网关不再持有
keypool 渠道元数据里的 ``base_url``（见本仓库 ADR-010），改为按请求寻址。

## 两条护栏（给未来人，别省略）

1. **该头的可信性完全依赖 nginx 无条件覆盖客户端同名头**（stask 的做法是
   ``proxy_set_header X-Upstream-Base-Url "..."``）。若 nginx 未覆盖，客户端可
   伪造此头把请求指向任意 host —— 所以 ``UPSTREAM_ALLOWLIST`` 是第二道防线，
   **两道都必须配**，缺一道都等于把用户凭证暴露给攻击者。
2. **这是防 SSRF 的关键点**：用户自己的 sk 会随请求发往该地址，放行野地址等于
   把用户凭证送到攻击者服务器。

## 安全三防线（照 ADR-010 §4 与 stask ``docs/stask-service-design.md`` §7）

1. 仅接受 ``http`` / ``https``；
2. **拒绝 URL userinfo**（``http://user@host`` 这类 ``user:password@`` 形态）；
3. host 必须命中 ``settings.upstream_allowlist``（逗号分隔），否则 ``400``。

## 取舍记录

- **端口不参与命中判定**：``host`` 与 ``host:port`` 视为同一 host（比较时统一取
  ``urlsplit().hostname``）。理由是同机新-api 可能换端口（如 :3000 -> :8080）、
  反代也可能改端口，若把端口写进白名单，运维每次改端口都要同步白名单、漏改即
  502；而白名单的真正目的是「只放行可信主机」，同一主机上的端口不改变信任边界。
- **白名单为空 = 全部拒绝**（fail-closed）。这是安全开关，失败方向恒为拒绝——
  与 ``app/deps/admin.py`` 未配 ``ADMIN_TOKEN`` 即 404 同一纪律。
- 用 ``urllib.parse.urlsplit`` 解析，**不用会吞掉非法输入的正则**。
"""

from __future__ import annotations

from urllib.parse import urlsplit

from fastapi import HTTPException, Request

from app.config import settings

#: 上游基址头（nginx 无条件覆盖，客户端不可信——见模块 docstring 护栏 1）。
UPSTREAM_BASE_HEADER = "X-Upstream-Base-Url"


def resolve_upstream_base(request: Request) -> str:
    """上游基址：``X-Upstream-Base-Url`` 头优先，回退 ``settings.upstream_base_url``。

    两者都没有 → 返回空串（调用方按 400 处理，**这里不抛**——缺地址是调用方
    的请求错误，不是本函数的解析失败）。
    """
    header = (request.headers.get(UPSTREAM_BASE_HEADER) or "").strip()
    if header:
        return header
    return (settings.upstream_base_url or "").strip()


def _normalize_host(entry: str) -> str:
    """白名单条目的 host 归一：去掉 scheme 与端口，仅留小写 host。

    ``newapi.internal`` / ``newapi.internal:3000`` / ``https://newapi.internal``
    归一后都是 ``newapi.internal``；非 host 形态（如 ``/foo``）返回空串，由调用方
    自然落空（不被任何 URL 命中）。
    """
    text = entry.strip()
    if not text:
        return ""
    # 无 scheme 时补 ``//`` 让 urlsplit 走 netloc 解析（否则 ``host:port`` 会被
    # 当成 path，hostname 为 None）。
    parts = urlsplit(text if "://" in text else f"//{text}")
    return (parts.hostname or "").lower()


def assert_upstream_allowed(base_url: str) -> None:
    """校验上游地址可信；不可信时抛 ``HTTPException(400)``（安全三防线见模块 docstring）。"""
    parts = urlsplit(base_url)

    if parts.scheme not in ("http", "https"):
        raise HTTPException(400, "upstream base url must be http or https")
    if parts.username or parts.password:
        raise HTTPException(400, "upstream base url must not contain userinfo")

    host = (parts.hostname or "").lower()
    if not host:
        raise HTTPException(400, "upstream base url has no host")

    allowlist = {
        normalized
        for entry in (settings.upstream_allowlist or "").split(",")
        if (normalized := _normalize_host(entry))
    }
    if not allowlist:
        # fail-closed：空白名单不是「放行全部」而是「拒绝全部」。
        raise HTTPException(400, "upstream allowlist is empty")
    if host not in allowlist:
        raise HTTPException(400, f"upstream host not allowed: {host}")
