"""用户回调地址准入：``X-Callback-Url`` 头优先，body 的 ``callback_url`` 兜底。

本模块是**用户回调地址的唯一校验入口**——受理时校验一次，终态时 ``notify.push``
按落库值出站。为什么必须校验：回调目标是**用户可控的任意 URL**，而终态投递是
**网关主动发起的出站请求**，不设防等于给互联网开一个 SSRF 跳板（打内网服务、
云元数据 ``169.254.169.254``、Redis/MySQL 的 HTTP 接口）。

## 安全防线（与 ``app/services/upstream_addr.py`` 同构，同一套纪律）

1. 仅接受 ``http`` / ``https``；
2. **拒绝 URL userinfo**（``http://user@host`` 这类 ``user:password@`` 形态）；
3. **字面 IP 必须是全局可路由地址**（``ipaddress.is_global``）——私网、回环、
   链路本地（含云元数据段）、保留段**无条件拒绝**。理由：本网关是公网面服务，
   用户回调目标按定义应当公网可达，放行内网地址没有正当场景，而它恰恰是 SSRF
   最有价值的目标；
4. host 必须命中 ``settings.callback_allowlist``（逗号分隔），**空白名单 =
   全部拒绝**（fail-closed）——与 ``UPSTREAM_ALLOWLIST``、``ADMIN_TOKEN`` 同一条
   纪律：失败方向恒为拒绝。

## 取舍与已知限制

- **端口不参与命中判定**（与 ``upstream_addr`` 一致，统一取 ``urlsplit().hostname``）：
  同一主机换端口不该要求同步改白名单，端口不改变信任边界。
- **白名单条目按 host 归一后精确匹配**，不支持通配（``*.example.com``）。需要放行
  多个子域就逐条列出——通配会让「一条配置放行一整个域」的误配后果不可见。
- **不做 DNS 解析校验**（已知限制）：白名单配域名时，若该域名被解析到内网
  （DNS rebinding / 域名被入侵），本模块发现不了。不做的理由是解析引入 TOCTOU：
  校验时的解析结果与 ``httpx`` 出站时的解析结果不保证一致，加一层校验只多一次
  DNS 往返而不改变攻击面。真正可靠的边界是白名单——只放行可信域名。
- **归一后大小写不敏感**（host 统一小写），白名单条目可写 ``https://HOST:8080``。
- 回调地址本身不是凭证，但它是用户可控的出站目标，故校验放在受理链路的**校验段**：
  非法地址 ``400`` 且不留痕（不落库、不占并发槽、不占幂等键）。
"""

from __future__ import annotations

import ipaddress
import json
from urllib.parse import urlsplit

from fastapi import HTTPException

from app.config import settings
from app.services.upstream_addr import normalize_host

#: 用户回调地址头（网关专有契约；body 的 ``callback_url`` 字段为等价兜底）。
CALLBACK_URL_HEADER = "X-Callback-Url"

#: body 里承载回调地址的字段名（上游 API 文档口径）。
CALLBACK_URL_BODY_FIELD = "callback_url"


def _parse_json_object(body: bytes, content_type: str) -> dict[str, object] | None:
    """浅解析 body 为顶层 JSON 对象；非 JSON / 坏 JSON / 非对象一律 ``None``。

    与 ``relayflow._extract_model`` 同一宽容策略：**解析失败不报错**，按「没给」
    处理——转发照常送出，请求不该因为网关的一个可选增强而失败。
    """
    if not body or "json" not in content_type.lower():
        return None
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    return {str(key): value for key, value in parsed.items()}


def callback_url_from(header_value: str | None, body: bytes, content_type: str) -> str:
    """生效的回调地址：``X-Callback-Url`` 头优先，body 顶层 ``callback_url`` 兜底。

    只取值、不校验（校验交 ``assert_callback_allowed``）。两者都没有 → 空串，
    **不抛**：「没给回调地址」是正常请求，不是解析失败——与
    ``upstream_addr.resolve_upstream_base`` 同一形态。
    """
    header = (header_value or "").strip()
    if header:
        return header
    parsed = _parse_json_object(body, content_type)
    if parsed is None:
        return ""
    value = parsed.get(CALLBACK_URL_BODY_FIELD)
    return str(value).strip() if value else ""


def strip_callback_url(body: bytes, content_type: str) -> str | None:
    """从转发体摘除顶层 ``callback_url``，返回重构文本；无可摘时返回 ``None``。

    为什么摘：默认「网关接管」模式下回调由本网关签名投递，而转发体里的地址会让
    **上游也回调一次**——客户端于是收到两份通知（其中一份无签名），且它的回调地址
    被额外暴露给上游。摘除是消除双投递的唯一干净做法。

    **代价（写清楚）**：返回值是 ``json.dumps(..., ensure_ascii=False)`` 的重构体，
    JSON 语义等价，但字节形态（键序 / 空白）不保证与客户端原文逐字节一致。受理时
    ``data.request_body`` 仍存原文（供排障与原文回放），只有真要摘除时才写
    ``data.submit_body``——所以**绝大多数请求的转发体仍是原文**。

    只在顶层有该键时才改写：顶层没有 → ``None``，调用方据此原样转发
    （「零改写」是默认路径，不是特例）。
    """
    parsed = _parse_json_object(body, content_type)
    if parsed is None or CALLBACK_URL_BODY_FIELD not in parsed:
        return None
    parsed.pop(CALLBACK_URL_BODY_FIELD)
    return json.dumps(parsed, ensure_ascii=False)


def assert_callback_allowed(url: str) -> None:
    """校验用户回调地址可信；不可信时抛 ``HTTPException(400)``（防线见模块 docstring）。"""
    parts = urlsplit(url)

    if parts.scheme not in ("http", "https"):
        raise HTTPException(400, "callback url must be http or https")
    if parts.username or parts.password:
        raise HTTPException(400, "callback url must not contain userinfo")

    host = (parts.hostname or "").lower()
    if not host:
        raise HTTPException(400, "callback url has no host")

    # 字面 IP 只放行全局可路由地址；显式注解是给 mypy 的（分支里要赋 None）。
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address | None
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None and not ip.is_global:
        # 私网 / 回环 / 链路本地 / 保留段：**即使白名单里显式列了它也拒**。
        # 白名单是「可信主机」声明，不是「允许任意地址」的开关。
        raise HTTPException(400, "callback url must not target a private address")

    allowlist = {
        normalized
        for entry in (settings.callback_allowlist or "").split(",")
        if (normalized := normalize_host(entry))
    }
    if not allowlist:
        # fail-closed：空白名单不是「放行全部」而是「拒绝全部」。
        raise HTTPException(400, "callback allowlist is empty")
    if host not in allowlist:
        raise HTTPException(400, f"callback host not allowed: {host}")
