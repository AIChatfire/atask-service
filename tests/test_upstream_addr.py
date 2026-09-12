"""上游寻址单测：``X-Upstream-Base-Url`` 头优先 + 白名单 fail-closed（ADR-010 §4）。

覆盖两个函数的契约与安全三防线：头/配置优先级、双空返回空串、白名单命中/落空、
非 http(s)、URL userinfo、空白名单全拒、大小写不敏感、端口不参与命中。
不触网：``Request`` 是纯内存构造。
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.config import settings
from app.services.upstream_addr import (
    UPSTREAM_BASE_HEADER,
    assert_upstream_allowed,
    resolve_upstream_base,
)


def _request(headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    return Request({"type": "http", "method": "POST", "headers": headers or []})


def _header_request(value: str) -> Request:
    # ASGI 规范：网线上的头名一律小写（真实服务器也不会给混合大小写）。
    return _request([(UPSTREAM_BASE_HEADER.lower().encode(), value.encode())])


@pytest.fixture
def addr_settings(monkeypatch: pytest.MonkeyPatch):
    """上游寻址两项配置的受控基线（默认双空）；测试内直接改属性即生效。"""
    monkeypatch.setattr(settings, "upstream_base_url", "")
    monkeypatch.setattr(settings, "upstream_allowlist", "")
    return settings


# ---------------------------------------------------------------------------
# resolve_upstream_base
# ---------------------------------------------------------------------------


def test_header_wins_over_config(addr_settings):
    """头存在时优先于配置（nginx 注入值应压过默认基址）。"""
    addr_settings.upstream_base_url = "http://from-config"
    assert resolve_upstream_base(_header_request("http://from-header")) == "http://from-header"


def test_falls_back_to_config_when_header_absent(addr_settings):
    """头缺失时回退配置。"""
    addr_settings.upstream_base_url = "http://from-config"
    assert resolve_upstream_base(_request()) == "http://from-config"


def test_both_empty_returns_empty_without_raising(addr_settings):
    """两者都空 → 返回空串，**不抛**（400 由调用方裁决）。"""
    assert resolve_upstream_base(_request()) == ""


# ---------------------------------------------------------------------------
# assert_upstream_allowed
# ---------------------------------------------------------------------------


def test_allowed_host_passes(addr_settings):
    """命中白名单直接通过（不抛异常）。"""
    addr_settings.upstream_allowlist = "newapi.internal,10.0.0.5"
    assert_upstream_allowed("http://newapi.internal/v1/tasks")
    assert_upstream_allowed("http://10.0.0.5:8080/v1/tasks")


def test_host_not_in_allowlist_is_400(addr_settings):
    """不在白名单 → 400。"""
    addr_settings.upstream_allowlist = "newapi.internal"
    with pytest.raises(HTTPException) as exc:
        assert_upstream_allowed("http://evil.example/v1/tasks")
    assert exc.value.status_code == 400


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://newapi.internal/x"])
def test_non_http_scheme_is_400(addr_settings, url):
    """仅接受 http / https；``file://`` / ``ftp://`` 一律 400。"""
    addr_settings.upstream_allowlist = "newapi.internal"
    with pytest.raises(HTTPException) as exc:
        assert_upstream_allowed(url)
    assert exc.value.status_code == 400


@pytest.mark.parametrize("url", [
    "http://user@newapi.internal/x",     # 仅用户名
    "http://:pass@newapi.internal/x",    # 仅口令（命中 password 分支）
])
def test_userinfo_is_400(addr_settings, url):
    """URL userinfo（``user:password@host`` 形态）→ 400。"""
    addr_settings.upstream_allowlist = "newapi.internal"
    with pytest.raises(HTTPException) as exc:
        assert_upstream_allowed(url)
    assert exc.value.status_code == 400


def test_empty_allowlist_rejects_everything(addr_settings):
    """空白名单 → 一律 400（fail-closed，这是本组用例里最重要的一条）。"""
    addr_settings.upstream_allowlist = ""
    for url in (
        "http://newapi.internal/x",
        "https://newapi.internal/x",
        "http://10.0.0.5/x",
        "https://anything.example/x",
    ):
        with pytest.raises(HTTPException) as exc:
            assert_upstream_allowed(url)
        assert exc.value.status_code == 400


def test_empty_base_url_is_400(addr_settings):
    """地址为空 → 无 host → 400（即使白名单已配）。"""
    addr_settings.upstream_allowlist = "newapi.internal"
    with pytest.raises(HTTPException) as exc:
        assert_upstream_allowed("")
    assert exc.value.status_code == 400


def test_host_match_is_case_insensitive(addr_settings):
    """白名单 ``NewAPI.Internal`` 应放行 ``http://newapi.internal/x``。"""
    addr_settings.upstream_allowlist = "NewAPI.Internal"
    assert_upstream_allowed("http://newapi.internal/x")


def test_port_not_part_of_match_on_request(addr_settings):
    """请求带端口时按 host 命中（端口不参与判定）。"""
    addr_settings.upstream_allowlist = "newapi.internal"
    assert_upstream_allowed("http://newapi.internal:3000/x")


def test_port_not_part_of_match_on_allowlist_entry(addr_settings):
    """白名单条目带端口时同样归一为 host（与上一条同一取舍的两端）。"""
    addr_settings.upstream_allowlist = "newapi.internal:3000"
    assert_upstream_allowed("http://newapi.internal/x")
