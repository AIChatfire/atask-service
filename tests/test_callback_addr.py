"""用户回调地址准入单测：取值（头/body）+ 摘除 + 白名单 fail-closed + SSRF 防线。

覆盖 ``app/services/callback_addr.py`` 的三个公开函数：

- ``callback_url_from``：``X-Callback-Url`` 头优先，body 顶层 ``callback_url`` 兜底
  （上游 API 文档口径），两者都无 → 空串；
- ``strip_callback_url``：默认「网关接管」模式下把该字段从转发体摘除，防上游与
  网关双投递；没有该键时返回 ``None``（调用方据此不改写转发体）；
- ``assert_callback_allowed``：仅 http(s)、拒 URL userinfo、拒私网/回环/链路本地
  字面 IP（**即使白名单显式列了它也拒**）、白名单 fail-closed（空 = 全拒）。

不触网、不打库：body 都是纯内存构造。
"""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from app.config import settings
from app.services.callback_addr import (
    CALLBACK_URL_HEADER,
    assert_callback_allowed,
    callback_url_from,
    strip_callback_url,
)


def _body(**fields: object) -> bytes:
    return json.dumps(fields).encode()


@pytest.fixture
def cb_settings(monkeypatch: pytest.MonkeyPatch):
    """回调白名单的受控基线（默认空 = fail-closed）；测试内直接改属性即生效。"""
    monkeypatch.setattr(settings, "callback_allowlist", "")
    return settings


# ---------------------------------------------------------------------------
# callback_url_from：头优先，body 兜底
# ---------------------------------------------------------------------------


def test_header_name_is_published_contract():
    """头名是对外契约，改名等于让所有已接入客户端静默失去回调。"""
    assert CALLBACK_URL_HEADER == "X-Callback-Url"


def test_header_wins_over_body():
    """两者都传时以头为准（头是网关专有契约，语义明确）。"""
    got = callback_url_from(
        "https://from-header.example/hook",
        _body(callback_url="https://from-body.example/hook"),
        "application/json")
    assert got == "https://from-header.example/hook"


def test_falls_back_to_body_field():
    """头缺失时取 body 顶层 ``callback_url``（客户端按上游文档填写的那条路径）。"""
    got = callback_url_from(
        None, _body(callback_url="https://from-body.example/hook"), "application/json")
    assert got == "https://from-body.example/hook"


def test_blank_header_falls_back_to_body():
    """头是空白串（``X-Callback-Url: ``）时按缺失处理，回退 body。"""
    got = callback_url_from(
        "   ", _body(callback_url="https://hook.example/x"), "application/json")
    assert got == "https://hook.example/x"


def test_no_callback_anywhere_returns_empty():
    assert callback_url_from(None, _body(model="sora"), "application/json") == ""


def test_blank_body_field_returns_empty():
    assert callback_url_from(None, _body(callback_url="  "), "application/json") == ""


def test_non_json_content_type_is_ignored():
    """非 JSON 体（multipart / 纯文本）不解析——与 ``_extract_model`` 同一宽容策略。"""
    assert callback_url_from(
        None, b"callback_url=https://hook.example/x", "text/plain") == ""


def test_malformed_json_body_is_ignored():
    """坏 JSON 不抛异常，按「没给」处理（请求仍照常转发上游）。"""
    assert callback_url_from(None, b"{not json", "application/json") == ""


def test_json_array_body_is_ignored():
    """顶层不是对象时不做字段提取。"""
    assert callback_url_from(None, b'["callback_url"]', "application/json") == ""


# ---------------------------------------------------------------------------
# strip_callback_url：摘除（默认网关接管，防上游与网关双投递）
# ---------------------------------------------------------------------------


def test_strip_removes_top_level_field():
    """摘除后重构体不再含 callback_url，其余字段逐字段保留。"""
    stripped = strip_callback_url(
        _body(model="sora", callback_url="https://hook.example/x", prompt="hi"),
        "application/json")
    assert stripped is not None
    parsed = json.loads(stripped)
    assert "callback_url" not in parsed
    assert parsed["model"] == "sora"
    assert parsed["prompt"] == "hi"


def test_strip_returns_none_when_field_absent():
    """没有该键 → None：调用方据此**不改写**转发体，原生提交保持逐字节同构。"""
    assert strip_callback_url(_body(model="sora"), "application/json") is None


def test_strip_returns_none_for_non_json():
    assert strip_callback_url(b"not json", "text/plain") is None
    assert strip_callback_url(b"", "application/json") is None


def test_strip_ignores_nested_field():
    """只认顶层键：顶层没有就不做任何改写（嵌套同名键不是回调契约的一部分）。"""
    body = _body(model="sora", options={"callback_url": "https://inner.example"})
    assert strip_callback_url(body, "application/json") is None


def test_strip_removes_only_top_level_key():
    """顶层与嵌套同时存在时，只删顶层那一个。"""
    body = _body(callback_url="https://hook.example/x",
                 options={"callback_url": "https://inner.example"})
    parsed = json.loads(strip_callback_url(body, "application/json") or "{}")
    assert "callback_url" not in parsed
    assert parsed["options"]["callback_url"] == "https://inner.example"


# ---------------------------------------------------------------------------
# assert_callback_allowed：白名单
# ---------------------------------------------------------------------------


def test_allowed_host_passes(cb_settings):
    cb_settings.callback_allowlist = "hook.example,partner.example"
    assert_callback_allowed("https://hook.example/task/done")
    assert_callback_allowed("https://partner.example/task/done")


def test_host_not_in_allowlist_is_400(cb_settings):
    cb_settings.callback_allowlist = "hook.example"
    with pytest.raises(HTTPException) as exc:
        assert_callback_allowed("https://evil.example/collect")
    assert exc.value.status_code == 400


def test_empty_allowlist_rejects_everything(cb_settings):
    """空白名单 → 一律 400（fail-closed，本组最重要的一条）。"""
    cb_settings.callback_allowlist = ""
    for url in (
        "https://hook.example/x",
        "https://anything.example/x",
        "https://8.8.8.8/x",
    ):
        with pytest.raises(HTTPException) as exc:
            assert_callback_allowed(url)
        assert exc.value.status_code == 400


def test_no_host_is_400(cb_settings):
    """空地址 → 无 host → 400（即使白名单已配）。"""
    cb_settings.callback_allowlist = "hook.example"
    with pytest.raises(HTTPException) as exc:
        assert_callback_allowed("")
    assert exc.value.status_code == 400


def test_empty_allowlist_says_so_in_the_error(cb_settings):
    """空白名单的错误文案必须指向「没配白名单」，而不是「host 未命中」。

    两条分支**行为等价**（空 set 会落到下一条 host 分支，同样 400），所以只断言
    状态码的用例抓不到差别；但排障方向完全不同：后者会把运维引向「这个 host 为什么
    没命中」，而真正的原因是压根没配 `CALLBACK_ALLOWLIST`。变异测试（删掉空白名单
    分支）证明本用例会变红、而其余用例不会——这条断言锁的就是那份诊断性。
    """
    cb_settings.callback_allowlist = ""
    with pytest.raises(HTTPException) as exc:
        assert_callback_allowed("https://hook.example/x")
    assert "allowlist is empty" in str(exc.value.detail)


@pytest.mark.parametrize("url", [
    "file:///etc/passwd",
    "ftp://hook.example/x",
    "gopher://hook.example/x",
])
def test_non_http_scheme_is_400(cb_settings, url):
    """仅接受 http / https。"""
    cb_settings.callback_allowlist = "hook.example"
    with pytest.raises(HTTPException) as exc:
        assert_callback_allowed(url)
    assert exc.value.status_code == 400


@pytest.mark.parametrize("url", [
    "http://user@hook.example/x",       # 仅用户名
    "http://:pass@hook.example/x",      # 仅口令（命中 password 分支）
])
def test_userinfo_is_400(cb_settings, url):
    """URL userinfo（``user:password@host`` 形态）→ 400。"""
    cb_settings.callback_allowlist = "hook.example"
    with pytest.raises(HTTPException) as exc:
        assert_callback_allowed(url)
    assert exc.value.status_code == 400


def test_host_match_is_case_insensitive(cb_settings):
    """白名单 ``Hook.Example`` 应放行 ``https://hook.example/x``。"""
    cb_settings.callback_allowlist = "Hook.Example"
    assert_callback_allowed("https://hook.example/x")


def test_port_not_part_of_match(cb_settings):
    """白名单条目带端口时归一为 host；请求带端口同理（端口不参与判定）。"""
    cb_settings.callback_allowlist = "hook.example:8443"
    assert_callback_allowed("https://hook.example/x")
    cb_settings.callback_allowlist = "hook.example"
    assert_callback_allowed("https://hook.example:8443/x")


def test_allowlist_entry_may_carry_scheme(cb_settings):
    """白名单条目写成整条 URL 时同样按 host 命中。"""
    cb_settings.callback_allowlist = "https://hook.example/callback"
    assert_callback_allowed("https://hook.example/x")


# ---------------------------------------------------------------------------
# assert_callback_allowed：SSRF 防线（字面 IP）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("url", [
    "http://127.0.0.1/x",
    "http://10.0.0.5/x",
    "http://172.16.0.1/x",
    "http://192.168.1.1/x",
    "http://169.254.169.254/latest/meta-data/",     # 云元数据：SSRF 的头号目标
    "http://0.0.0.0/x",
    "http://[::1]/x",
])
def test_private_literal_ip_is_400_even_if_allowlisted(cb_settings, url):
    """私网 / 回环 / 链路本地 / 保留段字面 IP **无条件拒绝**——白名单显式列了也拒。

    白名单是「可信主机」声明，不是「允许任意地址」的开关；这条是它与
    ``upstream_addr`` 的唯一差别（上游是同内网固定基址，回调是公网目标）。
    """
    host = url.split("//", 1)[1].split("/", 1)[0]
    cb_settings.callback_allowlist = f"{host},hook.example"
    with pytest.raises(HTTPException) as exc:
        assert_callback_allowed(url)
    assert exc.value.status_code == 400


def test_public_literal_ip_passes_when_allowlisted(cb_settings):
    """公网字面 IP 被显式列入白名单时放行（运维的显式信任声明）。"""
    cb_settings.callback_allowlist = "8.8.8.8"
    assert_callback_allowed("https://8.8.8.8/hook")


def test_decimal_encoded_private_ip_cannot_bypass_allowlist(cb_settings):
    """十进制/十六进制形态的 IP 不会被 ``ip_address`` 认成本地地址，但白名单仍挡住它。"""
    cb_settings.callback_allowlist = "hook.example"
    for url in ("http://2130706433/x", "http://0x7f000001/x"):
        with pytest.raises(HTTPException) as exc:
            assert_callback_allowed(url)
        assert exc.value.status_code == 400
