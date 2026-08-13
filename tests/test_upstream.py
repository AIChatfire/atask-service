"""上游调用引擎测试：渠道覆盖三层叠加、鉴权头、信封校验、状态码映射、路径提取。"""

from __future__ import annotations

import httpx
import pytest

from app.services import upstream
from app.services.upstream import UpstreamError

# ---------------------------------------------------------------------------
# build_submit_body：default_params < 用户 body < param_override
# ---------------------------------------------------------------------------


def test_build_body_layering(route_factory, key_lease_factory):
    route = route_factory(default_params={"duration": 5, "ratio": "16:9"})
    key = key_lease_factory(param_override={"aigc_watermark": False, "duration": 8})
    body = {"model": "MiniMax-H3", "content": [{"type": "text", "text": "hi"}],
            "duration": 10, "junk": "keep"}

    merged = upstream.build_submit_body(route, key, body)
    assert merged["duration"] == 8            # 渠道 param_override 最高优先
    assert merged["ratio"] == "16:9"          # 路由默认参数补全
    assert merged["aigc_watermark"] is False  # 渠道参数合入
    assert merged["junk"] == "keep"           # 用户字段原样透传


def test_build_body_model_mapping(route_factory, key_lease_factory):
    route = route_factory()
    key = key_lease_factory(model_mapping={"MiniMax-H3": "MiniMax-H3-2026"})
    merged = upstream.build_submit_body(route, key, {"model": "MiniMax-H3"})
    assert merged["model"] == "MiniMax-H3-2026"


def test_build_body_allowlist_and_callback(route_factory, key_lease_factory):
    route = route_factory(
        body_allowlist=["model", "content"],
        supports_callback=True, callback_param="callback_url",
    )
    merged = upstream.build_submit_body(
        route, key_lease_factory(),
        {"model": "m", "content": [], "evil": "drop"},
        callback_url="https://gw.test/callback/minimax/t-1",
    )
    assert "evil" not in merged               # 白名单过滤客户端注入
    assert merged["callback_url"] == "https://gw.test/callback/minimax/t-1"


def test_build_body_no_callback_when_unsupported(route_factory, key_lease_factory):
    route = route_factory(supports_callback=False)
    merged = upstream.build_submit_body(
        route, key_lease_factory(), {"model": "m"},
        callback_url="https://gw.test/callback/minimax/t-1",
    )
    assert "callback_url" not in merged       # 不支持回调的上游不注入


# ---------------------------------------------------------------------------
# auth_headers：鉴权形态 + 渠道头覆盖 + OpenAI-Organization
# ---------------------------------------------------------------------------


def test_auth_headers_bearer_plus_overrides(route_factory, key_lease_factory):
    route = route_factory(auth_type="bearer")
    key = key_lease_factory(
        key="sk-abc",
        header_override={"X-Custom-Header": "v"},
        openai_organization="org-xxx",
    )
    headers = upstream.auth_headers(route, key)
    assert headers["Authorization"] == "Bearer sk-abc"
    assert headers["X-Custom-Header"] == "v"
    assert headers["OpenAI-Organization"] == "org-xxx"


def test_auth_headers_x_api_key(route_factory, key_lease_factory):
    route = route_factory(auth_type="x-api-key")
    headers = upstream.auth_headers(route, key_lease_factory(key="ak-1"))
    assert headers["X-Api-Key"] == "ak-1"
    assert "Authorization" not in headers


def test_auth_headers_filters_nested_config(route_factory, key_lease_factory):
    """防御：header_override 混入嵌套配置值（upstream 块）时不透出为 HTTP 头。
    （KeyLease 的 dict[str,str] 类型本身是第一道防线，这里验证第二道兜底。）"""
    route = route_factory()
    key = key_lease_factory(header_override={"X-Custom-Header": "v"})
    key.header_override["upstream"] = {"biz": "minimax"}  # type: ignore[dict-item] 注入异常输入
    headers = upstream.auth_headers(route, key)
    assert headers["X-Custom-Header"] == "v"
    assert "upstream" not in headers


# ---------------------------------------------------------------------------
# submit / probe：base_url 覆盖、信封校验、status_code_mapping、错误分级
# ---------------------------------------------------------------------------


async def test_submit_uses_lease_base_url_and_headers(respx_router, route_factory, key_lease_factory, patch_redis):
    route = route_factory(upstream_base_url="http://should-not-use.test")
    key = key_lease_factory(base_url="http://upstream.test",
                            header_override={"X-Custom-Header": "v"})
    http = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "mm-1"})
    )
    data = await upstream.submit(route, key, {"model": "MiniMax-H3"})
    assert data["task_id"] == "mm-1"
    req = http.calls.last.request
    assert req.headers["Authorization"] == "Bearer sk-upstream-key"
    assert req.headers["X-Custom-Header"] == "v"


async def test_submit_envelope_ok_check(respx_router, route_factory, key_lease_factory, patch_redis):
    """信封型上游：HTTP 200 + code != 0 → 业务错误（envelope 标记）。"""
    route = route_factory(ok_check={"path": "code", "equals": 0, "message_path": "message"})
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"code": 1002, "message": "insufficient balance"})
    )
    with pytest.raises(UpstreamError) as exc_info:
        await upstream.submit(route, key_lease_factory(), {"model": "m"})
    assert exc_info.value.envelope is True
    assert "insufficient balance" in str(exc_info.value)


async def test_submit_status_code_mapping(respx_router, route_factory, key_lease_factory, patch_redis):
    """渠道 status_code_mapping：上游 503 重写为 500（按渠道运营语义归类）。"""
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(503, text="upstream busy")
    )
    key = key_lease_factory(status_code_mapping={"503": "500"})
    with pytest.raises(UpstreamError) as exc_info:
        await upstream.submit(route_factory(), key, {"model": "m"})
    assert exc_info.value.status == 500


async def test_submit_4xx_raises(respx_router, route_factory, key_lease_factory, patch_redis):
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(400, json={"error": "bad request"})
    )
    with pytest.raises(UpstreamError) as exc_info:
        await upstream.submit(route_factory(), key_lease_factory(), {"model": "m"})
    assert exc_info.value.status == 400


async def test_probe_path_format(respx_router, route_factory, key_lease_factory, patch_redis):
    http = respx_router.get("http://upstream.test/v2/query/video_generation/mm-9").mock(
        return_value=httpx.Response(200, json={"task": {"status": "processing"}})
    )
    data = await upstream.probe(route_factory(), key_lease_factory(), "mm-9")
    assert data["task"]["status"] == "processing"
    assert http.calls.last.request.headers["Authorization"] == "Bearer sk-upstream-key"


async def test_proxy_channel_creates_separate_client(route_factory, key_lease_factory):
    """渠道 setting.proxy → 独立连接池（舱壁键含代理地址）。"""
    plain = upstream.client_for(route_factory(), key_lease_factory())
    proxied = upstream.client_for(route_factory(), key_lease_factory(proxy="http://127.0.0.1:7890"))
    assert plain is not proxied
    await upstream.close_all()


# ---------------------------------------------------------------------------
# extract_path：点分路径 + 数组下标
# ---------------------------------------------------------------------------


def test_extract_path_nested_and_index():
    obj = {"task": {"content": {"url": "http://v"}, "usage": {"output_seconds": 4}},
           "data": [{"task_id": "x"}]}
    assert upstream.extract_path(obj, "task.content.url") == "http://v"
    assert upstream.extract_path(obj, "task.usage.output_seconds") == 4
    assert upstream.extract_path(obj, "data.0.task_id") == "x"
    assert upstream.extract_path(obj, "data.1.task_id") is None
    assert upstream.extract_path(obj, "missing.deep") is None
    assert upstream.extract_path(obj, "") is None
