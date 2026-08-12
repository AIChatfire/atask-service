"""W5 KlingAdapter 单元测试（SPEC §7.1：respx 打桩上游，不依赖真实网络）。

覆盖：请求构造（v1/v3 两代）、JWT 缓存（key 含 AK、过期前 60s 重签）、
状态映射（succeed/succeeded 拼写差异、未知→RUNNING）、错误分级
（429/4xx/5xx/信封 code!=0）、callback 解析（含坏报文）、用量估算上下文键。
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace

import httpx
import jwt
import pytest
import respx

from app.adapters.base import (
    CanonicalTaskRequest,
    SubmitContext,
    TaskStatus,
    UpstreamBizError,
    UpstreamRateLimitError,
    get_adapter,
)
from app.adapters.kling import KlingAdapter

BASE = "https://kling.test"


def _ctx(**overrides) -> SubmitContext:
    kwargs = {
        "biz": "kling-biz",
        "task_id": "task_gw123",
        "gateway_callback_url": "https://gw.test/callbacks/kling-biz/kling/cap-token",
        "upstream_base_url": BASE,
        "secrets": {"ak": "ak-test", "sk": "sk-test"},
    }
    kwargs.update(overrides)
    return SubmitContext(**kwargs)


def _req(**overrides) -> CanonicalTaskRequest:
    kwargs = {"model": "kling-v2-master", "prompt": "a cat", "action": "text2video"}
    kwargs.update(overrides)
    return CanonicalTaskRequest(**kwargs)


@pytest.fixture
def adapter() -> KlingAdapter:
    KlingAdapter._jwt_cache.clear()
    return KlingAdapter()


# ---------------------------------------------------------------------------
# 注册与协议常量
# ---------------------------------------------------------------------------


def test_registered_and_classvars() -> None:
    ad = get_adapter("kling")
    assert isinstance(ad, KlingAdapter)
    assert ad.name == "kling"
    assert ad.callback_capability is True
    assert ad.echoes_external_task_id is True


# ---------------------------------------------------------------------------
# JWT 鉴权与缓存
# ---------------------------------------------------------------------------


def test_jwt_payload_and_cache(adapter: KlingAdapter) -> None:
    ctx = _ctx()
    h1 = adapter.auth_headers(ctx)
    token = h1["Authorization"].removeprefix("Bearer ")
    payload = jwt.decode(token, "sk-test", algorithms=["HS256"])
    assert payload["iss"] == "ak-test"
    assert payload["exp"] - payload["iat" if "iat" in payload else "nbf"] >= 1800
    now = int(time.time())
    assert now + 1700 < payload["exp"] <= now + 1800
    assert payload["nbf"] <= now

    # 缓存命中：同 AK 第二次调用复用同一 token
    h2 = adapter.auth_headers(ctx)
    assert h2["Authorization"] == h1["Authorization"]
    assert "ak-test" in adapter._jwt_cache  # 缓存 key 含 AK

    # 不同 AK → 独立缓存项、不同 token
    h3 = adapter.auth_headers(_ctx(secrets={"ak": "ak-other", "sk": "sk-test"}))
    assert h3["Authorization"] != h1["Authorization"]
    assert "ak-other" in adapter._jwt_cache


def test_jwt_cache_refresh_before_expiry(adapter: KlingAdapter) -> None:
    ctx = _ctx()
    adapter.auth_headers(ctx)
    # 人为把缓存的 exp 压进过期前 60s 窗口 → 必须重签（缓存 exp 被刷新）
    stale = int(time.time()) + 30
    adapter._jwt_cache["ak-test"] = (adapter._jwt_cache["ak-test"][0], stale)
    adapter.auth_headers(ctx)
    # 同秒内 payload 相同 token 可能不变，但缓存必须已被重签刷新
    assert adapter._jwt_cache["ak-test"][1] > stale
    assert adapter._jwt_cache["ak-test"][1] - int(time.time()) > 1700


def test_auth_headers_from_env(monkeypatch: pytest.MonkeyPatch, adapter: KlingAdapter) -> None:
    """cfg 形态（biz 配置对象）：按 auth_secret_ref 直读 os.environ。"""
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-env")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-env")
    cfg = SimpleNamespace(auth_type="aksk_jwt", auth_secret_ref="UPSTREAM_SECRET_KLING")
    token = adapter.auth_headers(cfg)["Authorization"].removeprefix("Bearer ")
    payload = jwt.decode(token, "sk-env", algorithms=["HS256"])
    assert payload["iss"] == "ak-env"

    monkeypatch.setenv("UPSTREAM_KEY_KLING", "static-key")
    cfg2 = SimpleNamespace(auth_type="bearer_key", auth_secret_ref="UPSTREAM_KEY_KLING")
    assert adapter.auth_headers(cfg2) == {"Authorization": "Bearer static-key"}


# ---------------------------------------------------------------------------
# submit：v1 旧版
# ---------------------------------------------------------------------------


@respx.mock
async def test_submit_v1_text2video(adapter: KlingAdapter) -> None:
    route = respx.post(f"{BASE}/v1/videos/text2video").respond(
        200,
        json={
            "code": 0,
            "message": "ok",
            "request_id": "req-1",
            "data": {"task_id": "kl-task-1", "task_status": "submitted"},
        },
    )
    result = await adapter.submit(_req(duration=5.0, mode="std"), _ctx())
    assert result.upstream_task_id == "kl-task-1"
    assert result.raw["code"] == 0

    sent = json.loads(route.calls.last.request.content)
    assert sent["model_name"] == "kling-v2-master"
    assert sent["duration"] == "5"  # 旧版 duration 是字符串
    assert sent["mode"] == "std"
    assert sent["callback_url"] == "https://gw.test/callbacks/kling-biz/kling/cap-token"
    assert sent["external_task_id"] == "task_gw123"  # 网关 task_id 注入
    assert route.calls.last.request.headers["Authorization"].startswith("Bearer ")


@respx.mock
async def test_submit_v1_image2video_with_image(adapter: KlingAdapter) -> None:
    route = respx.post(f"{BASE}/v1/videos/image2video").respond(
        200, json={"code": 0, "data": {"task_id": "kl-task-2"}}
    )
    # new-api 动作枚举 firstTailGenerate → image2video
    req = _req(
        action="firstTailGenerate",
        image="https://img.test/a.png",
        extra={"negative_prompt": "blur"},
    )
    result = await adapter.submit(req, _ctx())
    assert result.upstream_task_id == "kl-task-2"
    sent = json.loads(route.calls.last.request.content)
    assert sent["image"] == "https://img.test/a.png"
    assert sent["negative_prompt"] == "blur"
    assert sent["duration"] == "5"  # 未指定默认 5


# ---------------------------------------------------------------------------
# submit：v3 新形态
# ---------------------------------------------------------------------------


@respx.mock
async def test_submit_v3_envelope(adapter: KlingAdapter) -> None:
    route = respx.post(f"{BASE}/text-to-video/kling-3.0").respond(
        200, json={"code": 0, "data": {"id": "kl3-task-1", "status": "submitted"}}
    )
    req = _req(model="kling-v3", resolution="1080p", duration=10.0)
    result = await adapter.submit(req, _ctx())
    assert result.upstream_task_id == "kl3-task-1"
    sent = json.loads(route.calls.last.request.content)
    assert sent["settings"] == {"resolution": "1080p", "duration": 10.0}
    assert sent["options"]["callback_url"].startswith("https://gw.test/callbacks/")
    assert sent["options"]["external_task_id"] == "task_gw123"


@respx.mock
async def test_submit_v3_image_to_video(adapter: KlingAdapter) -> None:
    route = respx.post(f"{BASE}/image-to-video/kling-3.0").respond(
        200, json={"code": 0, "data": {"id": "kl3-task-2"}}
    )
    req = _req(model="kling-v3", image="https://img.test/f.png", action="image2video")
    result = await adapter.submit(req, _ctx())
    assert result.upstream_task_id == "kl3-task-2"
    sent = json.loads(route.calls.last.request.content)
    assert sent["image"] == "https://img.test/f.png"


# ---------------------------------------------------------------------------
# 错误分级
# ---------------------------------------------------------------------------


@respx.mock
async def test_submit_envelope_biz_error(adapter: KlingAdapter) -> None:
    respx.post(f"{BASE}/v1/videos/text2video").respond(
        200, json={"code": 1001, "message": "invalid prompt"}
    )
    with pytest.raises(UpstreamBizError) as exc_info:
        await adapter.submit(_req(), _ctx())
    assert exc_info.value.code == 1001
    assert "invalid prompt" in str(exc_info.value)


@respx.mock
async def test_submit_429_retry_after(adapter: KlingAdapter) -> None:
    respx.post(f"{BASE}/v1/videos/text2video").respond(
        429, json={"code": 4290, "message": "too fast"}, headers={"Retry-After": "7"}
    )
    with pytest.raises(UpstreamRateLimitError) as exc_info:
        await adapter.submit(_req(), _ctx())
    assert exc_info.value.retry_after == 7.0


@respx.mock
async def test_submit_4xx_biz_error(adapter: KlingAdapter) -> None:
    respx.post(f"{BASE}/v1/videos/text2video").respond(
        400, json={"code": 400, "message": "bad request"}
    )
    with pytest.raises(UpstreamBizError) as exc_info:
        await adapter.submit(_req(), _ctx())
    assert exc_info.value.code == 400


@respx.mock
async def test_submit_5xx_raises_http_status_error(adapter: KlingAdapter) -> None:
    respx.post(f"{BASE}/v1/videos/text2video").respond(503, json={"message": "down"})
    with pytest.raises(httpx.HTTPStatusError):
        await adapter.submit(_req(), _ctx())


# ---------------------------------------------------------------------------
# poll 与状态映射
# ---------------------------------------------------------------------------


@respx.mock
async def test_poll_uses_action_path(adapter: KlingAdapter) -> None:
    route = respx.get(f"{BASE}/v1/videos/image2video/kl-task-9").respond(
        200,
        json={
            "code": 0,
            "data": {
                "task_id": "kl-task-9",
                "task_status": "processing",
                "created_at": 100,
                "updated_at": 200,
            },
        },
    )
    snap = await adapter.poll("kl-task-9", _ctx(action="image2video"))
    assert route.called
    assert snap.status is TaskStatus.RUNNING
    assert snap.upstream_status == "processing"
    assert snap.event_id == "kling:kl-task-9:processing:200"


@respx.mock
async def test_poll_succeed_result(adapter: KlingAdapter) -> None:
    respx.get(f"{BASE}/v1/videos/text2video/kl-task-10").respond(
        200,
        json={
            "code": 0,
            "data": {
                "task_id": "kl-task-10",
                "task_status": "succeed",
                "task_result": {
                    "videos": [{"id": "v1", "url": "https://cdn/x.mp4", "duration": "5.0"}]
                },
                "updated_at": 300,
            },
        },
    )
    snap = await adapter.poll("kl-task-10", _ctx(action="text2video"))
    assert snap.status is TaskStatus.SUCCEEDED
    assert snap.result == {"url": "https://cdn/x.mp4", "duration": 5.0}
    assert snap.usage == {"actual_duration": 5.0}
    assert snap.error is None


@respx.mock
async def test_poll_failed_error(adapter: KlingAdapter) -> None:
    respx.get(f"{BASE}/v1/videos/text2video/kl-task-11").respond(
        200,
        json={
            "code": 0,
            "data": {
                "task_id": "kl-task-11",
                "task_status": "failed",
                "task_status_msg": "content policy",
            },
        },
    )
    snap = await adapter.poll("kl-task-11", _ctx(action="text2video"))
    assert snap.status is TaskStatus.FAILED
    assert snap.error == {"code": 0, "message": "content policy"}


@respx.mock
async def test_poll_newapi_action_mapped(adapter: KlingAdapter) -> None:
    """ctx.action 为 new-api 枚举（W2 从 tasks 行取回）时映射到旧版路径。"""
    route = respx.get(f"{BASE}/v1/videos/text2video/kl-task-12").respond(
        200, json={"code": 0, "data": {"task_id": "kl-task-12", "task_status": "submitted"}}
    )
    snap = await adapter.poll("kl-task-12", _ctx(action="textGenerate"))
    assert route.called
    assert snap.status is TaskStatus.QUEUED


def test_map_status_spellings(adapter: KlingAdapter) -> None:
    assert adapter.map_status("submitted") is TaskStatus.QUEUED
    assert adapter.map_status("processing") is TaskStatus.RUNNING
    assert adapter.map_status("succeed") is TaskStatus.SUCCEEDED  # 旧版拼写
    assert adapter.map_status("succeeded") is TaskStatus.SUCCEEDED  # 3.0 拼写
    assert adapter.map_status("failed") is TaskStatus.FAILED
    assert adapter.map_status("whatever") is TaskStatus.RUNNING  # 未知→RUNNING


# ---------------------------------------------------------------------------
# parse_callback（推断 ≈ 查询响应）
# ---------------------------------------------------------------------------


def test_parse_callback_v1(adapter: KlingAdapter) -> None:
    body = json.dumps(
        {
            "code": 0,
            "data": {
                "task_id": "kl-cb-1",
                "task_status": "succeed",
                "task_result": {"videos": [{"url": "https://cdn/y.mp4", "duration": "10"}]},
                "updated_at": 999,
            },
        }
    ).encode()
    snap = adapter.parse_callback(body, {})
    assert snap.status is TaskStatus.SUCCEEDED
    assert snap.result["url"] == "https://cdn/y.mp4"
    assert snap.usage["actual_duration"] == 10.0


def test_parse_callback_v3_billing(adapter: KlingAdapter) -> None:
    """3.0 形态：outputs + billing[].amount 实收信号。"""
    body = json.dumps(
        {
            "code": 0,
            "data": {
                "id": "kl3-cb-1",
                "status": "succeeded",
                "outputs": [{"url": "https://cdn/z.mp4", "duration": 8}],
                "billing": [{"charge_type": "cash", "amount": "0.42"}],
                "update_time": 12345,
            },
        }
    ).encode()
    snap = adapter.parse_callback(body, {})
    assert snap.status is TaskStatus.SUCCEEDED
    assert snap.result["url"] == "https://cdn/z.mp4"
    assert snap.usage["upstream_amount"] == "0.42"
    assert snap.event_id == "kling:kl3-cb-1:succeeded:12345"


def test_parse_callback_bad_body(adapter: KlingAdapter) -> None:
    with pytest.raises(ValueError):
        adapter.parse_callback(b"not-json", {})
    with pytest.raises(ValueError):
        adapter.parse_callback(b"[1,2,3]", {})


# ---------------------------------------------------------------------------
# estimate_usage / rewrite_callback_url
# ---------------------------------------------------------------------------


def test_estimate_usage_context_keys(adapter: KlingAdapter) -> None:
    est = adapter.estimate_usage(_req())
    assert est.amount_usd == 0
    assert set(est.context) == {
        "duration",
        "resolution",
        "mode",
        "quantity",
        "usage_tokens",
        "generate_audio",
        "has_image_input",
        "service_tier",
    }
    # 顶格：未指定参数按最高档
    assert est.context["duration"] == 10.0
    assert est.context["resolution"] == "1080p"
    assert est.context["mode"] == "pro"
    assert est.context["quantity"] == 1.0

    est2 = adapter.estimate_usage(
        _req(
            duration=5.0,
            resolution="720p",
            mode="std",
            n=2,
            generate_audio=True,
            image="https://img.test/a.png",
        )
    )
    assert est2.context["duration"] == 5.0
    assert est2.context["resolution"] == "720p"
    assert est2.context["mode"] == "std"
    assert est2.context["quantity"] == 2.0
    assert est2.context["generate_audio"] == 1.0
    assert est2.context["has_image_input"] == 1.0


def test_rewrite_callback_url(adapter: KlingAdapter) -> None:
    body = json.dumps(
        {
            "prompt": "x",
            "callback_url": "https://evil/cb",
            "options": {"callback_url": "https://evil/cb2", "watermark": 1},
        }
    )
    rewritten = json.loads(adapter.rewrite_callback_url(body.encode(), None))
    assert "callback_url" not in rewritten
    assert "callback_url" not in rewritten["options"]
    assert rewritten["options"]["watermark"] == 1
    # 非 JSON 原样返回
    assert adapter.rewrite_callback_url(b"\x00\x01", None) == b"\x00\x01"
