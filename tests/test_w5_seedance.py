"""W5 SeedanceAdapter 单元测试（SPEC §7.1：respx 打桩上游，不依赖真实网络）。

覆盖：请求构造（content 数组/透传字段/callback_url 注入）、Bearer 鉴权、
状态映射（queued/running/succeeded/failed/expired、未知→RUNNING）、
错误分级（429/4xx/5xx）、callback 解析（=查询响应体，含坏报文）、
usage 实收信号（completion_tokens + 实际 resolution）、用量估算上下文键。
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
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
from app.adapters.seedance import SeedanceAdapter

BASE = "https://ark.test"
TASKS = f"{BASE}/api/v3/contents/generations/tasks"


def _ctx(**overrides) -> SubmitContext:
    kwargs = {
        "biz": "seedance-biz",
        "task_id": "task_gw456",
        "gateway_callback_url": "https://gw.test/callbacks/seedance-biz/seedance/cap-token",
        "upstream_base_url": BASE,
        "secrets": {"api_key": "ark-key-test"},
    }
    kwargs.update(overrides)
    return SubmitContext(**kwargs)


def _req(**overrides) -> CanonicalTaskRequest:
    kwargs = {"model": "doubao-seedance-2-5-260628", "prompt": "a dog", "action": "text2video"}
    kwargs.update(overrides)
    return CanonicalTaskRequest(**kwargs)


@pytest.fixture
def adapter() -> SeedanceAdapter:
    return SeedanceAdapter()


# ---------------------------------------------------------------------------
# 注册与协议常量
# ---------------------------------------------------------------------------


def test_registered_and_classvars() -> None:
    ad = get_adapter("seedance")
    assert isinstance(ad, SeedanceAdapter)
    assert ad.name == "seedance"
    assert ad.callback_capability is True
    assert ad.echoes_external_task_id is False  # 回调反查走索引表


# ---------------------------------------------------------------------------
# 鉴权
# ---------------------------------------------------------------------------


def test_auth_headers_from_ctx(adapter: SeedanceAdapter) -> None:
    assert adapter.auth_headers(_ctx()) == {"Authorization": "Bearer ark-key-test"}


def test_auth_headers_from_env(monkeypatch: pytest.MonkeyPatch, adapter: SeedanceAdapter) -> None:
    monkeypatch.setenv("UPSTREAM_KEY_ARK", "ark-env-key")
    cfg = SimpleNamespace(auth_type="bearer_key", auth_secret_ref="UPSTREAM_KEY_ARK")
    assert adapter.auth_headers(cfg) == {"Authorization": "Bearer ark-env-key"}


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


@respx.mock
async def test_submit_text2video(adapter: SeedanceAdapter) -> None:
    route = respx.post(TASKS).respond(200, json={"id": "cgt-2025-abc"})
    req = _req(
        duration=5.0,
        resolution="1080p",
        generate_audio=True,
        extra={"ratio": "16:9", "execution_expires_after": 172800, "service_tier": "default"},
    )
    result = await adapter.submit(req, _ctx())
    assert result.upstream_task_id == "cgt-2025-abc"

    sent = json.loads(route.calls.last.request.content)
    assert sent["model"] == "doubao-seedance-2-5-260628"
    assert sent["content"] == [{"type": "text", "text": "a dog"}]
    assert sent["duration"] == 5
    assert sent["resolution"] == "1080p"
    assert sent["generate_audio"] is True
    assert sent["ratio"] == "16:9"
    assert sent["execution_expires_after"] == 172800  # 透传
    assert sent["service_tier"] == "default"
    assert sent["callback_url"] == "https://gw.test/callbacks/seedance-biz/seedance/cap-token"
    assert route.calls.last.request.headers["Authorization"] == "Bearer ark-key-test"


@respx.mock
async def test_submit_image_first_frame(adapter: SeedanceAdapter) -> None:
    route = respx.post(TASKS).respond(200, json={"id": "cgt-img-1"})
    req = _req(image="https://img.test/first.png")
    result = await adapter.submit(req, _ctx())
    assert result.upstream_task_id == "cgt-img-1"
    sent = json.loads(route.calls.last.request.content)
    assert sent["content"] == [
        {"type": "text", "text": "a dog"},
        {
            "type": "image_url",
            "image_url": {"url": "https://img.test/first.png"},
            "role": "first_frame",
        },
    ]
    # 未指定参数不出现在请求体
    assert "duration" not in sent
    assert "resolution" not in sent
    assert "generate_audio" not in sent


@respx.mock
async def test_submit_missing_id(adapter: SeedanceAdapter) -> None:
    respx.post(TASKS).respond(200, json={"unexpected": 1})
    with pytest.raises(UpstreamBizError):
        await adapter.submit(_req(), _ctx())


# ---------------------------------------------------------------------------
# 错误分级
# ---------------------------------------------------------------------------


@respx.mock
async def test_submit_429_retry_after(adapter: SeedanceAdapter) -> None:
    respx.post(TASKS).respond(
        429, json={"error": {"message": "rate limited"}}, headers={"Retry-After": "3"}
    )
    with pytest.raises(UpstreamRateLimitError) as exc_info:
        await adapter.submit(_req(), _ctx())
    assert exc_info.value.retry_after == 3.0


@respx.mock
async def test_submit_4xx_biz_error(adapter: SeedanceAdapter) -> None:
    respx.post(TASKS).respond(
        400, json={"error": {"code": "InvalidParameter", "message": "bad prompt"}}
    )
    with pytest.raises(UpstreamBizError) as exc_info:
        await adapter.submit(_req(), _ctx())
    assert exc_info.value.code == "InvalidParameter"
    assert "bad prompt" in str(exc_info.value)


@respx.mock
async def test_submit_5xx_raises_http_status_error(adapter: SeedanceAdapter) -> None:
    respx.post(TASKS).respond(500, json={"error": {"message": "boom"}})
    with pytest.raises(httpx.HTTPStatusError):
        await adapter.submit(_req(), _ctx())


@respx.mock
async def test_poll_4xx_biz_error(adapter: SeedanceAdapter) -> None:
    respx.get(f"{TASKS}/cgt-x").respond(404, json={"error": {"message": "not found"}})
    with pytest.raises(UpstreamBizError):
        await adapter.poll("cgt-x", _ctx())


# ---------------------------------------------------------------------------
# poll 与状态映射
# ---------------------------------------------------------------------------


@respx.mock
async def test_poll_succeeded_full_snapshot(adapter: SeedanceAdapter) -> None:
    respx.get(f"{TASKS}/cgt-ok-1").respond(
        200,
        json={
            "id": "cgt-ok-1",
            "model": "doubao-seedance-2-5-260628",
            "status": "succeeded",
            "content": {
                "video_url": "https://tos.test/v.mp4",
                "last_frame_url": "https://tos.test/last.png",
            },
            "usage": {"completion_tokens": 246840, "total_tokens": 246840},
            "created_at": 1000,
            "updated_at": 1084,
            "resolution": "1080p",
            "duration": 5,
        },
    )
    snap = await adapter.poll("cgt-ok-1", _ctx())
    assert snap.status is TaskStatus.SUCCEEDED
    assert snap.result["url"] == "https://tos.test/v.mp4"
    assert snap.result["last_frame_url"] == "https://tos.test/last.png"
    assert snap.result["duration"] == 5.0
    assert snap.result["resolution"] == "1080p"
    # 实收信号：completion_tokens + 实际 resolution 档
    assert snap.usage["completion_tokens"] == 246840.0
    assert snap.usage["resolution"] == "1080p"
    assert snap.usage["actual_duration"] == 5.0
    assert snap.error is None
    assert snap.event_id == "seedance:cgt-ok-1:succeeded:1084"


@respx.mock
async def test_poll_failed_and_expired(adapter: SeedanceAdapter) -> None:
    respx.get(f"{TASKS}/cgt-fail").respond(
        200,
        json={
            "id": "cgt-fail",
            "status": "failed",
            "error": {"code": "ContentFiltered", "message": "blocked"},
            "updated_at": 1,
        },
    )
    snap = await adapter.poll("cgt-fail", _ctx())
    assert snap.status is TaskStatus.FAILED
    assert snap.error == {"code": "ContentFiltered", "message": "blocked"}
    assert snap.result is None

    respx.get(f"{TASKS}/cgt-exp").respond(
        200, json={"id": "cgt-exp", "status": "expired", "updated_at": 2}
    )
    snap = await adapter.poll("cgt-exp", _ctx())
    assert snap.status is TaskStatus.TIMEOUT  # expired → TIMEOUT


@respx.mock
async def test_poll_running(adapter: SeedanceAdapter) -> None:
    respx.get(f"{TASKS}/cgt-run").respond(
        200, json={"id": "cgt-run", "status": "running", "updated_at": 3}
    )
    snap = await adapter.poll("cgt-run", _ctx())
    assert snap.status is TaskStatus.RUNNING
    assert snap.result is None


def test_map_status(adapter: SeedanceAdapter) -> None:
    assert adapter.map_status("queued") is TaskStatus.QUEUED
    assert adapter.map_status("running") is TaskStatus.RUNNING
    assert adapter.map_status("succeeded") is TaskStatus.SUCCEEDED
    assert adapter.map_status("failed") is TaskStatus.FAILED
    assert adapter.map_status("expired") is TaskStatus.TIMEOUT
    assert adapter.map_status("mystery") is TaskStatus.RUNNING  # 未知→RUNNING


# ---------------------------------------------------------------------------
# parse_callback（回调体 = 查询响应体，简报 A §4 已确认）
# ---------------------------------------------------------------------------


def test_parse_callback_equals_query_body(adapter: SeedanceAdapter) -> None:
    body = json.dumps(
        {
            "id": "cgt-cb-1",
            "status": "succeeded",
            "content": {"video_url": "https://tos.test/cb.mp4"},
            "usage": {"completion_tokens": 1000},
            "resolution": "720p",
            "duration": 5,
            "updated_at": 55,
        }
    ).encode()
    snap = adapter.parse_callback(body, {})
    assert snap.status is TaskStatus.SUCCEEDED
    assert snap.result["url"] == "https://tos.test/cb.mp4"
    assert snap.usage["completion_tokens"] == 1000.0
    assert snap.usage["resolution"] == "720p"
    assert snap.event_id == "seedance:cgt-cb-1:succeeded:55"


def test_parse_callback_bad_body(adapter: SeedanceAdapter) -> None:
    with pytest.raises(ValueError):
        adapter.parse_callback(b"not-json", {})
    with pytest.raises(ValueError):
        adapter.parse_callback(b'"str"', {})


# ---------------------------------------------------------------------------
# estimate_usage / rewrite_callback_url
# ---------------------------------------------------------------------------


def test_estimate_usage_context_keys(adapter: SeedanceAdapter) -> None:
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
    assert est.context["duration"] == 10.0  # 顶格
    assert est.context["resolution"] == "1080p"
    assert est.context["service_tier"] == "default"

    est2 = adapter.estimate_usage(
        _req(
            duration=5.0,
            resolution="720p",
            generate_audio=True,
            image="https://img.test/f.png",
            extra={"service_tier": "flex"},
        )
    )
    assert est2.context["duration"] == 5.0
    assert est2.context["resolution"] == "720p"
    assert est2.context["generate_audio"] == 1.0
    assert est2.context["has_image_input"] == 1.0
    assert est2.context["service_tier"] == "flex"


def test_rewrite_callback_url(adapter: SeedanceAdapter) -> None:
    body = json.dumps({"model": "m", "callback_url": "https://evil/cb", "duration": 5})
    rewritten = json.loads(adapter.rewrite_callback_url(body.encode(), None))
    assert "callback_url" not in rewritten
    assert rewritten["duration"] == 5
    assert adapter.rewrite_callback_url(b"\xff", None) == b"\xff"  # 非 JSON 原样返回
