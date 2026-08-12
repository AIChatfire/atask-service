"""W1 videos 四端点测试：编排顺序、CanonicalTaskRequest 组装、幂等回放、
状态映射（to_video_status）、402/409/422 错误形制、remix 继承。"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from w1_helpers import (
    FakeRedis,
    build_test_app,
    make_biz,
    make_session,
    make_session_factory,
    make_token,
)

from app import errors
from app.routing import videos
from app.tasks.models import to_video_status

BIZ_CFG = make_biz()
TOKEN = make_token()
SUBMIT_RESULT = {"task_id": "task_1", "status": "queued", "created_at": 1_700_000_000}


@pytest.fixture
def tm() -> MagicMock:
    fake = MagicMock()
    fake.submit_task = AsyncMock(return_value=dict(SUBMIT_RESULT))
    fake.track_passthrough_task = AsyncMock(return_value="task_pt")
    videos.set_task_manager(fake)
    yield fake
    videos.set_task_manager(None)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _deps(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    fake = FakeRedis()
    import app.middleware as mw
    monkeypatch.setattr(mw, "get_redis", AsyncMock(return_value=fake))
    monkeypatch.setattr(videos.registry, "get", AsyncMock(return_value=BIZ_CFG))
    return fake


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test")


def _app(session=None) -> object:
    return build_test_app(videos.router, token=TOKEN,
                          session=session if session is not None else AsyncMock())


# ---------------------------------------------------------------------------
# POST /{biz}/v1/videos
# ---------------------------------------------------------------------------


async def test_submit_happy_path(tm: MagicMock) -> None:
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos", json={
            "model": "kling-v2", "prompt": "a cat", "duration": 5,
            "size": "1920x1080",
            "metadata": {"callback_url": "https://u.example.com/hook",
                         "resolution": "1080p", "negative_prompt": "blur"},
        })
    assert resp.status_code == 201
    body = resp.json()
    assert body["id"] == "task_1" and body["task_id"] == "task_1"
    assert body["status"] == "queued" and body["model"] == "kling-v2"
    assert body["created_at"] == SUBMIT_RESULT["created_at"]

    tm.submit_task.assert_awaited_once()
    kwargs = tm.submit_task.await_args.kwargs
    assert kwargs["form"] == "videos" and kwargs["biz_cfg"] is BIZ_CFG
    assert kwargs["token"] is TOKEN and kwargs["idem_key"] is None
    req = kwargs["req"]
    assert req.action == "textGenerate"                    # 无 image 推导
    assert req.callback_url == "https://u.example.com/hook"
    assert req.resolution == "1080p" and req.duration == 5
    assert req.extra["negative_prompt"] == "blur"          # 供应商扩展走 extra


async def test_submit_action_derivation(tm: MagicMock) -> None:
    async with _client(_app()) as c:
        await c.post("/kling/v1/videos", json={
            "model": "kling-v2", "prompt": "p", "image": "https://img/x.png"})
    assert tm.submit_task.await_args.kwargs["req"].action == "firstTailGenerate"

    tm.submit_task.reset_mock()
    async with _client(_app()) as c:
        await c.post("/kling/v1/videos", json={
            "model": "kling-v2", "prompt": "p",
            "metadata": {"action": "referenceGenerate"}})
    assert tm.submit_task.await_args.kwargs["req"].action == "referenceGenerate"


async def test_submit_invalid_action_400(tm: MagicMock) -> None:
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos", json={
            "model": "kling-v2", "prompt": "p", "metadata": {"action": "bogus"}})
    assert resp.status_code == 400
    assert resp.json()["error"]["type"] == "invalid_request_error"
    tm.submit_task.assert_not_awaited()


async def test_submit_payment_required_402(tm: MagicMock, _deps: FakeRedis) -> None:
    """PaymentRequired → 402 billing_error；幂等占位被释放（可修正后重试）。"""
    tm.submit_task.side_effect = videos.PaymentRequired("insufficient")
    headers = {"Idempotency-Key": "pay-1"}
    payload = {"model": "kling-v2", "prompt": "p"}
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos", json=payload, headers=headers)
    assert resp.status_code == 402
    err = resp.json()["error"]
    assert err["type"] == "billing_error" and err["code"] == "insufficient_quota"
    assert f"idem:{TOKEN.user_id}:pay-1" not in _deps.strings   # 键已释放


async def test_submit_billing_lock_busy_503(tm: MagicMock, _deps: FakeRedis) -> None:
    """BillingLockBusy（计费锁 409 重试仍忙）→ 503 背压 + Retry-After（不冒泡 500）。"""
    from app.billing.client import BillingLockBusy

    tm.submit_task.side_effect = BillingLockBusy(2500)
    headers = {"Idempotency-Key": "busy-1"}
    payload = {"model": "kling-v2", "prompt": "p"}
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos", json=payload, headers=headers)
    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "3"      # ceil(2500ms/1000)
    assert resp.json()["error"]["code"] == "backpressure"
    assert f"idem:{TOKEN.user_id}:busy-1" not in _deps.strings   # 键已释放


async def test_submit_idempotent_replay(tm: MagicMock) -> None:
    """同 Idempotency-Key 同 payload → 回放首个 201 响应，submit 只执行一次。"""
    headers = {"Idempotency-Key": "idem-v1"}
    payload = {"model": "kling-v2", "prompt": "p"}
    async with _client(_app()) as c:
        r1 = await c.post("/kling/v1/videos", json=payload, headers=headers)
        r2 = await c.post("/kling/v1/videos", json=payload, headers=headers)
    assert r1.status_code == 201 and r2.status_code == 201
    assert r2.json()["task_id"] == "task_1"
    assert tm.submit_task.await_count == 1


async def test_submit_task_manager_not_wired_503() -> None:
    videos.set_task_manager(None)  # type: ignore[arg-type]
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos",
                            json={"model": "kling-v2", "prompt": "p"})
    assert resp.status_code == 503
    assert "Retry-After" in resp.headers


# ---------------------------------------------------------------------------
# GET /{biz}/v1/videos/{task_id}
# ---------------------------------------------------------------------------


def _row(status: str, private_data: dict, fail_reason: str = "") -> dict:
    return {
        "task_id": "task_1", "platform": "gw_kling", "user_id": TOKEN.user_id,
        "status": status, "progress": "100%",
        "properties": json.dumps({"origin_model_name": "kling-v2"}),
        "private_data": json.dumps(private_data),
        "data": json.dumps({"format": "mp4"}), "fail_reason": fail_reason,
        "created_at": 1, "updated_at": 2, "finish_time": 3,
    }


async def test_get_completed(monkeypatch: pytest.MonkeyPatch, tm: MagicMock) -> None:
    pd = {"result_url": "https://cdn.example.com/v.mp4",
          "gateway": {"request_snapshot": {"duration": 5, "width": 1920},
                      "usage_actual": {"completion_tokens": 100}}}
    monkeypatch.setattr(videos, "get_owned_task",
                        AsyncMock(return_value=_row("SUCCESS", pd)))
    async with _client(_app()) as c:
        resp = await c.get("/kling/v1/videos/task_1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == to_video_status("SUCCESS") == "completed"
    assert body["url"] == "https://cdn.example.com/v.mp4"
    assert body["format"] == "mp4"
    assert body["metadata"]["duration"] == 5
    assert body["metadata"]["usage"] == {"completion_tokens": 100}
    assert body["error"] is None


async def test_get_failed_timeout_prefix(monkeypatch: pytest.MonkeyPatch,
                                         tm: MagicMock) -> None:
    """fail_reason 前缀归一：timeout: → error.code='timeout'。"""
    monkeypatch.setattr(videos, "get_owned_task", AsyncMock(return_value=_row(
        "FAILURE", {"gateway": {}}, fail_reason="timeout: swept by deadline")))
    async with _client(_app()) as c:
        resp = await c.get("/kling/v1/videos/task_1")
    body = resp.json()
    assert body["status"] == "failed"
    assert body["error"] == {"code": "timeout", "message": "swept by deadline"}
    assert body["url"] is None


async def test_get_in_progress(monkeypatch: pytest.MonkeyPatch, tm: MagicMock) -> None:
    monkeypatch.setattr(videos, "get_owned_task",
                        AsyncMock(return_value=_row("IN_PROGRESS", {"gateway": {}})))
    async with _client(_app()) as c:
        resp = await c.get("/kling/v1/videos/task_1")
    assert resp.json()["status"] == to_video_status("IN_PROGRESS") == "in_progress"


async def test_get_not_owned_404(monkeypatch: pytest.MonkeyPatch, tm: MagicMock) -> None:
    monkeypatch.setattr(videos, "get_owned_task",
                        AsyncMock(side_effect=errors.not_found("task not found")))
    async with _client(_app()) as c:
        resp = await c.get("/kling/v1/videos/task_x")
    assert resp.status_code == 404
    assert resp.json()["error"]["type"] == "invalid_request_error"


# ---------------------------------------------------------------------------
# GET /{biz}/v1/videos/{task_id}/content
# ---------------------------------------------------------------------------


async def test_content_redirect_302(monkeypatch: pytest.MonkeyPatch,
                                    tm: MagicMock) -> None:
    pd = {"result_url": "https://cdn.example.com/v.mp4", "gateway": {}}
    monkeypatch.setattr(videos, "get_owned_task",
                        AsyncMock(return_value=_row("SUCCESS", pd)))
    monkeypatch.setattr(videos, "get_session_factory",
                        lambda: make_session_factory(make_session()))
    async with _client(_app()) as c:
        resp = await c.get("/kling/v1/videos/task_1/content", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == "https://cdn.example.com/v.mp4"


async def test_content_not_ready_409(monkeypatch: pytest.MonkeyPatch,
                                     tm: MagicMock) -> None:
    monkeypatch.setattr(videos, "get_owned_task",
                        AsyncMock(return_value=_row("IN_PROGRESS", {"gateway": {}})))
    async with _client(_app()) as c:
        resp = await c.get("/kling/v1/videos/task_1/content")
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "task_not_completed"


# ---------------------------------------------------------------------------
# POST /{biz}/v1/videos/{video_id}/remix
# ---------------------------------------------------------------------------


async def test_remix_inherits_snapshot(monkeypatch: pytest.MonkeyPatch,
                                       tm: MagicMock) -> None:
    snapshot = {"model": "kling-v2", "prompt": "orig", "duration": 5,
                "resolution": "1080p", "extra": {"negative_prompt": "blur"}}
    pd = {"gateway": {"request_snapshot": snapshot}}
    monkeypatch.setattr(videos, "get_owned_task",
                        AsyncMock(return_value=_row("SUCCESS", pd)))
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos/task_1/remix",
                            json={"prompt": "new prompt",
                                  "metadata": {"resolution": "720p"}})
    assert resp.status_code == 201
    kwargs = tm.submit_task.await_args.kwargs
    assert kwargs["form"] == "videos_remix"
    req = kwargs["req"]
    assert req.action == "remixGenerate"
    assert req.prompt == "new prompt"          # 白名单覆盖
    assert req.model == "kling-v2"             # 继承
    assert req.resolution == "720p"            # metadata 覆盖
    assert req.duration == 5


async def test_remix_source_not_succeeded_422(monkeypatch: pytest.MonkeyPatch,
                                              tm: MagicMock) -> None:
    monkeypatch.setattr(videos, "get_owned_task",
                        AsyncMock(return_value=_row("IN_PROGRESS", {"gateway": {}})))
    async with _client(_app()) as c:
        resp = await c.post("/kling/v1/videos/task_1/remix", json={})
    assert resp.status_code == 422
    tm.submit_task.assert_not_awaited()

