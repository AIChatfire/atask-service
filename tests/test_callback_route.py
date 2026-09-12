"""回调地址接入链路的端到端断言（受理侧）。

单测（``tests/test_callback_addr.py``）只证明函数本身对，证明不了它真的接进了受理
链路。本文件补的是四件事在**真实路由**上的行为：取值（头优先 / body 兜底）、转发体
摘除、非法地址 400 且不留痕、透传模式既不校验也不改写。

用 ASGITransport 跑真实 app，出站由 respx 拦截，Redis / tasks 表走内存替身。
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import httpx
import pytest

from app.main import app

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"
CB = "https://hook.example/notify"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Upstream-Base-Url": UP_BASE, **extra}


@pytest.fixture
def cb_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "queue_deny_prefixes", "/api/,/console/")
    monkeypatch.setattr(settings, "callback_allowlist", "hook.example")
    monkeypatch.setattr(settings, "callback_passthrough_upstream", False)
    return settings


@pytest.fixture
def cb_queue(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """拦截 ``queue.publish_queue_submit``（不触真 broker），记录 task_id。"""
    import app.queue as q

    submitted: list[str] = []

    async def _publish(task_id: str) -> None:
        submitted.append(task_id)

    monkeypatch.setattr(q, "publish_queue_submit", AsyncMock(side_effect=_publish))
    return submitted


# ---------------------------------------------------------------------------
# 取值与摘除
# ---------------------------------------------------------------------------


async def test_body_callback_url_is_recorded_and_stripped_from_forwarded_body(
    cb_settings, patch_redis, task_store, cb_queue, respx_router,
):
    """body 顶层 ``callback_url``：落库 + **从转发体摘除**（防上游也回调形成双投递）。"""
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks",
            json={"model": "seedance-x", "callback_url": CB, "prompt": "hi"},
            headers=_headers())

    assert resp.status_code == 202, resp.text
    data = task_store.rows[resp.json()["task_id"]]["data"]

    assert data["callback_url"] == CB
    assert CB in data["request_body"]                 # 原文保留（排障 / 原文回放）
    forwarded = json.loads(data["submit_body"])       # 转发改走重构体
    assert "callback_url" not in forwarded
    assert forwarded["model"] == "seedance-x"
    assert forwarded["prompt"] == "hi"


async def test_header_callback_url_leaves_body_untouched(
    cb_settings, patch_redis, task_store, cb_queue, respx_router,
):
    """头传地址时**完全不碰 body**：不落 ``submit_body``，转发仍是原文。

    「零改写」是默认路径而不是特例——只有真要从 body 里摘东西时才产生重构体。
    """
    async with _client() as client:
        resp = await client.post("/queue/v1/tasks",
                                 json={"model": "seedance-x", "prompt": "hi"},
                                 headers=_headers(**{"X-Callback-Url": CB}))

    assert resp.status_code == 202, resp.text
    data = task_store.rows[resp.json()["task_id"]]["data"]
    assert data["callback_url"] == CB
    assert "submit_body" not in data


async def test_no_callback_url_writes_no_key(
    cb_settings, patch_redis, task_store, cb_queue, respx_router,
):
    """不给地址 → 不落 ``callback_url``、不改写转发体（终态收口点据此不投递）。"""
    async with _client() as client:
        resp = await client.post("/queue/v1/tasks", json={"model": "x"},
                                 headers=_headers())

    assert resp.status_code == 202, resp.text
    data = task_store.rows[resp.json()["task_id"]]["data"]
    assert "callback_url" not in data
    assert "submit_body" not in data


async def test_header_wins_over_body(
    cb_settings, patch_redis, task_store, cb_queue, respx_router,
):
    """两者都给且不同时以头为准；body 里的那个仍被摘除（它已不是生效地址）。"""
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks",
            json={"model": "x", "callback_url": "https://other.example/hook"},
            headers=_headers(**{"X-Callback-Url": CB}))

    assert resp.status_code == 202, resp.text
    data = task_store.rows[resp.json()["task_id"]]["data"]
    assert data["callback_url"] == CB
    assert "callback_url" not in json.loads(data["submit_body"])


# ---------------------------------------------------------------------------
# 非法地址：400 且不留痕
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [
    "http://127.0.0.1/hook",        # 私网字面 IP（即使白名单也不放行）
    "https://evil.example/hook",    # 不在白名单
    "ftp://hook.example/hook",      # 非 http(s)
])
async def test_illegal_callback_url_is_400_without_any_trace(
    cb_settings, patch_redis, task_store, cb_queue, respx_router, bad,
):
    """非法地址在**任何副作用之前**被拒：不落库、不入队、零上游往返、不占幂等键。"""
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks", json={"model": "x"},
            headers=_headers(**{"X-Callback-Url": bad, "Idempotency-Key": "k-1"}))

    assert resp.status_code == 400, resp.text
    assert task_store.rows == {}
    assert cb_queue == []
    assert len(respx_router.calls) == 0

    # 幂等键必须已回滚：同一个键换个合法地址应当能正常受理（否则会被误判成重放冲突）
    async with _client() as client:
        ok = await client.post(
            "/queue/v1/tasks", json={"model": "x"},
            headers=_headers(**{"X-Callback-Url": CB, "Idempotency-Key": "k-1"}))
    assert ok.status_code == 202, ok.text


async def test_illegal_callback_url_in_body_is_also_rejected(
    cb_settings, patch_redis, task_store, cb_queue, respx_router,
):
    """放在 body 里的非法地址同样 400——校验的是**生效地址**，与来自哪个通道无关。"""
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks",
            json={"model": "x",
                  "callback_url": "http://169.254.169.254/latest/meta-data/"},
            headers=_headers())

    assert resp.status_code == 400, resp.text
    assert task_store.rows == {}


# ---------------------------------------------------------------------------
# 透传模式
# ---------------------------------------------------------------------------


async def test_passthrough_mode_skips_extraction_and_validation(
    cb_settings, patch_redis, task_store, cb_queue, respx_router,
):
    """透传模式：不取值、不校验、不摘除——``callback_url`` 只是转发给上游的普通字段。

    这条是模式语义的关键：网关既然不投递，就**没有立场**判定该地址可信。此时用白名单
    拦住一个上游本来接受的地址，会把「透传」做成半透传（客户端按上游文档写的请求被
    网关 400 打回），而客户端完全无从得知原因。
    """
    cb_settings.callback_passthrough_upstream = True
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks",
            json={"model": "x", "callback_url": "https://anywhere.example/hook"},
            headers=_headers())

    assert resp.status_code == 202, resp.text
    data = task_store.rows[resp.json()["task_id"]]["data"]
    assert "callback_url" not in data        # 网关不接管，故不落键（终态不投递）
    assert "submit_body" not in data         # 转发体不改写，原样送上游
