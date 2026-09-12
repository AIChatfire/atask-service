"""提交体上限（``BODY_MAX_BYTES``）：防超大提交体把网关进程内存打爆。

覆盖三条硬断言：

1. 超限 → **413**，且**零副作用**：不落 ``tasks`` 行、不出站、不占幂等键、不占
   并发槽（被拒请求不留痕迹，否则污染幂等键并泄漏并发槽）；
2. **只靠 Content-Length 会被绕过**——构造 ``Content-Length`` 声称很小但实际体
   很大（以及无该头的 chunked 形态）的请求，仍必须在**读取过程中**被流式封顶拦下；
3. 边界不误杀：恰好等于上限放行，正常小请求行为不变。
"""

from __future__ import annotations

import hashlib

import httpx
import pytest

from app.main import app
from app.redis import K_CONC
from app.services import idem

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"
TOKEN_HASH = hashlib.sha256(b"sk-user-1").hexdigest()

#: 测试用的小上限（远小于默认 1 MiB，便于构造边界与超限体）
_LIMIT = 64


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Upstream-Base-Url": UP_BASE, **extra}


@pytest.fixture
def body_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "queue_deny_prefixes", "/api/,/console/")
    monkeypatch.setattr(settings, "body_max_bytes", _LIMIT)
    return settings


# ---------------------------------------------------------------------------
# 超限：413 且零副作用
# ---------------------------------------------------------------------------


async def test_oversized_body_rejected_with_zero_side_effects(
    body_settings, patch_redis, task_store, queue_events, respx_router,
):
    """Content-Length 已声明超限 → 快速拒绝；且不留下任何痕迹。"""
    key = "idem-oversize"
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks",
            content=b"x" * (_LIMIT + 1),
            headers=_headers(**{"Idempotency-Key": key}),
        )
        assert resp.status_code == 413, resp.text
        assert resp.json()["error"]["type"] == "invalid_request_error"
        assert not task_store.rows                              # 无 tasks 行落库
        assert len(respx_router.calls) == 0                     # 无上游出站
        assert await idem.get_task_id(TOKEN_HASH, key) is None  # 幂等键未被占位
        assert K_CONC.format(token_hash=TOKEN_HASH) not in patch_redis.dump()  # 未占并发槽

        # 同键随后发正常请求：若上一发曾占位幂等键，这里会被回放/409，而不是新建
        follow = await client.post(
            "/queue/v1/tasks",
            content=b'{"model": "m"}',
            headers=_headers(**{"Idempotency-Key": key}),
        )

    assert follow.status_code == 202, follow.text               # 同键正常请求应当成功
    assert len(task_store.rows) == 1


# ---------------------------------------------------------------------------
# Content-Length 不能作为唯一依据（本任务关键）
# ---------------------------------------------------------------------------


async def test_forged_small_content_length_cannot_bypass(
    body_settings, patch_redis, task_store, queue_events, respx_router,
):
    """``Content-Length`` 声称 10 字节、实际体远超上限 → 仍 413（流式封顶生效）。"""
    real = b"x" * (_LIMIT * 4)
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks",
            content=real,
            headers=_headers(**{"Content-Length": "10"}),
        )

    assert resp.status_code == 413, resp.text
    assert not task_store.rows
    assert len(respx_router.calls) == 0


async def test_missing_content_length_chunked_cannot_bypass(
    body_settings, patch_redis, task_store, queue_events, respx_router,
):
    """chunked（无 ``Content-Length``）：逐块累加超过上限即中止 → 413。"""

    async def chunks():
        for _ in range(10):          # 10 × 16 = 160 > 64，跨多块累加才暴露
            yield b"y" * 16

    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks", content=chunks(), headers=_headers(),
        )

    assert resp.status_code == 413, resp.text
    assert not task_store.rows
    assert len(respx_router.calls) == 0


# ---------------------------------------------------------------------------
# 边界与正常路径
# ---------------------------------------------------------------------------


async def test_body_exactly_at_limit_is_allowed(
    body_settings, patch_redis, task_store, queue_events, respx_router,
):
    """恰好等于上限 → 放行（边界不误杀）。"""
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks", content=b"z" * _LIMIT, headers=_headers(),
        )

    assert resp.status_code == 202, resp.text
    task_id = resp.json()["task_id"]
    assert task_store.rows[task_id]["data"]["request_body"] == "z" * _LIMIT


async def test_normal_small_request_unchanged(
    body_settings, patch_redis, task_store, queue_events, respx_router,
):
    """正常小请求仍 202，受理链路行为不变。"""
    async with _client() as client:
        resp = await client.post(
            "/queue/v1/tasks", json={"model": "MiniMax-H3"}, headers=_headers(),
        )

    assert resp.status_code == 202, resp.text
    view = resp.json()
    assert view["status"] == "SUBMITTED"
    row = task_store.rows[view["task_id"]]
    assert row["data"]["model"] == "MiniMax-H3"
    assert queue_events["queue_submit"] == [view["task_id"]]
    assert len(respx_router.calls) == 0
