"""``/batch`` 提交的失败三档（ADR-010 把旧五级分流降为三档）。

``errclass.py`` 随旧链路删除是合理的——前提是三档逻辑真的内联实现且可测。本文件
就是那三个「前提」的断言：

| 上游结果 | 期望 | 并发槽 | 令牌会话 | 任务状态 |
|---|---|---|---|---|
| 4xx（确定性拒绝） | FAILURE，不重试 | 释放 | 清 | 终态 FAILURE |
| 5xx / 传输错误（模糊失败） | 抛 ``RelayError`` 交 queue 退避重试 | **不释放** | **保留** | **留活**（非终态） |
| 2xx 但缺 id（约定被违反） | FAILURE，立即可见 | 释放 | 清 | 终态 FAILURE |

「留活」是核心：5xx 时上游可能已接单，判死会放过真实在跑的单。
"""

from __future__ import annotations

import hashlib

import httpx
import pytest

from app.main import app
from app.services import relay, relayflow, tokensession

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"
TOKEN_HASH = hashlib.sha256(b"sk-user-1").hexdigest()
CONC_KEY = f"gw:conc:{TOKEN_HASH}"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


@pytest.fixture
def batch_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    return settings


@pytest.fixture
def batch_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.queue as q

    async def _noop(*_a, **_k) -> None:
        return None

    monkeypatch.setattr(q, "publish_batch_submit", _noop)


async def _seed(client: httpx.AsyncClient) -> str:
    resp = await client.post("/batch/v1/tasks", json={"model": "m"},
                             headers={**AUTH, "X-Upstream-Base-Url": UP_BASE})
    assert resp.status_code == 202, resp.text
    return resp.json()["task_id"]


# ---------------------------------------------------------------------------
# 档 1：4xx 确定性拒绝 → FAILURE + 释槽 + 清会话
# ---------------------------------------------------------------------------


async def test_4xx_is_failure_releases_slot_and_clears_session(
    batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(400, json={"error": "bad request"})
    )
    async with _client() as client:
        task_id = await _seed(client)
        assert await patch_redis.get(CONC_KEY) == "1"
        assert await tokensession.get(task_id) == "sk-user-1"

    await relayflow.submit_batch_task(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert "upstream 400" in row["fail_reason"]
    assert await patch_redis.get(CONC_KEY) == "0"          # 释槽
    assert await tokensession.get(task_id) is None          # 清会话
    assert "upstream_snapshot" not in row["data"]


# ---------------------------------------------------------------------------
# 档 2：5xx / 传输错误 → 模糊失败，留活重试
# ---------------------------------------------------------------------------


async def test_5xx_is_ambiguous_keeps_task_alive_and_slot_held(
    batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(503, json={"error": "upstream busy"})
    )
    async with _client() as client:
        task_id = await _seed(client)

    with pytest.raises(relay.RelayError) as exc:
        await relayflow.submit_batch_task(task_id)

    assert exc.value.status == 503
    row = task_store.rows[task_id]
    assert row["status"] == "SUBMITTED"                    # 不判死
    assert row["fail_reason"] == ""
    assert await patch_redis.get(CONC_KEY) == "1"          # 槽不释放
    assert await tokensession.get(task_id) == "sk-user-1"   # 会话保留（重试还要用）


async def test_transport_error_is_599_ambiguous_keeps_task_alive(
    batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    respx_router.post(f"{UP_BASE}/v1/tasks").mock(side_effect=httpx.ConnectError("boom"))
    async with _client() as client:
        task_id = await _seed(client)

    with pytest.raises(relay.RelayError) as exc:
        await relayflow.submit_batch_task(task_id)

    assert exc.value.status == 599
    assert task_store.rows[task_id]["status"] == "SUBMITTED"
    assert await patch_redis.get(CONC_KEY) == "1"


# ---------------------------------------------------------------------------
# 档 3：2xx 缺 id → FAILURE（约定被违反，不静默挂起）
# ---------------------------------------------------------------------------


async def test_2xx_missing_id_is_failure_releases_slot_and_clears_session(
    batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(200, json={"status": "queued"})   # 无 id/task_id
    )
    async with _client() as client:
        task_id = await _seed(client)

    await relayflow.submit_batch_task(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert "missing task id" in row["fail_reason"]
    assert await patch_redis.get(CONC_KEY) == "0"
    assert await tokensession.get(task_id) is None


# ---------------------------------------------------------------------------
# 对照：2xx 有 id → QUEUED，槽与会话都保留（任务仍在跑）
# ---------------------------------------------------------------------------


async def test_2xx_with_id_queues_and_keeps_slot(
    batch_settings, patch_redis, task_store, batch_queue, respx_router,
):
    respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(200, json={"id": "up-9", "status": "queued"})
    )
    async with _client() as client:
        task_id = await _seed(client)

    await relayflow.submit_batch_task(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == "QUEUED"
    assert row["data"]["upstream_task_id"] == "up-9"
    assert await patch_redis.get(CONC_KEY) == "1"
    assert await tokensession.get(task_id) == "sk-user-1"
