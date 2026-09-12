"""``/queue`` 中继链路端到端（ADR-010）：受理 / 视图 / 取消 / 免费透传 / worker 提交。

用 ASGITransport 跑真实 app，出站由 respx 拦截，Redis / tasks 表走内存替身。
重点断言 ADR-010 的三条硬不变量：

1. 受理**请求内零上游往返**（提交交 worker）；
2. ``tasks.data`` **没有 freeze_amount / settled**（网关零资金动作）；
3. 明文用户 token **不落库**（只进 Redis 会话）。
"""

from __future__ import annotations

import hashlib
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from app.main import app
from app.services import relayflow, tokensession

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Upstream-Base-Url": UP_BASE, **extra}


@pytest.fixture
def queue_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    monkeypatch.setattr(settings, "queue_deny_prefixes", "/api/,/console/")
    return settings


@pytest.fixture
def queue_queue(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """拦截 ``queue.publish_queue_submit``（不触真 broker），记录 task_id。"""
    import app.queue as q

    events: dict[str, list] = {"submit": []}

    async def _publish(task_id: str) -> None:
        events["submit"].append(task_id)

    monkeypatch.setattr(q, "publish_queue_submit", AsyncMock(side_effect=_publish))
    return events


# ---------------------------------------------------------------------------
# 受理
# ---------------------------------------------------------------------------


async def test_create_returns_202_location_and_zero_upstream_roundtrip(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    async with _client() as client:
        resp = await client.post("/queue/v1/tasks", json={"model": "MiniMax-H3"},
                                 headers=_headers())

    assert resp.status_code == 202, resp.text
    view = resp.json()
    assert view["status"] == "SUBMITTED"
    task_id = view["task_id"]
    assert task_id.startswith("queue_")
    assert resp.headers["location"] == f"/queue/v1/tasks/{task_id}"
    assert len(respx_router.calls) == 0              # 请求内零上游往返
    assert queue_queue["submit"] == [task_id]        # 提交交给 worker

    row = task_store.rows[task_id]
    assert row["action"] == "task" and row["status"] == "SUBMITTED"
    data = row["data"]
    assert data["source"] == "queue"
    assert data["model"] == "MiniMax-H3"
    assert data["request_method"] == "POST"
    assert data["request_path"] == "/v1/tasks"
    assert data["request_query"] == ""
    assert data["upstream_base_url"] == UP_BASE
    assert data["token_hash"] == hashlib.sha256(b"sk-user-1").hexdigest()

    # ADR-010 §3：网关零资金动作，data 里没有资金字段
    assert "freeze_amount" not in data
    assert "settled" not in data
    # 红线：明文 token 不落库（只有 hash）
    assert "sk-user-1" not in json.dumps(row, ensure_ascii=False)
    # 令牌只进 Redis 会话
    assert await tokensession.get(task_id) == "sk-user-1"


@pytest.mark.parametrize("path", ["/queue/api/models", "/queue/console/tasks"])
async def test_deny_prefixes_are_rejected(
    path, queue_settings, patch_redis, task_store, queue_queue,
):
    async with _client() as client:
        resp = await client.post(path, json={"model": "m"}, headers=_headers())
    assert resp.status_code == 403
    assert not task_store.rows


async def test_empty_upstream_base_is_400(queue_settings, patch_redis, task_store, queue_queue):
    queue_settings.upstream_base_url = ""
    async with _client() as client:
        resp = await client.post("/queue/v1/tasks", json={"model": "m"}, headers=AUTH)
    assert resp.status_code == 400
    assert not task_store.rows


async def test_host_outside_allowlist_is_400(queue_settings, patch_redis, task_store,
                                             queue_queue):
    async with _client() as client:
        resp = await client.post("/queue/v1/tasks", json={"model": "m"},
                                 headers={**AUTH, "X-Upstream-Base-Url": "http://evil.example"})
    assert resp.status_code == 400
    assert not task_store.rows


async def test_empty_allowlist_fails_closed(queue_settings, patch_redis, task_store,
                                            queue_queue):
    queue_settings.upstream_allowlist = ""
    async with _client() as client:
        resp = await client.post("/queue/v1/tasks", json={"model": "m"}, headers=_headers())
    assert resp.status_code == 400
    assert not task_store.rows


async def test_missing_token_is_401(queue_settings, patch_redis, task_store, queue_queue):
    async with _client() as client:
        resp = await client.post("/queue/v1/tasks", json={"model": "m"},
                                 headers={"X-Upstream-Base-Url": UP_BASE})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# 视图
# ---------------------------------------------------------------------------


async def test_view_non_terminal_probes_and_rewrites_upstream_id(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m"},
                                     headers=_headers())).json()["task_id"]
        await task_store.patch_data(task_id, {"upstream_task_id": "up-1"}, status="QUEUED")
        probe = respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
            return_value=httpx.Response(200, json={"id": "up-1", "status": "processing"})
        )
        got = await client.get(f"/queue/v1/tasks/{task_id}")

    assert got.status_code == 200
    assert probe.calls                                  # 非终态走探测
    body = got.json()
    assert body["id"] == task_id                        # 上游 id 改写回本地 id
    assert body["status"] == "processing"               # 上游原话
    assert "up-1" not in got.text
    assert task_store.rows[task_id]["status"] == "IN_PROGRESS"
    assert task_store.rows[task_id]["data"]["upstream_status"] == "processing"


async def test_view_terminal_has_zero_upstream_roundtrip(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m"},
                                     headers=_headers())).json()["task_id"]
        await task_store.patch_data(
            task_id, {"upstream_task_id": "up-1", "upstream_status": "succeeded"},
            status="SUCCESS")
        got = await client.get(f"/queue/v1/tasks/{task_id}")

    assert got.status_code == 200
    assert len(respx_router.calls) == 0                 # 终态零上游往返
    assert got.json() == {"task_id": task_id, "status": "succeeded"}


async def test_view_before_submit_returns_local_queued(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m"},
                                     headers=_headers())).json()["task_id"]
        got = await client.get(f"/queue/v1/tasks/{task_id}")

    assert got.status_code == 200
    assert len(respx_router.calls) == 0                 # 还没上游 id，不问上游
    assert got.json() == {"task_id": task_id, "status": "queued"}


async def test_view_unknown_task_is_404(queue_settings, patch_redis, task_store):
    async with _client() as client:
        got = await client.get("/queue/v1/tasks/task_" + "0" * 32)
    assert got.status_code == 404


# ---------------------------------------------------------------------------
# 取消
# ---------------------------------------------------------------------------


async def test_cancel_sets_canceled_and_best_effort_upstream_delete(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    delete = respx_router.delete(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m"},
                                     headers=_headers())).json()["task_id"]
        await task_store.patch_data(task_id, {"upstream_task_id": "up-1"}, status="QUEUED")
        resp = await client.delete(f"/queue/v1/tasks/{task_id}")

    assert resp.status_code == 200
    assert task_store.rows[task_id]["status"] == "CANCELED"
    assert resp.json() == {"task_id": task_id, "status": "canceled"}
    assert delete.calls                                  # 尽力源头止损


async def test_cancel_still_local_when_upstream_delete_fails(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    respx_router.delete(f"{UP_BASE}/v1/tasks/up-1").mock(
        side_effect=httpx.ConnectError("boom")
    )
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m"},
                                     headers=_headers())).json()["task_id"]
        await task_store.patch_data(task_id, {"upstream_task_id": "up-1"}, status="QUEUED")
        resp = await client.delete(f"/queue/v1/tasks/{task_id}")

    assert resp.status_code == 200
    assert task_store.rows[task_id]["status"] == "CANCELED"    # 上游失败不影响本地结果


async def test_cancel_clears_token_session_after_upstream_stop_loss(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    """取消是终态：必须清令牌会话，**且必须在尽力止损之后清**。

    回归锁（本仓库 ADR-012 已知限制 ②）：`cancel_queue_task` 不复用 `_finalize_queue`，
    原先漏掉这一步，明文 sk 会一直留到 `SK_SESSION_TTL_SECONDS`（48h）才被 Redis 回收，
    违背「终态即清」纪律。

    顺序也不能反过来：`_best_effort_upstream_cancel` 要用会话里的 sk 去发上游 `DELETE`，
    先清会话会让它走 `if not token: return` **静默失效**——所以本条用例同时断言
    「上游确实收到了那次 DELETE」，把清会话的**位置**一并钉住。
    """
    delete = respx_router.delete(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m"},
                                     headers=_headers())).json()["task_id"]
        await task_store.patch_data(task_id, {"upstream_task_id": "up-1"}, status="QUEUED")
        assert await tokensession.get(task_id) == "sk-user-1"    # 受理时已暂存

        resp = await client.delete(f"/queue/v1/tasks/{task_id}")

    assert resp.status_code == 200, resp.text
    assert delete.calls                                          # 顺序：先止损
    assert await tokensession.session_info(task_id) == {"exists": False, "ttl_seconds": -2}


# ---------------------------------------------------------------------------
# 免费透传（GET 末段不是本地 task_id）
# ---------------------------------------------------------------------------


async def test_free_get_passthrough_does_not_create_task_row(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    route = respx_router.get(f"{UP_BASE}/v1/models").mock(
        return_value=httpx.Response(200, json={"data": [{"id": "m"}]})
    )
    async with _client() as client:
        resp = await client.get("/queue/v1/models", headers=_headers())

    assert resp.status_code == 200
    assert resp.json() == {"data": [{"id": "m"}]}
    assert route.calls
    assert not task_store.rows                           # 免费 GET 不落 tasks 行
    assert route.calls[0].request.headers["authorization"] == "Bearer sk-user-1"


async def test_free_get_passthrough_preserves_upstream_content_type(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    """免费转发可能是图片/二进制产物：必须透传上游 Content-Type，不硬写 JSON。"""
    respx_router.get(f"{UP_BASE}/v1/assets/cover.png").mock(
        return_value=httpx.Response(200, content=b"\x89PNG",
                                    headers={"content-type": "image/png"})
    )
    async with _client() as client:
        resp = await client.get("/queue/v1/assets/cover.png", headers=_headers())

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/png"
    assert resp.content == b"\x89PNG"
    assert not task_store.rows


# ---------------------------------------------------------------------------
# worker 提交
# ---------------------------------------------------------------------------


async def test_worker_submit_forwards_body_and_backfills_upstream_id(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    submit = respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(200, json={"id": "up-9", "status": "queued"})
    )
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m1"},
                                     headers=_headers())).json()["task_id"]

    await relayflow.submit_queue_task(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == "QUEUED"
    assert row["data"]["upstream_task_id"] == "up-9"
    assert row["data"]["upstream_status"] == "queued"
    request = submit.calls[0].request
    assert request.headers["authorization"] == "Bearer sk-user-1"   # token 原样透传
    assert json.loads(request.content) == {"model": "m1"}


async def test_worker_submit_terminal_response_advances_locally(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(200, json={"id": "up-9", "status": "succeeded"})
    )
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m1"},
                                     headers=_headers())).json()["task_id"]

    await relayflow.submit_queue_task(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == "SUCCESS"
    assert row["data"]["upstream_task_id"] == "up-9"


async def test_worker_submit_missing_upstream_id_is_failure_not_stuck(
    queue_settings, patch_redis, task_store, queue_queue, respx_router,
):
    """约定被违反（2xx 但无 id/task_id）：立即可见 FAILURE，不静默挂在 QUEUED。"""
    respx_router.post(f"{UP_BASE}/v1/tasks").mock(
        return_value=httpx.Response(200, json={"status": "queued"})
    )
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m1"},
                                     headers=_headers())).json()["task_id"]

    await relayflow.submit_queue_task(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert "missing task id" in row["fail_reason"]


async def test_worker_submit_success_must_not_resurrect_canceled_task(
    queue_settings, patch_redis, task_store, queue_queue, monkeypatch,
):
    """回填守卫的回归锁（旧链路 KI-D；ADR-010 重写时丢失过，别再丢）。

    窗口：**上游提交请求在飞期间**用户取消（最长 ``RELAY_TIMEOUT_SECONDS``）。
    上游仍可能接单、且已在上游 relay 计费，但网关**终态不可逆**——回填必须走带
    起点的 CAS：抢不到就只把孤儿 upstream id 记进 data 供运维追溯，绝不把
    CANCELED 复活成 QUEUED。复活的具体危害（本用例的负例）：
    ``QUEUED + progress=100% + finish_time 已写`` 的自相矛盾行 + 并发槽已在取消
    时释放 + sweep 随后会给一个已被取消的任务投递「成功」回调。

    注：取消发生在**提交开始之前**是另一条既有守卫（``submit_queue_task`` 首检
    ``status not in ACTIVE`` 直接返回），不在本用例窗口内。
    """
    async with _client() as client:
        task_id = (await client.post("/queue/v1/tasks", json={"model": "m1"},
                                     headers=_headers())).json()["task_id"]

    async def fake_call_upstream(method: str, base: str, path: str, **kw):
        if method == "DELETE":                      # 取消的尽力止损：上游接受
            return 204, b"", "application/json"
        # 上游已接单，但这段在飞窗口里用户先取消了
        await relayflow.cancel_queue_task(task_id)
        return (200, json.dumps({"id": "up-9", "status": "queued"}).encode(),
                "application/json")

    monkeypatch.setattr(relayflow.relay, "call_upstream", fake_call_upstream)
    await relayflow.submit_queue_task(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == "CANCELED"                  # 终态没被复活
    assert row["progress"] == "100%"                    # 取消写的终态口径不被改坏
    assert row["data"]["upstream_task_id"] == "up-9"    # 孤儿上游单可追溯
    assert row["data"]["upstream_status"] == "queued"
