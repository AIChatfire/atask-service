"""令牌会话（task_id → 用户令牌查询处）与 ops 任务诊断端点测试。

背景：new-api 渠道侧轮询任务状态不带用户 sk，终态 settle/cancel 的用户令牌
只能按 task_id 从 Redis 令牌会话查询取用（app.services.tokensession）；
``GET /ops/tasks/{task_id}`` 提供排障诊断视图——只暴露会话存在性与 TTL，
令牌本体绝不出 Redis。
"""

from __future__ import annotations

import json

import httpx
import pytest

from app.main import app
from app.services import tokensession

CHANNEL = {
    "id": 7, "name": "minimax-main", "base_url": "http://upstream.test",
    "setting": {
        "gateway": {
            "biz": "minimax",
            "submit_path": "/v2/video_generation",
            "probe_path": "/v2/query/video_generation/{upstream_task_id}",
            "status_path": "task.status",
            "billing": {"rule": "def calulate(request):\n    return 0.13", "type": "second"},
        },
    },
}

BODY = {"model": "MiniMax-H3", "duration": 5}


# ---------------------------------------------------------------------------
# tokensession（taskid → token 查询处）
# ---------------------------------------------------------------------------


async def test_tokensession_roundtrip(patch_redis):
    await tokensession.store("t-1", "sk-user-secret")

    assert await tokensession.get("t-1") == "sk-user-secret"
    info = await tokensession.session_info("t-1")
    assert info["exists"] is True and info["ttl_seconds"] > 0

    await tokensession.clear("t-1")
    assert await tokensession.get("t-1") is None
    info = await tokensession.session_info("t-1")
    assert info["exists"] is False and info["ttl_seconds"] == -2


async def test_tokensession_missing(patch_redis):
    assert await tokensession.get("t-none") is None
    assert (await tokensession.session_info("t-none"))["exists"] is False


# ---------------------------------------------------------------------------
# ops 任务诊断端点
# ---------------------------------------------------------------------------


@pytest.fixture
def diag_mocks(respx_router):
    m = type("Mocks", (), {})()
    m.inspect = respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 42, "token_id": 9})
    )
    m.freeze = respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}})
    )
    m.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json={
            "code": 0, "message": "ok",
            "data": {
                "channel_id": 7, "key_index": 1, "key": "sk-minimax-real",
                "base_url": "http://upstream.test", "epoch": "e1",
                "channel": CHANNEL,
            },
        })
    )
    m.report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    m.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "up-1"})
    )
    return m


async def _submit(client) -> str:
    from app.services.submit import submit_one

    resp = await client.post(
        "/minimax/v1/tasks", json=BODY,
        headers={"Authorization": "Bearer sk-user-42"},
    )
    assert resp.status_code == 202, resp.text
    task_id = resp.json()["task_id"]
    await submit_one(task_id)          # 异步提交架构：驱动 worker 侧提交
    return task_id


async def test_ops_task_diagnostics(
    diag_mocks, test_settings, patch_redis, task_store, queue_events, monkeypatch,
):
    """诊断视图：内部字段 + 令牌会话存在性/TTL；令牌本体绝不外泄。"""
    monkeypatch.setattr(test_settings, "admin_token", "adm-token")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        task_id = await _submit(client)

        # 未带/错误 admin token → 401
        assert (await client.get(f"/ops/tasks/{task_id}")).status_code == 401
        bad = await client.get(f"/ops/tasks/{task_id}", headers={"X-Admin-Token": "nope"})
        assert bad.status_code == 401

        resp = await client.get(f"/ops/tasks/{task_id}", headers={"X-Admin-Token": "adm-token"})
        assert resp.status_code == 200, resp.text
        view = resp.json()
        assert view["task_id"] == task_id and view["status"] == "QUEUED"
        assert view["user_id"] == 42 and view["channel_id"] == 7
        assert view["data"]["biz"] == "minimax"
        assert view["data"]["upstream_task_id"] == "up-1"
        assert view["data"]["freeze_amount"] == pytest.approx(0.13)
        # 令牌会话：只暴露存在性与 TTL，响应任何角落都不含令牌本体
        assert view["token_session"]["exists"] is True
        assert view["token_session"]["ttl_seconds"] > 0
        assert "sk-user-42" not in json.dumps(view)

        # 未知任务 → 404
        nf = await client.get("/ops/tasks/t-none", headers={"X-Admin-Token": "adm-token"})
        assert nf.status_code == 404


async def test_ops_task_diagnostics_session_cleared_after_finalize(
    diag_mocks, test_settings, patch_redis, task_store, queue_events,
):
    """终态结算取用令牌后会话即清（诊断视图反映 exists=False）。"""
    from app.services import flow

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        task_id = await _submit(client)

    assert (await tokensession.session_info(task_id))["exists"] is True
    task = await task_store.get(task_id)
    await flow.finalize_task(task, "SUCCESS", {"task": {"status": "succeeded"}})
    assert len(queue_events["settle"]) == 1
    assert queue_events["settle"][0]["user_sk"] == "sk-user-42"
    assert (await tokensession.session_info(task_id))["exists"] is False


async def test_proxy_task_record_is_homogeneous(
    diag_mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """同构约定：计费透传落下的 tasks 行与 flow.create_task 同一份数据形态
    （biz 取渠道权威值、model/key_index/request_body 全量快照）。

    路径特意取**非** submit_path 的计费端点——渠道 submit_path 已被原生提交
    拦截（走 flow.create_task 异步受理），此处验的是其余 POST 的透传语义。
    """
    respx_router.post("http://upstream.test/v2/image_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "up-1"})
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        resp = await client.post(
            "/minimax/v2/image_generation", json=BODY,
            headers={"Authorization": "Bearer sk-user-42"},
        )
        assert resp.status_code == 200, resp.text

    assert len(task_store.rows) == 1
    task_id, row = next(iter(task_store.rows.items()))
    data = row["data"]
    assert row["action"] == "proxy" and row["status"] == "QUEUED"
    assert data["biz"] == "minimax"                       # 渠道权威 biz（非 URL 段）
    assert data["source"] == "proxy"
    assert data["model"] == "MiniMax-H3"
    assert data["key_id"] == 7 and data["key_index"] == 1
    assert data["request_body"]["duration"] == 5          # 结算重估基底
    assert data["proxy_path"] == "v2/image_generation"    # 透传形态记录原路径
    assert data["upstream_task_id"] == "up-1"
    assert data["freeze_amount"] == pytest.approx(0.13)
    assert queue_events["poll"][-1]["task_id"] == task_id  # 已接入探测闭环
