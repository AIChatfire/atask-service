"""提交有限重试测试（[5]）：仅确定性拒绝（401/403/429，可配）换 key 重打；
模糊失败（超时/5xx/连接中断）绝不重试——防双重创建双扣费。

异步提交架构：HTTP 创建只落库 + 入队（202 立即返回本地 id），重打发生在
worker 侧 submit_one。纪律断言：全程只 freeze 一次；重打落到别的渠道时
tasks 行 channel_id 与 data.key_id/key_index 同步切换（探测钉回与对账口径
以实际渠道为准）。worker 重打矩阵的单测见 test_submit.py。
"""

from __future__ import annotations

import httpx
import pytest

from app.main import app
from app.services.submit import submit_one

BODY = {"model": "MiniMax-H3", "duration": 5}


def _channel(channel_id: int, key: str) -> dict:
    return {
        "code": 0, "message": "ok",
        "data": {
            "channel_id": channel_id, "key_index": 0, "key": key,
            "base_url": "http://upstream.test", "epoch": "e1",
            "channel": {
                "id": channel_id, "name": f"ch-{channel_id}",
                "base_url": "http://upstream.test",
                "setting": {"gateway": {
                    "biz": "minimax",
                    "submit_path": "/v2/video_generation",
                    "probe_path": "/v2/query/video_generation/{upstream_task_id}",
                    "status_path": "task.status",
                    "billing": {"rule": "def calulate(request):\n    return 0.13"},
                }},
            },
        },
    }


@pytest.fixture
def mocks(respx_router):
    m = type("Mocks", (), {})()
    m.inspect = respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 42, "token_id": 9})
    )
    m.freeze = respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}})
    )
    m.report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    return m


async def _create(client) -> str:
    """HTTP 创建（异步受理）：202 + 本地 task_id；请求内零上游调用。"""
    resp = await client.post(
        "/minimax/v1/tasks", json=BODY,
        headers={"Authorization": "Bearer sk-user-42"},
    )
    assert resp.status_code == 202, resp.text
    view = resp.json()
    assert view["status"] == "SUBMITTED"
    assert view["task_id"].startswith("minimax_")
    return view["task_id"]


async def test_key_level_rejected_retries_with_fresh_lease(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """首 key 401（key 级确定性拒绝）→ report 坏 key + 重新 lease 换渠道重打成功。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        side_effect=[
            httpx.Response(200, json=_channel(7, "sk-bad")),    # preflight 租约
            httpx.Response(200, json=_channel(7, "sk-bad")),    # worker 首打钉回
            httpx.Response(200, json=_channel(8, "sk-good")),   # 重打新鲜租约
        ]
    )
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        side_effect=[
            httpx.Response(401, text="invalid api key"),
            httpx.Response(200, json={"task_id": "up-9"}),
        ]
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        task_id = await _create(client)

    await submit_one(task_id)                                # worker 异步提交

    assert len(create.calls) == 2                        # 恰好重打一次
    assert len(mocks.freeze.calls) == 1                  # 只冻结一次（不重扣）
    row = task_store.rows[task_id]
    assert row["status"] == "QUEUED"
    assert row["channel_id"] == 8                        # 对账口径切到实际渠道
    assert row["data"]["key_id"] == 8                    # 探测钉回实际渠道
    assert row["data"]["upstream_task_id"] == "up-9"


async def test_ambiguous_failure_never_retried(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """5xx（模糊失败）绝不重试：创建照常 202，worker 只提交一次即 FAILURE + 解冻。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_channel(7, "sk-x"))
    )
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(500, text="boom")
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        task_id = await _create(client)

    await submit_one(task_id)

    assert len(create.calls) == 1                        # 不重试
    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-42"}]


async def test_non_retryable_4xx_fails_immediately(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """422 不在重试集合（默认 401/403/429）→ 一次失败即终。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_channel(7, "sk-x"))
    )
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(422, text="bad prompt")
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        task_id = await _create(client)

    await submit_one(task_id)

    assert len(create.calls) == 1
    assert task_store.rows[task_id]["status"] == "FAILURE"


async def test_retry_exhausted_after_max_attempts(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """连续确定性拒绝：重打到上限（3 次含首次）即止，FAILURE + cancel。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_channel(7, "sk-x"))
    )
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(401, text="invalid api key")
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        task_id = await _create(client)

    await submit_one(task_id)

    assert len(create.calls) == 3                        # 上限即止
    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-42"}]
    assert len(mocks.freeze.calls) == 1
