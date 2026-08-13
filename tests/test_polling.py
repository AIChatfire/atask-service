"""探测 worker 测试：租约钉回原渠道、状态推进、退避重投、超时收尾。"""

from __future__ import annotations

import time

import httpx

from app.schemas import FAILURE, IN_PROGRESS, QUEUED, SUCCESS
from app.services import tokensession
from app.services.polling import poll_one

KEYPOOL_SELECT = {
    "code": 0, "message": "ok",
    "data": {
        "channel_id": 7, "key_index": 0, "key": "sk-upstream-key",
        "base_url": "http://upstream.test", "epoch": "e1",
        "channel": {
            "id": 7, "base_url": "http://upstream.test",
            "setting": {"gateway": {
                "submit_path": "/v2/video_generation",
                "probe_path": "/v2/query/video_generation/{upstream_task_id}",
                "status_path": "task.status",
                "result_path": "task.content.url",
                "settle_usage_map": {"duration": "task.usage.output_seconds"},
            }},
        },
    },
}


def _seed(task_store, *, age: float = 10, status: str = QUEUED) -> str:
    task_id = "p" + "2" * 31
    row = {
        "task_id": task_id, "platform": "gateway", "action": "video",
        "status": status, "progress": "0%", "fail_reason": "",
        "data": {
            "biz": "minimax", "model": "MiniMax-H3", "token_hash": "h",
            "callback_url": None, "freeze_amount": 0.13, "settled": False,
            "key_id": 7, "request_body": {"model": "MiniMax-H3", "duration": 5},
            "upstream_task_id": "mm-1",
        },
        "user_id": 7, "channel_id": 7,
        "submit_time": int(time.time() - age), "created_at": int(time.time() - age),
        "updated_at": int(time.time() - age), "finish_time": 0,
    }
    task_store.rows[task_id] = row
    return task_id


def _mock_keypool(respx_router):
    return respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=KEYPOOL_SELECT)
    )


async def test_poll_terminal_task_skipped(respx_router, test_settings, patch_redis,
                                          task_store, queue_events):
    task_id = _seed(task_store, status=SUCCESS)
    await poll_one(task_id)
    assert not respx_router.calls              # 终态任务不产生任何出站请求
    assert queue_events["poll"] == []


async def test_poll_missing_upstream_id_skipped(test_settings, patch_redis,
                                                task_store, queue_events):
    task_id = _seed(task_store)
    task_store.rows[task_id]["data"]["upstream_task_id"] = None
    await poll_one(task_id)
    assert queue_events["poll"] == []


async def test_poll_lease_failure_reschedules(respx_router, test_settings, patch_redis,
                                              task_store, queue_events):
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(503, json={"code": 40001, "message": "no key"})
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]   # 下轮再来
    assert task_store.rows[task_id]["status"] == QUEUED                 # 状态不动


async def test_poll_probe_failure_reschedules(respx_router, test_settings, patch_redis,
                                              task_store, queue_events):
    _mock_keypool(respx_router)
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(500, text="boom")
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]
    assert task_store.rows[task_id]["status"] == QUEUED


async def test_poll_active_updates_and_reschedules(respx_router, test_settings, patch_redis,
                                                   task_store, queue_events):
    _mock_keypool(respx_router)
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(200, json={"task": {"status": "processing"}})
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    row = task_store.rows[task_id]
    assert row["status"] == IN_PROGRESS
    assert row["data"]["upstream_status"] == "processing"
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]


async def test_poll_unknown_status_reschedules(respx_router, test_settings, patch_redis,
                                               task_store, queue_events):
    _mock_keypool(respx_router)
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(200, json={"task": {"status": "flibberty"}})
    )
    task_id = _seed(task_store)
    await poll_one(task_id)
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]
    assert task_store.rows[task_id]["status"] == QUEUED


async def test_poll_terminal_success_finalizes(respx_router, test_settings, patch_redis,
                                               task_store, queue_events):
    _mock_keypool(respx_router)
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(200, json={
            "task": {"status": "succeeded",
                     "content": {"url": "http://cdn.test/v.mp4"},
                     "usage": {"output_seconds": 4}},
        })
    )
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")
    # 结算重估走真实 pricing provider？——这里定价服务挂 respx
    respx_router.get("http://pricing.test/v1/models/MiniMax-H3").mock(
        return_value=httpx.Response(200, json={
            "id": "MiniMax-H3", "status": 0, "discountRate": 1,
            "billing": {"rule": "def calulate(request):\n    return float(request.get('duration') or 5) * 0.026",
                        "type": "second", "price": []},
        })
    )
    await poll_one(task_id)
    row = task_store.rows[task_id]
    assert row["status"] == SUCCESS
    assert row["data"]["result"] == "http://cdn.test/v.mp4"
    assert queue_events["settle"][0]["actual_amount"] == 0.104
    assert queue_events["poll"] == []          # 终态不再排程


async def test_poll_age_exceeded_fails_task(respx_router, test_settings, patch_redis,
                                            task_store, queue_events, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "poll_max_age_seconds", 60)
    task_id = _seed(task_store, age=3600)
    await tokensession.store(task_id, "sk-user-1")
    await poll_one(task_id)
    row = task_store.rows[task_id]
    assert row["status"] == FAILURE
    assert "poll timeout" in row["fail_reason"]
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
    assert not respx_router.calls              # 超时直接收尾，不发出站请求
