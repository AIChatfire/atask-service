"""冻结续期测试（[7] 网关侧）：sweep 临期扫描 → renew 续期 / 400 止损 / 5xx 下轮再来。

 billing 侧 renew 语义（独立仓库已实现）：只推 expires_at 不动钱；
400 = 冻结已终态/超总量上限（非重试）；409/5xx 可重试；跨用户 403。
"""

from __future__ import annotations

import json
import time

import httpx

from app.schemas import FAILURE, QUEUED
from app.services import tokensession
from app.services.reconcile import sweep_once


def _seed(task_store, **data_overrides) -> str:
    task_id = "w" + "5" * 31
    now = int(time.time())
    data = {
        "biz": "minimax", "model": "MiniMax-H3", "token_hash": "h",
        "callback_url": None, "freeze_amount": 0.13, "settled": False,
        "key_id": 7, "request_body": {"model": "MiniMax-H3"},
        "upstream_task_id": "mm-1",
        "freeze_expires_at": now + 300,          # 临期（< margin 600s）
    }
    data.update(data_overrides)
    task_store.rows[task_id] = {
        "task_id": task_id, "platform": "gateway", "action": "video",
        "status": QUEUED, "progress": "0%", "fail_reason": "",
        "data": data, "user_id": 7, "channel_id": 7,
        "submit_time": now, "created_at": now, "updated_at": now, "finish_time": 0,
    }
    return task_id


async def test_expiring_freeze_renewed(respx_router, test_settings, patch_redis,
                                       task_store, queue_events):
    """临期冻结 → renew 成功 → data.freeze_expires_at 推后。"""
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")
    new_expiry = int(time.time()) + 1800
    renew = respx_router.post("http://billing.test/api/v1/billing/renew").mock(
        return_value=httpx.Response(200, json={"data": {"expires_at": new_expiry}})
    )

    await sweep_once()

    assert renew.calls
    req = renew.calls.last.request
    assert req.headers["Authorization"] == "Bearer sk-user-1"   # 用户令牌续期
    assert json.loads(req.content)["request_id"] == task_id
    assert task_store.rows[task_id]["data"]["freeze_expires_at"] == new_expiry
    assert task_store.rows[task_id]["status"] == QUEUED         # 任务不受影响


async def test_renew_400_fails_task(respx_router, test_settings, patch_redis,
                                    task_store, queue_events):
    """renew 400（冻结已终态，钱已被 billing sweeper 退用户）→ 任务 FAILURE 止损。"""
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")
    respx_router.post("http://billing.test/api/v1/billing/renew").mock(
        return_value=httpx.Response(400, json={"error": "freeze already settled"})
    )

    await sweep_once()

    row = task_store.rows[task_id]
    assert row["status"] == FAILURE
    assert "freeze renew rejected" in row["fail_reason"]


async def test_renew_5xx_keeps_task_active(respx_router, test_settings, patch_redis,
                                           task_store, queue_events):
    """renew 5xx（可重试）→ 任务不动，下轮再来。"""
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")
    respx_router.post("http://billing.test/api/v1/billing/renew").mock(
        return_value=httpx.Response(500, json={"error": "boom"})
    )

    await sweep_once()

    assert task_store.rows[task_id]["status"] == QUEUED


async def test_renew_skipped_without_token_session(respx_router, test_settings,
                                                   patch_redis, task_store, queue_events):
    """令牌会话丢失 → 跳过续期（freeze TTL 到期由 billing 自动解冻兜底）。"""
    _seed(task_store)
    renew = respx_router.post("http://billing.test/api/v1/billing/renew").mock(
        return_value=httpx.Response(200, json={"data": {"expires_at": 0}})
    )

    await sweep_once()

    assert not renew.calls


async def test_non_expiring_freeze_not_renewed(respx_router, test_settings,
                                               patch_redis, task_store, queue_events):
    """冻结尚远（> margin）不续期。"""
    _seed(task_store, freeze_expires_at=int(time.time()) + 3600)
    await tokensession.store(next(iter(task_store.rows)), "sk-user-1")
    renew = respx_router.post("http://billing.test/api/v1/billing/renew").mock(
        return_value=httpx.Response(200, json={"data": {"expires_at": 0}})
    )

    await sweep_once()

    assert not renew.calls
