"""HELD 挂起测试（[6]）：submit 撞账户级故障 → 挂起保留冻结 + 202；
恢复后金丝雀排空（重新租约不钉渠道 + 重提交 + 进探测）；再撞退避；超限判死。"""

from __future__ import annotations

import time

import httpx
import pytest

from app.main import app
from app.services import tokensession
from app.services.held import resume_held_once
from app.services.reconcile import sweep_once

BODY = {"model": "MiniMax-H3", "duration": 5}

_CHANNEL = {
    "id": 7, "name": "minimax-main", "base_url": "http://upstream.test",
    "setting": {"gateway": {
        "biz": "minimax",
        "submit_path": "/v2/video_generation",
        "probe_path": "/v2/query/video_generation/{upstream_task_id}",
        "status_path": "task.status",
        "billing": {"rule": "def calulate(request):\n    return 0.13"},
        # 该渠道 403 = 欠费（账户级；默认表 403 是 key 级，必须显式配置）
        "error_classify": {"account_level": [403]},
    }},
}

_SELECT = {
    "code": 0, "message": "ok",
    "data": {
        "channel_id": 7, "key_index": 0, "key": "sk-upstream-key",
        "base_url": "http://upstream.test", "epoch": "e1", "channel": _CHANNEL,
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
    m.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT)
    )
    m.report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    return m


def _seed_held(task_store, **overrides) -> str:
    task_id = "h" + "6" * 31
    now = int(time.time())
    row = {
        "task_id": task_id, "platform": "gateway", "action": "video",
        "status": "HELD", "progress": "0%", "fail_reason": "account overdue",
        "data": {
            "biz": "minimax", "model": "MiniMax-H3", "token_hash": "h",
            "callback_url": None, "freeze_amount": 0.13, "settled": False,
            "key_id": 7, "request_body": dict(BODY),
            "freeze_expires_at": now + 1800,
        },
        "user_id": 7, "channel_id": 7,
        "submit_time": now, "created_at": now, "updated_at": now, "finish_time": 0,
    }
    row.update(overrides)
    task_store.rows[task_id] = row
    return task_id


async def test_submit_account_level_holds_task(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """submit 撞账户级（403 欠费）→ HELD：202 对外 queued、冻结保留、
    并发槽释放、resume 已调度、不上报坏 key（账户级 ≠ key 级）。"""
    mocks.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(403, text="account overdue")
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        resp = await client.post(
            "/minimax/v1/tasks", json=BODY,
            headers={"Authorization": "Bearer sk-user-42"},
        )

    assert resp.status_code == 202, resp.text
    assert resp.json()["status"] == "QUEUED"          # 对外不暴露 HELD
    task_id = resp.json()["task_id"]
    row = task_store.rows[task_id]
    assert row["status"] == "HELD"
    assert queue_events["cancel"] == []               # 冻结保留（不解冻）
    assert queue_events["resume_held"] == [{"delay": 60}]
    assert not mocks.report.calls                     # 账户级不上报 keypool
    assert len(mocks.freeze.calls) == 1


async def test_resume_held_success_rejoins_poll_loop(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """恢复排空：重新 lease（不钉渠道）→ 重提交 → QUEUED + 进探测闭环。"""
    mocks.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "up-7"})
    )
    task_id = _seed_held(task_store)

    await resume_held_once()

    row = task_store.rows[task_id]
    assert row["status"] == "QUEUED"
    assert row["data"]["upstream_task_id"] == "up-7"
    assert row["data"]["key_id"] == 7 and row["channel_id"] == 7
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]
    assert not queue_events["resume_held"]            # 无更多 HELD，不再自调度
    assert patch_redis._data.get("gw:conc:h") == "1"  # 并发槽已重新占用


async def test_resume_held_account_level_again_backoff(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """再撞账户级 → 退避重投（1m 档），held_attempts 升档，仍 HELD。"""
    mocks.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(403, text="account overdue")
    )
    task_id = _seed_held(task_store)

    await resume_held_once()

    row = task_store.rows[task_id]
    assert row["status"] == "HELD"
    assert row["data"]["held_attempts"] == 1
    assert queue_events["resume_held"] == [{"delay": 60}]
    assert queue_events["cancel"] == []               # 挂起期间冻结不动
    assert patch_redis._data.get("gw:conc:h") == "0"  # 并发槽已还回


async def test_held_expired_sweep_fails_task(
    mocks, test_settings, patch_redis, task_store, queue_events,
):
    """HELD 超 hold_max_age（4h）→ sweep 判死：FAILURE + cancel 解冻。"""
    task_id = _seed_held(task_store, updated_at=int(time.time()) - 5 * 3600)
    await tokensession.store(task_id, "sk-user-1")

    await sweep_once()

    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert "held timeout" in row["fail_reason"]
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
