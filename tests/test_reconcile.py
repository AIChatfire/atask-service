"""不亏本三窟窿测试（[8]）：孤儿任务收口 / 反向对账告警 / 失败单计费策略 /
上游取消端点尽力止损。"""

from __future__ import annotations

import time

import httpx

from app.schemas import FAILURE, QUEUED, SUBMITTED
from app.services import flow, tokensession
from app.services.reconcile import sweep_once
from app.services.registry import registry

_CHANNEL = {
    "id": 7, "name": "minimax-main", "base_url": "http://upstream.test",
    "setting": {"gateway": {
        "biz": "minimax",
        "submit_path": "/v2/video_generation",
        "probe_path": "/v2/query/video_generation/{upstream_task_id}",
        "cancel_path": "/v2/cancel/{upstream_task_id}",
        "status_path": "task.status",
        "result_path": "task.content.url",
        "billing": {"rule": "def calulate(request):\n    return 0.13"},
    }},
}

_SELECT = {
    "code": 0, "message": "ok",
    "data": {
        "channel_id": 7, "key_index": 0, "key": "sk-upstream-key",
        "base_url": "http://upstream.test", "epoch": "e1", "channel": _CHANNEL,
    },
}


def _seed(task_store, **overrides) -> str:
    task_id = "r" + "4" * 31
    now = int(time.time())
    row = {
        "task_id": task_id, "platform": "gateway", "action": "video",
        "status": QUEUED, "progress": "0%", "fail_reason": "",
        "data": {
            "biz": "minimax", "model": "MiniMax-H3", "token_hash": "h",
            "callback_url": None, "freeze_amount": 0.13, "settled": False,
            "key_id": 7, "request_body": {"model": "MiniMax-H3", "duration": 5},
            "upstream_task_id": "mm-1",
        },
        "user_id": 7, "channel_id": 7,
        "submit_time": now, "created_at": now, "updated_at": now, "finish_time": 0,
    }
    for key_path, value in overrides.items():
        target = row
        parts = key_path.split("__")
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
    task_store.rows[task_id] = row
    return task_id


# ---------------------------------------------------------------------------
# 孤儿任务收口
# ---------------------------------------------------------------------------


async def test_orphan_task_closed_and_refunded(test_settings, patch_redis,
                                               task_store, queue_events):
    """非终态且无 upstream_task_id 超宽限期 → FAILURE + cancel 解冻。"""
    task_id = _seed(task_store, status=SUBMITTED,
                    data__upstream_task_id=None,
                    created_at=int(time.time()) - 2000)  # 超 orphan_grace 1800s
    await tokensession.store(task_id, "sk-user-1")

    await sweep_once()

    row = task_store.rows[task_id]
    assert row["status"] == FAILURE
    assert "orphan" in row["fail_reason"]
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]


async def test_orphan_closeout_ignores_failed_charge_policy(
    test_settings, patch_redis, task_store, queue_events, route_factory,
):
    """孤儿收口（上游从未接单）：即使渠道配 failed_billing=charge 也一律解冻——
    charge 只覆盖生成失败，不覆盖提交未接单（KI1 计费口径回归）。"""
    registry._cache.clear()
    registry.remember(route_factory(failed_billing="charge"))
    task_id = _seed(task_store, status=SUBMITTED,
                    data__upstream_task_id=None,
                    created_at=int(time.time()) - 2000)
    await tokensession.store(task_id, "sk-user-1")

    await sweep_once()

    row = task_store.rows[task_id]
    assert row["status"] == FAILURE
    assert "orphan" in row["fail_reason"]
    assert queue_events["settle"] == []
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
    registry._cache.clear()


async def test_young_task_not_treated_as_orphan(test_settings, patch_redis,
                                                task_store, queue_events):
    """宽限期内的在途任务（submit 可能刚发出）不误杀。"""
    _seed(task_store, status=SUBMITTED, data__upstream_task_id=None,
          created_at=int(time.time()))
    await sweep_once()
    row = next(iter(task_store.rows.values()))
    assert row["status"] == SUBMITTED
    assert queue_events["cancel"] == []


async def test_stale_submitted_task_resubmitted(test_settings, patch_redis,
                                                task_store, queue_events):
    """异步提交事件丢失（worker 崩溃/Redis 故障）：stale 的 SUBMITTED 任务由
    sweep 补投提交事件（而非探测）；未到孤儿宽限期不判死、不解冻。"""
    aged = int(time.time()) - 400         # 超 task_stale_seconds(300) 未及 orphan_grace(1800)
    task_id = _seed(task_store, status=SUBMITTED, data__upstream_task_id=None,
                    created_at=aged, updated_at=aged)

    await sweep_once()

    row = task_store.rows[task_id]
    assert row["status"] == SUBMITTED                  # 不误杀
    assert queue_events["submit"] == [task_id]         # 补投提交
    assert queue_events["poll"] == []                  # 无上游 id 不排探测
    assert queue_events["cancel"] == []


async def test_sweep_skips_resubmit_when_submit_in_flight(test_settings, patch_redis,
                                                          task_store, queue_events):
    """KI2：stale SUBMITTED 补投前查锁——锁在 = 有在飞提交，本轮让路，
    避免补投与在飞提交并发双建（锁 TTL 动态派生之外的第二道防线）。"""
    from app.redis import K_SUBMIT_LOCK

    aged = int(time.time()) - 400
    task_id = _seed(task_store, status=SUBMITTED, data__upstream_task_id=None,
                    created_at=aged, updated_at=aged)
    await patch_redis.set(K_SUBMIT_LOCK.format(task_id=task_id), "1", ex=300)

    await sweep_once()

    assert queue_events["submit"] == []                # 不补投（让路在飞提交）
    assert queue_events["poll"] == []
    assert queue_events["cancel"] == []
    assert task_store.rows[task_id]["status"] == SUBMITTED


# ---------------------------------------------------------------------------
# 反向对账
# ---------------------------------------------------------------------------


async def test_reverse_reconcile_alerts_upstream_success(
    respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """本地 FAILURE/已退 但上游 SUCCESS → 台账标记 + 告警。"""
    task_id = _seed(task_store, status=FAILURE,
                    data__settled=True, data__settled_amount=0,
                    finish_time=int(time.time()))
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT)
    )
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(200, json={"task": {"status": "succeeded"}})
    )

    await sweep_once()

    data = task_store.rows[task_id]["data"]
    assert data["reconcile_alert"] == "upstream_succeeded_but_local_failure"
    assert data["reconciled"] is True


async def test_reverse_reconcile_clean_when_upstream_failed(
    respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """上游也失败：账面一致，标记 reconciled 不再复查，无告警。"""
    task_id = _seed(task_store, status=FAILURE,
                    data__settled=True, data__settled_amount=0,
                    finish_time=int(time.time()))
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT)
    )
    respx_router.get("http://upstream.test/v2/query/video_generation/mm-1").mock(
        return_value=httpx.Response(200, json={"task": {"status": "failed"}})
    )

    await sweep_once()

    data = task_store.rows[task_id]["data"]
    assert data.get("reconciled") is True
    assert "reconcile_alert" not in data


# ---------------------------------------------------------------------------
# 失败单计费策略（failed_billing: charge）
# ---------------------------------------------------------------------------


async def test_failed_task_charge_policy_settles_actual(
    test_settings, patch_redis, task_store, queue_events, route_factory,
):
    """failed_billing=charge：失败单先查 actual_amount_path 实收结算（不退款）。"""
    route = route_factory(failed_billing="charge", actual_amount_path="bill.amount")
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")
    task = task_store.rows[task_id]

    await flow.finalize_task(task, FAILURE, {"bill": {"amount": 0.05}}, route=route)

    assert queue_events["cancel"] == []
    settle = queue_events["settle"][0]
    assert settle["request_id"] == task_id
    assert settle["actual_amount"] == 0.05
    assert settle["attrs"]["failed_charge"] is True


async def test_failed_task_default_absorb_refunds(
    test_settings, patch_redis, task_store, queue_events, route_factory,
):
    """默认 absorb：失败全额解冻（现状纪律回归）。"""
    route = route_factory()
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")
    task = task_store.rows[task_id]

    await flow.finalize_task(task, FAILURE, {}, route=route)

    assert queue_events["settle"] == []
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]


# ---------------------------------------------------------------------------
# 上游取消端点（cancel_path）
# ---------------------------------------------------------------------------


async def test_cancel_task_attempts_upstream_cancel(
    respx_router, test_settings, patch_redis, task_store, queue_events,
    route_factory,
):
    """用户取消：本地收口后尽力调上游 cancel_path 源头止损。"""
    registry._cache.clear()
    registry.remember(route_factory(cancel_path="/v2/cancel/{upstream_task_id}"))
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT)
    )
    cancel = respx_router.post("http://upstream.test/v2/cancel/mm-1").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")

    view = await flow.cancel_task(task_id)

    assert view["status"] == "CANCELED"
    assert cancel.calls, "渠道配了 cancel_path 必须尽力调上游取消"
    registry._cache.clear()


async def test_cancel_task_without_cancel_path_skips(
    test_settings, patch_redis, task_store, queue_events, route_factory,
):
    """渠道未配 cancel_path：零出站调用（现状回归）。"""
    registry._cache.clear()
    registry.remember(route_factory())       # cancel_path 缺省 ""
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")

    view = await flow.cancel_task(task_id)

    assert view["status"] == "CANCELED"
    registry._cache.clear()
