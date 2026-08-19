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
from app.services.submit import submit_one

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


def _seed_held(task_store, task_id: str | None = None, **overrides) -> str:
    task_id = task_id or "h" + "6" * 31
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
    """submit 撞账户级（403 欠费）→ HELD：创建即时 202（SUBMITTED），worker 侧
    挂起保留冻结、对外 GET 映射 queued、并发槽释放、resume 已调度、不上报坏 key。"""
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
        assert resp.json()["status"] == "SUBMITTED"       # 落库即返，提交在 worker
        assert not mocks.create.calls                     # 请求内零上游调用
        task_id = resp.json()["task_id"]
        assert task_id.startswith("minimax_")             # biz 前缀

        await submit_one(task_id)                         # worker 异步提交

        got = await client.get(f"/minimax/v1/tasks/{task_id}")
        assert got.json()["status"] == "QUEUED"           # 对外不暴露 HELD

    row = task_store.rows[task_id]
    assert row["status"] == "HELD"
    assert row["data"]["held_reason"] == "account_level"
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


async def test_submit_rate_limited_holds_task(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """submit 全部重打撞 429 → HELD（held_reason=rate_limited）：创建即时 202，
    worker 侧挂起保留冻结、并发槽释放、按 5m 固定退避调度 resume、不上报坏 key。"""
    mocks.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(429, text="rate limit exceeded")
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        resp = await client.post(
            "/minimax/v1/tasks", json=BODY,
            headers={"Authorization": "Bearer sk-user-42"},
        )

    assert resp.status_code == 202, resp.text
    assert resp.json()["status"] == "SUBMITTED"       # 落库即返，提交在 worker
    task_id = resp.json()["task_id"]

    await submit_one(task_id)                         # worker 异步提交

    row = task_store.rows[task_id]
    assert row["status"] == "HELD"
    assert row["data"]["held_reason"] == "rate_limited"
    assert queue_events["cancel"] == []               # 冻结保留（不解冻）
    assert queue_events["resume_held"] == [{"delay": 300}]   # 固定 5m 退避
    assert not mocks.report.calls                     # 限流不上报 keypool
    assert len(mocks.freeze.calls) == 1


async def test_resume_held_rate_limited_fixed_backoff(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """限流挂起恢复再撞 429 → 固定 5m 退避（不走 1m→5m→15m 阶梯），仍 HELD。"""
    mocks.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(429, text="rate limit exceeded")
    )
    task_id = _seed_held(task_store)
    task_store.rows[task_id]["data"]["held_reason"] = "rate_limited"

    await resume_held_once()

    row = task_store.rows[task_id]
    assert row["status"] == "HELD"
    assert row["data"]["held_attempts"] == 1
    assert queue_events["resume_held"] == [{"delay": 300}]   # 固定 5m，非阶梯 60s
    assert queue_events["cancel"] == []
    assert patch_redis._data.get("gw:conc:h") == "0"  # 并发槽已还回


async def test_held_expired_ignores_failed_charge_policy(
    mocks, test_settings, patch_redis, task_store, queue_events, route_factory,
):
    """HELD 超龄判死（从未被上游接单）：即使渠道配 failed_billing=charge 也
    一律解冻（KI1 计费口径回归；charge 只覆盖生成失败）。"""
    from app.services.registry import registry

    registry._cache.clear()
    registry.remember(route_factory(failed_billing="charge"))
    task_id = _seed_held(task_store, updated_at=int(time.time()) - 5 * 3600)
    await tokensession.store(task_id, "sk-user-1")

    await sweep_once()

    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert "held timeout" in row["fail_reason"]
    assert queue_events["settle"] == []
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
    registry._cache.clear()


async def test_resume_held_task_level_reject_releases_slot_once(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """恢复重提交撞任务级拒绝（400）→ 判死 + 解冻；并发槽恰好释放一次——
    同用户另一任务的占用槽不得被双重 DECR 误还（回归：finalize 统一释槽）。"""
    mocks.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(400, text="content rejected")
    )
    task_id = _seed_held(task_store)
    await tokensession.store(task_id, "sk-user-1")
    patch_redis._data["gw:conc:h"] = "1"            # 同用户另一任务占 1 槽

    await resume_held_once()

    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
    assert patch_redis._data.get("gw:conc:h") == "1"    # 只还回自己的槽


async def test_resume_held_missing_task_id_releases_slot_once(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """恢复重提交 200 但提取不到上游 id → 判死 + 解冻；并发槽同样恰好释放一次。"""
    mocks.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"unexpected": "shape"})
    )
    task_id = _seed_held(task_store)
    await tokensession.store(task_id, "sk-user-1")
    patch_redis._data["gw:conc:h"] = "1"            # 同用户另一任务占 1 槽

    await resume_held_once()

    row = task_store.rows[task_id]
    assert row["status"] == "FAILURE"
    assert "missing task id" in row["fail_reason"]
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
    assert patch_redis._data.get("gw:conc:h") == "1"    # 只还回自己的槽


async def test_held_rate_limited_expires_earlier(
    mocks, test_settings, patch_redis, task_store, queue_events,
):
    """判死分流：限流挂起超 1h 判死；同龄（2h）账户级挂起保留（上限 4h）。"""
    two_hours_ago = int(time.time()) - 2 * 3600
    rl_id = _seed_held(task_store, task_id="h" + "7" * 31, updated_at=two_hours_ago)
    task_store.rows[rl_id]["data"]["held_reason"] = "rate_limited"
    acct_id = _seed_held(task_store, task_id="h" + "8" * 31, updated_at=two_hours_ago)
    task_store.rows[acct_id]["data"]["held_reason"] = "account_level"
    await tokensession.store(rl_id, "sk-user-1")

    await sweep_once()

    assert task_store.rows[rl_id]["status"] == "FAILURE"
    assert "held timeout" in task_store.rows[rl_id]["fail_reason"]
    assert queue_events["cancel"] == [{"request_id": rl_id, "user_sk": "sk-user-1"}]
    assert task_store.rows[acct_id]["status"] == "HELD"   # 账户级 4h 上限未到
