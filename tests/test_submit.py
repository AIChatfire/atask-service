"""异步提交 worker（submit_one）测试：创建链路落库后的上游提交执行。

覆盖：成功回填 + 探测排程、渠道覆盖报文、回调注入、失败补偿（解冻/HELD）、
有限重打（key 级换租约/模糊失败不重试）、幂等短路与互斥锁。

边界：keypool/上游走 respx，taskstore 内存实现，queue 发布门面记录器，
Redis FakeRedis（提交互斥锁也落在 FakeRedis 上）。
"""

from __future__ import annotations

import json
import time

import httpx
import pytest

from app.deps import ratelimit
from app.redis import K_SUBMIT_LOCK
from app.schemas import FAILURE, QUEUED, SUBMITTED, SUCCESS
from app.services import tokensession
from app.services.submit import submit_one


def _channel(*, channel_id: int = 7, key: str = "sk-upstream-key",
             gateway_overrides: dict | None = None) -> dict:
    gateway = {
        "biz": "minimax",
        "submit_path": "/v2/video_generation",
        "probe_path": "/v2/query/video_generation/{upstream_task_id}",
        "status_path": "task.status",
        "result_path": "task.content.url",
        "billing": {"rule": "def calulate(request):\n    return 0.13"},
    }
    gateway.update(gateway_overrides or {})
    return {
        "code": 0, "message": "ok",
        "data": {
            "channel_id": channel_id, "key_index": 0, "key": key,
            "base_url": "http://upstream.test", "epoch": "e1",
            "channel": {
                "id": channel_id, "name": f"ch-{channel_id}",
                "base_url": "http://upstream.test",
                "param_override": {"aigc_watermark": False},
                "setting": {"gateway": gateway},
            },
        },
    }


def _seed(task_store, *, status: str = SUBMITTED, **data_overrides) -> str:
    task_id = "minimax_" + "s" * 32
    now = int(time.time())
    data = {
        "biz": "minimax", "source": "tasks", "model": "MiniMax-H3",
        "token_hash": "h", "idempotency_key": None, "callback_url": None,
        "freeze_amount": 0.13, "settled": False, "key_id": 7, "key_index": 0,
        "freeze_expires_at": now + 1800,
        "request_body": {"model": "MiniMax-H3", "duration": 5,
                         "content": [{"type": "text", "text": "a cat"}]},
    }
    data.update(data_overrides)
    task_store.rows[task_id] = {
        "task_id": task_id, "platform": "gateway", "action": "task",
        "status": status, "progress": "0%", "fail_reason": "",
        "data": data, "user_id": 7, "channel_id": 7,
        "submit_time": now, "start_time": now, "created_at": now,
        "updated_at": now, "finish_time": 0,
    }
    return task_id


@pytest.fixture
def mocks(respx_router):
    m = type("Mocks", (), {})()
    m.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_channel())
    )
    m.report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    return m


async def _flush_reports(mocks, n: int = 1) -> None:
    """key report 为 fire-and-forget（providers.keypool 内部 create_task）：
    等后台上报落地后再断言。"""
    import asyncio

    for _ in range(100):
        if len(mocks.report.calls) >= n:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"key report not flushed (expect {n}, got {len(mocks.report.calls)})")


# ---------------------------------------------------------------------------
# 成功路径
# ---------------------------------------------------------------------------


async def test_submit_success_backfills_and_schedules_poll(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """提交成功：回填 upstream_task_id → QUEUED → 进探测队列；key 成功上报。"""
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "mm-1"})
    )
    task_id = _seed(task_store)

    await submit_one(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == QUEUED
    assert row["data"]["upstream_task_id"] == "mm-1"
    assert queue_events["poll"] == [{"task_id": task_id, "delay": 5}]
    # 渠道覆盖（param_override）与用户报文透传在 worker 侧重建
    sent = json.loads(create.calls.last.request.content)
    assert sent["aigc_watermark"] is False
    assert sent["content"][0]["text"] == "a cat"
    assert sent["duration"] == 5
    assert "callback_url" not in sent          # supports_callback=False 不注入
    # key 成功上报（report 按实际渠道）
    await _flush_reports(mocks)
    report = json.loads(mocks.report.calls.last.request.content)
    assert report["success"] is True


async def test_submit_success_callback_channel_skips_poll(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """supports_callback 渠道：网关注入回调地址，不进探测队列。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_channel(
            gateway_overrides={"supports_callback": True}))
    )
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "mm-1"})
    )
    task_id = _seed(task_store)

    await submit_one(task_id)

    assert task_store.rows[task_id]["status"] == QUEUED
    assert queue_events["poll"] == []
    sent = json.loads(create.calls.last.request.content)
    assert sent["callback_url"] == f"https://gw.test/callback/minimax/{task_id}"


# ---------------------------------------------------------------------------
# 失败补偿
# ---------------------------------------------------------------------------


async def test_submit_rejected_fails_and_refunds(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """任务级 4xx（400 内容审核，不在重打集合）：FAILURE + 用令牌会话解冻 +
    释放并发槽 + key 失败上报；用户回调收到终态通知。"""
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(400, json={"error": "content policy"})
    )
    task_id = _seed(task_store, callback_url="https://user.test/hook")
    await tokensession.store(task_id, "sk-user-1")
    await ratelimit.conc_acquire("h")

    await submit_one(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == FAILURE
    assert "content policy" in row["fail_reason"]
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]
    assert queue_events["settle"] == []                # 提交未接单，绝不收费
    assert await tokensession.get(task_id) is None     # 终态即清
    assert patch_redis._data.get("gw:conc:h") == "0"   # 并发槽已释放
    assert queue_events["notify"][0]["url"] == "https://user.test/hook"
    await _flush_reports(mocks)
    report = json.loads(mocks.report.calls.last.request.content)
    assert report["success"] is False and report["status_code"] == 400


async def test_submit_rejection_ignores_failed_charge_policy(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """渠道配 failed_billing=charge 也不覆盖提交阶段失败（上游未接单）：
    一律全额解冻（charge 只覆盖生成失败）。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_channel(
            gateway_overrides={"failed_billing": "charge"}))
    )
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(400, json={"error": "content policy"})
    )
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")

    await submit_one(task_id)

    assert task_store.rows[task_id]["status"] == FAILURE
    assert queue_events["settle"] == []
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]


async def test_submit_missing_task_id_fails_visibly(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """傻瓜式防护：2xx 但提取不到上游 id（task_id_path 配错）→ FAILURE + 解冻。"""
    respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"unexpected": "shape"})
    )
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")

    await submit_one(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == FAILURE
    assert "missing task id" in row["fail_reason"]
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]


async def test_submit_ambiguous_failure_keeps_alive(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """5xx（模糊失败）绝不重打也不判死：只提交一次，任务保持 SUBMITTED，
    sweep 补投负责下轮重试（防双重创建双扣费；基础设施故障不误伤任务）。"""
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(500, text="boom")
    )
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")

    await submit_one(task_id)

    assert len(create.calls) == 1                          # 不重打
    row = task_store.rows[task_id]
    assert row["status"] == "SUBMITTED"                    # 留活（非 FAILURE）
    assert "boom" in row["data"]["last_submit_error"]      # 观测字段
    assert queue_events["cancel"] == []                    # 不解冻
    assert queue_events["settle"] == []


async def test_submit_base_url_missing_keeps_alive(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """渠道 base_url 缺失（keypool 元数据缺口）：哨兵 599 → 模糊失败留活，
    响亮文案进 last_submit_error，绝不产出 "Target host is not specified"
    这类不可操作信息，也绝不把配置故障判成任务失败。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json={
            "code": 0, "message": "ok",
            "data": {
                "channel_id": 7, "key_index": 0, "key": "sk-x",
                "base_url": "", "epoch": "e1",           # 租约 base_url 空
                "channel": {
                    "id": 7, "name": "ch-7",
                    "base_url": "",                       # 渠道 base_url 也空
                    "setting": {"gateway": {
                        "biz": "minimax",
                        "submit_path": "/v2/video_generation",
                        "probe_path": "/v2/query/video_generation/{upstream_task_id}",
                        "status_path": "task.status",
                        "billing": {"rule": "def calulate(request):\n    return 0.13"},
                    }},
                },
            },
        })
    )
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "up-9"})
    )
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")

    await submit_one(task_id)

    assert len(create.calls) == 0                          # 零出站（哨兵拦截）
    row = task_store.rows[task_id]
    assert row["status"] == "SUBMITTED"                    # 留活
    assert "base_url missing" in row["data"]["last_submit_error"]
    assert queue_events["cancel"] == []


# ---------------------------------------------------------------------------
# 有限重打（换租约）
# ---------------------------------------------------------------------------


async def test_submit_key_level_retries_with_fresh_lease(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """首 key 401（key 级确定性拒绝）→ report 坏 key + 重新 lease 换渠道重打成功；
    对账口径（channel_id/data.key_id）切到实际渠道。"""
    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        side_effect=[
            httpx.Response(200, json=_channel(channel_id=7, key="sk-bad")),
            httpx.Response(200, json=_channel(channel_id=8, key="sk-good")),
        ]
    )
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        side_effect=[
            httpx.Response(401, text="invalid api key"),
            httpx.Response(200, json={"task_id": "up-9"}),
        ]
    )
    task_id = _seed(task_store)

    await submit_one(task_id)

    assert len(create.calls) == 2
    row = task_store.rows[task_id]
    assert row["status"] == QUEUED
    assert row["channel_id"] == 8
    assert row["data"]["key_id"] == 8
    assert row["data"]["upstream_task_id"] == "up-9"
    await _flush_reports(mocks, 2)               # 坏 key 失败上报 + 成功上报
    first_report = json.loads(mocks.report.calls[0].request.content)
    assert first_report["success"] is False and first_report["status_code"] == 401


async def test_submit_retry_exhausted_after_max_attempts(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """连续确定性拒绝：重打到上限（3 次含首次）即止，FAILURE + 解冻。"""
    create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(401, text="invalid api key")
    )
    task_id = _seed(task_store)
    await tokensession.store(task_id, "sk-user-1")

    await submit_one(task_id)

    assert len(create.calls) == 3
    assert task_store.rows[task_id]["status"] == FAILURE
    assert queue_events["cancel"] == [{"request_id": task_id, "user_sk": "sk-user-1"}]


# ---------------------------------------------------------------------------
# 幂等 / 互斥 / 异常传播
# ---------------------------------------------------------------------------


async def test_submit_skips_when_already_submitted(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """已有 upstream_task_id（补投/DLQ 重放）→ 幂等短路，零出站。"""
    task_id = _seed(task_store, status=QUEUED, upstream_task_id="mm-1")
    await submit_one(task_id)
    assert not respx_router.calls
    assert task_store.rows[task_id]["status"] == QUEUED


async def test_submit_skips_terminal_task(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """终态任务（孤儿收口判死后重放到达）→ 直接返回，零出站。"""
    task_id = _seed(task_store, status=SUCCESS)
    await submit_one(task_id)
    assert not respx_router.calls


async def test_submit_lock_excludes_concurrent(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """互斥锁：在飞提交持锁期间，补投/重放直接让路（防双重创建）。"""
    task_id = _seed(task_store)
    await patch_redis.set(K_SUBMIT_LOCK.format(task_id=task_id), "1", ex=300)

    await submit_one(task_id)

    assert not respx_router.calls
    assert task_store.rows[task_id]["status"] == SUBMITTED


# ---------------------------------------------------------------------------
# KI2：锁 TTL 按路由动态派生（重打次数 × 渠道 timeout + buffer）
# ---------------------------------------------------------------------------


def test_submit_lock_ttl_derived_from_route(route_factory, test_settings):
    """KI2：锁 TTL = submit_max_attempts × 渠道 timeout_sec + buffer，
    不再硬编码 300s——渠道 timeout >90s 时锁不再先于在飞提交过期。"""
    from app.services.submit import submit_lock_ttl

    # 默认 submit_max_attempts=3、submit_lock_buffer_seconds=60
    assert submit_lock_ttl(route_factory(timeout_sec=60.0)) == 3 * 60 + 60
    assert submit_lock_ttl(route_factory(timeout_sec=95.0)) == 3 * 95 + 60
    assert submit_lock_ttl(route_factory(timeout_sec=30.0)) == 3 * 30 + 60


def test_route_timeout_sec_rejects_negative(route_factory):
    """KI-C：timeout_sec 负数在路由构建期（pydantic 校验）响亮报错，
    而非带到提交期让锁 TTL 派生/Redis SET ex 才炸。"""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        route_factory(timeout_sec=-1.0)
    with pytest.raises(ValidationError):
        route_factory(timeout_sec=-0.5)


async def test_submit_success_does_not_revive_terminal_task(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
    monkeypatch,
):
    """KI-D：提交在飞期间任务已被并发判死（孤儿收口抢先 FAILURE）——提交
    成功回调绝不复活终态：不转 QUEUED、不排探测；但 upstream_task_id
    **绝不丢**（纯 data 合并落库，反向对账/人工追款的唯一线索）。"""
    import app.services.upstream as upstream_mod

    task_id = _seed(task_store)

    async def _orphan_closeout_races(route, key, body):
        # 模拟在飞期间孤儿收口抢先判死（另一进程 finalize FAILURE + 退款）
        task_store.rows[task_id]["status"] = FAILURE
        return {"task_id": "up-orphan"}

    monkeypatch.setattr(upstream_mod, "submit", _orphan_closeout_races)

    await submit_one(task_id)

    row = task_store.rows[task_id]
    assert row["status"] == FAILURE                          # 终态不被复活
    assert row["data"]["upstream_task_id"] == "up-orphan"    # 线索绝不丢（对账/追款）
    assert queue_events["poll"] == []                        # 不进探测闭环
    assert queue_events["settle"] == []                      # 不产生结算事件


async def test_submit_lock_ttl_covers_worst_case_window(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
    monkeypatch,
):
    """KI2：长 timeout 渠道（120s）在飞提交持锁 TTL 覆盖最坏窗口
    （3 × 120 + 60 = 420s），不再是硬编码 300s。"""
    import app.services.upstream as upstream_mod

    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_channel(
            gateway_overrides={"timeout_sec": 120.0}))
    )
    task_id = _seed(task_store)
    captured: dict = {}

    async def _fake_submit(route, key, body):
        captured["ttl"] = await patch_redis.ttl(K_SUBMIT_LOCK.format(task_id=task_id))
        return {"task_id": "mm-1"}

    monkeypatch.setattr(upstream_mod, "submit", _fake_submit)

    await submit_one(task_id)

    expected = 3 * 120 + 60
    assert expected - 5 < captured["ttl"] <= expected
    assert task_store.rows[task_id]["status"] == QUEUED


async def test_submit_lock_ttl_refreshed_when_retry_switches_channel(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
    monkeypatch,
):
    """KI2：重打换渠道（新渠道 timeout 更长）→ 锁 TTL 按新路由刷新，
    防锁先于换渠道后的在飞提交过期。"""
    import app.services.upstream as upstream_mod

    mocks.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        side_effect=[
            httpx.Response(200, json=_channel(
                channel_id=7, key="sk-bad",
                gateway_overrides={"timeout_sec": 30.0})),
            httpx.Response(200, json=_channel(
                channel_id=8, key="sk-good",
                gateway_overrides={"timeout_sec": 150.0})),
        ]
    )
    task_id = _seed(task_store)
    ttls: list[int] = []

    async def _fake_submit(route, key, body):
        ttls.append(await patch_redis.ttl(K_SUBMIT_LOCK.format(task_id=task_id)))
        if key.key_id == 7:
            raise upstream_mod.UpstreamError("minimax", 401, "invalid api key")
        return {"task_id": "up-9"}

    monkeypatch.setattr(upstream_mod, "submit", _fake_submit)

    await submit_one(task_id)

    assert ttls[0] <= 3 * 30 + 60                      # 首打按短 timeout 渠道派生
    assert 3 * 150 + 60 - 5 < ttls[1] <= 3 * 150 + 60  # 重打按新渠道刷新
    assert task_store.rows[task_id]["status"] == QUEUED
    assert task_store.rows[task_id]["data"]["key_id"] == 8


async def test_submit_lease_failure_propagates_for_queue_retry(
    mocks, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """keypool 租约失败（无可用 key）→ 异常抛给 queue 层退避重试；
    任务保持 SUBMITTED 不动（sweep 亦会补投），冻结不动。"""
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(503, json={"code": 40001, "message": "no key"})
    )
    task_id = _seed(task_store)

    from app.services.providers import KeyLeaseError

    with pytest.raises(KeyLeaseError):
        await submit_one(task_id)

    assert task_store.rows[task_id]["status"] == SUBMITTED
    assert queue_events["cancel"] == []
