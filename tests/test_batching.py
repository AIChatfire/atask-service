"""攒批（ADR-011）的硬不变量。

用 ASGITransport 跑真实 app 与真实 ``relayflow``，Redis / tasks 表走内存替身
（conftest），三个发布门面替换成记录器——真 broker 与 ``RedisScheduleSource`` 都要
真 Redis，单测里一律不碰（与 ``test_queue_route`` 同一手法）。

逐个钉住的都是「丢了会真花钱或真丢单」的那几条：

1. 分批头非法 → 400 且**不留痕**（不留任务行、不占槽、不入批）；
2. 等待期**不占并发槽**，且**不提交上游**；
3. N 触发 → 整批放行，逐条占槽并投递提交；
4. 放行**恰好一次**：已放行的成员再被任何路径捞到都只 SKIPPED（上游绝不被调两次）；
5. 占不到槽 → 退避重排（不是失败、不是丢弃），退避次数落库；
6. 取消 → 退批 + **不还从未占过的槽**；
7. **还槽恰好一次**：同一任务被两条路径同时来还时，只能 DECR 一次；
8. 提交入口只有一个：等待态任务即使被直接投递（``/ops/requeue`` / DLQ 重放）也拒不执行；
9. Redis 丢数据 / 放行崩在半路 → sweep 按 DB 事实兜底捞回。
"""

from __future__ import annotations

import hashlib
import time
from unittest.mock import AsyncMock

import httpx
import pytest

from app.main import app
from app.services import batching, relayflow

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"
TH = hashlib.sha256(b"sk-user-1").hexdigest()
CONC_KEY = f"atask:conc:{TH}"
#: 归组键（默认 BATCH_GROUP_BY=model，模型名 "m" 归一后就是它）
KEY = "m"
BATCH_KEY = f"atask:batch:{KEY}"
DUE_KEY = "atask:batch:due"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Upstream-Base-Url": UP_BASE, **extra}


@pytest.fixture
def batch_settings(monkeypatch: pytest.MonkeyPatch):
    """开攒批（N=2）+ 受控上限；其余照抄 test_queue_route 的网关配置。"""
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    monkeypatch.setattr(settings, "queue_deny_prefixes", "/api/,/console/")
    monkeypatch.setattr(settings, "task_stale_seconds", 300)
    monkeypatch.setattr(settings, "queue_sweep_limit", 50)
    monkeypatch.setattr(settings, "queue_sweep_lock_ttl_seconds", 300)
    monkeypatch.setattr(settings, "batch_enabled", True)
    monkeypatch.setattr(settings, "batch_size", 2)
    monkeypatch.setattr(settings, "batch_wait_seconds", 30)
    monkeypatch.setattr(settings, "max_batch_wait_seconds", 300)
    monkeypatch.setattr(settings, "batch_group_by", "model")
    monkeypatch.setattr(settings, "max_concurrent_tasks", 5)
    monkeypatch.setattr(settings, "batch_release_concurrency", 8)
    monkeypatch.setattr(settings, "batch_backoff_max_seconds", 300)
    return settings


@pytest.fixture
def publish_log(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """拦截三个发布门面（真 broker / RedisScheduleSource 都需要真 Redis）。"""
    import app.queue as q

    events: dict[str, list] = {"submit": [], "batch_release": [], "task_release": []}

    async def _submit(task_id: str) -> None:
        events["submit"].append(task_id)

    async def _batch_release(key: str, source: str = "batch",
                             *, due_at: int | None = None) -> None:
        events["batch_release"].append(
            {"key": key, "source": source, "due_at": due_at})

    async def _task_release(task_id: str, due_at: int) -> None:
        events["task_release"].append({"task_id": task_id, "due_at": due_at})

    monkeypatch.setattr(q, "publish_queue_submit", AsyncMock(side_effect=_submit))
    monkeypatch.setattr(q, "publish_batch_release", AsyncMock(side_effect=_batch_release))
    monkeypatch.setattr(q, "publish_task_release", AsyncMock(side_effect=_task_release))
    return events


@pytest.fixture
def no_probe_candidates(monkeypatch: pytest.MonkeyPatch):
    """把探测通道的候选清空：本文件只测攒批兜底，避免打到真 MySQL。"""
    import app.services.taskstore as ts

    async def _none(stale_seconds: int, limit: int = 50) -> list[dict]:
        return []

    monkeypatch.setattr(ts, "stale_queue_active", _none)


async def _submit(client: httpx.AsyncClient, **extra: str) -> tuple[int, dict]:
    resp = await client.post("/queue/v1/tasks", json={"model": "m"},
                             headers=_headers(**extra))
    return resp.status_code, (resp.json() if resp.content else {})


# ---------------------------------------------------------------------------
# 1. 分批头校验：400 且不留痕
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("header", "value", "code"), [
    ("X-Batch-Size", "0", "invalid_batch_size"),
    ("X-Batch-Size", "-3", "invalid_batch_size"),
    ("X-Batch-Size", "1001", "invalid_batch_size"),
    ("X-Batch-Size", "abc", "invalid_batch_size"),
    ("X-Batch-Wait", "0", "invalid_batch_wait"),
    ("X-Batch-Wait", "99999", "batch_wait_too_long"),
])
async def test_bad_batch_header_rejected_without_trace(
    batch_settings, task_store, publish_log, patch_redis, header, value, code
):
    async with _client() as client:
        status, view = await _submit(client, **{header: value})
    assert status == 400
    assert view["error"]["code"] == code
    assert view["error"]["param"] == header.lower()
    assert not task_store.rows                       # 没留任务行
    assert publish_log["submit"] == []
    assert publish_log["batch_release"] == []
    assert await patch_redis.get(CONC_KEY) is None    # 也没占槽


async def test_bad_batch_header_also_rejected_when_batching_disabled(
    batch_settings, task_store, publish_log, patch_redis
):
    """关掉攒批时坏头**同样** 400：否则「关着不报错、打开才报错」会变成切换开关后才
    暴露的客户端 bug。"""
    batch_settings.batch_enabled = False
    async with _client() as client:
        status, view = await _submit(client, **{"X-Batch-Size": "0"})
    assert status == 400
    assert view["error"]["code"] == "invalid_batch_size"


# ---------------------------------------------------------------------------
# 2. 等待期：不占槽、不提交、T 触发已排
# ---------------------------------------------------------------------------


async def test_waiting_task_takes_no_slot_and_is_not_submitted(
    batch_settings, task_store, publish_log, patch_redis
):
    async with _client() as client:
        status, view = await _submit(client, **{"X-Batch-Key": KEY})
    assert status == 202
    assert view["status"] == "SUBMITTED"
    assert view["batch_key"] == KEY
    assert view["batch_state"] == "waiting"
    assert view["batch_size"] == 2 and view["batch_wait"] == 30

    row = task_store.rows[view["task_id"]]
    assert row["status"] == "SUBMITTED"
    assert row["data"]["batch_state"] == "waiting"
    assert row["data"]["slot_flags"] == 0              # 等待期不占槽
    assert row["data"]["batch_due_at"] > int(time.time())
    assert "upstream_task_id" not in row["data"]

    assert publish_log["submit"] == []                 # 没有提交上游
    assert await patch_redis.get(CONC_KEY) is None     # 并发槽仍是空的
    # 首个成员排了一次 T 触发（且只有一次）
    due = [e for e in publish_log["batch_release"] if e["source"] == "due"]
    assert len(due) == 1
    assert due[0]["key"] == KEY
    assert due[0]["due_at"] == row["data"]["batch_due_at"]
    assert publish_log["task_release"] == []


async def test_view_of_waiting_task_never_touches_upstream(
    batch_settings, task_store, publish_log, patch_redis, respx_router
):
    async with _client() as client:
        _, view = await _submit(client, **{"X-Batch-Key": KEY})
        got = await client.get(f"/queue/v1/tasks/{view['task_id']}")
    assert got.status_code == 200
    assert got.json() == {"task_id": view["task_id"], "status": "queued"}
    assert not respx_router.calls                      # 等待期零上游往返


async def test_only_first_member_schedules_due_trigger(
    batch_settings, task_store, publish_log, patch_redis
):
    """一批 3 条只排 1 个 T 触发：每个成员都排一次会排出 N 个延迟任务（N-1 个空转）。"""
    async with _client() as client:
        for _ in range(3):
            await _submit(client, **{"X-Batch-Key": KEY})
    due = [e for e in publish_log["batch_release"] if e["source"] == "due"]
    assert len(due) == 1
    assert await patch_redis.zcard(BATCH_KEY) == 3
    assert await patch_redis.zcard(DUE_KEY) == 1


# ---------------------------------------------------------------------------
# 3/4. N 触发整批放行 + 放行恰好一次
# ---------------------------------------------------------------------------


async def test_n_trigger_releases_whole_batch_exactly_once(
    batch_settings, task_store, publish_log, patch_redis
):
    async with _client() as client:
        ids = []
        for _ in range(2):
            _, view = await _submit(client, **{"X-Batch-Key": KEY})
            ids.append(view["task_id"])

    # 第 2 条入批即 N 触发（只投递放行任务，不同步放行）
    assert [e["source"] for e in publish_log["batch_release"]] == ["due", "size"]
    assert publish_log["submit"] == []

    tally = await batching.release(KEY, source="size")
    assert tally == {"claimed": 2, "released": 2, "requeued": 0, "skipped": 0}
    assert sorted(publish_log["submit"]) == sorted(ids)
    assert await patch_redis.get(CONC_KEY) == "2"
    assert await patch_redis.zcard(BATCH_KEY) == 0
    for task_id in ids:
        assert task_store.rows[task_id]["data"]["batch_state"] == "released"
        assert task_store.rows[task_id]["data"]["slot_flags"] == 1

    # 再放一次：整批已被摘走 → 什么都不做（N 与 T 并发时的第二方）
    assert await batching.release(KEY, source="due") == {
        "claimed": 0, "released": 0, "requeued": 0, "skipped": 0}
    assert sorted(publish_log["submit"]) == sorted(ids)     # 上游没被调第二次


async def test_already_released_member_is_skipped_by_any_path(
    batch_settings, task_store, publish_log, patch_redis
):
    """成员级幂等（快速路径）：已放行的成员再被任何路径捞到都只 SKIPPED。"""
    async with _client() as client:
        ids = [(await _submit(client, **{"X-Batch-Key": KEY}))[1]["task_id"]
               for _ in range(2)]
    await batching.release(KEY)

    for task_id in ids:
        assert await relayflow.release_batched_task(task_id) == "skipped"
    assert sorted(publish_log["submit"]) == sorted(ids)


async def test_lost_claim_race_never_takes_slot_nor_submits(
    batch_settings, task_store, publish_log, patch_redis, monkeypatch
):
    """**抢放行权失败**的那一方必须原地退出。

    这条用例专门打上面那条用例打不到的地方：快速路径只在「读到的状态已经不是等待态」
    时生效，而两条放行路径（T 触发的延迟任务与 sweep 兜底、或两条 sweep 轮次）会**先
    各自读到 waiting，再去抢权**——那是一个 TOCTOU 窗口，只有 ``claim_for_release``
    的 rowcount 能挡住它。

    这里把「抢不到」直接模拟出来（条件更新 rowcount==0 = 别人先抢到了，而本次读到的
    仍是旧的 waiting）：模型换成了不抢权也照走，就会**多占一个并发槽 + 多调一次上游**，
    而上游 relay 会照扣一次配额，网关零资金动作、无从补救。
    """
    import app.services.taskstore as ts

    async with _client() as client:
        _, view = await _submit(client, **{"X-Batch-Key": KEY})
    task_id = view["task_id"]

    async def _lost_race(_task_id: str) -> bool:
        return False                       # 等价于条件更新影响行数 0

    monkeypatch.setattr(ts, "claim_for_release", _lost_race)

    assert await relayflow.release_batched_task(task_id) == "skipped"
    assert publish_log["submit"] == []                  # 上游一次都没被调
    assert await patch_redis.get(CONC_KEY) is None      # 也没占槽（否则槽白漏一个）
    assert task_store.rows[task_id]["data"]["batch_state"] == "waiting"


# ---------------------------------------------------------------------------
# 5. 占不到槽 → 退避重排（不是失败）
# ---------------------------------------------------------------------------


async def test_no_slot_requeues_instead_of_failing(
    batch_settings, task_store, publish_log, patch_redis
):
    batch_settings.max_concurrent_tasks = 1
    async with _client() as client:
        for _ in range(2):
            await _submit(client, **{"X-Batch-Key": KEY})

    tally = await batching.release(KEY)
    assert tally["released"] == 1 and tally["requeued"] == 1
    assert await patch_redis.get(CONC_KEY) == "1"

    requeued = [r for r in task_store.rows.values()
                if r["data"]["batch_state"] == "requeued"]
    assert len(requeued) == 1
    row = requeued[0]
    assert row["status"] == "SUBMITTED"                # 排队，不是失败
    assert row["data"]["requeue_attempts"] == 1
    # 退避时刻同时写进 batch_due_at（sweep 的兜底只需这一个谓词）
    assert row["data"]["requeue_due_at"] == row["data"]["batch_due_at"]
    assert len(publish_log["task_release"]) == 1
    assert publish_log["task_release"][0]["task_id"] == row["task_id"]


async def test_requeued_task_can_be_released_again(
    batch_settings, task_store, publish_log, patch_redis
):
    """退避到点后的单条放行：``requeued`` 也是合法放行起点（否则它会永远卡住）。"""
    batch_settings.max_concurrent_tasks = 1
    async with _client() as client:
        for _ in range(2):
            await _submit(client, **{"X-Batch-Key": KEY})
    await batching.release(KEY)
    stuck = next(r["task_id"] for r in task_store.rows.values()
                 if r["data"]["batch_state"] == "requeued")

    batch_settings.max_concurrent_tasks = 5            # 槽空出来了
    assert await relayflow.release_batched_task(stuck, source="requeue") == "released"
    assert task_store.rows[stuck]["data"]["batch_state"] == "released"
    assert await patch_redis.get(CONC_KEY) == "2"


# ---------------------------------------------------------------------------
# 6/7. 取消：退批、不还从未占过的槽；还槽恰好一次
# ---------------------------------------------------------------------------


async def test_cancel_waiting_member_leaves_batch_and_keeps_empty_slot(
    batch_settings, task_store, publish_log, patch_redis
):
    async with _client() as client:
        _, view = await _submit(client, **{"X-Batch-Key": KEY})
        resp = await client.delete(f"/queue/v1/tasks/{view['task_id']}")
    assert resp.status_code == 200
    assert task_store.rows[view["task_id"]]["status"] == "CANCELED"

    assert await patch_redis.zcard(BATCH_KEY) == 0     # 退批（否则整批永远凑不满 N）
    assert await patch_redis.zcard(DUE_KEY) == 0       # 批次空了，到期索引一并清掉
    assert await patch_redis.get(CONC_KEY) is None     # 从未占槽 → 绝不 DECR
    assert publish_log["submit"] == []


async def test_slot_release_happens_exactly_once_per_task(
    batch_settings, task_store, publish_log, patch_redis
):
    """两条各占一个槽（合计 2）时，对同一条重复还槽只能还一次——多出的那次还的是
    别人的槽，而 ``LUA_CONC_RELEASE`` 只钳 0、发现不了。"""
    async with _client() as client:
        ids = [(await _submit(client, **{"X-Batch-Key": KEY}))[1]["task_id"]
               for _ in range(2)]
    await batching.release(KEY)
    assert await patch_redis.get(CONC_KEY) == "2"

    await relayflow._release_slot(ids[0], TH)
    await relayflow._release_slot(ids[0], TH)
    assert await patch_redis.get(CONC_KEY) == "1"      # 另一个任务的槽还在


# ---------------------------------------------------------------------------
# 8. 提交入口只有一个：等待态任务不得被直接提交
# ---------------------------------------------------------------------------


async def test_waiting_task_cannot_be_submitted_directly(
    batch_settings, task_store, publish_log, patch_redis, respx_router
):
    """``/ops/requeue``、DLQ 重放都直接投递 ``queue_submit_task``；等待期任务的并发槽
    还没占，必须被挡在 ``submit_queue_task`` 里，否则任何人工补单都能绕过并发闸门。"""
    async with _client() as client:
        _, view = await _submit(client, **{"X-Batch-Key": KEY})
    await relayflow.submit_queue_task(view["task_id"])
    assert not respx_router.calls
    assert task_store.rows[view["task_id"]]["status"] == "SUBMITTED"
    assert "upstream_task_id" not in task_store.rows[view["task_id"]]["data"]


# ---------------------------------------------------------------------------
# 9. 兜底：Redis 丢数据 / 放行崩在半路
# ---------------------------------------------------------------------------


async def test_sweep_rescues_overdue_waiting_member(
    batch_settings, task_store, publish_log, patch_redis, no_probe_candidates
):
    """T 触发的延迟任务丢了（Redis 掉数据）：客户端不轮询、探测通道也碰不到它
    （没有 upstream_task_id），只有 sweep 的超期兜底能把它捞回来。"""
    async with _client() as client:
        _, view = await _submit(client, **{"X-Batch-Key": KEY})
    task_id = view["task_id"]
    await patch_redis.delete(BATCH_KEY, DUE_KEY)        # 模拟 Redis 索引整个丢掉
    await task_store.patch_data(task_id, {"batch_due_at": int(time.time()) - 600})

    advanced = await relayflow.sweep_queue_once()
    assert advanced == 0                                # 探测通道没动它
    assert publish_log["submit"] == [task_id]
    assert task_store.rows[task_id]["data"]["batch_state"] == "released"
    assert await patch_redis.get(CONC_KEY) == "1"


async def test_sweep_rescues_task_stuck_in_releasing(
    batch_settings, task_store, publish_log, patch_redis, no_probe_candidates
):
    """抢到放行权之后进程崩了：行停在 ``releasing``，既不满足 ``claim_for_release``
    的起点、也没有别的通道会碰它——不救它就永远卡在非终态（ADR-010 明令不许静默挂起）。"""
    async with _client() as client:
        _, view = await _submit(client, **{"X-Batch-Key": KEY})
    task_id = view["task_id"]
    row = task_store.rows[task_id]
    row["data"]["batch_state"] = "releasing"
    row["updated_at"] = int(time.time()) - 600
    row["data"]["batch_due_at"] = int(time.time()) - 600

    await relayflow.sweep_queue_once()
    assert publish_log["submit"] == [task_id]
    assert task_store.rows[task_id]["data"]["batch_state"] == "released"


async def test_sweep_leaves_fresh_release_alone(
    batch_settings, task_store, publish_log, patch_redis, no_probe_candidates
):
    """正在飞的放行不会被误判成卡死：``releasing`` 的判据是 ``updated_at``（抢权时
    刷新），而不是 ``batch_due_at``（放行本就由到期触发，它必然已是过去时刻）。"""
    async with _client() as client:
        _, view = await _submit(client, **{"X-Batch-Key": KEY})
    task_id = view["task_id"]
    row = task_store.rows[task_id]
    row["data"]["batch_state"] = "releasing"
    row["data"]["batch_due_at"] = int(time.time()) - 600     # 已过期，但在飞

    await relayflow.sweep_queue_once()
    assert publish_log["submit"] == []
    assert task_store.rows[task_id]["data"]["batch_state"] == "releasing"


# ---------------------------------------------------------------------------
# 10. 归组键与对外状态（纯函数）
# ---------------------------------------------------------------------------


def test_group_key_prefers_client_key_and_sanitizes():
    assert batching.group_key("", model="MiniMax-H3", token_hash=TH,
                              group_by="model") == "minimax-h3"
    assert batching.group_key("", model="MiniMax-H3", token_hash=TH,
                              group_by="token_model") == f"{TH[:12]}:minimax-h3"
    # 客户端显式指定的键优先，且**照样**过白名单（它会直接拼进 Redis 键名）
    assert batching.group_key("my batch!", model="m", token_hash=TH,
                              group_by="model") == "my_batch_"
    assert batching.sanitize_key("a b:c") == "a_b:c"        # 冒号是合法分隔符
    assert batching.sanitize_key("x" * 200) == "x" * batching.MAX_KEY_LEN


def test_internal_states_do_not_leak_to_clients():
    """``requeued`` 必须映射成 ``released``：漏出去会让「靠 batch_state 判断是否在
    排队」的客户端去等一个永远不会到达的批次事件。"""
    assert batching.public_state("waiting") == "waiting"
    for state in ("releasing", "released", "requeued"):
        assert batching.public_state(state) == "released"
    assert batching.public_state("") == ""


def test_sweep_states_cover_stuck_releasing():
    """兜底候选必须包含 ``releasing``（否则卡死的任务无人救）；而放行起点**不得**
    包含 ``releasing``（否则同一条任务能被两条路径同时抢到）。"""
    from app.services.taskstore import BATCH_SWEEP_STATES, BATCH_WAITING_STATES

    assert "releasing" in BATCH_SWEEP_STATES
    assert "releasing" not in BATCH_WAITING_STATES
    assert set(BATCH_WAITING_STATES) < set(BATCH_SWEEP_STATES)
