"""W2 TaskManager 单元测试（SPEC §7.1；全 mock，不依赖真实 MySQL/Redis/上游）。

覆盖：submit 幂等链路字段纪律（quota=0/platform=gw_*/时间列非 NULL/
private_data.gateway 键全量）、402 不落库、上游失败 cancel 补偿、
transition CAS 竞态/终态不可逆/乱序防护、终态 outbox+delivery commit 后
入队 Redis 延迟队列（决策 A-3/A-4）、tidx 回调反查索引（决策 A-2）、
freeze_shard 两级读取、track_passthrough_task。
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

import app.tasks.manager as mgr
from app.adapters.base import (
    CanonicalTaskRequest,
    SubmitResult,
    TaskSnapshot,
    TaskStatus,
    UpstreamBizError,
    UsageEstimate,
)
from app.registry import BizConfig
from app.tasks.manager import PaymentRequired, TaskManager, current_freeze_shard
from app.tasks.models import GW_PLATFORM_LIKE

# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------


class FakeResult:
    def __init__(self, rows: list[dict] | None = None, rowcount: int = 0) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def mappings(self) -> FakeResult:
        return self

    def first(self) -> dict | None:
        return self._rows[0] if self._rows else None

    def all(self) -> list[dict]:
        return self._rows


class FakeSession:
    """按 SQL 子串路由预置结果的 fake AsyncSession。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.handlers: list[tuple[str, Any]] = []  # (sql_substring, FakeResult|exc)
        self.commits = 0
        self.rollbacks = 0

    def on(self, sql_substring: str, result: Any) -> None:
        self.handlers.append((sql_substring, result))

    async def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> FakeResult:
        sql = " ".join(str(stmt).split())
        params = params or {}
        self.calls.append((sql, params))
        for sub, res in self.handlers:
            if sub in sql:
                if isinstance(res, Exception):
                    raise res
                return res
        return FakeResult()

    def last_params(self, sql_substring: str) -> dict[str, Any]:
        for sql, params in reversed(self.calls):
            if sql_substring in sql:
                return params
        raise AssertionError(f"no execute matching {sql_substring!r}")

    def count(self, sql_substring: str) -> int:
        return sum(1 for sql, _ in self.calls if sql_substring in sql)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class FakeRedis:
    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.expires: dict[str, int] = {}

    async def set(self, key: str, value: Any, ex: int | None = None, **_: Any) -> bool:
        self.strings[key] = str(value)
        if ex is not None:
            self.expires[key] = ex
        return True

    async def get(self, key: str) -> str | None:
        return self.strings.get(key)

    async def delete(self, *keys: str) -> int:
        n = 0
        for key in keys:
            if self.strings.pop(key, None) is not None:
                n += 1
        return n

    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        mapping: dict[str, Any] | None = None,
    ) -> int:
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update({k: str(v) for k, v in mapping.items()})
        elif field is not None:
            h[field] = str(value)
        return 1

    async def hget(self, key: str, field: str) -> str | None:
        return self.hashes.get(key, {}).get(field)

    async def expire(self, key: str, ttl: int) -> bool:
        self.expires[key] = ttl
        return True


class FakeAdapter:
    name = "kling"
    callback_capability = True
    echoes_external_task_id = True

    def __init__(self, *, submit_exc: Exception | None = None) -> None:
        self.submit_exc = submit_exc
        self.submitted: list[tuple[Any, Any]] = []

    def estimate_usage(self, req: CanonicalTaskRequest) -> UsageEstimate:
        return UsageEstimate(amount_usd=Decimal("0"), context={"duration": 10.0})

    async def submit(self, req: CanonicalTaskRequest, ctx: Any) -> SubmitResult:
        self.submitted.append((req, ctx))
        if self.submit_exc:
            raise self.submit_exc
        return SubmitResult(upstream_task_id="up-123", raw={"id": "up-123"})


class FakeBilling:
    def __init__(self, *, freeze_exc: Exception | None = None) -> None:
        self.freeze_exc = freeze_exc
        self.freeze_calls: list[dict] = []
        self.cancel_calls: list[dict] = []

    async def freeze(self, **kw: Any) -> dict:
        self.freeze_calls.append(kw)
        if self.freeze_exc:
            raise self.freeze_exc
        return {"ok": True}

    async def cancel(self, **kw: Any) -> dict:
        self.cancel_calls.append(kw)
        return {"ok": True}


class FakePricing:
    def __init__(self, *, settle_exc: Exception | None = None) -> None:
        self.settle_exc = settle_exc
        self.eval_calls: list[dict] = []

    async def get_logic(self, biz: str, model: str, action: str) -> str:
        return "logic"

    async def get_logic_for_task(self, session: Any, task_id: str) -> str:
        return "logic"

    async def evaluate(self, logic: Any, context: dict, *, phase: str = "freeze") -> Decimal:
        self.eval_calls.append({"context": context, "phase": phase})
        if phase == "settle" and self.settle_exc:
            raise self.settle_exc
        return Decimal("0.5") if phase == "freeze" else Decimal("0.2")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


def make_biz_cfg() -> BizConfig:
    return BizConfig(
        biz="kling-biz",
        adapter="kling",
        upstream_base_url="https://upstream.example.com",
        auth_type="aksk_jwt",
        auth_secret_ref="UPSTREAM_SECRET_KLING",
        native_prefixes=["v1/videos"],
        enabled=True,
        billing_keys={"biz_type": "video", "metric": "call"},
        default_freeze_amount_usd="1.0",
        rate_limit={},
        newapi_channel_id=50,
        version=1,
    )


def make_token(**over: Any) -> Any:
    kw: dict[str, Any] = dict(
        user_id=42, sk_hash="h" * 16, raw="sk-testtoken", group="vip",
        is_system=False,
    )
    kw.update(over)
    return SimpleNamespace(**kw)


def make_req(**over: Any) -> CanonicalTaskRequest:
    kw: dict[str, Any] = {
        "model": "kling-v3",
        "prompt": "a cat",
        "action": "textGenerate",
        "callback_url": "https://user.example.com/cb",
    }
    kw.update(over)
    return CanonicalTaskRequest(**kw)


def make_manager(
    monkeypatch: pytest.MonkeyPatch,
    *,
    adapter: FakeAdapter | None = None,
    billing: FakeBilling | None = None,
    pricing: FakePricing | None = None,
    redis: FakeRedis | None = None,
) -> tuple[TaskManager, FakeAdapter, FakeBilling, FakePricing, FakeRedis]:
    adapter = adapter or FakeAdapter()
    billing = billing or FakeBilling()
    pricing = pricing or FakePricing()
    redis = redis or FakeRedis()
    # SubmitContext.secrets 由 resolve_submit_secrets 按 auth_secret_ref 从环境解析
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-test")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-test")
    monkeypatch.setattr(mgr, "get_adapter", lambda name: adapter)

    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(mgr, "get_redis", _get_redis)
    monkeypatch.setattr("app.auth.get_redis", _get_redis)  # sksess 钩子
    return TaskManager(billing=billing, pricing=pricing), adapter, billing, pricing, redis


def gateway_pdata(**gw_over: Any) -> str:
    gw: dict[str, Any] = {
        "biz": "kling-biz",
        "form": "videos",
        "sk_hash": "h" * 16,
        "idempotency_key": None,
        "callback_url": "https://user.example.com/cb",
        "billing_state": "frozen",
        "deadline_unix": 9999999999,
        "freeze_shard_seq": 0,
        "next_poll_at": 0,
        "usage_actual": None,
        "request_snapshot": {"model": "kling-v3", "action": "textGenerate", "n": 1},
    }
    gw.update(gw_over)
    return json.dumps({"upstream_task_id": "up-123", "gateway": gw})


# ---------------------------------------------------------------------------
# submit_task
# ---------------------------------------------------------------------------


async def test_submit_task_success(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, adapter, billing, pricing, redis = make_manager(monkeypatch)
    session = FakeSession()

    out = await tm.submit_task(
        session,
        biz_cfg=make_biz_cfg(),
        req=make_req(),
        token=make_token(),
        form="videos",
        idem_key="idem-1",
    )

    assert out["status"] == "queued"
    assert out["task_id"].startswith("task_") and len(out["task_id"]) == 37
    assert out["created_at"] > 0
    tid = out["task_id"]

    # freeze 预冻：首片 request_id={task_id}:0，分片 ttl=min(任务TTL, 23h)
    assert len(billing.freeze_calls) == 1
    frz = billing.freeze_calls[0]
    assert frz["request_id"] == f"{tid}:0"
    assert frz["ttl_seconds"] == min(172800, mgr.FREEZE_SHARD_TTL)
    assert frz["user_sk"] == "sk-testtoken"
    assert frz["biz_type"] == "video"
    assert frz["amount_usd"] == Decimal("0.5")
    assert pricing.eval_calls[0]["phase"] == "freeze"

    # 上游提交注入 capability 回调地址 + auth_secret_ref 解析后的真实凭证
    _, ctx = adapter.submitted[0]
    assert ctx.task_id == tid
    assert "/callbacks/kling-biz/kling/" in ctx.gateway_callback_url
    assert ctx.upstream_base_url == "https://upstream.example.com"
    assert ctx.secrets == {"ak": "ak-test", "sk": "sk-test"}

    # tasks INSERT 三件套字段纪律（SPEC §4.1）
    assert session.count("INSERT INTO tasks") == 1
    insert_sql = next(sql for sql, _ in session.calls if "INSERT INTO tasks" in sql)
    assert "`group`" in insert_sql  # 保留字反引号
    assert ", 0," in insert_sql  # quota 恒 0
    assert "'', :now, 0, 0, :now, :now" in insert_sql  # fail_reason='' + 时间列补零
    p = session.last_params("INSERT INTO tasks")
    assert p["platform"] == "gw_kling"
    assert p["user_id"] == 42
    assert p["group"] == "vip"
    assert p["channel_id"] == 50
    assert p["status"] == "SUBMITTED" and p["progress"] == "10%"
    assert p["now"] > 0

    # private_data.gateway 键清单全量（SPEC §3.10.1）
    pdata = json.loads(p["private_data"])
    assert pdata["upstream_task_id"] == "up-123"
    gw = pdata["gateway"]
    assert set(gw) == {
        "biz", "form", "sk_hash", "idempotency_key", "callback_url",
        "billing_state", "skip_billing", "deadline_unix", "freeze_shard_seq",
        "freeze_shard_amount_usd", "freeze_shard_expires_at",
        "next_poll_at", "usage_actual", "request_snapshot",
    }
    assert gw["billing_state"] == "frozen"
    assert gw["form"] == "videos"
    assert gw["idempotency_key"] == "idem-1"
    assert gw["freeze_shard_seq"] == 0
    assert gw["next_poll_at"] == 0
    assert gw["usage_actual"] is None
    assert gw["request_snapshot"]["model"] == "kling-v3"
    assert gw["deadline_unix"] > p["now"]
    props = json.loads(p["properties"])
    assert props["origin_model_name"] == "kling-v3"

    # 反查索引入 Redis（决策 A-2），DB 单次 commit（无自有表写入）
    assert session.count("gateway_task_upstream_index") == 0
    assert session.commits == 1

    # Redis 台账
    assert redis.strings[f"cb:cap:{tid}"]
    shard = redis.hashes[f"freeze:shard:{tid}"]
    assert shard["seq"] == "0" and shard["amount_usd"] == "0.5"
    assert int(shard["expires_at"]) > out["created_at"]
    assert redis.expires[f"freeze:shard:{tid}"] == 172800
    # 回调反查索引 tidx:{biz}:{upstream_task_id} → task_id（决策 A-2）
    assert redis.strings["tidx:kling-biz:up-123"] == tid
    # sksess：raw sk 写入（EX=deadline+1h），后台流程 user_sk 取回凭据
    assert redis.strings[f"sksess:{tid}"] == "sk-testtoken"
    assert redis.expires[f"sksess:{tid}"] > 0


async def test_submit_task_idem_key_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    """幂等语义分工：回放识别在 W1 idempotency_guard（Redis NX 占位），
    W2 的职责是把 idem_key 落 private_data.gateway.idempotency_key 供审计/
    排查；同一 idem_key 的两次真实调用仍是两个独立任务（W1 保证不会到达）。"""
    tm, *_ = make_manager(monkeypatch)
    for _ in range(2):
        session = FakeSession()
        await tm.submit_task(
            session,
            biz_cfg=make_biz_cfg(),
            req=make_req(),
            token=make_token(),
            form="videos",
            idem_key="same-key",
        )
        pdata = json.loads(session.last_params("INSERT INTO tasks")["private_data"])
        assert pdata["gateway"]["idempotency_key"] == "same-key"


async def test_submit_system_identity_skips_billing(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """skip 路径（is_system 系统身份）：计费 no-op——零 freeze、
    tasks 行 billing_state=none + skip_billing 标记，其余字段纪律不变。"""
    tm, adapter, billing, _, _ = make_manager(monkeypatch)
    session = FakeSession()

    out = await tm.submit_task(
        session,
        biz_cfg=make_biz_cfg(),
        req=make_req(),
        token=make_token(is_system=True),
        form="videos",
        idem_key=None,
    )

    assert out["status"] == "queued"
    assert billing.freeze_calls == []          # freeze 调用 0 次
    assert billing.cancel_calls == []
    assert len(adapter.submitted) == 1         # 上游提交照常
    p = session.last_params("INSERT INTO tasks")
    gw = json.loads(p["private_data"])["gateway"]
    assert gw["billing_state"] == "none"       # 终态不再入队计费 outbox
    assert gw["skip_billing"] is True


async def test_transition_skip_billing_delivery_only(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """skip 路径终态：无计费 outbox（op no-op），仅用户回调 delivery 入队。"""
    tm, *_ = make_manager(monkeypatch)
    outbox_calls, delivery_calls = capture_finalize_enqueue(monkeypatch)
    session = FakeSession()
    session.on("FOR UPDATE", lock_row(
        "IN_PROGRESS", gateway_pdata(billing_state="none", skip_billing=True)))
    session.on("UPDATE tasks", FakeResult(rowcount=1))

    won = await tm.transition(
        session, task_id="task_abc", snapshot=success_snapshot(), channel="poll"
    )

    assert won is True
    assert session.commits == 1
    assert outbox_calls == []                  # 计费 outbox 零入队
    assert len(delivery_calls) == 1            # delivery 照常
    assert delivery_calls[0]["url"] == "https://user.example.com/cb"
    assert delivery_calls[0]["event_type"] == "task.succeeded"


async def test_submit_task_402_no_row(monkeypatch: pytest.MonkeyPatch) -> None:
    billing = FakeBilling(freeze_exc=mgr.InsufficientBalance("402"))
    tm, adapter, billing, _, _ = make_manager(monkeypatch, billing=billing)
    session = FakeSession()

    with pytest.raises(PaymentRequired):
        await tm.submit_task(
            session,
            biz_cfg=make_biz_cfg(),
            req=make_req(),
            token=make_token(),
            form="videos",
            idem_key=None,
        )
    assert session.calls == []  # 任务不落库
    assert adapter.submitted == []  # 未触及上游


async def test_submit_task_upstream_failure_cancels(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = FakeAdapter(submit_exc=UpstreamBizError("bad request", code=1001))
    tm, adapter, billing, _, _ = make_manager(monkeypatch, adapter=adapter)
    session = FakeSession()

    with pytest.raises(UpstreamBizError):
        await tm.submit_task(
            session,
            biz_cfg=make_biz_cfg(),
            req=make_req(),
            token=make_token(),
            form="videos",
            idem_key=None,
        )
    # 补偿：cancel 当前分片
    assert len(billing.cancel_calls) == 1
    tid = billing.freeze_calls[0]["request_id"].removesuffix(":0")
    assert billing.cancel_calls[0] == {"request_id": f"{tid}:0", "user_sk": "sk-testtoken"}
    assert session.calls == []
    assert session.commits == 0


async def test_submit_task_insert_failure_cancels(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, _, billing, _, _ = make_manager(monkeypatch)
    session = FakeSession()
    session.on("INSERT INTO tasks", RuntimeError("db down"))

    with pytest.raises(RuntimeError, match="db down"):
        await tm.submit_task(
            session,
            biz_cfg=make_biz_cfg(),
            req=make_req(),
            token=make_token(),
            form="videos",
            idem_key=None,
        )
    assert session.rollbacks == 1
    assert len(billing.cancel_calls) == 1


# ---------------------------------------------------------------------------
# transition
# ---------------------------------------------------------------------------


def lock_row(status: str, pdata: str | None = None) -> FakeResult:
    return FakeResult(
        rows=[{"status": status, "private_data": pdata or gateway_pdata(), "user_id": 42}]
    )


def capture_finalize_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """捕获终态副作用入队（Redis obx/dlv，决策 A-3/A-4）。"""
    outbox_calls: list[dict[str, Any]] = []
    delivery_calls: list[dict[str, Any]] = []

    async def _enqueue_outbox(**kw: Any) -> str:
        outbox_calls.append(kw)
        return "obx_test"

    async def _enqueue_delivery(**kw: Any) -> None:
        delivery_calls.append(kw)

    monkeypatch.setattr("app.billing.outbox.enqueue_outbox", _enqueue_outbox)
    monkeypatch.setattr("app.callbacks.dispatcher.enqueue_delivery", _enqueue_delivery)
    return outbox_calls, delivery_calls


def success_snapshot() -> TaskSnapshot:
    return TaskSnapshot(
        upstream_status="succeeded",
        status=TaskStatus.SUCCEEDED,
        result={"url": "https://cdn.example.com/v.mp4"},
        usage={"completion_tokens": 10, "actual_duration": 5.0},
        error=None,
        event_id="kling:up-123:succeeded:1",
        raw={"status": "succeeded"},
    )


async def test_transition_success_win(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, _, billing, pricing, redis = make_manager(monkeypatch)
    outbox_calls, delivery_calls = capture_finalize_enqueue(monkeypatch)
    redis.hashes["freeze:shard:task_abc"] = {"seq": "0"}
    redis.strings["sksess:task_abc"] = "sk-testtoken"  # 在途任务的 sksess
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS"))
    session.on("UPDATE tasks", FakeResult(rowcount=1))

    won = await tm.transition(
        session, task_id="task_abc", snapshot=success_snapshot(), channel="poll"
    )

    assert won is True
    assert session.commits == 1
    assert session.rollbacks == 0

    # CAS UPDATE 形制
    upd_sql = next(sql for sql, _ in session.calls if sql.startswith("UPDATE tasks"))
    assert "platform LIKE :gw_like" in upd_sql
    assert "status = :old_status" in upd_sql
    assert "NOT IN ('SUCCESS', 'FAILURE')" in upd_sql
    up = session.last_params("UPDATE tasks")
    assert up["status"] == "SUCCESS" and up["progress"] == "100%"
    assert up["old_status"] == "IN_PROGRESS"
    assert up["gw_like"] == GW_PLATFORM_LIKE
    assert up["mark_terminal"] == 1 and up["mark_failing"] == 0
    assert up["data"] is not None
    pdata = json.loads(up["private_data"])
    assert pdata["result_url"] == "https://cdn.example.com/v.mp4"
    assert pdata["gateway"]["usage_actual"] == {"completion_tokens": 10, "actual_duration": 5.0}

    # settle 求值 phase=settle，实收上下文含同名键（§5.3）
    settle_call = pricing.eval_calls[-1]
    assert settle_call["phase"] == "settle"
    assert settle_call["context"]["usage_tokens"] == 10.0
    assert settle_call["context"]["duration"] == 5.0

    # outbox settle 条（commit 后入队 Redis obx 队列，决策 A-4）
    assert len(outbox_calls) == 1
    ob = outbox_calls[0]
    assert ob["task_id"] == "task_abc" and ob["op"] == "settle"
    payload = ob["payload"]
    assert payload["request_id"] == "task_abc:0"
    assert payload["actual_amount"] == "0.2"
    assert payload["reevaluate"] is False
    assert payload["cancel_prev_shards"] == []
    assert payload["user_id"] == 42
    # sksess 随 payload 携带（终态后 sksess 即清除，outbox 重放仍能取 sk）
    assert payload["user_sk"] == "sk-testtoken"
    assert "sksess:task_abc" not in redis.strings  # 终态清除

    # delivery 条（有 callback_url；入队 Redis dlv 队列，决策 A-3）
    assert len(delivery_calls) == 1
    dl = delivery_calls[0]
    assert dl["url"] == "https://user.example.com/cb"
    assert dl["event_type"] == "task.succeeded"
    assert dl["delivery_id"].startswith("evt_")
    env = dl["envelope"]
    assert env["id"] == dl["delivery_id"]
    assert env["data"]["status"] == "completed"
    assert env["data"]["url"] == "https://cdn.example.com/v.mp4"
    assert session.count("gateway_") == 0  # 零自有表：无任何 gateway_ SQL


async def test_transition_cas_race_lost(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, *_ = make_manager(monkeypatch)
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS"))
    session.on("UPDATE tasks", FakeResult(rowcount=0))  # 另一通道已推进

    won = await tm.transition(
        session, task_id="task_abc", snapshot=success_snapshot(), channel="callback"
    )

    assert won is False
    assert session.rollbacks == 1
    assert session.commits == 0
    assert session.count("INSERT INTO gateway_billing_outbox") == 0
    assert session.count("INSERT INTO gateway_callback_deliveries") == 0


async def test_transition_terminal_irreversible(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, *_ = make_manager(monkeypatch)
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("SUCCESS"))

    won = await tm.transition(
        session, task_id="task_abc", snapshot=success_snapshot(), channel="poll"
    )

    assert won is False
    assert session.count("UPDATE tasks") == 0  # 终态行不再 UPDATE


async def test_transition_out_of_order_no_regress(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, *_ = make_manager(monkeypatch)
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS"))
    stale = TaskSnapshot(
        upstream_status="submitted",
        status=TaskStatus.QUEUED,
        result=None,
        usage=None,
        error=None,
        event_id="kling:up-123:submitted:0",
    )

    won = await tm.transition(session, task_id="task_abc", snapshot=stale, channel="poll")

    assert won is False
    assert session.count("UPDATE tasks") == 0  # rank 不回退


async def test_transition_missing_row(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, *_ = make_manager(monkeypatch)
    session = FakeSession()  # FOR UPDATE 无行

    won = await tm.transition(
        session, task_id="task_ghost", snapshot=success_snapshot(), channel="callback"
    )
    assert won is False
    assert session.count("UPDATE tasks") == 0


async def test_transition_settle_eval_failure_reevaluate(monkeypatch: pytest.MonkeyPatch) -> None:
    pricing = FakePricing(settle_exc=mgr.PricingEvalError("sandbox down"))
    tm, _, _, pricing, redis = make_manager(monkeypatch, pricing=pricing)
    outbox_calls, _ = capture_finalize_enqueue(monkeypatch)
    redis.hashes["freeze:shard:task_abc"] = {"seq": "0"}
    redis.strings["sksess:task_abc"] = "sk-testtoken"  # 在途任务的 sksess
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS"))
    session.on("UPDATE tasks", FakeResult(rowcount=1))

    won = await tm.transition(
        session, task_id="task_abc", snapshot=success_snapshot(), channel="poll"
    )

    assert won is True  # 状态仍推进，金额交给补偿 worker 重估
    payload = outbox_calls[0]["payload"]
    assert payload["actual_amount"] is None
    assert payload["reevaluate"] is True


async def test_transition_failed_cancel_outbox(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, _, _, _, redis = make_manager(monkeypatch)
    outbox_calls, delivery_calls = capture_finalize_enqueue(monkeypatch)
    redis.hashes["freeze:shard:task_abc"] = {"seq": "2"}  # 已续期到第 2 片
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS", gateway_pdata(callback_url=None)))
    session.on("UPDATE tasks", FakeResult(rowcount=1))
    snap = TaskSnapshot(
        upstream_status="failed",
        status=TaskStatus.FAILED,
        result=None,
        usage=None,
        error={"code": "E1", "message": "boom"},
        event_id="kling:up-123:failed:2",
    )

    won = await tm.transition(session, task_id="task_abc", snapshot=snap, channel="poll")

    assert won is True
    up = session.last_params("UPDATE tasks")
    assert up["status"] == "FAILURE" and up["mark_failing"] == 1
    assert up["reason"] == "failed: boom"

    ob = outbox_calls[0]
    assert ob["op"] == "cancel"
    payload = ob["payload"]
    assert payload["request_id"] == "task_abc:2"  # 打当前活跃分片
    assert payload["cancel_prev_shards"] == ["task_abc:0", "task_abc:1"]
    # 无 callback_url → 无 delivery 条
    assert delivery_calls == []


async def test_transition_timeout_reason_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, _, _, _, redis = make_manager(monkeypatch)
    outbox_calls, _ = capture_finalize_enqueue(monkeypatch)
    redis.hashes["freeze:shard:task_abc"] = {"seq": "0"}
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS", gateway_pdata(callback_url=None)))
    session.on("UPDATE tasks", FakeResult(rowcount=1))
    snap = TaskSnapshot(
        upstream_status="gateway_deadline",
        status=TaskStatus.TIMEOUT,
        result=None,
        usage=None,
        error={"code": "deadline_exceeded", "message": "deadline 100 exceeded"},
        event_id="gw:task_abc:timeout:1",
    )

    won = await tm.transition(session, task_id="task_abc", snapshot=snap, channel="sweep")

    assert won is True
    assert session.last_params("UPDATE tasks")["reason"].startswith("timeout: ")
    assert outbox_calls[0]["op"] == "cancel"


async def test_transition_running_start_time(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, *_ = make_manager(monkeypatch)
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("SUBMITTED"))
    session.on("UPDATE tasks", FakeResult(rowcount=1))
    snap = TaskSnapshot(
        upstream_status="processing",
        status=TaskStatus.RUNNING,
        result=None,
        usage=None,
        error=None,
        event_id="kling:up-123:processing:1",
    )

    won = await tm.transition(session, task_id="task_abc", snapshot=snap, channel="poll")

    assert won is True
    up = session.last_params("UPDATE tasks")
    assert up["status"] == "IN_PROGRESS" and up["progress"] == "30%"
    assert up["mark_in_progress"] == 1 and up["mark_terminal"] == 0
    # 非终态无副作用行
    assert session.count("INSERT INTO gateway_billing_outbox") == 0


# ---------------------------------------------------------------------------
# current_freeze_shard 两级读取
# ---------------------------------------------------------------------------


async def test_current_freeze_shard_redis_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()
    redis.hashes["freeze:shard:task_abc"] = {"seq": "3"}

    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(mgr, "get_redis", _get_redis)
    session = FakeSession()
    assert await current_freeze_shard(session, "task_abc") == 3
    assert session.calls == []  # Redis 命中不读 DB


async def test_current_freeze_shard_db_fallback_rebuild(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(mgr, "get_redis", _get_redis)
    session = FakeSession()
    session.on("freeze_shard_seq", FakeResult(rows=[{"seq": "5"}]))

    assert await current_freeze_shard(session, "task_abc") == 5
    sql = session.calls[0][0]
    assert "platform LIKE :gw_like" in sql
    assert redis.hashes["freeze:shard:task_abc"]["seq"] == "5"  # 顺带重建热台账


async def test_current_freeze_shard_double_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = FakeRedis()

    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(mgr, "get_redis", _get_redis)
    session = FakeSession()
    assert await current_freeze_shard(session, "task_abc") == 0  # 双失回退 0


# ---------------------------------------------------------------------------
# track_passthrough_task
# ---------------------------------------------------------------------------


async def test_track_passthrough_task(monkeypatch: pytest.MonkeyPatch) -> None:
    tm, _, billing, _, redis = make_manager(monkeypatch)
    session = FakeSession()

    tid = await tm.track_passthrough_task(
        session,
        biz_cfg=make_biz_cfg(),
        token=make_token(),
        upstream_task_id="up-pt-9",
        action="textGenerate",
        request_snapshot={"model": "kling-v3", "prompt": "pt prompt", "callback_url": None},
        raw_response={"data": {"task_id": "up-pt-9"}},
    )

    assert tid.startswith("task_")
    assert billing.freeze_calls == []  # 透传 tracked 不 freeze
    p = session.last_params("INSERT INTO tasks")
    assert p["platform"] == "gw_kling"
    assert p["action"] == "textGenerate"
    pdata = json.loads(p["private_data"])
    assert pdata["upstream_task_id"] == "up-pt-9"
    gw = pdata["gateway"]
    assert gw["form"] == "passthrough_tracked"
    assert gw["billing_state"] == "none"
    assert gw["freeze_shard_seq"] == 0 and gw["next_poll_at"] == 0
    assert gw["request_snapshot"]["prompt"] == "pt prompt"
    # 反查索引入 Redis（决策 A-2）
    assert session.count("gateway_task_upstream_index") == 0
    assert redis.strings["tidx:kling-biz:up-pt-9"] == tid
    assert session.commits == 1


# ---------------------------------------------------------------------------
# resolve_submit_secrets（auth_secret_ref → SubmitContext.secrets）
# ---------------------------------------------------------------------------


async def test_resolve_submit_secrets_aksk(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.tasks.manager import resolve_submit_secrets

    monkeypatch.setenv("UPSTREAM_SECRET_KLING_AK", "ak-env")
    monkeypatch.setenv("UPSTREAM_SECRET_KLING_SK", "sk-env")
    assert await resolve_submit_secrets(make_biz_cfg()) == {"ak": "ak-env", "sk": "sk-env"}


async def test_resolve_submit_secrets_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.tasks.manager import resolve_submit_secrets

    monkeypatch.setenv("UPSTREAM_KEY_ARK", "ark-env-key")
    cfg = make_biz_cfg()
    cfg.auth_type = "bearer_key"
    cfg.auth_secret_ref = "UPSTREAM_KEY_ARK"
    assert await resolve_submit_secrets(cfg) == {"api_key": "ark-env-key"}


async def test_resolve_submit_secrets_case_insensitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """环境键名大小写兼容（{ref}_ak 小写也能解析）。"""
    from app.tasks.manager import resolve_submit_secrets

    monkeypatch.delenv("UPSTREAM_SECRET_KLING_AK", raising=False)
    monkeypatch.delenv("UPSTREAM_SECRET_KLING_SK", raising=False)
    monkeypatch.setenv("upstream_secret_kling_ak", "ak-lower")
    monkeypatch.setenv("upstream_secret_kling_sk", "sk-lower")
    assert await resolve_submit_secrets(make_biz_cfg()) == {
        "ak": "ak-lower", "sk": "sk-lower"}


async def test_resolve_submit_secrets_missing_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """缺失即明确报错（绝不空凭证放行：kling 必抛、seedance 必 401）。"""
    from app.tasks.manager import resolve_submit_secrets

    monkeypatch.delenv("UPSTREAM_SECRET_KLING_AK", raising=False)
    monkeypatch.delenv("UPSTREAM_SECRET_KLING_SK", raising=False)
    with pytest.raises(RuntimeError, match="UPSTREAM_SECRET_KLING_AK"):
        await resolve_submit_secrets(make_biz_cfg())


async def test_submit_task_missing_credentials_fails_before_freeze(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """凭证缺失：freeze 前明确报错——不预冻、不落库、不调上游。"""
    tm, adapter, billing, _, _ = make_manager(monkeypatch)
    monkeypatch.delenv("UPSTREAM_SECRET_KLING_AK", raising=False)
    monkeypatch.delenv("UPSTREAM_SECRET_KLING_SK", raising=False)
    session = FakeSession()
    with pytest.raises(RuntimeError, match="credentials missing"):
        await tm.submit_task(
            session, biz_cfg=make_biz_cfg(), req=make_req(),
            token=make_token(), form="videos", idem_key=None,
        )
    assert billing.freeze_calls == []
    assert adapter.submitted == []
    assert session.count("INSERT INTO tasks") == 0


# ---------------------------------------------------------------------------
# service_tier 口径：request_snapshot.extra（与 outbox settle 重估对齐）
# ---------------------------------------------------------------------------


async def test_transition_settle_service_tier_from_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """service_tier 从 request_snapshot.extra 取（顶层恒无此键，恒 default 是 bug）。"""
    tm, _, _, pricing, redis = make_manager(monkeypatch)
    capture_finalize_enqueue(monkeypatch)
    redis.hashes["freeze:shard:task_abc"] = {"seq": "0"}
    snapshot_with_tier = gateway_pdata(request_snapshot={
        "model": "kling-v3", "action": "textGenerate", "n": 1,
        "extra": {"service_tier": "flex"},
    })
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS", pdata=snapshot_with_tier))
    session.on("UPDATE tasks", FakeResult(rowcount=1))

    won = await tm.transition(
        session, task_id="task_abc", snapshot=success_snapshot(), channel="poll"
    )

    assert won is True
    settle_call = pricing.eval_calls[-1]
    assert settle_call["phase"] == "settle"
    assert settle_call["context"]["service_tier"] == "flex"


async def test_transition_settle_service_tier_default_when_no_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tm, _, _, pricing, redis = make_manager(monkeypatch)
    redis.hashes["freeze:shard:task_abc"] = {"seq": "0"}
    session = FakeSession()
    session.on("FOR UPDATE", lock_row("IN_PROGRESS"))
    session.on("UPDATE tasks", FakeResult(rowcount=1))

    await tm.transition(
        session, task_id="task_abc", snapshot=success_snapshot(), channel="poll"
    )
    assert pricing.eval_calls[-1]["context"]["service_tier"] == "default"
