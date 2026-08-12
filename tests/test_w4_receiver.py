"""W4 receiver 测试（SPEC §6 验收口径）：

capability 校验两路径（external_task_id 回显直取 / 索引表反查+platform 双保险）、
错误 capability 401、重复事件 200 幂等 ACK、坏报文 400、HMAC 验签时间窗、
not-found 延迟重试后丢弃、可靠队列消费语义。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.adapters.base import TaskSnapshot, TaskStatus, register
from app.callbacks import receiver
from app.db import get_session

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeRedis:
    """按 SPEC §3.6 key 语义模拟的进程内 Redis。"""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.expires: dict[str, int] = {}
        self.lists: dict[str, list[str]] = {}
        self.counts: dict[str, int] = {}
        self.lrem_calls: list[tuple[str, str]] = []

    async def incr(self, key: str) -> int:
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]

    async def expire(self, key: str, ttl: int) -> None:
        self.expires[key] = ttl

    async def get(self, key: str) -> str | None:
        return self.strings.get(key)

    async def set(self, key: str, value: Any, nx: bool = False, ex: int | None = None):
        if nx and key in self.strings:
            return False
        self.strings[key] = str(value)
        if ex:
            self.expires[key] = ex
        return True

    async def rpush(self, key: str, payload: str) -> int:
        self.lists.setdefault(key, []).append(payload)
        return len(self.lists[key])

    async def brpoplpush(
        self, src: str, dst: str, timeout: int = 0  # noqa: ASYNC109 - 模拟 redis 签名
    ):
        if self.lists.get(src):
            payload = self.lists[src].pop(0)
            self.lists.setdefault(dst, []).append(payload)
            return payload
        return None

    async def lrem(self, key: str, count: int, value: str) -> int:
        self.lrem_calls.append((key, value))
        lst = self.lists.get(key, [])
        if value in lst:
            lst.remove(value)
            return 1
        return 0


class FakeResult:
    def __init__(self, row: Any) -> None:
        self._row = row

    def first(self):
        return self._row


class FakeSession:
    """按 SQL 文本分派的假会话：索引表查询 / tasks 回表校验。

    ``task_unlock_at``：前 N 次 tasks 查询返回 None（模拟提交事务尚未 commit）。
    """

    def __init__(
        self,
        index_rows: dict[tuple[str, str], str] | None = None,
        task_rows: set[str] | None = None,
        task_unlock_at: int = 0,
    ) -> None:
        self.index_rows = index_rows or {}
        self.task_rows = task_rows or set()
        self.task_unlock_at = task_unlock_at
        self.task_queries = 0

    async def execute(self, stmt: Any, params: dict | None = None):
        sql = str(stmt)
        params = params or {}
        if "JSON_EXTRACT(private_data" in sql:
            # tidx miss 时的 SQL 兜底（决策 A-2）：按 upstream_task_id 反查
            self.task_queries += 1
            for (_plat, uid), tid in self.index_rows.items():
                if uid == params.get("u"):
                    return FakeResult(SimpleNamespace(task_id=tid))
            return FakeResult(None)
        if "FROM tasks" in sql:
            self.task_queries += 1
            hit = self.task_queries > self.task_unlock_at and params["id"] in self.task_rows
            return FakeResult(
                SimpleNamespace(task_id=params["id"]) if hit else None
            )
        return FakeResult(None)

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False


class FakeAdapter:
    """最小适配器（SPEC §3.2 协议 mock）：bad 标记抛异常，事件 ID 按 §7.1 组合。"""

    name = "fakeup"
    callback_capability = True
    echoes_external_task_id = True

    def parse_callback(self, raw_body: bytes, headers: Any) -> TaskSnapshot:
        body = json.loads(raw_body)
        if body.get("bad"):
            raise ValueError("bad payload")
        inner = body.get("data", body)
        status = inner.get("status", "succeeded")
        uid = inner.get("id") or inner.get("external_task_id") or "unknown"
        return TaskSnapshot(
            upstream_status=status,
            status=TaskStatus.SUCCEEDED,
            result={"url": "https://cdn/x.mp4"},
            usage=None,
            error=None,
            event_id=f"fakeup:{uid}:{status}:{inner.get('updated_at', 0)}",
        )


class FakeTaskManager:
    """按 SPEC §3.10.1 transition 签名记录调用。"""

    def __init__(self, won: bool = True) -> None:
        self.won = won
        self.calls: list[dict[str, Any]] = []

    async def transition(self, session: Any, *, task_id: str, snapshot: Any, channel: str):
        self.calls.append({"task_id": task_id, "snapshot": snapshot, "channel": channel})
        return self.won


register(FakeAdapter())


# ---------------------------------------------------------------------------
# 端点测试装配
# ---------------------------------------------------------------------------


@pytest.fixture
def endpoint(monkeypatch: pytest.MonkeyPatch):
    redis = FakeRedis()
    session = FakeSession()

    async def _get_redis() -> FakeRedis:
        return redis

    monkeypatch.setattr(receiver, "get_redis", _get_redis)
    app = FastAPI()
    app.include_router(receiver.router)
    app.dependency_overrides[get_session] = lambda: session
    return SimpleNamespace(client=TestClient(app), redis=redis, session=session)


ECHO_BODY = json.dumps(
    {"data": {"external_task_id": "task_1", "id": "up-1",
              "status": "succeeded", "updated_at": 100}}
).encode()


def test_capability_echo_path_202(endpoint) -> None:
    """路径①：external_task_id 回显直取 → cb:cap 校验通过 → 入队 → 202。"""
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    resp = endpoint.client.post("/callbacks/kling/fakeup/cap-good", content=ECHO_BODY)
    assert resp.status_code == 202
    queued = endpoint.redis.lists[receiver.QUEUE_KEY]
    assert len(queued) == 1
    msg = json.loads(queued[0])
    assert msg["biz"] == "kling" and msg["provider"] == "fakeup"
    assert msg["event_id"] == "fakeup:up-1:succeeded:100"
    assert endpoint.redis.expires["wh:seen:fakeup:up-1:succeeded:100"] == 86400


def test_capability_index_path_redis_hit_with_double_check(endpoint) -> None:
    """路径②Redis 命中：tidx:{biz}:{uid} → 回表双保险校验 → 202（决策 A-2）。"""
    endpoint.redis.strings["tidx:vid:cgt-abc"] = "task_9"
    endpoint.session.task_rows.add("task_9")
    endpoint.redis.strings["cb:cap:task_9"] = "cap-x"
    body = json.dumps({"data": {"id": "cgt-abc", "status": "succeeded"}}).encode()
    resp = endpoint.client.post("/callbacks/vid/fakeup/cap-x", content=body)
    assert resp.status_code == 202
    assert endpoint.session.task_queries == 1  # 回表双保险（防脏索引串号）


def test_capability_index_path_sql_fallback_and_rewarm(endpoint) -> None:
    """路径②Redis miss：SQL 兜底（JSON_EXTRACT private_data + 7d 窗口）命中
    → 回热 tidx → 202（决策 A-2）。"""
    endpoint.session.index_rows[("gw_fakeup", "cgt-abc")] = "task_9"
    endpoint.redis.strings["cb:cap:task_9"] = "cap-x"
    body = json.dumps({"data": {"id": "cgt-abc", "status": "succeeded"}}).encode()
    resp = endpoint.client.post("/callbacks/vid/fakeup/cap-x", content=body)
    assert resp.status_code == 202
    assert endpoint.session.task_queries == 1
    # 命中回热：后续回调直接走 Redis 快路径
    assert endpoint.redis.strings["tidx:vid:cgt-abc"] == "task_9"


def test_capability_index_dirty_row_rejected(endpoint) -> None:
    """双保险：脏索引命中但 tasks 回表校验失败（非自有行）→ 401。"""
    endpoint.redis.strings["tidx:vid:cgt-abc"] = "task_9"
    # task_rows 不含 task_9 → 回表校验失败
    endpoint.redis.strings["cb:cap:task_9"] = "cap-x"
    body = json.dumps({"data": {"id": "cgt-abc", "status": "succeeded"}}).encode()
    resp = endpoint.client.post("/callbacks/vid/fakeup/cap-x", content=body)
    assert resp.status_code == 401


def test_capability_wrong_401(endpoint) -> None:
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    resp = endpoint.client.post("/callbacks/kling/fakeup/cap-WRONG", content=ECHO_BODY)
    assert resp.status_code == 401
    assert receiver.QUEUE_KEY not in endpoint.redis.lists


def test_capability_unresolvable_task_401(endpoint) -> None:
    """回调体无法反查出 task_id（无回显、无上游 id）→ 401。"""
    body = json.dumps({"data": {"status": "succeeded"}}).encode()
    resp = endpoint.client.post("/callbacks/kling/fakeup/cap-good", content=body)
    assert resp.status_code == 401


def test_duplicate_event_200_idempotent_ack(endpoint) -> None:
    """重复事件：第二次 SET NX 失败 → 200 幂等 ACK，且不再入队。"""
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    url = "/callbacks/kling/fakeup/cap-good"
    assert endpoint.client.post(url, content=ECHO_BODY).status_code == 202
    resp = endpoint.client.post(url, content=ECHO_BODY)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "duplicate": True}
    assert len(endpoint.redis.lists[receiver.QUEUE_KEY]) == 1


def test_bad_payload_400(endpoint) -> None:
    """坏报文（adapter.parse_callback 抛异常）→ 400，上游不应重试。"""
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    body = json.dumps({"bad": True, "data": {"external_task_id": "task_1"}}).encode()
    resp = endpoint.client.post("/callbacks/kling/fakeup/cap-good", content=body)
    assert resp.status_code == 400
    assert receiver.QUEUE_KEY not in endpoint.redis.lists


def _stripe_sig(secret: str, ts: int, raw: bytes) -> str:
    v1 = hmac.new(secret.encode(), f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()
    return f"t={ts},v1={v1}"


def test_hmac_replay_window(endpoint, monkeypatch: pytest.MonkeyPatch) -> None:
    """HMAC 框架：时间戳超 ±300s 重放窗拒绝；窗内正确签名放行。"""
    monkeypatch.setenv("UPSTREAM_CALLBACK_SECRET_FAKEUP", "s3cret")
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    url = "/callbacks/kling/fakeup/cap-good"

    stale = int(time.time()) - 1000
    resp = endpoint.client.post(
        url, content=ECHO_BODY,
        headers={"X-Signature": _stripe_sig("s3cret", stale, ECHO_BODY)},
    )
    assert resp.status_code == 401

    now = int(time.time())
    resp = endpoint.client.post(
        url, content=ECHO_BODY,
        headers={"X-Signature": _stripe_sig("s3cret", now, ECHO_BODY)},
    )
    assert resp.status_code == 202


def test_hmac_wrong_signature_and_github_format(endpoint, monkeypatch: pytest.MonkeyPatch) -> None:
    """错误签名 401；GitHub 形制 sha256= + X-Timestamp 亦可验。"""
    monkeypatch.setenv("UPSTREAM_CALLBACK_SECRET_FAKEUP", "s3cret")
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    url = "/callbacks/kling/fakeup/cap-good"

    now = int(time.time())
    resp = endpoint.client.post(
        url, content=ECHO_BODY, headers={"X-Signature": f"t={now},v1=deadbeef"}
    )
    assert resp.status_code == 401

    hexdigest = hmac.new(
        b"s3cret", f"{now}.".encode() + ECHO_BODY, hashlib.sha256
    ).hexdigest()
    resp = endpoint.client.post(
        url, content=ECHO_BODY,
        headers={"X-Hub-Signature-256": f"sha256={hexdigest}", "X-Timestamp": str(now)},
    )
    assert resp.status_code == 202


def test_hmac_header_without_secret_fail_closed(endpoint) -> None:
    """带签名头但 provider 未配置密钥 → fail-closed 401（协议已变的信号）。"""
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    resp = endpoint.client.post(
        "/callbacks/kling/fakeup/cap-good", content=ECHO_BODY,
        headers={"X-Signature": "t=1,v1=abc"},
    )
    assert resp.status_code == 401


def test_rate_limit_429(endpoint, monkeypatch: pytest.MonkeyPatch) -> None:
    """端点级防刷固定窗口超限 → 429（验签 CPU 保护）。"""
    monkeypatch.setattr(receiver, "CALLBACK_RATE_LIMIT_PER_MINUTE", 1)
    endpoint.redis.strings["cb:cap:task_1"] = "cap-good"
    url = "/callbacks/kling/fakeup/cap-good"
    assert endpoint.client.post(url, content=ECHO_BODY).status_code == 202
    assert endpoint.client.post(url, content=ECHO_BODY).status_code == 429


# ---------------------------------------------------------------------------
# 消费侧：process_upstream_callback / CallbackQueueConsumer
# ---------------------------------------------------------------------------


def _msg(raw: bytes = ECHO_BODY) -> dict[str, Any]:
    return {
        "biz": "kling",
        "provider": "fakeup",
        "raw": raw.decode(),
        "event_id": "fakeup:up-1:succeeded:100",
        "received_at": time.time(),
    }


async def test_process_callback_drives_transition(monkeypatch: pytest.MonkeyPatch) -> None:
    """反查命中 → transition(channel='callback')，快照来自 adapter.parse_callback。"""
    tm = FakeTaskManager()
    session = FakeSession(task_rows={"task_1"})
    monkeypatch.setattr(receiver, "task_manager", tm)
    monkeypatch.setattr(receiver, "_session_factory", lambda: session)

    await receiver.process_upstream_callback(_msg())

    assert len(tm.calls) == 1
    call = tm.calls[0]
    assert call["task_id"] == "task_1"
    assert call["channel"] == "callback"
    assert call["snapshot"].status is TaskStatus.SUCCEEDED


async def test_process_callback_not_found_retries_then_drops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """not-found 按 [2,5,15,30,60] 延迟重试，仍无 → 丢弃（不调 transition）。"""
    tm = FakeTaskManager()
    session = FakeSession()  # task_rows 空：永远查不到
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(receiver.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(receiver, "task_manager", tm)
    monkeypatch.setattr(receiver, "_session_factory", lambda: session)

    await receiver.process_upstream_callback(_msg())

    assert sleeps == receiver.NOT_FOUND_RETRY_DELAYS == [2, 5, 15, 30, 60]
    assert session.task_queries == 1 + len(receiver.NOT_FOUND_RETRY_DELAYS)
    assert tm.calls == []


async def test_process_callback_found_after_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """回调先于 tasks 行 commit：前两次查不到，第三次可见 → 正常 transition。"""
    tm = FakeTaskManager()
    session = FakeSession(task_rows={"task_1"}, task_unlock_at=2)
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(receiver.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(receiver, "task_manager", tm)
    monkeypatch.setattr(receiver, "_session_factory", lambda: session)

    await receiver.process_upstream_callback(_msg())

    assert sleeps == [2, 5]  # 第 3 次（第 2 次重试后）命中即停
    assert len(tm.calls) == 1


async def test_consumer_reliable_queue_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    """BRPOPLPUSH → 处理成功 → LREM 摘除 processing；处理失败 → 保留待恢复。"""
    tm = FakeTaskManager()
    session = FakeSession(task_rows={"task_1"})
    monkeypatch.setattr(receiver, "task_manager", tm)
    monkeypatch.setattr(receiver, "_session_factory", lambda: session)

    redis = FakeRedis()
    payload = json.dumps(_msg())
    consumer = receiver.CallbackQueueConsumer(tm, lambda: session)
    try:
        # 成功路径：LREM 摘除
        await redis.rpush(receiver.QUEUE_KEY, payload)
        moved = await redis.brpoplpush(
            receiver.QUEUE_KEY, receiver.PROCESSING_QUEUE_KEY, timeout=0
        )
        await consumer._consume_one(redis, moved)
        assert (receiver.PROCESSING_QUEUE_KEY, moved) in redis.lrem_calls

        # 失败路径（未注册 provider → process 抛异常）：留在 processing，不 LREM
        bad = json.dumps({"biz": "b", "provider": "nope", "raw": "{}", "event_id": "e"})
        before = len(redis.lrem_calls)
        await consumer._consume_one(redis, bad)
        assert len(redis.lrem_calls) == before
    finally:
        receiver.set_task_manager(None)
        receiver.set_session_factory(None)
