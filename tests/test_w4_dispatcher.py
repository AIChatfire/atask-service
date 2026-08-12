"""W4 dispatcher 测试（SPEC §6 验收口径；零自有表决策 A-3：Redis 延迟队列）。

X-Signature Stripe 形制（原始字节 + 双密钥轮换）、投递状态机
pending→delivering(lease)→delivered|dead（dlv:due/dlv:{id}/dlv:lease/dlv:dead，
Lua 原子领取 + 租约回收）、指数退避序列 [1m,5m,30m,2h,6h]+jitter、
2xx 停重试 / 410 与其余 4xx 死信 / 408·429·5xx·超时重排、域级熔断 open 直接
重排不投递、死信人工重放入口。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from typing import Any

import httpx
import pytest

from app import redis_queue
from app.callbacks import dispatcher
from app.callbacks.dispatcher import NS, enqueue_delivery
from app.config import settings
from tests.conftest import FakeRedis

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeResp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class FakeDeliveryClient:
    """按队列返回状态码；元素为 Exception 实例时抛出（模拟超时/传输错误）。"""

    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    async def post(
        self,
        url: str,
        content: bytes | None = None,
        headers: dict | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 - 模拟 httpx post 签名
    ):
        self.calls.append(
            {"url": url, "content": content, "headers": headers, "timeout": timeout}
        )
        outcome = self.outcomes.pop(0) if self.outcomes else 200
        if isinstance(outcome, Exception):
            raise outcome
        return FakeResp(outcome)


def _envelope() -> dict[str, Any]:
    return {"id": "evt_01JABC", "type": "task.succeeded"}


async def _seed(qredis: FakeRedis, attempts: int = 0) -> dict[str, Any]:
    """入队一条 delivery 并返回 dispatcher._deliver 期望的条目 dict。"""
    await enqueue_delivery(
        delivery_id="evt_01JABC", task_id="task_1", user_id=42,
        url="https://user.example.com/hook", event_type="task.succeeded",
        envelope=_envelope(),
    )
    if attempts:
        await qredis.hset(f"{NS}:evt_01JABC", "attempts", str(attempts))
    return {
        "id": "evt_01JABC",
        "task_id": "task_1",
        "user_id": 42,
        "url": "https://user.example.com/hook",
        "event_type": "task.succeeded",
        "payload_json": json.dumps(_envelope(), ensure_ascii=False),
        "attempts": attempts,
    }


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch):
    """装配：内存 Redis 队列 + 假投递客户端 + 零 jitter + 域级熔断默认关闭。"""
    qredis = FakeRedis()

    async def _get_redis() -> FakeRedis:
        return qredis

    monkeypatch.setattr(dispatcher, "get_redis", _get_redis)
    client = FakeDeliveryClient([])
    monkeypatch.setattr(dispatcher, "delivery_client", lambda: client)

    async def _circuit_closed(domain: str) -> bool:
        return False

    monkeypatch.setattr(dispatcher, "_domain_circuit_open", _circuit_closed)
    monkeypatch.setattr(dispatcher.random, "uniform", lambda a, b: 0.0)
    # 域级熔断记账（§8.2）：投递结果接 on_success/on_failure 的记录器
    circuit_calls: list[tuple[str, str]] = []

    async def _on_success(domain: str) -> None:
        circuit_calls.append(("success", domain))

    async def _on_failure(domain: str) -> None:
        circuit_calls.append(("failure", domain))

    monkeypatch.setattr(dispatcher, "_domain_on_success", _on_success)
    monkeypatch.setattr(dispatcher, "_domain_on_failure", _on_failure)
    disp = dispatcher.DeliveryDispatcher()
    disp.circuit_calls = circuit_calls  # type: ignore[attr-defined]
    return qredis, client, disp


async def _item(qredis: FakeRedis, item_id: str = "evt_01JABC") -> dict[str, str]:
    item = await redis_queue.get_item(qredis, NS, item_id)
    assert item is not None, "queue item vanished unexpectedly"
    return item


def _due_score_delay(qredis: FakeRedis, item_id: str, before: float) -> float:
    """due ZSET score 相对调用前的延迟秒数。"""
    scores = dict(qredis._data[f"{NS}:due"])
    return scores[item_id] - before


# ---------------------------------------------------------------------------
# sign_headers
# ---------------------------------------------------------------------------


def test_sign_headers_stripe_format_raw_bytes() -> None:
    """t={ts},v1={hmac}；对原始字节计算；X-Delivery-Id = delivery id。"""
    raw = b'{"id":"evt_1","data":{"a":1}}'
    headers = dispatcher.sign_headers(42, raw, "evt_1")
    assert headers["X-Delivery-Id"] == "evt_1"
    m = re.fullmatch(r"t=(\d+),v1=([0-9a-f]{64})", headers["X-Signature"])
    assert m, headers["X-Signature"]
    ts, v1 = int(m.group(1)), m.group(2)
    assert abs(time.time() - ts) < 5
    expected = hmac.new(
        settings.callback_signing_secret_current.encode(),
        f"{ts}.".encode() + raw,
        hashlib.sha256,
    ).hexdigest()
    assert v1 == expected


def test_sign_headers_dual_key_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    """轮换期双密钥并存：同时携带 v1 与 v1_old。"""
    monkeypatch.setattr(settings, "callback_signing_secret_old", "old-secret")
    raw = b"payload"
    headers = dispatcher.sign_headers(42, raw, "evt_2")
    m = re.fullmatch(
        r"t=(\d+),v1=([0-9a-f]{64}),v1_old=([0-9a-f]{64})", headers["X-Signature"]
    )
    assert m, headers["X-Signature"]
    ts = int(m.group(1))
    assert m.group(3) == hmac.new(
        b"old-secret", f"{ts}.".encode() + raw, hashlib.sha256
    ).hexdigest()


# ---------------------------------------------------------------------------
# 投递状态机：响应语义
# ---------------------------------------------------------------------------


async def test_deliver_2xx_marks_delivered(harness) -> None:
    """2xx → delivered（终态出队，停重试）；投递带签名头与 X-Delivery-Id。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(200)
    await disp._deliver(d)
    # 出队收口：HASH 删除 + due/lease 摘除
    assert await redis_queue.get_item(qredis, NS, "evt_01JABC") is None
    assert await qredis.zcard(f"{NS}:due") == 0
    assert await qredis.zcard(f"{NS}:lease") == 0
    call = client.calls[0]
    assert call["headers"]["X-Delivery-Id"] == "evt_01JABC"
    assert "X-Signature" in call["headers"]
    assert call["timeout"] == dispatcher.DELIVERY_TIMEOUT_SECONDS


async def test_deliver_410_gone_dead_lettered(harness) -> None:
    """410 Gone = 接收方约定「不再投递」→ dead，无重排。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(410)
    await disp._deliver(d)
    item = await _item(qredis)
    assert item["state"] == "dead"
    assert item["dead_reason"] == "receiver terminal"
    assert item["last_status_code"] == "410"
    assert item["attempts"] == "0"  # 终态死信不推进 attempts、不重排
    assert await qredis.zcard(f"{NS}:dead") == 1
    assert await qredis.zcard(f"{NS}:due") == 0


@pytest.mark.parametrize("status", [400, 404, 422])
async def test_deliver_4xx_terminal_dead(harness, status: int) -> None:
    """其余 4xx（除 408/429）视为终态 → 死信。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(status)
    await disp._deliver(d)
    assert (await _item(qredis))["state"] == "dead"


@pytest.mark.parametrize("status", [408, 429])
async def test_deliver_408_429_rescheduled(harness, status: int) -> None:
    """408/429 不算终态：退避重排。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(status)
    before = time.time()
    await disp._deliver(d)
    item = await _item(qredis)
    assert item["state"] == "pending"
    assert item["attempts"] == "1"
    delay = _due_score_delay(qredis, "evt_01JABC", before)
    assert dispatcher.BACKOFF_SECONDS[0] <= delay <= dispatcher.BACKOFF_SECONDS[0] + 5


async def test_deliver_timeout_rescheduled(harness) -> None:
    """传输超时 → 退避重排（至少一次投递语义）。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(httpx.TimeoutException("boom"))
    await disp._deliver(d)
    item = await _item(qredis)
    assert item["state"] == "pending"
    assert item["attempts"] == "1"
    assert "last_status_code" not in item  # 无响应码


# ---------------------------------------------------------------------------
# 退避序列与死信
# ---------------------------------------------------------------------------


async def test_backoff_sequence_1m_5m_30m_2h_6h(harness) -> None:
    """退避序列 [1m,5m,30m,2h,6h]（jitter 已置 0）：attempts 1..5 逐级对应。"""
    qredis, client, disp = harness
    for attempts in range(len(dispatcher.BACKOFF_SECONDS)):
        d = await _seed(qredis, attempts=attempts)
        client.outcomes.append(500)
        before = time.time()
        await disp._deliver(d)
        item = await _item(qredis)
        assert item["state"] == "pending"
        assert item["attempts"] == str(attempts + 1)
        delay = _due_score_delay(qredis, "evt_01JABC", before)
        expected = dispatcher.BACKOFF_SECONDS[attempts]
        assert expected <= delay <= expected + 5
        await redis_queue.mark_done(qredis, NS, "evt_01JABC")  # 清场下轮
    assert dispatcher.BACKOFF_SECONDS == [60, 300, 1800, 7200, 21600]


async def test_max_attempts_exhausted_dead_lettered(harness) -> None:
    """attempts 超退避序列上限 → dead + dead_reason='max attempts'。"""
    qredis, client, disp = harness
    d = await _seed(qredis, attempts=len(dispatcher.BACKOFF_SECONDS))
    client.outcomes.append(500)
    await disp._deliver(d)
    item = await _item(qredis)
    assert item["state"] == "dead"
    assert item["dead_reason"] == "max attempts"
    assert item["last_status_code"] == "500"
    assert item["attempts"] == str(len(dispatcher.BACKOFF_SECONDS) + 1)
    assert await qredis.zcard(f"{NS}:dead") == 1


# ---------------------------------------------------------------------------
# 域级熔断 / 原始字节 / 领取 / 重放
# ---------------------------------------------------------------------------


async def test_deliver_2xx_records_domain_circuit_success(harness) -> None:
    """域级熔断记账（§8.2）：2xx → on_success(user-callback:{domain})。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(200)
    await disp._deliver(d)
    assert disp.circuit_calls == [("success", "user.example.com")]


async def test_deliver_4xx_records_domain_circuit_success(harness) -> None:
    """收到 HTTP 响应（410/4xx 终态）= 接收方在线 → 计成功不计失败。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(410)
    await disp._deliver(d)
    assert disp.circuit_calls == [("success", "user.example.com")]


async def test_deliver_5xx_records_domain_circuit_failure(harness) -> None:
    """5xx → on_failure（累计阈值由 CircuitBreaker 维护）。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    client.outcomes.append(500)
    await disp._deliver(d)
    assert disp.circuit_calls == [("failure", "user.example.com")]


async def test_deliver_timeout_records_domain_circuit_failure(harness) -> None:
    """超时/传输错误 → on_failure；429/408 属响应，绝不计失败（§8.2）。"""
    qredis, client, disp = harness
    client.outcomes.append(httpx.TimeoutException("boom"))
    d = await _seed(qredis)
    await disp._deliver(d)
    assert disp.circuit_calls == [("failure", "user.example.com")]

    disp.circuit_calls.clear()
    client.outcomes.extend([408, 429])
    d = await _seed(qredis, attempts=0)
    await disp._deliver(d)
    d = await _seed(qredis, attempts=1)
    await disp._deliver(d)
    assert disp.circuit_calls == [("success", "user.example.com"),
                                  ("success", "user.example.com")]


async def test_circuit_open_reschedules_without_delivering(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """域级熔断 open：直接重排不投递（不发 HTTP、不计 attempts）。"""
    qredis, client, disp = harness
    d = await _seed(qredis, attempts=2)

    async def _open(domain: str) -> bool:
        return True

    monkeypatch.setattr(dispatcher, "_domain_circuit_open", _open)
    before = time.time()
    await disp._deliver(d)
    assert client.calls == []
    item = await _item(qredis)
    assert item["state"] == "pending"
    assert item["attempts"] == "2"  # 熔断不是接收方响应，不消耗退避预算
    delay = _due_score_delay(qredis, "evt_01JABC", before)
    assert 0 < delay <= dispatcher.CIRCUIT_OPEN_RESCHEDULE_SECONDS + 5


async def test_deliver_signs_exact_raw_bytes(harness) -> None:
    """payload 以 JSON 文本存储：投递与签名共用同一份原始字节。"""
    qredis, client, disp = harness
    d = await _seed(qredis)
    raw = '{"id":"evt_01JABC", "weird" : " spacing "}'
    d["payload_json"] = raw
    client.outcomes.append(200)
    await disp._deliver(d)
    call = client.calls[0]
    assert call["content"] == raw.encode()
    ts = int(re.search(r"t=(\d+)", call["headers"]["X-Signature"]).group(1))
    v1 = re.search(r"v1=([0-9a-f]{64})", call["headers"]["X-Signature"]).group(1)
    expected = hmac.new(
        settings.callback_signing_secret_current.encode(),
        f"{ts}.".encode() + raw.encode(),
        hashlib.sha256,
    ).hexdigest()
    assert v1 == expected


async def test_claim_batch_lua_and_lease(harness) -> None:
    """领取：Lua 原子（due → delivering + lease ZSET）；过期 lease 回收回 due。"""
    qredis, client, disp = harness
    await _seed(qredis, attempts=1)
    claimed = await disp._claim_batch(50)
    assert [r["id"] for r in claimed] == ["evt_01JABC"]
    assert claimed[0]["attempts"] == 1
    item = await _item(qredis)
    assert item["state"] == "delivering"
    assert float(item["lease_until"]) > time.time()
    assert await qredis.zcard(f"{NS}:lease") == 1
    # 未到期条目不领取
    assert await redis_queue.claim(qredis, NS, limit=50, lease_seconds=60) == []
    # lease 过期回收（副本死亡兜底）→ 回到 due 可再领
    await redis_queue.reschedule(qredis, NS, "evt_01JABC", delay_seconds=0)
    reclaimed = await redis_queue.reclaim_expired_leases(qredis, NS)
    assert reclaimed == []  # lease 已被 reschedule 摘除
    assert await qredis.zcard(f"{NS}:due") == 1


async def test_replay_dead_delivery(harness) -> None:
    """死信人工重放：dead → pending（重置 attempts），Lua 条件防竞态。"""
    qredis, client, disp = harness
    await _seed(qredis, attempts=5)
    # 未 dead 不可重放
    assert await dispatcher.replay_dead_delivery("evt_01JABC") is False
    await redis_queue.dead_letter(qredis, NS, "evt_01JABC", reason="max attempts")
    assert await dispatcher.replay_dead_delivery("evt_01JABC") is True
    item = await _item(qredis)
    assert item["state"] == "pending"
    assert item["attempts"] == "0"
    assert item["dead_reason"] == ""
    assert await qredis.zcard(f"{NS}:dead") == 0
    assert await qredis.zcard(f"{NS}:due") == 1
    # 不存在的 id
    assert await dispatcher.replay_dead_delivery("evt_nope") is False
