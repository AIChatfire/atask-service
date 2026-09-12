"""``/batch`` 后台收敛 + 用户回调（ADR-010 补链）。

纯客户端轮询驱动的后果是「客户端不轮询 → 任务永远停在非终态、用户回调整条断链」。
本文件钉住补齐后台收敛（``relayflow.sweep_batch_once``）后的硬不变量：

1. 陈旧非终态任务被探测推进到终态，落快照、**释放并发槽**、清令牌会话；
2. 上游仍非终态时**什么都不做**（不误判、不释槽）；
3. 给了 ``X-Callback-Url`` 时终态后投递用户回调，且签名可被同一密钥验过；
4. 没给 callback_url 就绝不凭空投递；
5. 令牌会话过期 → 跳过、不抛异常、不误判终态；
6. 收敛查询的时间比较必须走 ``_secs()``，且**排序为 ASC（最旧优先）**；
7. 收敛候选**按最旧优先**被探测（limit 截断时不能饿死最老的）；
8. 重入锁：已有并发轮在跑时本轮直接返回 0，不叠加探测。

出站由 respx 拦截，Redis / tasks 表走内存替身（见 conftest）。
"""

from __future__ import annotations

import hashlib
import hmac
import time
from unittest.mock import AsyncMock

import httpx
import pytest

from app.main import app
from app.redis import K_BATCH_SWEEP_LOCK
from app.services import notify, relayflow

AUTH = {"Authorization": "Bearer sk-user-1"}
UP_BASE = "http://upstream.test"
TOKEN_HASH = hashlib.sha256(b"sk-user-1").hexdigest()
CONC_KEY = f"gw:conc:{TOKEN_HASH}"
SESSION_KEY = "gw:sk:{task_id}"
CB_URL = "https://user.example/callback"


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Upstream-Base-Url": UP_BASE, **extra}


@pytest.fixture
def batch_settings(monkeypatch: pytest.MonkeyPatch):
    from app.config import settings

    monkeypatch.setattr(settings, "upstream_allowlist", "upstream.test")
    monkeypatch.setattr(settings, "upstream_base_url", UP_BASE)
    monkeypatch.setattr(settings, "relay_timeout_seconds", 60.0)
    monkeypatch.setattr(settings, "batch_deny_prefixes", "/api/,/console/")
    monkeypatch.setattr(settings, "task_stale_seconds", 300)
    monkeypatch.setattr(settings, "batch_sweep_batch", 50)
    monkeypatch.setattr(settings, "batch_sweep_lock_ttl_seconds", 300)
    return settings


@pytest.fixture
def batch_queue(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """拦截 ``queue.publish_batch_submit``（不触真 broker）。"""
    import app.queue as q

    events: dict[str, list] = {"submit": []}

    async def _publish(task_id: str) -> None:
        events["submit"].append(task_id)

    monkeypatch.setattr(q, "publish_batch_submit", AsyncMock(side_effect=_publish))
    return events


@pytest.fixture
def notified(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """记录并**真实执行** ``publish_notify``（→ ``notify.push``，签名+出站交 respx）。

    刻意不走 AsyncMock 记录：那样只证明「调了 publish_notify」，证明不了签名
    真的由 ``notify.sign`` 产出。这里让真实投递跑起来，再从出站请求验签。
    """
    import app.queue as q

    calls: list[dict] = []

    async def _publish(task_id: str, url: str, payload: dict) -> None:
        calls.append({"task_id": task_id, "url": url, "payload": payload})
        await notify.push(url, payload)

    monkeypatch.setattr(q, "publish_notify", _publish)
    return calls


@pytest.fixture
def sweep_source(monkeypatch: pytest.MonkeyPatch, task_store):
    """给内存 taskstore 补 ``stale_batch_active``（conftest 未覆盖该查询）。

    **按 updated_at ASC 排序后再截断**——镜像真实 SQL 的「最旧优先」语义，
    否则第 7 条「最旧优先」断言就成了摆设。
    """
    import app.services.taskstore as ts

    async def _stale(stale_seconds: int, limit: int = 50) -> list[dict]:
        cutoff = int(time.time()) - stale_seconds
        out = []
        for t in task_store.rows.values():
            d = t.get("data") or {}
            if d.get("source") != "batch":
                continue
            if t["status"] not in ("SUBMITTED", "QUEUED", "IN_PROGRESS"):
                continue
            if t["updated_at"] >= cutoff:
                continue
            if not d.get("upstream_task_id"):
                continue
            out.append({
                "task_id": t["task_id"], "status": t["status"],
                "upstream_base_url": d.get("upstream_base_url"),
                "request_path": d.get("request_path"),
                "upstream_task_id": d.get("upstream_task_id"),
                "callback_url": d.get("callback_url"),
                "token_hash": d.get("token_hash"),
                "source": d.get("source"),
                "_updated_at": t["updated_at"],
            })
        out.sort(key=lambda r: r["_updated_at"])          # 最旧优先（同真实 SQL）
        return out[:limit]

    monkeypatch.setattr(ts, "stale_batch_active", _stale)
    return task_store


async def _seed(client: httpx.AsyncClient, task_store, *, callback: str = "",
                status: str = "QUEUED", upstream_id: str = "up-1",
                age_seconds: int = 10_000, path: str = "/v1/tasks") -> str:
    """受理一条 ``/batch`` 任务并把它做成「陈旧非终态」候选。"""
    headers = _headers()
    if callback:
        headers["X-Callback-Url"] = callback
    resp = await client.post(f"/batch{path}", json={"model": "m"}, headers=headers)
    assert resp.status_code == 202, resp.text
    task_id = resp.json()["task_id"]
    await task_store.patch_data(task_id, {"upstream_task_id": upstream_id}, status=status)
    task_store.rows[task_id]["updated_at"] = int(time.time()) - age_seconds
    return task_id


# ---------------------------------------------------------------------------
# 1. 陈旧非终态 → 推进终态（快照 / 释槽 / 清会话 / 锁归零）
# ---------------------------------------------------------------------------


async def test_sweep_advances_stale_task_to_terminal(
    batch_settings, patch_redis, task_store, batch_queue, sweep_source, respx_router,
):
    probe = respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"id": "up-1", "status": "succeeded"})
    )
    async with _client() as client:
        task_id = await _seed(client, task_store)

    assert await patch_redis.get(CONC_KEY) == "1"        # 受理时占了一个并发槽

    advanced = await relayflow.sweep_batch_once()

    assert advanced == 1
    row = task_store.rows[task_id]
    assert row["status"] == "SUCCESS"
    assert row["data"]["upstream_status"] == "succeeded"
    assert row["data"]["upstream_snapshot"] == {"id": "up-1", "status": "succeeded"}
    assert await patch_redis.get(CONC_KEY) == "0"        # 槽被 DECR 释放
    assert await patch_redis.get(SESSION_KEY.format(task_id=task_id)) is None  # 终态清会话
    assert await patch_redis.get(K_BATCH_SWEEP_LOCK) is None                    # 锁已释放
    assert probe.calls
    # 探测用**用户本人 token**（从 Redis 会话取，不落库）
    assert probe.calls[0].request.headers["authorization"] == "Bearer sk-user-1"


# ---------------------------------------------------------------------------
# 2. 非终态不误判
# ---------------------------------------------------------------------------


async def test_sweep_leaves_non_terminal_untouched(
    batch_settings, patch_redis, task_store, batch_queue, sweep_source, respx_router,
):
    respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"id": "up-1", "status": "running"})
    )
    async with _client() as client:
        task_id = await _seed(client, task_store)

    advanced = await relayflow.sweep_batch_once()

    assert advanced == 0
    row = task_store.rows[task_id]
    assert row["status"] == "QUEUED"                     # 保持非终态
    assert "upstream_snapshot" not in row["data"]
    assert await patch_redis.get(CONC_KEY) == "1"        # 槽不释放


# ---------------------------------------------------------------------------
# 3. 用户回调：送达且签名可验
# ---------------------------------------------------------------------------


async def test_sweep_delivers_signed_user_callback(
    batch_settings, patch_redis, task_store, batch_queue, sweep_source, notified,
    respx_router, monkeypatch,
):
    monkeypatch.setattr(batch_settings, "callback_sign_secret", "test-secret")
    respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"id": "up-1", "status": "succeeded"})
    )
    cb = respx_router.post(CB_URL).mock(return_value=httpx.Response(200))
    async with _client() as client:
        task_id = await _seed(client, task_store, callback=CB_URL)

    advanced = await relayflow.sweep_batch_once()

    assert advanced == 1
    assert len(notified) == 1
    assert notified[0] == {
        "task_id": task_id, "url": CB_URL,
        # 上游 id 改写回本地 id（原生报文同构）
        "payload": {"id": task_id, "status": "succeeded"},
    }
    req = cb.calls[0].request
    header = req.headers["x-gateway-signature"]
    parts = dict(pair.split("=", 1) for pair in header.split(","))
    body = req.content
    expected = hmac.new(b"test-secret", f"{parts['t']}.".encode() + body,
                        hashlib.sha256).hexdigest()
    assert parts["v1"] == expected                        # 签名可被同一密钥验过


# ---------------------------------------------------------------------------
# 4. 没给 callback_url 就不投递
# ---------------------------------------------------------------------------


async def test_sweep_does_not_notify_without_callback_url(
    batch_settings, patch_redis, task_store, batch_queue, sweep_source, notified,
    respx_router,
):
    respx_router.get(f"{UP_BASE}/v1/tasks/up-1").mock(
        return_value=httpx.Response(200, json={"id": "up-1", "status": "failed"})
    )
    async with _client() as client:
        task_id = await _seed(client, task_store)          # 无 X-Callback-Url

    assert "callback_url" not in task_store.rows[task_id]["data"]
    advanced = await relayflow.sweep_batch_once()

    assert advanced == 1
    assert notified == []                                  # 绝不凭空投递


# ---------------------------------------------------------------------------
# 5. 令牌会话过期 → 跳过，不报错、不误判
# ---------------------------------------------------------------------------


async def test_sweep_skips_when_token_session_expired(
    batch_settings, patch_redis, task_store, batch_queue, sweep_source, respx_router,
):
    # 任何出站都会撞 respx 的 assert_all_mocked —— 没有会话就不该探测
    async with _client() as client:
        task_id = await _seed(client, task_store)
    await patch_redis.delete(SESSION_KEY.format(task_id=task_id))

    advanced = await relayflow.sweep_batch_once()

    assert advanced == 0
    row = task_store.rows[task_id]
    assert row["status"] == "QUEUED"                       # 未误判终态
    assert await patch_redis.get(CONC_KEY) == "1"          # 槽不释放


# ---------------------------------------------------------------------------
# 6. 收敛查询：时间比较走 _secs() + 最旧优先的 SQL
# ---------------------------------------------------------------------------


async def test_stale_batch_query_normalizes_millisecond_rows(monkeypatch):
    """直接断言真实 SQL：时间比较套 ``_secs('updated_at')``，排序为 ``ASC``。

    变异说明（两条独立断言各自可被变异打红）：
    - 把 ``{_secs('updated_at')}`` 换回裸 ``updated_at``：表达式不再出现在 SQL，
      ``ts._secs("updated_at") in sql`` 变红——这正是 ADR-004 的教训（共享表混入
      毫秒写入方时，裸比较会让毫秒行永远躲过 stale 判定）；
    - 把 ``ASC`` 改回 ``DESC``：``ORDER BY ... ASC`` 断言变红——DESC + limit 会
      饿死最旧的一批（它们最可能已在上游成功、只差没人回来轮询）。
    """
    from app.services import taskstore as ts

    captured: dict = {}

    class _Result:
        def mappings(self):
            return self

        def all(self) -> list:
            return []

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def execute(self, stmt, params=None):
            captured["sql"] = str(stmt)
            captured["params"] = params
            return _Result()

    monkeypatch.setattr(ts, "get_session_factory", lambda: (lambda: _Session()))

    rows = await ts.stale_batch_active(stale_seconds=300, limit=50)

    assert rows == []
    sql = captured["sql"]
    assert ts._secs("updated_at") in sql                  # 秒口径归一表达式在场
    assert f"ORDER BY {ts._secs('updated_at')} ASC" in sql  # 最旧优先（非 DESC）
    assert "SELECT data" not in sql                       # 绝不整列投影（token_hash 泄露红线）
    for field in ("upstream_base_url", "request_path", "upstream_task_id",
                  "callback_url", "token_hash", "source"):
        assert f"data ->> '$.{field}'" in sql
    assert "data ->> '$.source' = 'batch'" in sql          # 落库判别值已改 batch
    # cutoff 是秒（毫秒直比会让所有真实行都不满足 < cutoff 或全部误判）
    assert int(time.time()) - 400 < captured["params"]["cutoff"] <= int(time.time()) - 299
    # 归一函数本身：毫秒值折算为秒
    now_ms = int(time.time()) * 1000
    assert ts.as_unix_seconds(now_ms) == int(time.time())


# ---------------------------------------------------------------------------
# 7. 最旧优先：limit 截断时不能饿死最老的任务
# ---------------------------------------------------------------------------


async def test_sweep_probes_oldest_candidate_first_when_limited(
    batch_settings, patch_redis, task_store, batch_queue, sweep_source, respx_router,
):
    old = respx_router.get(f"{UP_BASE}/v1/tasks/up-old").mock(
        return_value=httpx.Response(200, json={"id": "up-old", "status": "succeeded"})
    )
    new = respx_router.get(f"{UP_BASE}/v1/tasks/up-new").mock(
        return_value=httpx.Response(200, json={"id": "up-new", "status": "succeeded"})
    )
    async with _client() as client:
        old_id = await _seed(client, task_store, upstream_id="up-old", age_seconds=20_000)
        new_id = await _seed(client, task_store, upstream_id="up-new", age_seconds=10_000)

    advanced = await relayflow.sweep_batch_once(limit=1)

    assert advanced == 1
    assert old.calls and not new.calls                    # 只探最旧那条
    assert task_store.rows[old_id]["status"] == "SUCCESS"
    assert task_store.rows[new_id]["status"] == "QUEUED"  # 较新那条本轮不碰


# ---------------------------------------------------------------------------
# 8. 重入锁：已有并发轮时本轮不叠加探测
# ---------------------------------------------------------------------------


async def test_sweep_skips_when_lock_held(
    batch_settings, patch_redis, task_store, batch_queue, sweep_source, respx_router,
):
    # 预置锁（模拟上一轮还没跑完）；任何出站都会撞 respx assert_all_mocked
    await patch_redis.set(K_BATCH_SWEEP_LOCK, "someone-else", ex=300)
    async with _client() as client:
        task_id = await _seed(client, task_store)

    advanced = await relayflow.sweep_batch_once()

    assert advanced == 0
    assert task_store.rows[task_id]["status"] == "QUEUED"  # 未探测、未推进
    assert await patch_redis.get(K_BATCH_SWEEP_LOCK) == "someone-else"  # 他人锁不被动
