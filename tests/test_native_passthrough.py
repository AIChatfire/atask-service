"""原生路径（透传形态）生命周期拦截测试。

被测语义（三点优化的验收）：

1. **原生提交**（命中渠道 ``submit_path``）：请求内零上游往返，秒级返回，
   响应体与上游 100% 同构（只有 ``task_id_path`` 一个字段），值是**本地**
   task_id；上游提交交给 worker（``queue.publish_submit``）。
2. **原生查询**（命中 ``probe_path``）：客户端持本地 id 来查 → 网关按 path
   里的 id 反查 tasks 行、用 ``channel_id`` 钉回直达租约（**不问** keypool 的
   ``select(group, model)``），把 URL 里的 id 换成上游 id 转发，响应缓冲后把
   上游 id 改写回本地 id；上游还没接单时按本地快照直出（零往返，绝不 404）。
3. **原生取消**（命中 ``cancel_path``）：走本地 cancel 链路（解冻 + 尽力源头
   止损），绝不当"新任务"报价冻结。
"""

from __future__ import annotations

import json

import httpx
import pytest

from app.main import app
from app.services.registry import registry
from app.services.submit import submit_one

UP_ID = "424010985738629"

CHANNEL = {
    "id": 7, "name": "minimax-main", "base_url": "http://upstream.test",
    "setting": {
        "gateway": {
            "biz": "minimax",
            "submit_path": "/v2/video_generation",
            "probe_path": "/v2/query/video_generation/{upstream_task_id}",
            "cancel_path": "/v2/cancel/video_generation/{upstream_task_id}",
            "status_path": "task.status",
            "result_path": "task.content.url",
            "error_path": "task.error",
            "settle_usage_map": {"duration": "task.usage.output_seconds"},
            "billing": {
                "rule": "def calulate(request):\n"
                        "    return round(float(request.get('duration') or 5) * 0.026, 6)",
                "type": "second",
            },
        },
    },
}

BODY = {
    "model": "MiniMax-H3",
    "content": [{"type": "text", "text": "史诗级太空歌剧院线预告"}],
    "resolution": "2K", "duration": 5, "ratio": "16:9",
}

AUTH = {"Authorization": "Bearer sk-user-42"}


@pytest.fixture
def _reset_registry():
    registry._cache.clear()
    yield
    registry._cache.clear()


@pytest.fixture
def native_mocks(respx_router):
    m = type("Mocks", (), {})()
    m.inspect = respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 42, "token_id": 9})
    )
    m.freeze = respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}})
    )
    m.select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json={
            "code": 0, "message": "ok",
            "data": {
                "channel_id": 7, "key_index": 1, "key": "sk-minimax-real",
                "base_url": "http://upstream.test", "epoch": "e1",
                "channel": CHANNEL,
            },
        })
    )
    m.report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    m.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": UP_ID})
    )
    return m


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://gw.test")


# ---------------------------------------------------------------------------
# ① 原生提交：本地 id + 同构报文 + 零上游往返
# ---------------------------------------------------------------------------


async def test_native_submit_returns_local_task_id_without_upstream_call(
    _reset_registry, native_mocks, test_settings, patch_redis, task_store, queue_events,
):
    async with _client() as client:
        resp = await client.post("/minimax/v2/video_generation", json=BODY, headers=AUTH)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert list(body) == ["task_id"]                     # 与上游报文严格同构
    task_id = body["task_id"]
    assert task_id.startswith("minimax_")                # 本地 id（非上游 id）
    assert not native_mocks.create.calls                 # 请求内零上游往返
    assert queue_events["submit"] == [task_id]           # 提交交给 worker

    row = task_store.rows[task_id]
    assert row["action"] == "task" and row["status"] == "SUBMITTED"
    assert row["data"]["source"] == "native"
    assert row["data"]["biz"] == "minimax"               # 渠道权威 biz
    assert row["data"]["request_body"]["duration"] == 5  # 结算重估基底
    assert row["data"]["freeze_amount"] == pytest.approx(0.13)


async def test_native_submit_shapes_body_by_channel_task_id_path(
    _reset_registry, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """报文形状由渠道 ``task_id_path`` + ``ok_check`` 驱动（零硬编码）：
    嵌套路径与业务信封都能自造出客户端认得的同构响应。"""
    channel = json.loads(json.dumps(CHANNEL))
    channel["setting"]["gateway"]["task_id_path"] = "data.task_id"
    channel["setting"]["gateway"]["ok_check"] = {"path": "code", "equals": 0,
                                                 "message_path": "message"}
    respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 42, "token_id": 9}))
    respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}}))
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json={
            "code": 0, "message": "ok",
            "data": {"channel_id": 7, "key_index": 1, "key": "k",
                     "base_url": "http://upstream.test", "epoch": "e1",
                     "channel": channel},
        }))

    async with _client() as client:
        resp = await client.post("/minimax/v2/video_generation", json=BODY, headers=AUTH)

    assert resp.status_code == 200
    task_id = next(iter(task_store.rows))
    assert resp.json() == {"code": 0, "message": "ok", "data": {"task_id": task_id}}


# ---------------------------------------------------------------------------
# ② 原生查询：id 双向改写 + 钉渠道直达 + 首探前本地快照
# ---------------------------------------------------------------------------


async def test_native_query_swaps_ids_both_ways(
    _reset_registry, native_mocks, respx_router, test_settings, patch_redis,
    task_store, queue_events,
):
    """客户端持本地 id 查询：上游收到的是**上游 id**，客户端拿到的报文里
    **只有本地 id**（其余字节 100% 同构）。"""
    probe = respx_router.get(
        f"http://upstream.test/v2/query/video_generation/{UP_ID}").mock(
        return_value=httpx.Response(200, json={
            "task": {"id": UP_ID, "status": "processing", "extra": {"ref": UP_ID}},
        })
    )
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        await submit_one(task_id)                       # worker 提交，回填上游 id
        native_mocks.select.reset()

        got = await client.get(f"/minimax/v2/query/video_generation/{task_id}")

    assert got.status_code == 200
    assert probe.calls                                   # 上游收到的是上游 id
    raw = got.text
    assert UP_ID not in raw                              # 上游 id 一个字节都不外泄
    assert got.json() == {
        "task": {"id": task_id, "status": "processing", "extra": {"ref": task_id}},
    }
    # 钉渠道直达：唯一一次 keypool select 带 channel_id，不问 group+model
    assert len(native_mocks.select.calls) == 1
    sel = json.loads(native_mocks.select.calls[0].request.content)
    assert sel["channel_id"] == 7 and "group" not in sel
    # 客户端轮询顺带驱动状态推进
    assert task_store.rows[task_id]["status"] == "IN_PROGRESS"


async def test_native_query_before_upstream_accepted_uses_local_snapshot(
    _reset_registry, native_mocks, test_settings, patch_redis, task_store, queue_events,
):
    """worker 还没提交（无 upstream_task_id）：本地快照直出同构报文，
    200 + 排队态，绝不 404、零上游往返。"""
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        got = await client.get(f"/minimax/v2/query/video_generation/{task_id}")

    assert got.status_code == 200
    assert got.json() == {"task": {"status": "queued"}, "task_id": task_id}


async def test_native_query_snapshot_uses_probe_task_id_path(
    _reset_registry, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """快照报文的 id 字段路径可由渠道 ``probe_task_id_path`` 指定
    （探测报文形态与提交响应不同时，如 ``task.id``）。"""
    channel = json.loads(json.dumps(CHANNEL))
    channel["setting"]["gateway"]["probe_task_id_path"] = "task.id"
    respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 42, "token_id": 9}))
    respx_router.post("http://billing.test/api/v1/billing/freeze").mock(
        return_value=httpx.Response(200, json={"data": {"status": "frozen"}}))
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json={
            "code": 0, "message": "ok",
            "data": {"channel_id": 7, "key_index": 1, "key": "k",
                     "base_url": "http://upstream.test", "epoch": "e1",
                     "channel": channel},
        }))

    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        got = await client.get(f"/minimax/v2/query/video_generation/{task_id}")

    assert got.json() == {"task": {"status": "queued", "id": task_id}}


async def test_native_query_terminal_and_upstream_id_lookup(
    _reset_registry, native_mocks, respx_router, test_settings, patch_redis,
    task_store, queue_events,
):
    """终态闭环不受影响：探测终态 → 重估结算；且客户端持**上游 id** 查询
    （旧客户端兼容）同样命中同一任务。"""
    terminal_payload = {
        "task": {"id": UP_ID, "status": "succeeded",
                 "content": {"url": "http://cdn.test/out.mp4"},
                 "usage": {"output_seconds": 4, "input_seconds": 0},
                 "trace_id": "tr-9", "resolution": "2K"},
    }
    probe = respx_router.get(
        f"http://upstream.test/v2/query/video_generation/{UP_ID}").mock(
        return_value=httpx.Response(200, json=terminal_payload)
    )
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        got = await client.get(f"/minimax/v2/query/video_generation/{task_id}")
        again = await client.get(f"/minimax/v2/query/video_generation/{task_id}")
        by_up = await client.get(f"/minimax/v2/query/video_generation/{UP_ID}")

    assert got.status_code == 200
    assert got.json()["task"]["content"]["url"] == "http://cdn.test/out.mp4"
    assert task_store.rows[task_id]["status"] == "SUCCESS"
    assert len(queue_events["settle"]) == 1
    assert queue_events["settle"][0]["actual_amount"] == pytest.approx(0.104)

    # 终态后的再次查询：零上游往返（只有第一次打了上游），且**逐字段同构**
    # 回放上游终态原始报文（usage/trace_id 等网关不认识的字段全都在）
    assert len(probe.calls) == 1
    expected = json.loads(json.dumps(terminal_payload).replace(UP_ID, task_id))
    assert again.json() == expected
    # 上游 id 反查命中同一任务，且回显仍是本地 id
    assert by_up.status_code == 200
    assert UP_ID not in by_up.text
    assert by_up.json() == expected


async def test_native_query_upstream_failure_falls_back_to_snapshot(
    _reset_registry, native_mocks, respx_router, test_settings, patch_redis,
    task_store, queue_events,
):
    """上游探测不可达：回本地快照（客户端轮询不被打断），不 5xx。"""
    respx_router.get(
        f"http://upstream.test/v2/query/video_generation/{UP_ID}"
    ).mock(side_effect=httpx.ConnectError("boom"))
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        got = await client.get(f"/minimax/v2/query/video_generation/{task_id}")

    assert got.status_code == 200
    assert got.json() == {"task": {"status": "queued"}, "task_id": task_id}


async def test_native_query_snapshot_replays_upstream_status_word(
    _reset_registry, native_mocks, respx_router, test_settings, patch_redis,
    task_store, queue_events,
):
    """快照状态词优先用 poller 记下的**上游原话**（``data.upstream_status``），
    不用网关自造的近似词——同一状态在两条路径上措辞一致。"""
    probe = respx_router.get(
        f"http://upstream.test/v2/query/video_generation/{UP_ID}").mock(
        side_effect=[
            httpx.Response(200, json={"task": {"status": "Generating"}}),
            httpx.ConnectError("boom"),
        ]
    )
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        first = await client.get(f"/minimax/v2/query/video_generation/{task_id}")
        second = await client.get(f"/minimax/v2/query/video_generation/{task_id}")

    assert len(probe.calls) == 2
    assert first.json()["task"]["status"] == "Generating"      # 上游原文透传
    # 第二次上游不可达 → 本地快照，状态词沿用上游原话（不退化成 "processing"）
    assert second.json() == {"task": {"status": "Generating"}, "task_id": task_id}
    assert task_store.rows[task_id]["status"] == "IN_PROGRESS"


async def test_native_query_snapshot_terminal_ignores_stale_upstream_word(
    _reset_registry, native_mocks, respx_router, test_settings, patch_redis,
    task_store, queue_events,
):
    """本地已终态但 ``upstream_status`` 还停在活跃态（取消/判死走本地收口）：
    快照必须用本地终态词，绝不回显自相矛盾的活跃态原话；且终态查询**零上游
    往返**（本地即权威）。"""
    from app.services import flow

    probe = respx_router.get(
        f"http://upstream.test/v2/query/video_generation/{UP_ID}").mock(
        return_value=httpx.Response(200, json={"task": {"status": "Generating"}})
    )
    respx_router.post(
        f"http://upstream.test/v2/cancel/video_generation/{UP_ID}").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        await task_store.patch_data(task_id, {"upstream_status": "Generating"},
                                    status="IN_PROGRESS")
        await flow.cancel_task(task_id)
        got = await client.get(f"/minimax/v2/query/video_generation/{task_id}")

    assert not probe.calls                      # 终态不再问上游
    assert got.json() == {
        "task": {"status": "canceled", "error": "canceled by user"},
        "task_id": task_id,
    }


# ---------------------------------------------------------------------------
# ③ 原生取消：本地 cancel 链路，绝不当新任务计费
# ---------------------------------------------------------------------------


async def test_native_cancel_goes_local_cancel_not_new_task(
    _reset_registry, native_mocks, respx_router, test_settings, patch_redis,
    task_store, queue_events,
):
    remote_cancel = respx_router.post(
        f"http://upstream.test/v2/cancel/video_generation/{UP_ID}").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    async with _client() as client:
        task_id = (await client.post("/minimax/v2/video_generation",
                                     json=BODY, headers=AUTH)).json()["task_id"]
        await submit_one(task_id)
        freeze_calls = len(native_mocks.freeze.calls)

        resp = await client.post(f"/minimax/v2/cancel/video_generation/{task_id}",
                                 headers=AUTH)

    assert resp.status_code == 200
    assert task_store.rows[task_id]["status"] == "CANCELED"
    assert len(task_store.rows) == 1                       # 没有被当成新任务落行
    assert len(native_mocks.freeze.calls) == freeze_calls  # 也没有二次冻结
    assert queue_events["cancel"][-1]["request_id"] == task_id   # 解冻已发
    assert remote_cancel.calls                             # 源头止损尽力调用
    assert UP_ID not in resp.text


# ---------------------------------------------------------------------------
# 边界：非生命周期路径维持原透传语义
# ---------------------------------------------------------------------------


async def test_non_lifecycle_paths_keep_passthrough_semantics(
    _reset_registry, native_mocks, respx_router, test_settings, patch_redis,
    task_store, queue_events,
):
    """既不是 submit/probe/cancel 的路径：原样流式透传（上游报文逐字节回吐）。"""
    other = respx_router.post("http://upstream.test/v2/image_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "up-img-1"})
    )
    async with _client() as client:
        resp = await client.post("/minimax/v2/image_generation",
                                 json={"model": "MiniMax-H3", "duration": 5},
                                 headers=AUTH)

    assert resp.status_code == 200
    assert resp.json() == {"task_id": "up-img-1"}          # 上游原始报文
    assert other.calls
    task_id = next(iter(task_store.rows))
    assert task_store.rows[task_id]["action"] == "proxy"
    assert task_store.rows[task_id]["data"]["upstream_task_id"] == "up-img-1"
