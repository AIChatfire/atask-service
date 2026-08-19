"""MiniMax-H3 端到端完整性测试（AI_TODO.md 适配任务验收）。

全链路真实代码路径：HTTP 入口 → preflight（鉴权内省 ∥ keypool 租约，计费规则
随租约下发本地报价）→ freeze → 渠道覆盖提交 → 落库 → 探测推进（processing →
succeeded）→ 渠道规则重估结算（多退少补）→ 用户回调通知 → 任务查询视图。

外部边界全部 fake/mock：两微服务与上游走 respx，Redis/taskstore/queue 走
内存实现；**网关内部链路零 mock**。
"""

from __future__ import annotations

import json

import httpx
import pytest

from app.main import app
from app.services.polling import poll_one
from app.services.registry import registry
from app.services.submit import submit_one

MINIMAX_CHANNEL = {
    "id": 7, "name": "minimax-main", "base_url": "http://upstream.test",
    "model_mapping": {"MiniMax-H3": "MiniMax-H3"},
    "param_override": {"aigc_watermark": False},
    "header_override": {
        "X-Channel-Tag": "paid",
        # 网关配置块（与 setting.gateway 等价，优先）：随租约下发，不下发到上游
        "upstream": {
            "biz": "minimax",
            "submit_path": "/v2/video_generation",
            "probe_path": "/v2/query/video_generation/{upstream_task_id}",
            "status_path": "task.status",
            "result_path": "task.content.url",
            "error_path": "task.error",
            "settle_usage_map": {"duration": "task.usage.output_seconds"},
            "pricing_biz_type": "video_generation",
            # 计费规则（唯一事实源）：平台价 $0.026/秒，按请求 duration 顶格预估
            "billing": {
                "rule": "def calulate(request):\n    return round(float(request.get('duration') or 5) * 0.026, 6)",
                "type": "second",
            },
        },
    },
}

T2VA_BODY = {
    "model": "MiniMax-H3",
    "content": [{"type": "text", "text": "史诗级太空歌剧院线预告"}],
    "resolution": "2K", "duration": 5, "ratio": "16:9",
    "callback_url": "https://user.test/done",
}


@pytest.fixture
def _reset_registry():
    registry._cache.clear()
    yield
    registry._cache.clear()


@pytest.fixture
def e2e_mocks(respx_router):
    """三微服务 + 上游的完整契约 mock。"""
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
                "channel": MINIMAX_CHANNEL,
            },
        })
    )
    m.report = respx_router.post("http://keypool.test/v1/keys/report").mock(
        return_value=httpx.Response(200, json={"code": 0, "data": {"action": "none"}})
    )
    m.create = respx_router.post("http://upstream.test/v2/video_generation").mock(
        return_value=httpx.Response(200, json={"task_id": "424010985738629"})
    )
    m.query = respx_router.get(
        "http://upstream.test/v2/query/video_generation/424010985738629"
    ).mock(side_effect=[
        httpx.Response(200, json={"task": {"id": "424010985738629", "status": "processing"}}),
        httpx.Response(200, json={
            "task": {
                "id": "424010985738629", "status": "succeeded",
                "content": {"url": "http://cdn.test/h3-output.mp4"},
                "usage": {"total_seconds": 4, "output_seconds": 4, "input_seconds": 0},
                "resolution": "2K", "duration": 5, "ratio": "16:9",
            },
        }),
    ])
    return m


async def test_minimax_h3_full_lifecycle(
    _reset_registry, e2e_mocks, test_settings, patch_redis, task_store, queue_events,
):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        # ---- ① 提交（t2va 形态，AI_TODO.md 样例报文原样透传）----
        # 异步受理：preflight + 落库后立即返回本地 task_id，请求内零上游调用
        resp = await client.post(
            "/minimax/v1/videos",
            json=T2VA_BODY,
            headers={"Authorization": "Bearer sk-user-42", "Idempotency-Key": "req-1"},
        )
        assert resp.status_code == 202, resp.text
        view = resp.json()
        assert view["status"] == "submitted"
        task_id = view["task_id"]
        assert task_id.startswith("minimax_")           # biz 前缀本地 id
        assert len(task_id) <= 64                       # tasks.task_id String(64)
        assert "upstream_task_id" not in view           # 202 受理响应不泄上游 id
        assert len(e2e_mocks.create.calls) == 0         # 上游提交在 worker 异步执行
        assert queue_events["submit"] == [task_id]

        # keypool 选 key 契约：统一分组 keypool + model + include_channel
        select_body = json.loads(e2e_mocks.select.calls[0].request.content)
        assert select_body["group"] == "keypool"
        assert select_body["model"] == "MiniMax-H3"
        assert select_body["include_channel"] is True

        # freeze 契约：顶格金额 = 5s × 0.026 = 0.13，用户令牌鉴权
        freeze_body = json.loads(e2e_mocks.freeze.calls[0].request.content)
        assert freeze_body["amount"] == pytest.approx(0.13)
        assert freeze_body["biz_type"] == "video_generation"
        assert freeze_body["metric"] == "second"
        assert e2e_mocks.freeze.calls[0].request.headers["Authorization"] == "Bearer sk-user-42"

        # ---- ①b worker 异步提交：回写上游 id 后才进探测闭环 ----
        await submit_one(task_id)
        assert task_store.rows[task_id]["status"] == "QUEUED"

        # 上游提交契约：Bearer 渠道 key + 渠道覆盖全部生效
        create_req = e2e_mocks.create.calls[0].request
        assert create_req.headers["Authorization"] == "Bearer sk-minimax-real"
        assert create_req.headers["X-Channel-Tag"] == "paid"          # header_override
        assert "upstream" not in create_req.headers                  # 配置块不透出为头
        assert "Upstream" not in create_req.headers
        sent = json.loads(create_req.content)
        assert sent["model"] == "MiniMax-H3"                          # model_mapping 生效
        assert sent["aigc_watermark"] is False                        # param_override
        assert sent["content"][0]["text"] == "史诗级太空歌剧院线预告"   # 用户报文透传
        assert sent["resolution"] == "2K" and sent["duration"] == 5
        assert "callback_url" not in sent                             # 探测模式不注入

        # ---- ② 探测推进：processing → 仍在途 ----
        await poll_one(task_id)
        row = task_store.rows[task_id]
        assert row["status"] == "IN_PROGRESS"
        assert queue_events["poll"][-1]["task_id"] == task_id         # 继续下轮探测

        # ---- ③ 探测终态：succeeded → 重估结算（4s × 0.026 = 0.104，退 0.026）----
        await poll_one(task_id)
        row = task_store.rows[task_id]
        assert row["status"] == "SUCCESS"
        assert row["data"]["result"] == "http://cdn.test/h3-output.mp4"

        assert len(queue_events["settle"]) == 1
        settle = queue_events["settle"][0]
        assert settle["request_id"] == task_id
        assert settle["actual_amount"] == pytest.approx(0.104)        # 多退少补
        assert settle["user_sk"] == "sk-user-42"                      # 用户令牌结算
        assert settle["units"] == 4
        assert queue_events["cancel"] == []

        # ---- ④ 用户回调通知（HMAC 签名投递载荷）----
        assert len(queue_events["notify"]) == 1
        notify = queue_events["notify"][0]
        assert notify["url"] == "https://user.test/done"
        assert notify["payload"]["status"] == "SUCCESS"
        assert notify["payload"]["result"] == "http://cdn.test/h3-output.mp4"
        # 回调载荷不泄上游任务 id：字段与值都不出现（内部实现细节）
        assert "upstream_task_id" not in notify["payload"]
        assert "424010985738629" not in json.dumps(notify["payload"], ensure_ascii=False)

        # ---- ⑤ 任务查询视图（task_id 即凭证，免鉴权）----
        got = await client.get(f"/minimax/v1/tasks/{task_id}")
        assert got.status_code == 200
        public = got.json()
        assert public["status"] == "SUCCESS"
        assert public["result"] == "http://cdn.test/h3-output.mp4"
        assert "freeze_amount" not in json.dumps(public)              # 不泄内部字段
        assert "upstream_task_id" not in public                       # 上游 id 同级收紧
        assert "424010985738629" not in json.dumps(public)            # 值本身也不出现
        assert 0 <= public["duration"] <= 300         # 耗时（秒）：终态-创建，量级健康

        # ⑤b 上游 id 反查兼容入口：客户端持上游 id 轮询也命中同一任务，
        #    但回显视图同样不含上游 id（反查是兜底，不等于授权回显）
        by_upstream = await client.get("/minimax/v1/tasks/424010985738629")
        assert by_upstream.status_code == 200
        assert by_upstream.json()["task_id"] == task_id
        assert "upstream_task_id" not in by_upstream.json()

        # ⑤c videos 形态 GET 同一白名单契约
        got_video = await client.get(f"/minimax/v1/videos/{task_id}")
        assert got_video.status_code == 200
        assert "upstream_task_id" not in got_video.json()
        assert "424010985738629" not in json.dumps(got_video.json())

        # ---- ⑥ 幂等重放：同 Idempotency-Key 不产生新任务/新扣费 ----
        replay = await client.post(
            "/minimax/v1/videos",
            json=T2VA_BODY,
            headers={"Authorization": "Bearer sk-user-42", "Idempotency-Key": "req-1"},
        )
        assert replay.status_code == 202
        assert replay.json()["task_id"] == task_id
        assert len(e2e_mocks.create.calls) == 1                       # 上游只收到一次提交
        assert len(e2e_mocks.freeze.calls) == 1                       # 只冻结一次


async def test_minimax_i2va_and_r2va_shapes_accepted(
    _reset_registry, e2e_mocks, test_settings, patch_redis, task_store, queue_events,
):
    """i2va / r2va 多模态 content[] 报文同样原样透传（适配层零模型知识）。"""
    i2va = {
        "model": "MiniMax-H3",
        "content": [
            {"type": "text", "text": "拉面碗加更多蒸汽"},
            {"type": "image_url",
             "image_url": {"url": "https://cdn.test/first.png"}, "role": "first_frame"},
        ],
        "resolution": "2K", "duration": 5, "ratio": "adaptive",
    }
    r2va = {
        "model": "MiniMax-H3",
        "content": [
            {"type": "text", "text": "角色说话：Follow the wind"},
            {"type": "video_url",
             "video_url": {"url": "https://cdn.test/ref.mp4"}, "role": "reference_video"},
            {"type": "audio_url",
             "audio_url": {"url": "https://cdn.test/ref.mp3"}, "role": "reference_audio"},
        ],
        "resolution": "2K", "duration": 5, "ratio": "adaptive",
    }
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        task_ids = []
        for body in (i2va, r2va):
            resp = await client.post(
                "/minimax/v1/videos", json=body,
                headers={"Authorization": "Bearer sk-user-42"},
            )
            assert resp.status_code == 202, resp.text
            task_ids.append(resp.json()["task_id"])
        assert len(e2e_mocks.create.calls) == 0     # 创建请求不触上游
        for task_id in task_ids:                    # worker 异步提交
            await submit_one(task_id)

    # 两次提交的多模态 content 结构原样到达上游（含 role 标注）
    for i, expect_roles in enumerate((["first_frame"], ["reference_video", "reference_audio"])):
        sent = json.loads(e2e_mocks.create.calls[i].request.content)
        types = [c["type"] for c in sent["content"]]
        roles = [c.get("role") for c in sent["content"] if c.get("role")]
        assert types[0] == "text"
        assert roles == expect_roles


async def test_unknown_biz_503_with_error_shape(
    _reset_registry, respx_router, test_settings, patch_redis, task_store, queue_events,
):
    """未接入的 biz（keypool 无渠道）→ 503 + OpenAI 风格错误体。"""
    respx_router.post("http://billing.test/api/v1/auth/inspect").mock(
        return_value=httpx.Response(200, json={"valid": True, "user_id": 1, "token_id": 1})
    )
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(503, json={"code": 40001, "message": "no available key",
                                               "data": {"retry_after_ms": 1000}})
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        resp = await client.post(
            "/unknownbiz/v1/videos", json=T2VA_BODY,
            headers={"Authorization": "Bearer sk-user-42"},
        )
    assert resp.status_code == 503
    body = resp.json()
    assert "error" in body and "message" in body["error"]            # 错误形制统一
