"""任务级租约钉回（``app.services.leasing``）测试。

纪律：任务级操作（探测/取消/原生查询/回调/对账）必须打回**创建时那把 key**
——keypool 的 ``channel_id + key_index`` 单 key 精确直达。同渠道挂多个上游
账号时，换 key 就查不到任务。

降级：key 级失败（40010 索引越界 / 40001 该 key 被禁用）→ 退渠道直达；
渠道级失败（40002）→ 原样上抛。
"""

from __future__ import annotations

import json

import httpx
import pytest

from app.services import leasing
from app.services.providers import KeyLeaseError

SELECT_OK = {
    "code": 0, "message": "ok",
    "data": {
        "channel_id": 7, "key_index": 3, "key": "sk-acct-b",
        "base_url": "http://upstream.test", "epoch": "e1", "mode": "direct",
        "channel": {"id": 7, "name": "minimax-main",
                    "base_url": "http://upstream.test",
                    "setting": {"gateway": {"biz": "minimax",
                                            "submit_path": "/v2/video_generation"}}},
    },
}

TASK_DATA = {"biz": "minimax", "model": "MiniMax-H3", "key_id": 7, "key_index": 3}


async def test_lease_for_task_uses_exact_key_index(respx_router, test_settings):
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=SELECT_OK)
    )
    key = await leasing.lease_for_task("minimax", TASK_DATA)

    assert key.key == "sk-acct-b" and key.key_index == 3
    assert len(select.calls) == 1
    body = json.loads(select.calls[0].request.content)
    assert body["channel_id"] == 7 and body["key_index"] == 3


async def test_lease_for_task_key_index_zero_is_sent(respx_router, test_settings):
    """下标 0 起：``key_index=0`` 必须照发（不能被 falsy 判断吃掉）。"""
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=SELECT_OK)
    )
    await leasing.lease_for_task("minimax", {**TASK_DATA, "key_index": 0})
    assert json.loads(select.calls[0].request.content)["key_index"] == 0


async def test_lease_for_task_falls_back_to_channel_when_key_gone(
    respx_router, test_settings,
):
    """索引越界（该 key 已被移出渠道，40010 永久性错误）→ 退渠道直达。"""
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        side_effect=[
            httpx.Response(400, json={"code": 40010, "message": "key_index out of range"}),
            httpx.Response(200, json=SELECT_OK),
        ]
    )
    key = await leasing.lease_for_task("minimax", TASK_DATA)

    assert key.key == "sk-acct-b"
    assert len(select.calls) == 2
    first = json.loads(select.calls[0].request.content)
    second = json.loads(select.calls[1].request.content)
    assert first["key_index"] == 3                     # 先精确直达
    assert second["channel_id"] == 7                   # 再渠道直达
    assert "key_index" not in second


async def test_lease_for_task_falls_back_when_key_disabled(respx_router, test_settings):
    """该 key 被禁用（40001）：同样降级渠道直达，换渠道内健康 key。"""
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        side_effect=[
            httpx.Response(503, json={"code": 40001, "message": "no available key",
                                      "data": {"retry_after_ms": 1000}}),
            httpx.Response(200, json=SELECT_OK),
        ]
    )
    key = await leasing.lease_for_task("minimax", TASK_DATA)
    assert key.key == "sk-acct-b" and len(select.calls) == 2


async def test_lease_for_task_channel_missing_raises(respx_router, test_settings):
    """渠道不存在（40002）不是 key 级问题：不做无谓的第二次调用，原样上抛。"""
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(404, json={"code": 40002, "message": "channel not found"})
    )
    with pytest.raises(KeyLeaseError):
        await leasing.lease_for_task("minimax", TASK_DATA)
    assert len(select.calls) == 1


async def test_lease_for_task_without_key_index_uses_channel(respx_router, test_settings):
    """旧任务没有 ``key_index`` 快照：直接渠道直达（不发无效的精确直达）。"""
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=SELECT_OK)
    )
    await leasing.lease_for_task("minimax", {"biz": "minimax", "key_id": 7})
    body = json.loads(select.calls[0].request.content)
    assert body["channel_id"] == 7 and "key_index" not in body


async def test_lease_for_task_channel_id_from_task_column(respx_router, test_settings):
    """``data.key_id`` 缺失时用 tasks.channel_id 列兜底。"""
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=SELECT_OK)
    )
    await leasing.lease_for_task("minimax", {"biz": "minimax", "key_index": 3},
                                 {"channel_id": 7})
    body = json.loads(select.calls[0].request.content)
    assert body["channel_id"] == 7 and body["key_index"] == 3


async def test_route_for_task_builds_and_caches_route(respx_router, test_settings):
    from app.services.registry import registry

    registry._cache.clear()
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=SELECT_OK)
    )
    try:
        key, route = await leasing.route_for_task("minimax", TASK_DATA)
        assert key.key_index == 3
        assert route.biz == "minimax" and route.submit_path == "/v2/video_generation"
        assert registry.get_cached("minimax") is route      # 已回填进程缓存
    finally:
        registry._cache.clear()
