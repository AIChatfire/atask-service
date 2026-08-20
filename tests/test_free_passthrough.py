"""GET 免费透传选渠道测试。

纪律：免费请求不带 model，keypool ``select(group, model)`` 对空 model 直接拒绝
（40010）——所以网关**永不发起空 model 的 select**，按
「Redis biz→channel_id 记忆 → 进程路由缓存」钉回 ``channel_id`` 直达租约，
两级都落空才 404。
"""

from __future__ import annotations

import json

import httpx

from app.main import app
from app.services import routecache
from app.services.registry import registry

_SELECT_PAYLOAD = {
    "code": 0, "message": "ok",
    "data": {
        "channel_id": 7, "key_index": 0, "key": "sk-upstream-key",
        "base_url": "http://upstream.test", "epoch": "e1",
        "channel": {"id": 7, "name": "minimax-main",
                    "base_url": "http://upstream.test", "setting": {}},
    },
}


async def test_free_get_pins_back_to_cached_channel(
    respx_router, test_settings, patch_redis, route_factory,
):
    """进程缓存有该 biz 渠道：**第一次** select 就带 channel_id 直达，
    绝不先发一次必然 40010 的空 model 请求。"""
    registry._cache.clear()
    registry.remember(route_factory(channel_id=7))          # biz=minimax 有缓存渠道
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT_PAYLOAD)
    )
    upstream = respx_router.get("http://upstream.test/v2/models").mock(
        return_value=httpx.Response(200, json={"data": ["m1"]})
    )
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
            resp = await client.get("/minimax/v2/models")
    finally:
        registry._cache.clear()

    assert resp.status_code == 200
    assert resp.json() == {"data": ["m1"]}
    assert upstream.calls
    assert len(select.calls) == 1                             # 零浪费：只问一次
    body = json.loads(select.calls[0].request.content)
    assert body["channel_id"] == 7                            # 钉渠道直达
    assert "group" not in body and "model" not in body


async def test_free_get_pins_back_via_redis_memory(
    respx_router, test_settings, patch_redis,
):
    """进程缓存为空（冷启动/多副本）但 Redis 有 biz→channel_id 记忆：
    照样钉回直达，不退化为 404。"""
    registry._cache.clear()
    await routecache.remember("minimax", 7)
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(200, json=_SELECT_PAYLOAD)
    )
    upstream = respx_router.get("http://upstream.test/v2/models").mock(
        return_value=httpx.Response(200, json={"data": ["m1"]})
    )
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
            resp = await client.get("/minimax/v2/models")
    finally:
        registry._cache.clear()

    assert resp.status_code == 200 and upstream.calls
    assert len(select.calls) == 1
    assert json.loads(select.calls[0].request.content)["channel_id"] == 7


async def test_free_get_without_any_channel_memory_404(
    respx_router, test_settings, patch_redis,
):
    """两级记忆全落空：404 unknown biz，且**一次 keypool 都不问**。"""
    registry._cache.clear()
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(400, json={"code": 40010, "message": "model required"})
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        resp = await client.get("/nobiz/v2/models")
    assert resp.status_code == 404
    assert not select.calls
