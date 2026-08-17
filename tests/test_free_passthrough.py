"""GET 免费透传选渠道测试（[19]）：keypool 空 model 40010 时，
按进程缓存里该 biz 最近使用的渠道 channel_id 直达反查（钉渠道）。"""

from __future__ import annotations

import json

import httpx

from app.main import app
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
    registry._cache.clear()
    registry.remember(route_factory(channel_id=7))          # biz=minimax 有缓存渠道
    select = respx_router.post("http://keypool.test/v1/keys/select").mock(
        side_effect=[
            httpx.Response(400, json={"code": 40010, "message": "model required"}),
            httpx.Response(200, json=_SELECT_PAYLOAD),
        ]
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
    first = json.loads(select.calls[0].request.content)
    assert first.get("model") == ""                          # 首次空 model 被拒
    second = json.loads(select.calls[1].request.content)
    assert second["channel_id"] == 7                         # 回落钉渠道直达
    assert "group" not in second and "model" not in second


async def test_free_get_without_cached_channel_404(
    respx_router, test_settings, patch_redis,
):
    """进程缓存无该 biz 渠道：维持 404 unknown biz。"""
    registry._cache.clear()
    respx_router.post("http://keypool.test/v1/keys/select").mock(
        return_value=httpx.Response(400, json={"code": 40010, "message": "model required"})
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as client:
        resp = await client.get("/nobiz/v2/models")
    assert resp.status_code == 404
