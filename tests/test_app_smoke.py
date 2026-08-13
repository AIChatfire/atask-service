"""应用冒烟测试：无 DB/Redis 环境可导入；探针与鉴权/限流边界行为。"""

from __future__ import annotations

import httpx


def test_app_importable():
    from app.main import app

    assert app.title == "atask-service"
    paths = {r.path for r in app.routes}
    assert "/healthz/live" in paths
    assert "/{biz}/v1/tasks" in paths
    assert "/{biz}/v1/videos" in paths
    assert "/callback/{biz}/{task_id}" in paths
    assert "/{biz}/{path:path}" in paths


async def test_healthz_live(patch_redis):
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/healthz/live")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_missing_auth_401(patch_redis, task_store, test_settings):
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.post("/minimax/v1/videos", json={"model": "MiniMax-H3"})
    assert resp.status_code == 401
    body = resp.json()
    assert "error" in body                       # OpenAI 风格错误形制


async def test_get_task_404(patch_redis, task_store, test_settings, respx_router):
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/minimax/v1/tasks/nonexistent")
    assert resp.status_code == 404
    assert "error" in resp.json()
