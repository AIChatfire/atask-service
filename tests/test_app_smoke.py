"""应用冒烟测试：无 DB/Redis 环境可导入；探针与新链路鉴权边界行为。"""

from __future__ import annotations

import httpx


def test_app_importable():
    from app.main import app

    assert app.title == "atask-service"
    paths = {r.path for r in app.routes}
    assert "/healthz/live" in paths
    assert "/batch/{path:path}" in paths
    assert "/ops/queue" in paths
    assert "/admin/api/overview" in paths


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
        resp = await client.post("/batch/v1/videos", json={"model": "MiniMax-H3"})
    assert resp.status_code == 401
    body = resp.json()
    assert "error" in body                       # OpenAI 风格错误形制


async def test_get_unknown_task_404(patch_redis, task_store, test_settings):
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/batch/v1/tasks/task_" + "0" * 32)
    assert resp.status_code == 404
    assert "error" in resp.json()
