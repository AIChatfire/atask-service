"""W1 路由注册顺序断言（§4.2 不可妥协的不变量）。

两层断言：
1. 路由表顺序：healthz/callbacks/docs → /{biz}/v1/videos 四端点 → catch-all 最后；
2. 逐路径首匹配模拟：固定路径必须命中各自处理器，不被 catch-all 截获；
   四层原生路径（/kling/v1/videos/text2video）必须落入 catch-all（层级冲突约定）。
"""

from __future__ import annotations

import pytest
from fastapi import APIRouter
from fastapi.testclient import TestClient
from starlette.routing import BaseRoute, Match

import app.main as main_mod


def _make_app(monkeypatch: pytest.MonkeyPatch, with_callbacks: bool = True):
    """create_app + 假 callbacks 路由（W4 未交付；顺序占位由注入替身验证）。"""
    if with_callbacks:
        fake = APIRouter()

        @fake.post("/callbacks/{biz}/{provider}/{capability}")
        async def _fake_callback() -> dict[str, bool]:
            return {"ok": True}

        monkeypatch.setattr(main_mod, "_load_callbacks_router", lambda: fake)
    return main_mod.create_app()


def _route_paths(app) -> list[str]:
    return [getattr(r, "path", "") for r in app.routes]


def _first_match(app, path: str, method: str) -> BaseRoute | None:
    scope = {"type": "http", "path": path, "method": method}
    for route in app.routes:
        match, _ = route.matches(scope)
        if match == Match.FULL:
            return route
    return None


def test_registration_order(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _make_app(monkeypatch)
    paths = _route_paths(app)

    catch_all = paths.index("/{biz}/{native_path:path}")
    assert catch_all == len(paths) - 1, "catch-all 必须最后注册"

    for fixed in ("/healthz/live", "/healthz/ready",
                  "/callbacks/{biz}/{provider}/{capability}",
                  "/openapi.json", "/docs"):
        assert paths.index(fixed) < catch_all, f"{fixed} 必须先于 catch-all"
    for videos_path in ("/{biz}/v1/videos", "/{biz}/v1/videos/{task_id}",
                        "/{biz}/v1/videos/{task_id}/content",
                        "/{biz}/v1/videos/{video_id}/remix"):
        assert paths.index(videos_path) < catch_all
    # 相对顺序：healthz → callbacks → videos → catch-all
    assert paths.index("/healthz/live") < paths.index(
        "/callbacks/{biz}/{provider}/{capability}")
    assert paths.index("/callbacks/{biz}/{provider}/{capability}") < paths.index(
        "/{biz}/v1/videos")


@pytest.mark.parametrize(("path", "method", "endpoint"), [
    ("/healthz/live", "GET", "healthz_live"),
    ("/healthz/ready", "GET", "healthz_ready"),
    ("/callbacks/kling/kling/cap123", "POST", "_fake_callback"),
    ("/kling/v1/videos", "POST", "videos_submit"),
    ("/kling/v1/videos/task_abc", "GET", "videos_get"),
    ("/kling/v1/videos/task_abc/content", "GET", "videos_content"),
    ("/kling/v1/videos/task_abc/remix", "POST", "videos_remix"),
])
def test_fixed_paths_not_swallowed(monkeypatch: pytest.MonkeyPatch,
                                   path: str, method: str, endpoint: str) -> None:
    app = _make_app(monkeypatch)
    route = _first_match(app, path, method)
    assert route is not None
    assert getattr(route, "name", "") == endpoint, (
        f"{method} {path} 被 {getattr(route, 'path', route)} 截获"
    )


def test_native_four_segment_path_falls_to_catch_all(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """层级冲突约定（§4.2）：四层原生路径落 catch-all 透传。"""
    app = _make_app(monkeypatch)
    route = _first_match(app, "/kling/v1/videos/text2video", "POST")
    assert route is not None
    assert getattr(route, "name", "") == "passthrough"
    route = _first_match(app, "/seedance/api/v3/contents/generations/tasks", "POST")
    assert getattr(route, "name", "") == "passthrough"


def test_healthz_live_200_and_callbacks_not_swallowed(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """端到端：探针 200（零依赖）；callbacks 走替身路由而非 catch-all（401）。"""
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        assert client.get("/healthz/live").json() == {"status": "ok"}
        resp = client.post("/callbacks/kling/kling/cap123", content=b"{}")
        assert resp.status_code == 200 and resp.json() == {"ok": True}
        # catch-all 需 Bearer 认证：无头 → 401 OpenAI 风格错误体
        resp = client.post("/kling/v1/videos/text2video", content=b"{}")
        assert resp.status_code == 401
        assert resp.json()["error"]["type"] == "authentication_error"
