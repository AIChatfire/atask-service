"""状态映射与路由构建测试。

- statusmap：内置字典 / 显式 status_map / 前缀猜测 / 归一化（MiniMax-H3
  实际状态词 succeeded/failed/cancelled/queued/running/processing 全覆盖）；
- registry：RouteConfig 从 keypool 渠道 setting.gateway 块构建（零路由文件
  的核心契约）+ 进程缓存行为。
"""

from __future__ import annotations

import pytest

from app.schemas import (
    CANCELED,
    FAILURE,
    IN_PROGRESS,
    QUEUED,
    SUCCESS,
    RouteConfig,
)
from app.services import statusmap
from app.services.registry import route_from_channel

# ---------------------------------------------------------------------------
# statusmap
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("raw", "expected"), [
    ("succeeded", SUCCESS), ("Success", SUCCESS), ("SUCCESS", SUCCESS),
    ("failed", FAILURE), ("Fail", FAILURE),
    ("cancelled", CANCELED), ("canceled", CANCELED),
    ("queued", QUEUED), ("pending", QUEUED), ("Preparing", QUEUED),
    ("running", IN_PROGRESS), ("processing", IN_PROGRESS),
    ("TASK_STATUS_SUCCEED", SUCCESS),          # 命名空间前缀剥离
    ("task.succeeded", SUCCESS),
    ("State: Running", IN_PROGRESS),
])
def test_map_status_builtin(raw, expected):
    assert statusmap.map_status(None, raw) == expected


def test_map_status_unknown_returns_none():
    assert statusmap.map_status(None, "flibbertigibbet") is None
    assert statusmap.map_status(None, "") is None
    assert statusmap.map_status(None, None) is None


def test_map_status_explicit_route_map_priority():
    route = RouteConfig(biz="x", status_map={"done_succeeded": "SUCCESS"})
    assert statusmap.map_status(route, "done_succeeded") == SUCCESS
    # 显式 map 不覆盖时回落内置字典
    assert statusmap.map_status(route, "failed") == FAILURE


# ---------------------------------------------------------------------------
# registry.route_from_channel（零路由文件核心契约）
# ---------------------------------------------------------------------------


def _minimax_channel(gateway: dict | None = None) -> dict:
    setting = {"gateway": gateway} if gateway is not None else {}
    return {"id": 7, "name": "minimax-a", "base_url": "https://api.minimaxi.com",
            "setting": setting}


def test_route_from_channel_full_gateway_block():
    route = route_from_channel("minimax", _minimax_channel({
        "submit_path": "/v2/video_generation",
        "probe_path": "/v2/query/video_generation/{upstream_task_id}",
        "status_path": "task.status",
        "result_path": "task.content.url",
        "error_path": "task.error",
        "settle_usage_map": {"duration": "task.usage.output_seconds"},
        "pricing_biz_type": "video_generation",
    }))
    assert route.biz == "minimax-a"         # biz 缺省取渠道 name
    assert route.upstream_base_url == "https://api.minimaxi.com"
    assert route.submit_path == "/v2/video_generation"
    assert route.probe_path == "/v2/query/video_generation/{upstream_task_id}"
    assert route.status_path == "task.status"
    assert route.result_path == "task.content.url"
    assert route.settle_usage_map == {"duration": "task.usage.output_seconds"}
    assert route.pricing_biz_type == "video_generation"
    # 缺省值兜底
    assert route.task_id_path == "task_id"
    assert route.auth_type == "bearer"
    assert route.supports_callback is False


def test_route_from_channel_defaults_when_no_gateway_block():
    """渠道没配 setting.gateway：全部缺省值（submit_path 空 → 提交立即可见报错）。"""
    route = route_from_channel("minimax", _minimax_channel(None))
    assert route.submit_path == ""
    assert route.status_path == "status"
    assert route.task_id_path == "task_id"
    assert route.upstream_base_url == "https://api.minimaxi.com"


def test_route_from_channel_other_gateway_fallback():
    """兼容渠道 other.gateway（旧版配置位）。"""
    route = route_from_channel("minimax", {
        "id": 7, "other": {"gateway": {"submit_path": "/x"}}})
    assert route.submit_path == "/x"


# ---- header_override.upstream 配置源（与 setting.gateway 等价，优先级最高）----


def test_route_from_header_override_upstream():
    """header_override.upstream 嵌套块完整解析（与 setting.gateway 同构）。"""
    route = route_from_channel("minimax", {
        "id": 7, "name": "minimax-main", "base_url": "https://api.minimaxi.com",
        "header_override": {
            "X-Channel-Tag": "paid",
            "upstream": {
                "biz": "minimax",
                "submit_path": "/v2/video_generation",
                "probe_path": "/v2/query/video_generation/{upstream_task_id}",
                "status_path": "task.status",
                "result_path": "task.content.url",
                "error_path": "task.error",
                "settle_usage_map": {"duration": "task.usage.output_seconds"},
                "pricing_biz_type": "video_generation",
            },
        },
    })
    assert route.biz == "minimax"                        # upstream.biz 显式指定
    assert route.submit_path == "/v2/video_generation"
    assert route.probe_path == "/v2/query/video_generation/{upstream_task_id}"
    assert route.status_path == "task.status"
    assert route.result_path == "task.content.url"
    assert route.error_path == "task.error"
    assert route.settle_usage_map == {"duration": "task.usage.output_seconds"}
    assert route.pricing_biz_type == "video_generation"
    assert route.upstream_base_url == "https://api.minimaxi.com"


def test_route_header_override_upstream_wins_over_setting_gateway():
    """两处同时存在：header_override.upstream 优先（配置源 precedence）。"""
    route = route_from_channel("minimax", {
        "id": 7,
        "header_override": {"upstream": {"submit_path": "/new"}},
        "setting": {"gateway": {"submit_path": "/old"}},
    })
    assert route.submit_path == "/new"


def test_route_malformed_upstream_falls_back():
    """header_override.upstream 不是对象（被当成普通头）→ 回落 setting.gateway。"""
    route = route_from_channel("minimax", {
        "id": 7,
        "header_override": {"upstream": "just-a-header"},
        "setting": {"gateway": {"submit_path": "/fallback"}},
    })
    assert route.submit_path == "/fallback"


def test_route_from_empty_channel():
    route = route_from_channel("minimax", {})
    assert route.biz == "minimax" and route.submit_path == ""
    assert route.upstream_base_url == ""


def test_route_biz_resolution_priority():
    """biz 从渠道取：gateway.biz 显式 → 渠道 name → URL 段兜底。"""
    # 显式 gateway.biz 最高优先
    route = route_from_channel("url-label", {
        "name": "channel-name", "setting": {"gateway": {"biz": "explicit-biz"}}})
    assert route.biz == "explicit-biz"
    # 缺省取渠道 name
    route = route_from_channel("url-label", _minimax_channel({"submit_path": "/x"}))
    assert route.biz == "minimax-a"
    # 渠道 name 缺失时 URL 段兜底
    route = route_from_channel("url-label", {"setting": {"gateway": {"submit_path": "/x"}}})
    assert route.biz == "url-label"


def test_registry_cache_remember_and_get_cached():
    from app.services.registry import RouteRegistry

    reg = RouteRegistry(ttl=60)
    route = route_from_channel("minimax", _minimax_channel({"submit_path": "/v2/x"}))
    assert reg.get_cached(route.biz) is None
    reg.remember(route)
    assert reg.get_cached(route.biz).submit_path == "/v2/x"
    # TTL 过期后失效
    reg2 = RouteRegistry(ttl=-1)
    reg2.remember(route)
    assert reg2.get_cached(route.biz) is None
