"""管理面：看板页面 + 看板 API + 运行时热配置读写。

## 路由注册顺序（main.py）

管理面必须注册在通配路由 ``/{biz}/{path:path}`` **之前**——否则
``/admin/api/overview`` 会被通配当成 ``biz=admin / path=api/overview`` 吞掉
（通配永远最后，这是本仓库的装配纪律）。

## 鉴权

统一走 ``app.deps.admin.require_admin``：``X-Admin-Token`` 头，未配置
``ADMIN_TOKEN`` 时**整个管理面 404**（不暴露端点存在）。密钥字段与
``/ops/*`` 复用同一个 ``settings.admin_token``（见 deps/admin.py 的说明）。

## 脱敏纪律（不因为是管理面就放松）

- 任务详情/列表只做**白名单投影**：``token_hash``、原始 ``request_body``、
  上游 key、上游原始报文一律不出现在响应里；
- 令牌会话只给存在性与 TTL（``tokensession.session_info``），令牌本体绝不
  离开 Redis；
- 配置读写只覆盖 ``dynconf.MUTABLE`` 白名单，连接串/密钥/白名单永不可写。

## 破坏性操作边界

只提供「重投提交」（对 ``/batch`` 非终态任务重发队列消息）与「配置回落」两类
写操作。**没有删除任务**之类的入口——终态推进的唯一入口是 relayflow，管理面
绝不绕过它。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse

from app import queue
from app.config import settings
from app.deps.admin import admin_enabled, require_admin
from app.logging import log
from app.schemas import TERMINAL
from app.services import dynconf, taskstore, tokensession

router = APIRouter(prefix="/admin")

_STATIC = Path(__file__).resolve().parent.parent / "static"

#: 列表视图白名单字段（与 taskstore.search_tasks 的列投影一一对应）。
#: 凡不在此列的字段一律不返回——即便数据层多带了列，本层也会剥掉。
_LIST_FIELDS = (
    "task_id", "status", "progress", "action", "user_id", "channel_id",
    "created_at", "finish_time", "updated_at",
    "model", "biz", "source", "result", "freeze_amount", "settled",
)
#: 详情视图的 data 白名单（任务行经 taskstore.get 拿到的是嵌套 data）
_DETAIL_DATA_FIELDS = (
    "biz", "source", "model", "upstream_status", "key_index",
    "freeze_amount", "settled", "settled_amount",
)
#: 详情视图的顶层白名单
_DETAIL_FIELDS = (
    "task_id", "status", "action", "user_id", "channel_id",
    "created_at", "submit_time", "updated_at", "finish_time",
    "fail_reason", "progress",
)


def _list_item(row: dict) -> dict:
    """列表行脱敏投影：只取白名单键，并补一个归一后的 duration。"""
    item: dict[str, Any] = {key: row.get(key) for key in _LIST_FIELDS}
    item["duration"] = taskstore.duration_seconds(row)
    return item


def _task_view(task: dict) -> dict:
    """单任务脱敏投影（白名单，绝不整行透传 ``data``）。

    刻意**不返回** ``data.token_hash`` / ``data.request_body`` / 上游 key /
    ``data.upstream_snapshot``（终态原始报文）。诊断需要的令牌信息只给
    存在性与 TTL。
    """
    data = task.get("data") or {}
    view: dict[str, Any] = {key: task.get(key) for key in _DETAIL_FIELDS}
    view["duration"] = taskstore.duration_seconds(task)
    view["data"] = {key: data.get(key) for key in _DETAIL_DATA_FIELDS}
    view["has_callback_url"] = bool(data.get("callback_url"))
    view["has_idempotency_key"] = bool(data.get("idempotency_key"))
    view["has_token_session_key"] = bool(data.get("token_hash"))
    return view


async def _search_tasks(*, status: str = "", model: str = "", task_id: str = "",
                        since_seconds: int = 0, limit: int = 50,
                        offset: int = 0) -> dict:
    """任务检索的统一接线点，把 ``(items, total)`` 归成 HTTP 形状的 dict。

    真实查询在数据访问单点 ``app.services.taskstore.search_tasks``（带
    ``platform`` 过滤与精确等值筛选、无前导通配）——本仓库静态门禁规定 tasks
    表 SQL 只能出现在那里。后端函数缺席时明确 503，不静默返回空列表：空列表
    会被误读成「窗口内没有失败任务」，是最坏的一种谎报。
    """
    search = getattr(taskstore, "search_tasks", None)
    if search is None:
        raise HTTPException(503, "task listing backend unavailable (taskstore.search_tasks missing)")
    items, total = await search(
        status=status, model=model, task_id=task_id,
        since_seconds=since_seconds, limit=limit, offset=offset,
    )
    return {"items": list(items), "total": int(total)}


# ---------------------------------------------------------------------------
# 看板页面
# ---------------------------------------------------------------------------


@router.get("", include_in_schema=False)
@router.get("/", include_in_schema=False)
async def dashboard() -> HTMLResponse:
    """看板页面（单文件 HTML，零构建）。

    页面本身**不鉴权**——它只是一个空壳，所有数据都要带密钥调 API 才拿得到；
    密钥存浏览器 sessionStorage，不落 URL。管理面未启用时同样 404。
    """
    if not admin_enabled():
        raise HTTPException(404, "not found")
    page = _STATIC / "admin.html"
    if not page.exists():
        raise HTTPException(500, "dashboard asset missing")
    return HTMLResponse(page.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# 看板 API
# ---------------------------------------------------------------------------


@router.get("/api/overview")
async def overview(
    window: int = Query(3600, ge=60, le=7 * 86400),
    _: None = Depends(require_admin),
) -> dict:
    """概览：队列健康 + 任务状态分布 + 窗口内失败数 + 运行时信息。"""
    stats = await queue.queue_stats()
    tasks_by_status = stats.get("tasks_by_status") or {}

    recent_failures: int | None = None
    try:
        found = await _search_tasks(status="FAILURE", since_seconds=window, limit=1)
        recent_failures = int(found.get("total", 0))
    except Exception:
        # 次要指标失败不应拖垮整个概览页：检索后端缺席（503）或查询出错时
        # 失败数标为未知（HTML 显示「-」），队列/状态分布等主指标照常展示。
        log.opt(exception=True).debug("overview recent-failures probe failed")
        recent_failures = None

    return {
        "window_seconds": window,
        "queue": {
            "pending": stats.get("pending", 0),
            "delayed": stats.get("delayed", 0),
            "dlq": stats.get("dlq", 0),
        },
        "tasks_by_status": tasks_by_status,
        "recent_failures": recent_failures,
        "service": {
            "version": settings.app_version,
            "env": settings.app_env,
            "platform": settings.gateway_platform,
            "override_count": (await dynconf.snapshot())["override_count"],
        },
    }


@router.get("/api/tasks")
async def list_tasks(
    status: str = Query(
        "", pattern="^(SUBMITTED|QUEUED|IN_PROGRESS|HELD|SUCCESS|FAILURE|CANCELED)?$"),
    model: str = Query("", max_length=128),
    task_id: str = Query("", max_length=128),
    since: int = Query(0, ge=0, le=7 * 86400),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100_000),
    _: None = Depends(require_admin),
) -> dict:
    """任务列表（分页 + 精确筛选）。不含请求体/令牌/上游报文。

    ``task_id`` 只做**精确匹配**，不做前缀或片段通配——``LIKE '%...'`` 会让
    task_id 索引失效退化成全表扫描，把看板查询变成生产库的负载源。

    脱敏在**本层**再兜一次底：即便数据层多带回了列（含 ``token_hash`` /
    ``request_body``），这里也统一过 ``_list_item`` 白名单投影后才出 HTTP。
    """
    found = await _search_tasks(status=status, model=model, task_id=task_id,
                                since_seconds=since, limit=limit, offset=offset)
    raw_items = found.get("items") or []
    return {
        "items": [_list_item(row) for row in raw_items],
        "total": int(found.get("total", len(raw_items))),
        "limit": limit,
        "offset": offset,
    }


@router.get("/api/tasks/{task_id}")
async def task_detail(task_id: str, _: None = Depends(require_admin)) -> dict:
    """单任务详情（脱敏投影 + 令牌会话存在性/TTL）。"""
    task = await taskstore.get(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    view = _task_view(task)
    view["token_session"] = await tokensession.session_info(task_id)
    return view


@router.post("/api/tasks/{task_id}/requeue")
async def requeue(task_id: str, _: None = Depends(require_admin)) -> dict:
    """手动补投：立即把 ``/batch`` 任务重新放入提交队列（``queue.publish_batch_submit``）。

    **只对非终态任务开放**。终态任务重投毫无意义，且会绕过 relayflow 的
    单一终态收口点，制造重复事件的口子——所以这里对终态明确 409 拒绝。
    """
    task = await taskstore.get(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    if task["status"] in TERMINAL:
        raise HTTPException(
            409, f"task already terminal: {task['status']} (requeue only for non-terminal)")
    await queue.publish_batch_submit(task_id)
    log.warning("task requeued from admin: task_id={} status={}", task_id, task["status"])
    return {"task_id": task_id, "status": task["status"], "requeued": True}


# ---------------------------------------------------------------------------
# 运行时热配置
# ---------------------------------------------------------------------------


@router.get("/api/config")
async def read_config(_: None = Depends(require_admin)) -> dict:
    """可热改配置的全量视图 + 只读项及其原因。"""
    return await dynconf.snapshot()


@router.put("/api/config")
async def write_config(
    updates: dict[str, Any] = Body(...),
    _: None = Depends(require_admin),
) -> dict:
    """批量更新覆盖值。白名单外的键一律拒绝，校验失败整批回退。"""
    if not isinstance(updates, dict) or not updates:
        raise HTTPException(400, "expected a non-empty JSON object of key -> value")
    try:
        return await dynconf.set_many(updates)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        # Redis 写失败：明确 503，绝不谎报成功（运维会以为已生效）
        log.opt(exception=True).warning("dynconf write failed")
        raise HTTPException(503, f"dynamic config write requires Redis: {exc}") from exc


@router.post("/api/config/reset")
async def reset_config(
    keys: list[str] | None = Body(None),
    _: None = Depends(require_admin),
) -> dict:
    """删除覆盖值回落 env。``keys`` 为空/省略则清空全部覆盖。

    用 POST 而不是 DELETE：DELETE 带请求体在很多 HTTP 客户端与代理上
    行为不一致（有的直接丢掉 body）。
    """
    try:
        return await dynconf.reset(keys)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        log.opt(exception=True).warning("dynconf reset failed")
        raise HTTPException(503, f"dynamic config reset requires Redis: {exc}") from exc
