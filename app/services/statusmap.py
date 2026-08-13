"""上游任务状态自动映射 → 内部状态（兼容 NewAPI tasks 状态枚举）。

三级体系，优先级从高到低：
  1. 路由配置里的显式 status_map（配置中心/YAML/Redis 均可下发，免发版扩展）
  2. 内置字典全枚举（归一化后精确匹配，覆盖常见上游措辞）
  3. 前缀猜测（归一化后前缀命中，应对没见过的新状态）
未命中 → 返回 None 并日志告警（每进程每种状态只报一次），
日志中收集到的未知状态先加到该 biz 的 status_map 应急，稳定后回流到本文件字典。

归一化：小写、非字母数字折叠为下划线、剥离常见命名空间前缀
（task_/job_/status_/state_/stage_/generation_/video_/result_），
兼容 "TASK_STATUS_SUCCEED"、"task.succeeded"、"State: Running" 等风格。
"""

import logging
import re

from app.schemas import (
    CANCELED,
    FAILURE,
    IN_PROGRESS,
    QUEUED,
    SUCCESS,
    RouteConfig,
)

log = logging.getLogger("gateway.statusmap")

# ---- 内置字典（归一化后的精确枚举）----
_ENUM: dict[str, str] = {
    # SUCCESS
    "success": SUCCESS, "succeeded": SUCCESS, "succeed": SUCCESS, "successful": SUCCESS,
    "completed": SUCCESS, "complete": SUCCESS, "done": SUCCESS, "finished": SUCCESS,
    "ok": SUCCESS, "resolved": SUCCESS,
    # FAILURE
    "failure": FAILURE, "failed": FAILURE, "fail": FAILURE, "error": FAILURE,
    "errored": FAILURE, "timeout": FAILURE, "timed_out": FAILURE, "expired": FAILURE,
    "rejected": FAILURE, "crashed": FAILURE, "abnormal": FAILURE,
    # CANCELED
    "canceled": CANCELED, "cancelled": CANCELED, "canceled_by_user": CANCELED,
    "aborted": CANCELED, "revoked": CANCELED, "terminated": CANCELED, "killed": CANCELED,
    # IN_PROGRESS
    "in_progress": IN_PROGRESS, "processing": IN_PROGRESS, "running": IN_PROGRESS,
    "started": IN_PROGRESS, "generating": IN_PROGRESS, "executing": IN_PROGRESS,
    "working": IN_PROGRESS, "ongoing": IN_PROGRESS,
    # QUEUED / SUBMITTED（都归入 QUEUED，提交瞬间的 SUBMITTED 由网关自己写入）
    "queued": QUEUED, "pending": QUEUED, "submitted": QUEUED, "accepted": QUEUED,
    "created": QUEUED, "scheduled": QUEUED, "waiting": QUEUED, "received": QUEUED,
    "preparing": QUEUED, "initializing": QUEUED,
}

# ---- 前缀猜测（有序，先终态后活跃，首个命中生效）----
_PREFIX: tuple[tuple[str, str], ...] = (
    ("succeed", SUCCESS), ("success", SUCCESS), ("complete", SUCCESS),
    ("finish", SUCCESS), ("done", SUCCESS),
    ("fail", FAILURE), ("error", FAILURE), ("timeout", FAILURE),
    ("expire", FAILURE), ("reject", FAILURE), ("crash", FAILURE),
    ("cancel", CANCELED), ("abort", CANCELED), ("revoke", CANCELED), ("terminat", CANCELED),
    ("progress", IN_PROGRESS), ("process", IN_PROGRESS), ("run", IN_PROGRESS),
    ("generat", IN_PROGRESS), ("execut", IN_PROGRESS), ("start", IN_PROGRESS),
    ("queue", QUEUED), ("pend", QUEUED), ("submit", QUEUED),
    ("accept", QUEUED), ("creat", QUEUED), ("schedul", QUEUED), ("wait", QUEUED),
)

_NS_PREFIX = re.compile(r"^(task|job|status|state|stage|generation|video|result)_")
_seen_unknown: set[str] = set()


def _normalize(raw: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", raw.strip().lower()).strip("_")
    for _ in range(3):                      # 逐层剥离命名空间前缀
        stripped = _NS_PREFIX.sub("", s)
        if stripped == s:
            break
        s = stripped
    return s


def map_status(route: RouteConfig | None, raw: object) -> str | None:
    """上游原始状态 → 内部状态；未识别返回 None"""
    if raw is None:
        return None
    raw_str = str(raw).strip()
    if not raw_str:
        return None

    # 1) 显式配置（per-biz，配置中心可热更）
    if route and route.status_map:
        hit = route.status_map.get(raw_str) or route.status_map.get(raw_str.lower())
        if hit:
            return hit.upper()

    norm = _normalize(raw_str)

    # 2) 内置字典精确枚举
    if norm in _ENUM:
        return _ENUM[norm]

    # 3) 前缀猜测
    for prefix, target in _PREFIX:
        if norm.startswith(prefix):
            return target

    if raw_str not in _seen_unknown:
        _seen_unknown.add(raw_str)
        log.warning("unknown upstream status %r (biz=%s) — 请加入 status_map 或内置字典",
                    raw_str, route.biz if route else "?")
    return None
