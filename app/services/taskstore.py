"""tasks 表读写（NewAPI 现有表，网关零建表）。
- 扩展字段全部在 data JSON 列（JSON_MERGE_PATCH 合并）
- 状态机迁移一律 CAS：rowcount=1 才视为抢到推进权（恰好一次）
- 时间字段为 int64 unix 秒
"""

import json
import logging
import time

from sqlalchemy import bindparam, text

from app.config import settings
from app.db import SessionLocal
from app.schemas import ACTIVE, TERMINAL

log = logging.getLogger("gateway.taskstore")


def _now() -> int:
    return int(time.time())


async def create(
    task_id: str,
    user_id: int,
    channel_id: int,
    action: str,
    data: dict,
) -> None:
    now = _now()
    async with SessionLocal() as db:
        await db.execute(
            text(
                """
                INSERT INTO tasks
                  (task_id, platform, action, status, progress, data,
                   user_id, channel_id, quota, submit_time, created_at, updated_at)
                VALUES
                  (:task_id, :platform, :action, 'SUBMITTED', '0%', CAST(:data AS JSON),
                   :user_id, :channel_id, 0, :now, :now, :now)
                """
            ),
            {
                "task_id": task_id,
                "platform": settings.gateway_platform,
                "action": action,
                "data": json.dumps(data, ensure_ascii=False),
                "user_id": user_id,
                "channel_id": channel_id,
                "now": now,
            },
        )
        await db.commit()


async def get(task_id: str) -> dict | None:
    async with SessionLocal() as db:
        row = (
            await db.execute(
                text("SELECT * FROM tasks WHERE task_id = :t LIMIT 1"), {"t": task_id}
            )
        ).mappings().first()
    if not row:
        return None
    result = dict(row)
    data = result.get("data")
    if isinstance(data, str):
        result["data"] = json.loads(data)
    elif data is None:
        result["data"] = {}
    return result


async def cas(
    task_id: str,
    from_statuses: tuple[str, ...],
    to_status: str,
    patch: dict | None = None,
    fail_reason: str = "",
) -> bool:
    """CAS 状态迁移。返回 True = 本调用者抢到推进权（负责后续事件/额度释放）"""
    now = _now()
    stmt = text(
        """
        UPDATE tasks
        SET status = :to,
            updated_at = :now,
            finish_time = IF(:terminal = 1, :now, finish_time),
            progress = IF(:to_status = 'SUCCESS', '100%', progress),
            fail_reason = :reason,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()), CAST(:patch AS JSON))
        WHERE task_id = :tid AND status IN :froms
        """
    ).bindparams(bindparam("froms", expanding=True))
    async with SessionLocal() as db:
        res = await db.execute(
            stmt,
            {
                "to": to_status,
                "to_status": to_status,
                "now": now,
                "terminal": 1 if to_status in TERMINAL else 0,
                "reason": fail_reason,
                "patch": json.dumps(patch or {}, ensure_ascii=False),
                "tid": task_id,
                "froms": from_statuses,
            },
        )
        await db.commit()
        return res.rowcount == 1


async def patch_data(task_id: str, patch: dict, status: str | None = None) -> None:
    """非迁移性的数据合并（如回填 upstream_task_id）；可选顺带更新状态列"""
    set_status = "status = :status, " if status else ""
    sql = f"""
        UPDATE tasks
        SET {set_status}updated_at = :now,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()), CAST(:patch AS JSON))
        WHERE task_id = :tid
    """
    params: dict = {
        "now": _now(),
        "patch": json.dumps(patch, ensure_ascii=False),
        "tid": task_id,
    }
    if status:
        params["status"] = status
    async with SessionLocal() as db:
        await db.execute(text(sql), params)
        await db.commit()


async def mark_settled(task_id: str, amount: float) -> None:
    await patch_data(task_id, {"settled": True, "settled_amount": amount})


async def stale_active(stale_seconds: int, limit: int = 200) -> list[str]:
    """Sweeper：非终态且长时间未更新的任务"""
    cutoff = _now() - stale_seconds
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT task_id FROM tasks
                    WHERE platform = :p AND status IN :acts AND updated_at < :cutoff
                    LIMIT :lim
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE, "cutoff": cutoff, "lim": limit},
            )
        ).scalars().all()
    return list(rows)


async def counts_by_status() -> dict[str, int]:
    """任务状态分布（队列观测用）"""
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT status, COUNT(*) AS n FROM tasks WHERE platform = :p GROUP BY status"
                ),
                {"p": settings.gateway_platform},
            )
        ).all()
    return {row[0]: row[1] for row in rows}


async def terminal_unsettled(limit: int = 200) -> list[dict]:
    """Sweeper：终态但结算标记未落的任务（事件丢失的兜底重发）"""
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT task_id, status, data FROM tasks
                    WHERE platform = :p AND status IN :terms
                      AND COALESCE(data ->> '$.settled', 'false') <> 'true'
                    LIMIT :lim
                    """
                ).bindparams(bindparam("terms", expanding=True)),
                {"p": settings.gateway_platform, "terms": TERMINAL, "lim": limit},
            )
        ).mappings().all()
    out = []
    for row in rows:
        item = dict(row)
        if isinstance(item.get("data"), str):
            item["data"] = json.loads(item["data"])
        out.append(item)
    return out
