"""tasks 表读写（NewAPI 现有表，网关零建表）。
- 扩展字段全部在 data JSON 列（JSON_MERGE_PATCH 合并）
- 状态机迁移一律 CAS：rowcount=1 才视为抢到推进权（恰好一次）
- 时间字段为 int64 unix 秒
"""

import json
import time
from typing import Any, cast

from sqlalchemy import CursorResult, bindparam, text

from app.config import settings
from app.db import get_session_factory
from app.schemas import ACTIVE, TERMINAL


def _now() -> int:
    return int(time.time())


#: 时间列统一 int64 unix **秒**；超过该阈值（1e11 秒 ≈ 5138 年）视为混入的
#: 毫秒时间戳——tasks 表是共享表（new-api 原生任务模块用 UnixMilli 写法），
#: 历史行/其他写入方留下的毫秒值在**读侧统一归一**，网关一切时间计算
#: （探测超龄、duration、对外视图）永远拿到秒，杜绝混用单位的误判
_UNIX_MS_THRESHOLD = 100_000_000_000

#: tasks 表的全部时间列（读侧归一的作用面）
_TIME_COLUMNS = ("submit_time", "start_time", "finish_time", "created_at", "updated_at")


def _secs(column: str) -> str:
    """SQL 侧时间列归一表达式（毫秒 → 秒）。

    读侧的 Python 归一救不了**在 SQL 里做的比较**（stale 判定/检索窗口全是
    `col < :cutoff`）：cutoff 恒为秒，列里混进毫秒值会让判定彻底失真——毫秒行
    永远躲过判定，而秒行一旦被拿去与毫秒口径比较就会被瞬间误判。所有时间比较
    统一套这个表达式，口径只有一种。
    """
    return f"IF({column} > {_UNIX_MS_THRESHOLD}, {column} DIV 1000, {column})"


def as_unix_seconds(value: Any) -> int:
    """时间值归一为 unix 秒：毫秒时间戳折算，缺失/非法 → 0。"""
    try:
        ts = int(value or 0)
    except (TypeError, ValueError):
        return 0
    if ts > _UNIX_MS_THRESHOLD:
        ts //= 1000
    return ts


def duration_seconds(task: dict) -> int:
    """耗时 = 终态时间 - 创建时间（统一秒）。终态时间缺失（非终态/为 0）或
    字段异常时返回 0——绝不产出天文数字。"""
    finish = as_unix_seconds(task.get("finish_time"))
    created = as_unix_seconds(task.get("created_at"))
    if not finish or not created:
        return 0
    return max(0, finish - created)


async def create(
    task_id: str,
    user_id: int,
    channel_id: int,
    action: str,
    data: dict,
) -> None:
    now = _now()
    async with get_session_factory()() as db:
        await db.execute(
            text(
                """
                INSERT INTO tasks
                  (task_id, platform, action, status, progress, data,
                   user_id, channel_id, quota, submit_time, start_time,
                   created_at, updated_at)
                VALUES
                  (:task_id, :platform, :action, 'SUBMITTED', '0%', CAST(:data AS JSON),
                   :user_id, :channel_id, 0, :now, :now, :now, :now)
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


def _row_to_dict(row) -> dict:
    result = dict(row)
    data = result.get("data")
    if isinstance(data, str):
        result["data"] = json.loads(data)
    elif data is None:
        result["data"] = {}
    for col in _TIME_COLUMNS:          # 读侧时间单位归一（毫秒 → 秒）
        if col in result:
            result[col] = as_unix_seconds(result[col])
    return result


async def get(task_id: str) -> dict | None:
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text("SELECT * FROM tasks WHERE task_id = :t LIMIT 1"), {"t": task_id}
            )
        ).mappings().first()
    if not row:
        return None
    return _row_to_dict(row)


async def cas(
    task_id: str,
    from_statuses: tuple[str, ...],
    to_status: str,
    patch: dict | None = None,
    fail_reason: str = "",
) -> bool:
    """CAS 状态迁移。返回 True = 本调用者抢到推进权（负责后续事件/额度释放）

    纪律：
    - ``WHERE`` 必含 ``platform``——tasks 是与 new-api 共享的表，网关只
      读写自有行（红线：绝不动别人的任务行）；
    - 终态一律把 ``progress`` 置 ``100%``（不只 SUCCESS）——失败/取消停在
      ``0%`` 会让看板与客户端以为任务还在跑；
    - 终态一律用**秒**刷 ``finish_time``（``_now()``），杜绝毫秒写入让
      duration/对账窗口算出天文数字。
    """
    now = _now()
    stmt = text(
        """
        UPDATE tasks
        SET status = :to,
            updated_at = :now,
            finish_time = IF(:terminal = 1, :now, finish_time),
            progress = IF(:terminal = 1, '100%', progress),
            fail_reason = :reason,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()), CAST(:patch AS JSON))
        WHERE task_id = :tid AND platform = :p AND status IN :froms
        """
    ).bindparams(bindparam("froms", expanding=True))
    async with get_session_factory()() as db:
        # DML 返回 CursorResult（带 rowcount）；execute() 标注为 Result 需收窄
        res = cast("CursorResult[Any]", await db.execute(
            stmt,
            {
                "to": to_status,
                "now": now,
                "terminal": 1 if to_status in TERMINAL else 0,
                "reason": fail_reason,
                "patch": json.dumps(patch or {}, ensure_ascii=False),
                "tid": task_id,
                "p": settings.gateway_platform,
                "froms": from_statuses,
            },
        ))
        await db.commit()
        return res.rowcount == 1


async def patch_data(task_id: str, patch: dict, status: str | None = None) -> None:
    """非迁移性的数据合并（如回填 ``upstream_task_id``）；可选顺带更新状态列。"""
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
    async with get_session_factory()() as db:
        await db.execute(text(sql), params)
        await db.commit()


async def stale_batch_active(stale_seconds: int, limit: int = 50) -> list[dict]:
    """``/batch`` 中继链路的收敛候选：``data.source='batch'`` 且**非终态**、
    最后更新时间超过 ``stale_seconds`` 的任务行（ADR-010 后台收敛）。

    返回的是**探测/终态收口所需的字段投影**，供 ``relayflow.sweep_batch_once``
    使用。

    纪律（照 ``search_tasks``，每条都有原因）：
    - **绝不 ``SELECT data``**——``data`` 含 ``token_hash`` 与 ``request_body``
      全文，整列捞出会把用户令牌暴露给调用栈；逐字段 ``data ->> '$.xxx'`` 投影；
    - **不带 ``WHERE token_hash``**——按 token_hash 过滤需要先知道值，而它正是
      投影出来的字段，语义上只能取回后在 Python 侧使用；
    - 时间比较走 ``_secs('updated_at')``——tasks 是共享表，混入的毫秒写入方
      会让裸比较失真（毫秒行永远躲过 stale 判定），见 ADR-004；
    - **排序 ``ASC``（最旧优先）**——收敛扫描的语义是「先把最老的收掉」：这些
      最老的任务最可能已在上游成功、只差没有客户端回来轮询。用 DESC + limit
      会让最旧的一批长期排在批次尾部、永远轮不到探测（饿死）；
    - 过滤掉没有 ``upstream_task_id`` 的行：这类任务无上游可问，sweep 无法推进，
      留在结果里会长期占据有限批次，其补投由 ``submit_batch_task`` 的
      重试/DLQ 路径负责。
    """
    cutoff = _now() - stale_seconds
    lim = max(1, min(int(limit), 200))
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT task_id, status,
                           data ->> '$.upstream_base_url' AS upstream_base_url,
                           data ->> '$.request_path' AS request_path,
                           data ->> '$.upstream_task_id' AS upstream_task_id,
                           data ->> '$.callback_url' AS callback_url,
                           data ->> '$.token_hash' AS token_hash,
                           data ->> '$.source' AS source
                    FROM tasks
                    WHERE platform = :p AND data ->> '$.source' = 'batch'
                      AND status IN :acts
                      AND COALESCE(data ->> '$.upstream_task_id', '') <> ''
                      AND {_secs('updated_at')} < :cutoff
                    ORDER BY {_secs('updated_at')} ASC
                    LIMIT :lim
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE,
                 "cutoff": cutoff, "lim": lim},
            )
        ).mappings().all()
    return [dict(row) for row in rows]


async def counts_by_status() -> dict[str, int]:
    """任务状态分布（队列观测用）"""
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT status, COUNT(*) AS n FROM tasks WHERE platform = :p GROUP BY status"
                ),
                {"p": settings.gateway_platform},
            )
        ).all()
    return {row[0]: row[1] for row in rows}


# ---------------------------------------------------------------------------
# 管理面检索（app/routers/admin.py 的列表端点后端）
# ---------------------------------------------------------------------------

#: ``search_tasks`` 的列投影白名单。**绝不 ``SELECT data`` 整列**——``data`` 里
#: 含 ``token_hash`` 与 ``request_body`` 全文，管理面把它透出等于经看板泄露用户
#: 令牌（本项目红线）。只按名取白名单字段。
_SEARCH_COLUMNS = (
    "task_id, status, progress, action, channel_id, user_id, "
    f"{_secs('created_at')} AS created_at, "
    f"{_secs('finish_time')} AS finish_time, "
    f"{_secs('updated_at')} AS updated_at, "
    "data ->> '$.model' AS model, "
    "data ->> '$.biz' AS biz, "
    "data ->> '$.source' AS source, "
    "data ->> '$.result' AS result, "
    "data ->> '$.freeze_amount' AS freeze_amount, "
    "data ->> '$.settled' AS settled"
)


async def search_tasks(status: str = "", model: str = "", task_id: str = "",
                       since_seconds: int = 0, limit: int = 50,
                       offset: int = 0) -> tuple[list[dict], int]:
    """管理面任务检索：分页 + 精确筛选，只返回白名单列。返回 ``(items, total)``。

    纪律（每条都有原因，别省）：
    - ``platform = :p`` 恒带——tasks 是与 new-api 共享的表，不带会读到别人的行；
    - ``task_id`` **精确匹配**，绝不前缀/通配——``LIKE '%...'`` 会让 task_id
      索引失效退化成全表扫描，把看板查询变成生产库的负载源；
    - ``model`` 走 ``data ->> '$.model'`` 等值；
    - 排序用 ``_secs('created_at')``——共享表可能混入毫秒时间戳，直接排会让
      毫秒行错位（见 ``_secs`` 的 docstring）；
    - items 与 total **共用同一份 where 与 params**，保证分页计数一致；
    - ``limit`` 钳到 1..200、``offset`` 非负。
    """
    where = ["platform = :p"]
    params: dict[str, Any] = {"p": settings.gateway_platform}
    if status:
        where.append("status = :status")
        params["status"] = status
    if task_id:
        where.append("task_id = :task_id")
        params["task_id"] = task_id
    if model:
        where.append("data ->> '$.model' = :model")
        params["model"] = model
    if since_seconds:
        where.append(f"{_secs('created_at')} >= :since")
        params["since"] = _now() - int(since_seconds)
    clause = " AND ".join(where)

    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))

    async with get_session_factory()() as db:
        total = int((await db.execute(
            text(f"SELECT COUNT(*) FROM tasks WHERE {clause}"), params
        )).scalar() or 0)
        rows = (
            await db.execute(
                text(
                    f"SELECT {_SEARCH_COLUMNS} FROM tasks WHERE {clause} "
                    f"ORDER BY {_secs('created_at')} DESC LIMIT :lim OFFSET :off"
                ),
                {**params, "lim": lim, "off": off},
            )
        ).mappings().all()
    return [dict(row) for row in rows], total
