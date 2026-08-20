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
from app.logging import log
from app.redis import K_TIDX, r
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

    读侧的 Python 归一救不了**在 SQL 里做的比较**（stale/孤儿/HELD 判死/
    对账窗口全是 `col < :cutoff`）：cutoff 恒为秒，列里混进毫秒值会让判定
    彻底失真——毫秒行永远躲过判死，而秒行一旦被拿去与毫秒口径比较就会被
    瞬间判死（"任务秒失败"）。所有时间比较统一套这个表达式，口径只有一种。
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


async def get_by_upstream_id(upstream_task_id: str) -> dict | None:
    """按上游任务 id 反查本地任务（客户端持上游 id 轮询的兼容入口；
    权威 id 仍是本地 task_id，data.upstream_task_id 为两者的关联点）。

    先查 Redis 反查索引（``gw:tidx:*``，回填 upstream_task_id 时写入）——
    ``data ->> '$.upstream_task_id'`` 无索引（零建表红线，不能加虚拟列），
    SQL 兜底是全表扫描，表大后必须靠索引挡住热路径。"""
    try:
        local_id = await r.get(K_TIDX.format(upstream_task_id=upstream_task_id))
        if local_id:
            task = await get(str(local_id))
            if task and str((task.get("data") or {}).get("upstream_task_id")) \
                    == upstream_task_id:
                return task     # 命中且校验一致（防索引指向被复用/脏数据）
    except Exception:
        log.opt(exception=True).debug("tidx lookup failed, falling back to SQL")
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text(
                    """
                    SELECT * FROM tasks
                    WHERE platform = :p AND data ->> '$.upstream_task_id' = :u
                    ORDER BY id DESC LIMIT 1
                    """
                ),
                {"p": settings.gateway_platform, "u": upstream_task_id},
            )
        ).mappings().first()
    if not row:
        return None
    task = _row_to_dict(row)
    try:    # SQL 兜底命中：回写索引（下次直达），失败不影响返回
        await r.set(K_TIDX.format(upstream_task_id=upstream_task_id),
                    task["task_id"], ex=settings.upstream_index_ttl_seconds)
    except Exception:
        pass
    return task


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


async def patch_data(task_id: str, patch: dict, status: str | None = None,
                     channel_id: int | None = None) -> None:
    """非迁移性的数据合并（如回填 upstream_task_id）；可选顺带更新状态列。
    ``channel_id``：提交重打落到别的渠道时同步对账口径列。

    补丁含 ``upstream_task_id`` 时顺带维护 Redis 反查索引（上游 id → 本地
    id，TTL ``GW_UPSTREAM_INDEX_TTL_SECONDS``）——覆盖 submit/held 恢复/
    proxy 回填三个写入点，get_by_upstream_id 靠它免全表扫描。"""
    set_status = "status = :status, " if status else ""
    set_channel = "channel_id = :channel_id, " if channel_id else ""
    sql = f"""
        UPDATE tasks
        SET {set_status}{set_channel}updated_at = :now,
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
    if channel_id:
        params["channel_id"] = channel_id
    async with get_session_factory()() as db:
        await db.execute(text(sql), params)
        await db.commit()
    upstream_task_id = str(patch.get("upstream_task_id") or "")
    if upstream_task_id:
        try:
            await r.set(K_TIDX.format(upstream_task_id=upstream_task_id),
                        task_id, ex=settings.upstream_index_ttl_seconds)
        except Exception:
            log.opt(exception=True).debug("tidx write failed (SQL fallback covers)")


async def mark_settled(task_id: str, amount: float) -> None:
    await patch_data(task_id, {"settled": True, "settled_amount": amount})


async def stale_active(stale_seconds: int, limit: int = 200) -> list[str]:
    """Sweeper：非终态且长时间未更新的任务（HELD 除外——挂起由 resume 排空与
    hold_max_age 判死两条专用路径处理，探测重投对它无意义）"""
    cutoff = _now() - stale_seconds
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT task_id FROM tasks
                    WHERE platform = :p AND status IN :acts AND status <> 'HELD'
                      AND {_secs('updated_at')} < :cutoff
                    LIMIT :lim
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE, "cutoff": cutoff, "lim": limit},
            )
        ).scalars().all()
    return list(rows)


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


async def active_counts_by_token() -> dict[str, int]:
    """活跃任务数按 token_hash 分布（并发槽校准的事实源）。

    HELD 不计——挂起任务的槽已在转 HELD 时释放（resume 重占），
    与 conc_acquire/release 的占用口径保持一致。"""
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT data ->> '$.token_hash' AS th, COUNT(*) AS n FROM tasks
                    WHERE platform = :p AND status IN :acts AND status <> 'HELD'
                      AND COALESCE(data ->> '$.token_hash', '') <> ''
                    GROUP BY th
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE},
            )
        ).all()
    return {str(row[0]): int(row[1]) for row in rows}


async def terminal_unsettled(limit: int = 200) -> list[dict]:
    """Sweeper：终态但结算标记未落的任务（事件丢失的兜底重发）"""
    async with get_session_factory()() as db:
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


async def orphan_active(older_than_seconds: int, limit: int = 50) -> list[str]:
    """孤儿任务：非终态但长时间没有 upstream_task_id（submit 前崩溃的残留，
    永远不会有上游任务，冻结必须由 sweeper 收口解冻）。
    HELD 除外——挂起任务设计上就没有 upstream_task_id，由 hold_max_age 判死。"""
    cutoff = _now() - older_than_seconds
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT task_id FROM tasks
                    WHERE platform = :p AND status IN :acts AND status <> 'HELD'
                      AND COALESCE(data ->> '$.upstream_task_id', '') = ''
                      AND {_secs('created_at')} < :cutoff
                    LIMIT :lim
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE, "cutoff": cutoff, "lim": limit},
            )
        ).scalars().all()
    return list(rows)


async def oldest_held() -> str | None:
    """最老一个 HELD 任务（金丝雀排空每次只试一只）。"""
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text(
                    f"""
                    SELECT task_id FROM tasks
                    WHERE platform = :p AND status = 'HELD'
                    ORDER BY {_secs('created_at')} LIMIT 1
                    """
                ),
                {"p": settings.gateway_platform},
            )
        ).scalars().first()
    return row


async def held_expired(max_age_seconds: int, rate_limited_max_age_seconds: int = 3600,
                       limit: int = 100) -> list[str]:
    """挂起超上限的 HELD 任务（hold_max_age 判死：续期也救不回的挂起收口）。
    限流挂起（held_reason=rate_limited）用独立的更短上限（默认 1h）。"""
    cutoff = _now() - max_age_seconds
    cutoff_rl = _now() - rate_limited_max_age_seconds
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT task_id FROM tasks
                    WHERE platform = :p AND status = 'HELD' AND {_secs('updated_at')} <
                        CASE WHEN COALESCE(data ->> '$.held_reason', '') = 'rate_limited'
                             THEN :cutoff_rl ELSE :cutoff END
                    LIMIT :lim
                    """
                ),
                {"p": settings.gateway_platform, "cutoff": cutoff,
                 "cutoff_rl": cutoff_rl, "lim": limit},
            )
        ).scalars().all()
    return list(rows)


async def expiring_freezes(margin_seconds: int, limit: int = 100) -> list[dict]:
    """冻结临期的非终态任务（sweep 续期扫描）：freeze_expires_at 距今不足
    margin 且未结算。freeze_expires_at=0（免费/旧数据）不参与。"""
    deadline = _now() + margin_seconds
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT task_id, data FROM tasks
                    WHERE platform = :p AND status IN :acts
                      AND CAST(COALESCE(data ->> '$.freeze_expires_at', '0') AS UNSIGNED)
                          BETWEEN 1 AND :deadline
                      AND COALESCE(data ->> '$.settled', 'false') <> 'true'
                    LIMIT :lim
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE,
                 "deadline": deadline, "lim": limit},
            )
        ).mappings().all()
    out = []
    for row in rows:
        item = dict(row)
        if isinstance(item.get("data"), str):
            item["data"] = json.loads(item["data"])
        out.append(item)
    return out


async def reconcile_candidates(window_seconds: int, recheck_seconds: int,
                               limit: int = 20) -> list[dict]:
    """反向对账候选：本地 FAILURE 且已退款、有 upstream_task_id、窗口期内完成、
    距上次核对超过 recheck 周期的任务（比对"上游其实成功了"的亏损面）。"""
    since = _now() - window_seconds
    recheck_before = _now() - recheck_seconds
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT task_id, data FROM tasks
                    WHERE platform = :p AND status = 'FAILURE'
                      AND COALESCE(data ->> '$.settled', 'false') = 'true'
                      AND COALESCE(data ->> '$.reconciled', 'false') <> 'true'
                      AND COALESCE(data ->> '$.upstream_task_id', '') <> ''
                      AND {_secs('finish_time')} > :since
                      AND CAST(COALESCE(data ->> '$.reconcile_checked_at', '0') AS UNSIGNED)
                          < :recheck
                    LIMIT :lim
                    """
                ),
                {"p": settings.gateway_platform, "since": since,
                 "recheck": recheck_before, "lim": limit},
            )
        ).mappings().all()
    out = []
    for row in rows:
        item = dict(row)
        if isinstance(item.get("data"), str):
            item["data"] = json.loads(item["data"])
        out.append(item)
    return out
