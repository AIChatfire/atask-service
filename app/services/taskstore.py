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
from app.schemas import ACTIVE, SUBMITTED, TERMINAL


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
    """非迁移性的数据合并（如回填 ``upstream_task_id``）；可选顺带更新状态列。

    纪律（别省，两条都有反例后果）：

    - ``WHERE`` 恒带 ``platform``——与 ``cas`` 同一条共享表红线：tasks 是与
      new-api 共享的表，网关只读写自有行，绝不动别人的任务行；
    - 本函数**没有起点约束**，因此 ``status`` 只允许用于「非终态 → 非终态」的
      补写。凡涉及**终态不可逆**的推进（提交回填、判死、取消）必须走 ``cas``——
      裸改状态列会把已被取消/判死的任务复活，且没有任何机械手段能补救
      （详见 ``relayflow.submit_queue_task`` 的回填守卫）。
    """
    set_status = "status = :status, " if status else ""
    sql = f"""
        UPDATE tasks
        SET {set_status}updated_at = :now,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()), CAST(:patch AS JSON))
        WHERE task_id = :tid AND platform = :p
    """
    params: dict = {
        "now": _now(),
        "patch": json.dumps(patch, ensure_ascii=False),
        "tid": task_id,
        "p": settings.gateway_platform,
    }
    if status:
        params["status"] = status
    async with get_session_factory()() as db:
        await db.execute(text(sql), params)
        await db.commit()


async def stale_queue_active(stale_seconds: int, limit: int = 50) -> list[dict]:
    """``/queue`` 中继链路的收敛候选：``data.source='queue'`` 且**非终态**、
    最后更新时间超过 ``stale_seconds`` 的任务行（ADR-010 后台收敛）。

    返回的是**探测/终态收口所需的字段投影**，供 ``relayflow.sweep_queue_once``
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
      留在结果里会长期占据有限批次，其补投由 ``submit_queue_task`` 的
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
                    WHERE platform = :p AND data ->> '$.source' = 'queue'
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
#: 注意**不再投影** ``biz`` / ``freeze_amount`` / ``settled``：ADR-010 后这三者
#: 永远不会被写入（渠道分组与资金动作都在上游），留着只会喂给看板恒 null 的列。
_SEARCH_COLUMNS = (
    "task_id, status, progress, action, channel_id, user_id, "
    f"{_secs('created_at')} AS created_at, "
    f"{_secs('finish_time')} AS finish_time, "
    f"{_secs('updated_at')} AS updated_at, "
    "data ->> '$.model' AS model, "
    "data ->> '$.source' AS source, "
    "data ->> '$.result' AS result"
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


# ---------------------------------------------------------------------------
# 攒批（batching）的放行权 / 还槽权（app/services/batching.py 与 relayflow 共用）
# ---------------------------------------------------------------------------
# 两个「抢权」函数长得像，**语义完全不同，别混用**：
#   - ``claim_for_release`` 抢的是**放行权**（这条任务由我来放行）→ 状态列不动，
#     只把 ``data.batch_state`` 从等待态推到 ``releasing``；
#   - ``claim_slot_release`` 抢的是**还并发槽的权利**（这个槽由我来还）→ 把
#     ``data.slot_flags`` 从 >0 原子置 0。
# 前者的对偶是 ``unclaim_for_release``（放行失败时退还，绝不改状态列）；
# 后者的对偶是 Redis 里真的一次 DECR。

#: ``data.batch_state`` 的等待态取值——放行权抢占的合法起点。
#: ``waiting`` = 在批次里等 N/T；``requeued`` = 放过一次但占不到并发槽，正在退避
#: 重排。两者都「可被放行」，且都**没有**占着并发槽。``releasing`` 是抢占后的瞬态，
#: ``released`` 已放行，都不再是起点（否则会对同一条任务二次投递上游）。
BATCH_WAITING_STATES = ("waiting", "requeued")


async def get_batch_meta(task_id: str) -> dict | None:
    """放行热路径的元数据投影（攒批放行对每条成员调用一次，且放行途中会复核一次）。

    **绝不 ``SELECT data`` 整列**：``data`` 含 ``request_body``（网关上限定为
    1MiB，见 ``BODY_MAX_BYTES``）与 ``token_hash``；一批几百条整列捞出会把
    几百 MB 的请求体拉进内存，而放行只需要下面这几个字段。

    ``requeue_attempts`` 也在投影里：退避重排要按次数算下一次延迟，放行路径已经把
    这行读进来了，再单独查一次只为拿到一个计数是纯浪费（它同时保证退避次数在
    Redis 丢数据后不归零——落库的计数才是事实）。
    """
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text(
                    """
                    SELECT task_id, status,
                           data ->> '$.token_hash' AS token_hash,
                           data ->> '$.batch_state' AS batch_state,
                           COALESCE(data ->> '$.slot_flags', '0') AS slot_flags,
                           data ->> '$.batch_key' AS batch_key,
                           COALESCE(data ->> '$.requeue_attempts', '0')
                             AS requeue_attempts
                    FROM tasks
                    WHERE task_id = :t AND platform = :p
                    LIMIT 1
                    """
                ),
                {"t": task_id, "p": settings.gateway_platform},
            )
        ).mappings().first()
    return dict(row) if row else None


async def claim_for_release(task_id: str) -> bool:
    """抢「放行权」：``status='SUBMITTED'`` 且 ``batch_state`` ∈ 等待态 → ``releasing``。

    rowcount==1 才算抢到。这是「同一条成员不得被两条放行路径同时捞出」的**唯一**
    保证：Redis 侧的批次 claim（``LUA_BATCH_CLAIM``）只保证**批次级**互斥——整批
    只会被摘走一次，但救不了「同一成员被 N 触发与 T 触发各捞到一次」。少了这一关，
    同一条任务会被提交上游两次，而上游 relay 会按两次扣减配额，网关零资金动作、
    无从补救。

    **只动 ``data``，绝不动状态列**：状态列是终态不可逆的载体（取消/判死都在抢它），
    放行权是另一件正交的事。老链路 KI-D 修过「裸改状态列把 CANCELED 复活成
    QUEUED」那类事故，别在这里重犯。
    """
    sql = """
        UPDATE tasks
        SET updated_at = :now,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()),
                                    CAST(:patch AS JSON))
        WHERE task_id = :tid AND platform = :p AND status = :st
          AND data ->> '$.batch_state' IN :states
    """
    async with get_session_factory()() as db:
        res = cast("CursorResult[Any]", await db.execute(
            text(sql).bindparams(bindparam("states", expanding=True)),
            {
                "now": _now(),
                "patch": json.dumps({"batch_state": "releasing"}),
                "tid": task_id,
                "p": settings.gateway_platform,
                "st": SUBMITTED,
                "states": BATCH_WAITING_STATES,
            },
        ))
        await db.commit()
        return res.rowcount == 1


async def unclaim_for_release(task_id: str, restore: str = "waiting") -> None:
    """退还放行权（占槽失败 / 落 flags 失败时）：``releasing`` → ``restore``。

    ``restore`` 必须是抢占前的**真实**等待态（``waiting`` 或 ``requeued``），不能
    一律写回 ``waiting``：退避重排中的任务被写回 ``waiting`` 会与「在批里等 N/T」
    混淆，排障时看不出它其实已经不再属于任何批次。

    ``status`` 仍是 ``SUBMITTED`` 才退还：放行途中被取消的任务不该被复活成「等待
    放行」（它的 ``batch_state`` 停在 ``releasing`` 只是陈述性的痕迹，无人再读）。

    失败只告警不上抛：残留的 ``releasing`` 会让这条任务不再被放行（它是「放行在飞」
    的痕迹），由 sweep 的超期兜底按 ``batch_due_at`` 捞回；抛出去则会把一次可自愈的
    抖动升级成提交链路失败。
    """
    sql = """
        UPDATE tasks
        SET updated_at = :now,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()),
                                    CAST(:patch AS JSON))
        WHERE task_id = :tid AND platform = :p AND status = :st
          AND data ->> '$.batch_state' = 'releasing'
    """
    try:
        async with get_session_factory()() as db:
            await db.execute(text(sql), {
                "now": _now(),
                "patch": json.dumps({"batch_state": restore}),
                "tid": task_id,
                "p": settings.gateway_platform,
                "st": SUBMITTED,
            })
            await db.commit()
    except Exception:
        log.opt(exception=True).warning(
            "unclaim_for_release failed: task_id={}", task_id)


async def claim_slot_release(task_id: str) -> bool:
    """抢「还并发槽的权利」：把 ``data.slot_flags`` 从 >0 原子置 0。rowcount==1 才算抢到。

    并发槽只能被还一次，而三条路径都可能来还：终态收口、取消、放行后复核发现
    「放行途中已被取消」。它们会**真并发**——例如放行刚落下 ``slot_flags=1``，取消侧
    读到了它并去还槽，放行侧的复核也读到「已取消」并去还槽：两次 DECR 会把**别人的
    槽**还掉，而 ``LUA_CONC_RELEASE`` 只钳 0、发现不了（症状是该 token 的并发额度
    永久变多，且没有任何日志）。

    所以把「判定」与「置零」放进同一条 UPDATE：谁把掩码置 0，谁才是那个去 DECR 的人。

    **缺键视为已占槽**（``COALESCE(..., 1)``）：本特性上线前创建的在途任务没有
    ``slot_flags`` 键，而受理时占槽是当时的唯一路径——把缺键当「没占过」会让这批
    在途任务终态时不还槽，槽位一直漏到 TTL。

    只对 ``source='queue'`` 自有行与自己的 platform 生效（``WHERE`` 恒带 platform）。
    """
    sql = """
        UPDATE tasks
        SET updated_at = :now,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()),
                                    CAST(:patch AS JSON))
        WHERE task_id = :tid AND platform = :p
          AND COALESCE(CAST(data ->> '$.slot_flags' AS SIGNED), 1) > 0
    """
    async with get_session_factory()() as db:
        res = cast("CursorResult[Any]", await db.execute(text(sql), {
            "now": _now(),
            "patch": json.dumps({"slot_flags": 0}),
            "tid": task_id,
            "p": settings.gateway_platform,
        }))
        await db.commit()
        return res.rowcount == 1


#: ``stale_batch_waiting`` 的候选状态：等待态 + ``releasing``。
#: 前者是「该放行但没人放」（延迟任务丢了 / Redis 索引丢了），后者是「抢到放行权之后
#: 崩了」。两种都得捞，但**处置不同**（``releasing`` 要先退回等待态，否则
#: ``claim_for_release`` 不放行它），所以查询把状态一并投影给调用方分支。
BATCH_SWEEP_STATES = (*BATCH_WAITING_STATES, "releasing")


async def stale_batch_waiting(stale_seconds: int, limit: int = 200) -> list[dict]:
    """攒批的超期兜底候选：``source='queue'``、仍是 ``SUBMITTED``（从未提交上游）、
    且「已经该放行了却还没放」的行。

    用途：Redis 丢数据、T 触发的延迟任务投递丢失、退避重排的延迟任务丢失、放行抢到
    权之后进程崩溃——这些情况下任务在 DB 里仍是「等待放行」，**没有任何别的东西会
    推动它们**：放行只由 taskiq 延迟任务驱动，而 sweep 的探测通道
    （``stale_queue_active``）要求 ``upstream_task_id`` 非空，天然不会碰它们。于是
    必须有一条**只依赖 DB 事实源**的补数通道（「Redis 只放可重建索引」原则的直接
    应用：索引丢了能重建，靠的就是 DB 里这些字段）。不加这条通道，攒批会让任务卡在
    非终态直到客户端放弃——ADR-010 明令「绝不静默挂起」。

    ``batch_due_at`` 是「本任务的**下一次**可放行时刻」：批次窗口到期由首个成员写定；
    放行时占不到并发槽则退避重排时改写为新的重试时刻。一个谓词因此同时覆盖「批次
    到期」与「退避到点」两种情况，不再多引一个字段。

    两类候选、两个不同的判据（**别合并成一个**）：

    - 等待态（``waiting`` / ``requeued``）：判 ``batch_due_at <= cutoff``——它已经该
      被放行了，过期这么久还没放 = 触发链断了；
    - ``releasing``：判 ``updated_at <= cutoff``——**不能用 batch_due_at**：放行正是
      由「到期」触发的，所以它的 ``batch_due_at`` 必然已是过去时刻，用它判定会把
      **正在飞的放行**（毫秒级）也捞出来。而抢权那一步会把 ``updated_at`` 刷新成此刻，
      所以「``updated_at`` 也老了」才等于「那次放行真的没跑完」。用错判据的后果不是
      多放一次（DB 幂等挡得住），而是把健康在飞的行退回等待态，破坏成员级互斥。

    纪律照 ``stale_queue_active``（每条都有反例后果）：逐字段投影、**绝不 SELECT
    ``data`` 整列**（含 ``request_body`` 全文与 ``token_hash``）、时间列比较套
    ``_secs``（共享表可能混入毫秒值）、排序 ``ASC``（最旧优先——最老的那批最可能已被
    客户端等急了；用 DESC 会让它们永远轮不到）。
    """
    cutoff = _now() - max(0, int(stale_seconds))
    lim = max(1, min(int(limit), 500))
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT task_id, status,
                           data ->> '$.token_hash' AS token_hash,
                           data ->> '$.batch_state' AS batch_state,
                           COALESCE(CAST(data ->> '$.batch_due_at' AS SIGNED), 0)
                             AS batch_due_at
                    FROM tasks
                    WHERE platform = :p AND data ->> '$.source' = 'queue'
                      AND status = :st
                      AND (
                        (data ->> '$.batch_state' IN :states
                         AND COALESCE(CAST(data ->> '$.batch_due_at' AS SIGNED), 0) > 0
                         AND COALESCE(CAST(data ->> '$.batch_due_at' AS SIGNED), 0)
                             <= :cutoff)
                        OR (data ->> '$.batch_state' = 'releasing'
                            AND {_secs('updated_at')} <= :cutoff)
                      )
                    ORDER BY COALESCE(CAST(data ->> '$.batch_due_at' AS SIGNED), 0) ASC
                    LIMIT :lim
                    """
                ).bindparams(bindparam("states", expanding=True)),
                {"p": settings.gateway_platform, "st": SUBMITTED,
                 "states": BATCH_WAITING_STATES, "cutoff": cutoff, "lim": lim},
            )
        ).mappings().all()
    return [dict(row) for row in rows]
