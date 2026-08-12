"""轮询 worker：网关自有在途行的推进通道之一（SPEC §3.10.2 / 架构 §4.3）。

每轮：
1. **领取（同事务 claim-and-bump）**：``SELECT ... FOR UPDATE SKIP LOCKED``
   扫 ``platform LIKE 'gw\\_%'`` 在途行（``next_poll_at <= now``），同事务把
   ``next_poll_at`` 前推（退避序列 ``settings.poll_backoff_seconds`` 封顶 +
   jitter）后 commit 放锁——长 IO 不持有行锁，多副本 SKIP LOCKED 天然互斥，
   且下次领取天然按退避节奏到来。
2. **推进（锁外 IO）**：``deadline_unix`` 到期 → transition(timeout 快照,
   channel="sweep")；否则按行内 biz 取适配器 ``poll()``（SubmitContext.action
   从行内 action 取回，kling 旧版查询路径需要）→ ``transition(channel="poll")``。
   状态收敛（双通道竞态/乱序）由 ``TaskManager.transition`` 的 status-CAS
   唯一仲裁，轮询侧不做任何状态判断。

纪律：绝不扫描/触碰 platform 非 'gw\\_' 前缀的行（§4.1）；轮询异常不中断
主循环（logfire.exception 后继续）；空轮 sleep(poll_interval_seconds)。

退避游标推导（无新增 private_data 键的约定）：claim 时 ``updated_at`` 即上一次
bump 的时间（transition 也会刷新 updated_at——状态刚推进后回到序列起点，语义
合理），``prev_delay = next_poll_at - updated_at`` 反推上一档退避，升一档取
序列中首个大于它的值。
"""

from __future__ import annotations

import asyncio
import json
import random
from datetime import UTC, datetime
from typing import Any

import httpx
import logfire
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters import get_adapter
from app.adapters.base import SubmitContext, TaskSnapshot, TaskStatus, UpstreamError
from app.config import settings
from app.registry import registry
from app.tasks.manager import TaskManager, resolve_submit_secrets
from app.tasks.models import GW_PLATFORM_LIKE

_CLAIM_SQL = """
SELECT task_id, action, updated_at,
       CAST(JSON_UNQUOTE(JSON_EXTRACT(private_data, '$.gateway.next_poll_at'))
            AS UNSIGNED) AS next_poll_at
FROM tasks
WHERE platform LIKE :gw_like
  AND status IN ('SUBMITTED', 'QUEUED', 'IN_PROGRESS')
  AND CAST(JSON_UNQUOTE(JSON_EXTRACT(private_data, '$.gateway.next_poll_at'))
           AS UNSIGNED) <= :now
ORDER BY submit_time
LIMIT {batch}
FOR UPDATE SKIP LOCKED
"""

_BUMP_SQL = """
UPDATE tasks SET private_data = JSON_SET(private_data, '$.gateway.next_poll_at', :np),
    updated_at = :now
WHERE task_id = :task_id AND platform LIKE :gw_like
"""

_ROW_SQL = """
SELECT task_id, action, private_data FROM tasks
WHERE task_id = :task_id AND platform LIKE :gw_like
"""


def _now_unix() -> int:
    return int(datetime.now(UTC).timestamp())


def _next_delay(prev_delay: float, backoff: tuple[int, ...]) -> float:
    """退避升档：取序列中首个大于 prev_delay 的值，封顶末档；±20% jitter。"""
    base = backoff[-1]
    for step in backoff:
        if step > prev_delay + 0.5:  # 0.5s 容差吃掉取整误差
            base = step
            break
    return max(1.0, base * random.uniform(0.8, 1.2))


def _load_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}

    return json.loads(value)


class PollWorker:
    """网关自有在途行轮询 worker（无状态，多副本 + SKIP LOCKED 互斥）。"""

    def __init__(
        self,
        task_manager: TaskManager,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._tm = task_manager
        self._sf = session_factory

    async def run_forever(self) -> None:
        """主循环：异常 logfire.exception 后继续；空轮 sleep(poll_interval_seconds)。"""
        while True:
            try:
                claimed = await self._poll_once()
            except Exception:
                logfire.exception("poll worker loop error")
                claimed = 0
            if claimed == 0:
                await asyncio.sleep(settings.poll_interval_seconds)

    async def _poll_once(self) -> int:
        rows = await self._claim_batch()
        now = _now_unix()
        for row in rows:
            try:
                await self._poll_one(row["task_id"], now)
            except Exception:
                # 单行失败不中断批次；next_poll_at 已前推，下轮再试
                logfire.exception("poll task failed", task_id=row["task_id"])
        return len(rows)

    async def _claim_batch(self) -> list[dict[str, Any]]:
        """同事务领取 + 前推 next_poll_at（短事务，不持锁做上游 IO）。"""
        now = _now_unix()
        batch = int(settings.poll_batch_size)
        async with self._sf() as session:
            rows = (
                await session.execute(
                    text(_CLAIM_SQL.format(batch=batch)),
                    {"gw_like": GW_PLATFORM_LIKE, "now": now},
                )
            ).mappings().all()
            for row in rows:
                next_poll_at = int(row["next_poll_at"] or 0)
                prev_delay = max(0.0, float(next_poll_at - int(row["updated_at"] or 0)))
                delay = _next_delay(prev_delay, tuple(settings.poll_backoff_seconds))
                await session.execute(
                    text(_BUMP_SQL),
                    {
                        "np": int(now + delay),
                        "now": now,
                        "task_id": row["task_id"],
                        "gw_like": GW_PLATFORM_LIKE,
                    },
                )
            await session.commit()
        return [dict(r) for r in rows]

    async def _poll_one(self, task_id: str, now: int) -> None:
        """推进单行：deadline 收敛 timeout；否则 adapter.poll → transition。"""
        async with self._sf() as session:
            row = (
                await session.execute(
                    text(_ROW_SQL), {"task_id": task_id, "gw_like": GW_PLATFORM_LIKE}
                )
            ).mappings().first()
            if row is None:
                return
            pdata = _load_json(row["private_data"])
            gateway = pdata.get("gateway") or {}

            deadline = int(gateway.get("deadline_unix") or 0)
            if deadline and now >= deadline:
                # 兜底超时收敛（网关 deadline 恒早于 new-api 清扫器，§4.1）
                snapshot = TaskSnapshot(
                    upstream_status="gateway_deadline",
                    status=TaskStatus.TIMEOUT,
                    result=None,
                    usage=None,
                    error={
                        "code": "deadline_exceeded",
                        "message": f"gateway deadline {deadline} exceeded",
                    },
                    event_id=f"gw:{task_id}:timeout:{now}",
                )
                await self._tm.transition(
                    session, task_id=task_id, snapshot=snapshot, channel="sweep"
                )
                return

            biz = gateway.get("biz")
            upstream_task_id = pdata.get("upstream_task_id")
            if not biz or not upstream_task_id:
                logfire.warning("poll row missing biz/upstream id", task_id=task_id)
                return
            biz_cfg = await registry.get(biz, session)
            adapter = get_adapter(biz_cfg.adapter)
            # 凭证先 keys 轮询微服务 acquire、env 静态密钥兜底（缺失抛
            # RuntimeError，由 _poll_once 单行 catch 记录 logfire.exception）
            ctx = SubmitContext(
                biz=biz,
                task_id=task_id,
                gateway_callback_url="",
                upstream_base_url=biz_cfg.upstream_base_url,
                secrets=await resolve_submit_secrets(biz_cfg),
                action=row["action"],  # kling 旧版查询路径需要（§3.2.1）
            )
            try:
                snapshot = await adapter.poll(upstream_task_id, ctx)
            except UpstreamError as exc:
                # 上游业务错/429：下轮再试（next_poll_at 已前推），不推进状态机
                logfire.warning(
                    "upstream poll failed", task_id=task_id, error=str(exc)
                )
                return
            except httpx.HTTPError as exc:
                logfire.warning(
                    "upstream poll transport error", task_id=task_id, error=str(exc)
                )
                return
            await self._tm.transition(
                session, task_id=task_id, snapshot=snapshot, channel="poll"
            )


__all__ = ["PollWorker"]
