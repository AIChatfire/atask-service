"""freeze 分片续期 worker（SPEC §3.11.5，架构 §5.4.1/§13.4 FreezeRenewer）。

**问题**：计费服务 ``ttl_seconds > 86400`` 截断为 86400，而任务兜底 deadline
默认 48h——直接传 172800 时，>24h 未完成任务会被 billing sweeper 自动解冻，
终态 settle 打已解冻单 → 确定性漏扣。

**方案（分片冻结 + 到期续期）**：

- 初始 ``freeze({task_id}:0, ttl=min(剩余, FREEZE_SHARD_TTL=82800))``（23h，
  留 1h 续期窗口），同一时刻一个任务只有一个活跃分片，不加倍冻结；
- 本 worker 每 5min 扫 ``billing_state='frozen'`` 在途行，分片
  ``expires_at - now < RENEW_WINDOW`` → ``freeze:renew:{task_id}`` NX 锁
  （多副本互斥）→ ``freeze({task_id}:{seq+1}, 同金额, ttl=min(剩余, 82800))``
  → **主动 cancel 旧分片**立即释放资金（不依赖 sweeper 自然到期）；
- 台账两级：Redis ``freeze:shard:{task_id}`` HASH ``{seq, amount_usd,
  expires_at}`` 为热台账；**持久真相源是 tasks 行
  ``private_data.gateway.freeze_shard_seq``**（每次续期成功后 JSON_SET 定向
  回写，WHERE 含 ``platform LIKE 'gw\\_%'``）+ 自有表
  ``gateway_freeze_shards`` 双写（§10.8）；Redis 丢失时按 自有表 → tasks 行
  顺序重建，金额不可恢复时告警人工（绝不按 0 续冻）；
- 续期失败（5xx/409）→ 告警 + 下轮重试；窗口 1h 内有 12 次机会，
  最坏由旧分片撑到自然到期；网关全灭时 sweeper 自动解冻，资金不锁死；
- 续期 **402**（余额不足，重试无意义）→ 不与 5xx 同等重排：立即 cancel
  旧分片止损并触发任务收敛（transition timeout，终态 outbox 全额解冻）；
- 终态竞态防护：续期执行前复查任务非终态；seq 回写 UPDATE 带在途状态
  条件，rowcount=0（已被终态通道收敛）→ 补偿 cancel 新分片；
- ``user_sk`` 取回走 Redis ``sksess:{task_id}``（submit 时写入、终态 DEL，
  §3.9.1 补充），取不到告警人工介入。

注：freeze → cancel → 台账回写 严格按序。freeze 幂等（同 request_id 重放
返回首次结果），cancel 失败时整行下轮重试安全（不会重复冻结）。
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import logfire
from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.base import TaskSnapshot, TaskStatus
from app.billing.client import (
    BillingServiceClient,
    InsufficientBalance,
)
from app.config import settings
from app.redis_client import get_redis
from app.registry import registry
from app.tasks.models import GW_PLATFORM_LIKE

if TYPE_CHECKING:
    from app.tasks.manager import TaskManager

FREEZE_SHARD_TTL: int = settings.freeze_shard_ttl_seconds  # 82800（23h）
RENEW_WINDOW: int = settings.freeze_renew_window_seconds  # 3600（1h 续期窗口）
SWEEP_INTERVAL_SECONDS = 300  # 5min 一轮，窗口 1h 有 12 次机会
_RENEW_LOCK_TTL_SECONDS = 120  # freeze:renew:{task_id} NX 锁
_MIN_REMAINING_SECONDS = 3600  # deadline 缺失/已过时的保守剩余时长


class FreezeRenewer:
    """扫 ``billing_state='frozen'`` 在途任务，当前分片到期前 RENEW_WINDOW 内
    用新 request_id ``{task_id}:{seq+1}`` 续冻，成功后 cancel 旧分片并回写两级台账。

    无状态多副本（Redis NX 锁互斥）；续期失败告警 + 下轮重试。
    """

    def __init__(
        self,
        billing: BillingServiceClient,
        session_factory: async_sessionmaker[AsyncSession],
        task_manager: TaskManager | None = None,
    ) -> None:
        self._billing = billing
        self._sf = session_factory
        self._tm = task_manager  # 续期 402 收敛通道；未注入时 402 退回告警重试

    async def run_forever(self) -> None:
        while True:
            try:
                await self._sweep_once()
            except Exception:
                logfire.exception("freeze renewer loop error")
            await asyncio.sleep(SWEEP_INTERVAL_SECONDS)

    async def _sweep_once(self) -> None:
        redis = await get_redis()
        async with self._sf() as session:
            # 只扫网关自有在途行（platform 前缀，§4.5）；billing_state 存于
            # private_data.gateway —— 索引列（status/platform）先行过滤，JSON 提取在后
            rows = (
                await session.execute(
                    text(
                        "SELECT task_id, user_id, private_data FROM tasks"
                        f" WHERE platform LIKE '{GW_PLATFORM_LIKE}'"
                        " AND status IN ('SUBMITTED','QUEUED','IN_PROGRESS')"
                        " AND JSON_UNQUOTE(JSON_EXTRACT(private_data,"
                        " '$.gateway.billing_state')) = 'frozen'"
                    )
                )
            ).mappings().all()

        now = int(datetime.now(UTC).timestamp())
        for t in rows:
            task_id = str(t["task_id"])
            try:
                shard = await redis.hgetall(f"freeze:shard:{task_id}")  # type: ignore[misc]
                if not shard:
                    # Redis 热台账丢失：按持久真相源重建（自有表 → tasks 行）
                    shard = await self._rebuild_shard(redis, t, now)
                    if shard is None:
                        continue  # 金额不可恢复：已告警，人工介入
                if int(shard["expires_at"]) - now > RENEW_WINDOW:
                    continue  # 分片还健康，跳过
                # Redis 锁互斥多副本并发续期
                if not await redis.set(
                    f"freeze:renew:{task_id}", 1, nx=True, ex=_RENEW_LOCK_TTL_SECONDS
                ):
                    continue
                await self._renew_one(t, shard, now)
            except Exception as exc:
                # 告警指标：续期失败数 >0 即告警（资金链路零容忍，§9.2）
                logfire.error("freeze renew failed, will retry next sweep",
                              task_id=task_id, error=str(exc))

    async def _rebuild_shard(
        self, redis: Any, t: Any, now: int
    ) -> dict[str, str] | None:
        """Redis 丢失时从 tasks 行持久真相源重建热台账（决策 A-7：
        ``freeze_shard_seq`` + ``freeze_shard_amount_usd`` +
        ``freeze_shard_expires_at`` 三键齐全才可重建；缺金额 → 告警人工，
        绝不按 0 续冻）。重建成功即回写 Redis。"""
        del now
        task_id = str(t["task_id"])
        pdata = t["private_data"]
        if isinstance(pdata, str):
            pdata = json.loads(pdata)
        gateway = (pdata or {}).get("gateway") or {}
        seq = int(gateway.get("freeze_shard_seq", 0))
        amount = gateway.get("freeze_shard_amount_usd")
        expires_at = gateway.get("freeze_shard_expires_at")
        if amount is None or expires_at is None:
            logfire.error("freeze shard ledger lost, amount unrecoverable, manual check",
                          task_id=task_id, seq=seq)
            return None
        shard = {"seq": str(seq), "amount_usd": str(amount),
                 "expires_at": str(int(expires_at))}
        await redis.hset(f"freeze:shard:{task_id}", mapping=shard)  # type: ignore[misc]
        logfire.warning("freeze shard ledger rebuilt from tasks row",
                        task_id=task_id, seq=shard["seq"])
        return shard

    async def _task_in_flight(self, task_id: str) -> bool:
        """终态竞态复查（§4.3）：任务仍是在途且 billing_state='frozen' 才允许续期。

        扫描快照与 freeze 之间存在窗口：任务可能已被 poll/callback/sweep 通道
        收敛到终态，续期会白白冻结新分片。执行前以 DB 为准复查。
        """
        async with self._sf() as session:
            row = (
                await session.execute(
                    text(
                        "SELECT status, JSON_UNQUOTE(JSON_EXTRACT(private_data,"
                        " '$.gateway.billing_state')) AS billing_state FROM tasks"
                        f" WHERE task_id = :tid AND platform LIKE '{GW_PLATFORM_LIKE}'"
                    ),
                    {"tid": task_id},
                )
            ).mappings().first()
        if row is None:
            return False
        return (
            str(row["status"]) in ("SUBMITTED", "QUEUED", "IN_PROGRESS")
            and str(row["billing_state"] or "") == "frozen"
        )

    async def _renew_one(self, t: Any, shard: dict[str, str], now: int) -> None:
        """单片续期：终态复查 → freeze 新分片 → cancel 旧分片 → Redis/DB 两级台账
        回写（带回写竞态补偿）。

        freeze 幂等（request_id={task_id}:{seq+1} 重放返回首次结果），任何一步
        失败下轮整体重试安全；cancel 失败不推进台账，下轮重试（不会重复冻结）。
        402 不与 5xx 同等重排：cancel 旧分片止损 + transition timeout 收敛。
        """
        task_id = str(t["task_id"])
        if not await self._task_in_flight(task_id):
            logfire.info("freeze renew skipped: task already terminal", task_id=task_id)
            return
        seq = int(shard["seq"])
        amount_usd = Decimal(str(shard["amount_usd"]))
        pdata = t["private_data"]
        if isinstance(pdata, str):
            pdata = json.loads(pdata)
        gateway = (pdata or {}).get("gateway") or {}
        deadline_unix = int(gateway.get("deadline_unix") or (now + _MIN_REMAINING_SECONDS))
        remaining = max(deadline_unix - now, _MIN_REMAINING_SECONDS)
        ttl = min(remaining, FREEZE_SHARD_TTL)

        user_sk = await _session_sk(task_id)
        if not user_sk:
            raise RuntimeError(f"user_sk unavailable for task {task_id}, manual介入")

        biz = str(gateway.get("biz") or "")
        async with self._sf() as session:
            biz_cfg = await registry.get(biz, session)
        biz_type = str(biz_cfg.billing_keys.get("biz_type") or biz)
        metric = str(biz_cfg.billing_keys.get("metric") or "call")

        new_id = f"{task_id}:{seq + 1}"
        try:
            await self._billing.freeze(
                request_id=new_id, biz_type=biz_type, metric=metric,
                amount_usd=amount_usd, ttl_seconds=ttl, user_sk=user_sk,
                attrs={"renewed_from": f"{task_id}:{seq}"},
            )
        except InsufficientBalance:
            # 402：余额不足，下轮重试无意义——止损 + 收敛（详见 _handle_renew_402）
            await self._handle_renew_402(t, task_id, seq, user_sk, now)
            return
        # 新分片冻结成功后主动 cancel 旧分片，立即释放资金
        await self._billing.cancel(request_id=f"{task_id}:{seq}", user_sk=user_sk)

        expires_at = now + ttl
        redis = await get_redis()
        await redis.hset(  # type: ignore[misc]
            f"freeze:shard:{task_id}", mapping={
            "seq": str(seq + 1),
            "amount_usd": str(amount_usd),
            "expires_at": str(expires_at),
        })

        # 持久真相源回写（§5.4.1）：seq/金额/到期三键落 tasks 行（JSON_SET 局部
        # 改写不冲其他键，定向 UPDATE 带 platform 前缀 + 在途状态条件，§4.5；
        # 决策 A-7 后 Redis 丢失重建源即此三键）
        async with self._sf() as session:
            # DML 语句实际返回 CursorResult（带 rowcount）；execute() 标注为 Result 需收窄
            res = cast("CursorResult[Any]", await session.execute(
                text(
                    "UPDATE tasks SET private_data = JSON_SET(private_data,"
                    " '$.gateway.freeze_shard_seq', :seq,"
                    " '$.gateway.freeze_shard_amount_usd', :amount,"
                    " '$.gateway.freeze_shard_expires_at', :expires),"
                    " updated_at = :now"
                    f" WHERE task_id = :tid AND platform LIKE '{GW_PLATFORM_LIKE}'"
                    " AND status IN ('SUBMITTED','QUEUED','IN_PROGRESS')"
                ),
                {"seq": seq + 1, "amount": str(amount_usd), "expires": expires_at,
                 "now": now, "tid": task_id},
            ))
            if res.rowcount != 1:
                # 终态竞态落败：任务已被其他通道收敛——回滚台账，补偿 cancel
                # 新分片（旧分片由终态 outbox cancel_prev_shards 幂等收口），
                # Redis 热台账摘除避免指向已补偿取消的新分片
                await session.rollback()
                logfire.error(
                    "freeze shard writeback lost terminal race, compensating",
                    task_id=task_id, request_id=new_id,
                )
                await redis.delete(f"freeze:shard:{task_id}")
                await self._cancel_shard_quiet(new_id, user_sk, task_id)
                return
            await session.commit()
        # 计费审计（决策 A-5）：logfire 结构化日志
        logfire.info("billing audit", event="billing.freeze", task_id=task_id,
                     user_id=int(t["user_id"]), biz=biz,
                     request_id=new_id, amount_usd=str(amount_usd), ttl=ttl,
                     renewed_from=f"{task_id}:{seq}")
        logfire.info("freeze shard renewed", task_id=task_id, seq=seq + 1,
                     amount_usd=str(amount_usd), ttl=ttl)

    async def _handle_renew_402(
        self, t: Any, task_id: str, seq: int, user_sk: str, now: int
    ) -> None:
        """续期 402（余额不足）：不与 5xx 同等下轮重试（重试无意义且窗口耗尽后
        确定性漏扣）。①立即 cancel 旧分片止损；②transition timeout 收敛任务
        （终态 outbox cancel 行全额解冻剩余分片，同事务兜底）。TaskManager 未
        注入时退回告警 + 下轮重试（资金链路保守口径）。"""
        logfire.error("freeze renew got 402, converging task", task_id=task_id)
        await self._cancel_shard_quiet(f"{task_id}:{seq}", user_sk, task_id)
        if self._tm is None:
            logfire.error("freeze renew 402 but task manager not wired,"
                          " will retry next sweep", task_id=task_id)
            return
        snapshot = TaskSnapshot(
            upstream_status="billing_insufficient_balance",
            status=TaskStatus.TIMEOUT,
            result=None,
            usage=None,
            error={
                "code": "payment_required",
                "message": "freeze renew 402: insufficient balance, task converged",
            },
            event_id=f"gw:{task_id}:timeout:renew402:{now}",
        )
        async with self._sf() as session:
            won = await self._tm.transition(
                session, task_id=task_id, snapshot=snapshot, channel="sweep"
            )
        if not won:
            logfire.info("freeze renew 402 converge lost race (already terminal)",
                         task_id=task_id)

    async def _cancel_shard_quiet(self, request_id: str, user_sk: str, task_id: str) -> None:
        """补偿性解冻：cancel 失败只告警（幂等，由对账/人工收口），不掩盖主流程。"""
        try:
            await self._billing.cancel(request_id=request_id, user_sk=user_sk)
        except Exception:
            logfire.error("compensation freeze cancel failed",
                          task_id=task_id, request_id=request_id)


async def _session_sk(task_id: str) -> str | None:
    """sksess 取回在途任务的用户 sk（submit 写入、终态 DEL；auth.py 提供）。"""
    from app.auth import get_user_sk_for_task

    return await get_user_sk_for_task(task_id)
