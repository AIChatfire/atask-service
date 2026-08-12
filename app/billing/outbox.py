"""事务性 outbox 补偿 worker（SPEC §3.11.4/§4.7，架构 §5.5 第 3 层幂等）。

**零自有表（决策 A-4）**：队列载体为 Redis 延迟队列（``app/redis_queue.py``
``obx`` 命名空间：``obx:due``/``obx:{id}``/``obx:lease``/``obx:dead``），语义
与原 ``gateway_billing_outbox`` 表完全一致；计费审计（原
``gateway_billing_audit`` 表）改为 logfire 结构化日志（决策 A-5），对账读
计费服务 ``/billing/logs``（它记真实资金流水）。

语义：

- 领取：调度器每轮先 Lua 回收过期 lease 回 due，再 Lua 原子领取到期项
  （置 delivering + lease_until + 移入 ``obx:lease``）——多副本互斥与
  原 ``SELECT ... FOR UPDATE SKIP LOCKED`` 等价；副本死亡时租约到期由
  回收路径放回 due，不丢条目；
- 重放：按 ``op`` 调 :class:`BillingServiceClient`，``payload.request_id``
  **原样**（服务端唯一索引幂等，重放安全）；``op='settle'`` 且
  ``payload.reevaluate=true`` → 先 ``PricingEvaluator.evaluate(phase='settle')``
  重估，仍失败**保持挂起**（不计 attempts，不静默顶格，§13.3）；
- 历史分片收口：``payload.cancel_prev_shards`` 逐个 cancel（幂等无副作用，
  已过期/已解冻分片重放返回首次结果）——在成功收口前执行，失败则整条
  走重试（settle 重放仍幂等），保证任何分片不漏结算；
- 成功（SPEC §4.7）：条目出队 + tasks 行 ``billing_state`` 回写
  （settle→``settled``、cancel→``cancelled``、charge→``charged``；定向
  UPDATE 带 ``platform LIKE 'gw\\_%'`` 条件，**不校验 status**——终态后
  billing_state 回写与 status 无关；tasks 表是 new-api 的，继续写）+
  logfire 审计事件 ``billing.{op}``；
- 失败：``attempts+1``、指数退避 + jitter；``attempts>20`` 死信
  （``obx:dead``）+ 告警人工介入（资金链路零容忍，>0 即告警口径）。

透传 charge 402（上游已执行但余额不足，§5.6）→ **欠费三连**：①落欠费单
Redis ``debt:order:{request_id}`` HASH + ``debt:orders`` SET（与 outbox 重试
并存，用户充值后即扣回）；②Redis ``debt:{user_id}`` 熔断名单（提交类 402
拒绝、查询类放行，W1 消费）；③告警。
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import logfire
import ulid
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import redis_queue
from app.billing.client import (
    BillingLockBusy,
    BillingServiceClient,
    InsufficientBalance,
)
from app.billing.pricing import PricingEvalError, PricingEvaluator
from app.redis_client import get_redis
from app.tasks.models import GW_PLATFORM_LIKE

NS = "obx"  # Redis 队列命名空间（obx:due / obx:{id} / obx:lease / obx:dead）

OUTBOX_BATCH_SIZE = 50
OUTBOX_IDLE_SLEEP_SECONDS = 2.0
OUTBOX_ERROR_SLEEP_SECONDS = 1.0
OUTBOX_MAX_ATTEMPTS = 20  # 超过进死信（obx:dead）
OUTBOX_CLAIM_LEASE_SECONDS = 300  # 领取租约：副本死亡后条目回 due 可领取
_BACKOFF_BASE_SECONDS = 1.0  # 1s/2s/4s… 指数退避
_BACKOFF_CAP_SECONDS = 3600.0
REEVALUATE_RETRY_SECONDS = 300.0  # settle 重估失败保持挂起的重试间隔

_BILLING_STATE_BY_OP = {"settle": "settled", "cancel": "cancelled", "charge": "charged"}

# payload 键契约（终态 _build_finalize_plan / 透传 charge 写入方共用）：
# 公共: request_id, user_sk(可选), user_id(可选)
# settle: actual_amount(str|None), reevaluate(bool), context(可选 dict),
#         cancel_prev_shards(list[str]), attrs
# cancel: cancel_prev_shards(list[str])
# freeze: biz_type, metric, amount(str), ttl_seconds, attrs
# charge: biz_type, metric, amount(str), verify_only(bool), debt(bool)


async def enqueue_outbox(
    *,
    task_id: str,
    op: str,
    payload: dict[str, Any],
    last_error: str | None = None,
) -> str:
    """生产侧入队（终态副作用 / 透传 charge 调用方共用）。返回 outbox id。

    ``payload.request_id`` 原样保留（服务端幂等键，重放安全）。
    """
    redis = await get_redis()
    outbox_id = f"obx_{ulid.new()}"
    await redis_queue.enqueue(
        redis,
        NS,
        outbox_id,
        {
            "task_id": task_id,
            "op": op,
            "payload": json.dumps(payload, ensure_ascii=False),
            "last_error": last_error or "",
        },
    )
    return outbox_id


class OutboxWorker:
    """扣费失败补偿队列消费者（无状态多副本，Lua 原子领取互斥）。"""

    def __init__(
        self,
        billing: BillingServiceClient,
        pricing: PricingEvaluator,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._billing = billing
        self._pricing = pricing
        self._sf = session_factory

    async def run_forever(self) -> None:
        """主循环：空转 sleep；异常 logfire.exception 后继续（worker.py 装配）。"""
        while True:
            try:
                processed = await self._sweep_once()
            except Exception:
                logfire.exception("billing outbox loop error")
                await asyncio.sleep(OUTBOX_ERROR_SLEEP_SECONDS)
                continue
            if processed == 0:
                await asyncio.sleep(OUTBOX_IDLE_SLEEP_SECONDS)

    async def _sweep_once(self) -> int:
        """一轮清扫：回收过期 lease → Lua 原子领取 → 逐条独立处理。"""
        redis = await get_redis()
        await redis_queue.reclaim_expired_leases(redis, NS, limit=OUTBOX_BATCH_SIZE)
        ids = await redis_queue.claim(
            redis, NS, limit=OUTBOX_BATCH_SIZE, lease_seconds=OUTBOX_CLAIM_LEASE_SECONDS
        )
        processed = 0
        for outbox_id in ids:
            try:
                item = await redis_queue.get_item(redis, NS, outbox_id)
                if item is None:
                    continue                      # 并发下已被收口，跳过
                await self._process_row(redis, outbox_id, item)
                processed += 1
            except Exception:
                # 单条处理崩溃：下轮租约到期后重领；不拖垮批次
                logfire.exception("billing outbox row crashed", outbox_id=outbox_id)
        return processed

    # ---------- 单条处理 ----------

    async def _process_row(
        self, redis: Any, outbox_id: str, item: dict[str, str]
    ) -> None:
        task_id = str(item.get("task_id") or "")
        op = str(item.get("op") or "")
        attempts = int(item.get("attempts") or 0)
        payload = json.loads(item.get("payload") or "{}")
        async with self._sf() as session:
            try:
                response = await self._call_billing(session, task_id, op, payload)
            except PricingEvalError as exc:
                # settle 重估仍失败：保持挂起（不计 attempts，不静默顶格），等待下轮/人工
                logfire.error("outbox settle reevaluate failed, keep pending",
                              outbox_id=outbox_id, task_id=task_id, error=str(exc))
                await redis_queue.reschedule(
                    redis, NS, outbox_id,
                    delay_seconds=REEVALUATE_RETRY_SECONDS,
                    fields={"last_error": f"reevaluate: {exc}"[:4000]},
                )
                return
            except InsufficientBalance:
                if op == "charge":
                    await self._handle_charge_debt(
                        redis, session, outbox_id, task_id, attempts, payload
                    )
                    return
                # freeze/settle/cancel 402 属异常（冻结单余额已担保）：死信人工
                logfire.error("outbox op got 402, dead letter",
                              outbox_id=outbox_id, task_id=task_id, op=op)
                await redis_queue.dead_letter(
                    redis, NS, outbox_id, reason="unexpected 402",
                    attempts=attempts + 1,
                    fields={"last_error": "unexpected 402"},
                )
                return
            except Exception as exc:
                # BillingLockBusy（客户端内已重试 5 次仍忙）/5xx/超时：退避重排
                await self._retry_or_dead(redis, outbox_id, task_id, attempts, exc)
                return

            # 历史分片统一收口：逐个 cancel（幂等无副作用）；失败走整条重试
            try:
                for prev_id in payload.get("cancel_prev_shards") or []:
                    user_sk = await self._resolve_user_sk(session, task_id, payload)
                    await self._billing.cancel(request_id=str(prev_id), user_sk=user_sk)
            except Exception as exc:
                logfire.warning("outbox cancel_prev_shards failed, row will retry",
                                outbox_id=outbox_id, task_id=task_id, error=str(exc))
                await self._retry_or_dead(redis, outbox_id, task_id, attempts, exc)
                return

            await self._mark_done(redis, session, outbox_id, task_id, op, payload, response)
            await session.commit()

    async def _call_billing(
        self, session: AsyncSession, task_id: str, op: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """按 op 重放计费调用（payload.request_id 原样，幂等安全）。"""
        user_sk = await self._resolve_user_sk(session, task_id, payload)
        request_id = str(payload["request_id"])
        attrs = payload.get("attrs")
        if op == "settle":
            actual = await self._settle_amount(session, task_id, payload)
            return await self._billing.settle(request_id=request_id, actual_usd=actual,
                                              user_sk=user_sk, attrs=attrs)
        if op == "cancel":
            return await self._billing.cancel(request_id=request_id, user_sk=user_sk)
        if op == "freeze":
            return await self._billing.freeze(
                request_id=request_id,
                biz_type=str(payload["biz_type"]),
                metric=str(payload["metric"]),
                amount_usd=Decimal(str(payload["amount"])),
                ttl_seconds=int(payload["ttl_seconds"]),
                user_sk=user_sk,
                attrs=attrs,
            )
        if op == "charge":
            return await self._billing.charge(
                request_id=request_id,
                biz_type=str(payload["biz_type"]),
                metric=str(payload["metric"]),
                amount_usd=Decimal(str(payload["amount"])),
                user_sk=user_sk,
                verify_only=bool(payload.get("verify_only", False)),
            )
        raise ValueError(f"unknown outbox op: {op!r}")

    async def _settle_amount(
        self, session: AsyncSession, task_id: str, payload: dict[str, Any]
    ) -> Decimal:
        """settle 金额：reevaluate=true → settle 相位重估（失败抛 PricingEvalError
        保持挂起）；否则取 payload.actual_amount。"""
        if not payload.get("reevaluate"):
            return Decimal(str(payload["actual_amount"]))
        logic = await self._pricing.get_logic_for_task(session, task_id)
        context = payload.get("context")
        if context is None:
            context = await self._settle_context_from_task(session, task_id)
        return await self._pricing.evaluate(logic, context, phase="settle")

    async def _settle_context_from_task(
        self, session: AsyncSession, task_id: str
    ) -> dict[str, float | str]:
        """从 tasks 行重建实收上下文（payload 未内嵌 context 时的兜底）。

        变量名严格按 SPEC §3.11.2 契约；实收信号取
        ``private_data.gateway.usage_actual``（completion_tokens/actual_duration/
        upstream_amount/resolution，§3.2.2），缺省回退 request_snapshot 顶格值。
        """
        pdata = await _load_gateway_pdata(session, task_id)
        snapshot = pdata.get("request_snapshot") or {}
        usage = pdata.get("usage_actual") or {}
        extra = snapshot.get("extra") or {}
        return {
            "duration": float(usage.get("actual_duration")
                              or snapshot.get("duration") or 0),
            "resolution": str(usage.get("resolution")
                              or snapshot.get("resolution") or ""),
            "mode": str(snapshot.get("mode") or ""),
            "quantity": float(snapshot.get("n") or 1),
            "usage_tokens": float(usage.get("completion_tokens") or 0),
            "generate_audio": float(bool(snapshot.get("generate_audio"))),
            "has_image_input": float(bool(snapshot.get("image"))),
            "service_tier": str(extra.get("service_tier", "default")),
        }

    async def _resolve_user_sk(
        self, session: AsyncSession, task_id: str, payload: dict[str, Any]
    ) -> str:
        """user_sk 取回：payload 直带（透传 pt:/终态 outbox）→ Redis
        ``sksess:{task_id}``（submit 时写入，在途任务有效）。取不到告警。"""
        if payload.get("user_sk"):
            return str(payload["user_sk"])
        from app.auth import get_user_sk_for_task

        sk = await get_user_sk_for_task(task_id)
        if sk:
            return sk
        raise RuntimeError(f"user_sk unavailable for task {task_id}, manual介入 required")

    # ---------- 成功路径（出队 + billing_state 回写 + logfire 审计） ----------

    async def _mark_done(
        self,
        redis: Any,
        session: AsyncSession,
        outbox_id: str,
        task_id: str,
        op: str,
        payload: dict[str, Any],
        response: dict[str, Any],
    ) -> None:
        await redis_queue.mark_done(redis, NS, outbox_id)
        billing_state = _BILLING_STATE_BY_OP.get(op)
        if billing_state is not None:
            # 定向 UPDATE：platform 前缀条件（§4.5），不校验 status（§4.7）
            await session.execute(
                text(
                    "UPDATE tasks SET private_data = JSON_SET(private_data,"
                    " '$.gateway.billing_state', :state), updated_at = :now"
                    f" WHERE task_id = :tid AND platform LIKE '{GW_PLATFORM_LIKE}'"
                ),
                {"state": billing_state, "now": int(datetime.now(UTC).timestamp()),
                 "tid": task_id},
            )
        # charge 402 欠费单清偿：状态 cleared + 熔断名单解除（§5.6）
        if op == "charge" and payload.get("debt"):
            await clear_debt_order(str(payload["request_id"]))
            if payload.get("user_id") is not None:
                await clear_debt_block(int(payload["user_id"]))
        # 计费审计（决策 A-5）：logfire 结构化日志（对账读计费服务 /billing/logs）
        logfire.info(
            "billing audit",
            event=f"billing.{op}", task_id=task_id, outbox_id=outbox_id,
            user_id=payload.get("user_id"), biz=payload.get("biz"),
            request_id=str(payload.get("request_id")), response=response,
        )
        logfire.info("billing outbox op done", outbox_id=outbox_id, task_id=task_id, op=op,
                     request_id=str(payload.get("request_id")))

    # ---------- charge 402：欠费三连（§5.6） ----------

    async def _handle_charge_debt(
        self,
        redis: Any,
        session: AsyncSession,
        outbox_id: str,
        task_id: str,
        attempts: int,
        payload: dict[str, Any],
    ) -> None:
        """①落欠费单（request_id 幂等）②debt:{user_id} 熔断名单③告警；
        outbox 条目保持重试（用户充值后即扣回清偿）。"""
        user_id = payload.get("user_id")
        if user_id is None:
            user_id = await _load_task_user_id(session, task_id)
        if user_id is not None:
            await write_debt_order(
                user_id=int(user_id),
                task_id=task_id,
                request_id=str(payload["request_id"]),
                amount_usd=Decimal(str(payload["amount"])),
                biz=payload.get("biz"),
            )
            await set_debt_block(int(user_id))
        logfire.error("billing charge 402: debt order registered",
                      outbox_id=outbox_id, task_id=task_id, user_id=user_id,
                      request_id=str(payload.get("request_id")),
                      amount_usd=str(payload.get("amount")))
        await self._retry_or_dead(redis, outbox_id, task_id, attempts,
                                  InsufficientBalance())

    # ---------- 失败重排 / 死信 ----------

    async def _retry_or_dead(
        self,
        redis: Any,
        outbox_id: str,
        task_id: str,
        attempts: int,
        exc: Exception,
    ) -> None:
        new_attempts = attempts + 1
        if new_attempts > OUTBOX_MAX_ATTEMPTS:
            await redis_queue.dead_letter(
                redis, NS, outbox_id, reason=f"dead_letter: {exc}"[:4000],
                attempts=new_attempts, fields={"last_error": str(exc)[:4000]},
            )
            logfire.error("billing outbox dead letter",
                          outbox_id=outbox_id, task_id=task_id, attempts=new_attempts,
                          error=str(exc))
            return
        delay = min(_BACKOFF_BASE_SECONDS * 2 ** attempts, _BACKOFF_CAP_SECONDS)
        delay += random.uniform(0, 1)  # jitter 防多副本同步重试
        if isinstance(exc, BillingLockBusy):
            delay = max(delay, exc.retry_after_ms / 1000.0)
        await redis_queue.reschedule(
            redis, NS, outbox_id, delay_seconds=delay, attempts=new_attempts,
            fields={"last_error": str(exc)[:4000]},
        )
        logfire.warning("billing outbox op failed, rescheduled",
                        outbox_id=outbox_id, task_id=task_id, attempts=new_attempts,
                        delay_seconds=delay, error=str(exc))


# ---------- 欠费单与熔断名单（§5.6；W1 check_debt_block 同口径消费） ----------
#
# 零自有表（决策 A-9）：欠费单存 Redis
#   ``debt:order:{request_id}`` HASH(user_id/task_id/biz/amount/created_at/reason/status)
#   ``debt:orders`` SET（open 欠费单 request_id 清单，清偿时 SREM）


async def write_debt_order(
    *,
    user_id: int,
    task_id: str | None,
    request_id: str,
    amount_usd: Decimal,
    biz: str | None = None,
    reason: str = "charge_402",
) -> None:
    """落欠费单（request_id 幂等：已存在同 request_id 的 open 单不重复计）。"""
    redis = await get_redis()
    key = f"debt:order:{request_id}"
    if await redis.exists(key):
        return                                   # 幂等重放（对齐原 INSERT IGNORE）
    await redis.hset(  # type: ignore[misc]
        key,
        mapping={
            "user_id": str(user_id),
            "task_id": task_id or "",
            "biz": biz or "",
            "amount": str(amount_usd),
            "created_at": str(int(time.time())),
            "reason": reason,
            "status": "open",
        },
    )
    await redis.sadd("debt:orders", request_id)  # type: ignore[misc]


async def clear_debt_order(request_id: str) -> None:
    """欠费清偿：状态 cleared + open 清单摘除（§5.6）。"""
    redis = await get_redis()
    key = f"debt:order:{request_id}"
    if await redis.exists(key):
        await redis.hset(  # type: ignore[misc]
            key,
            mapping={"status": "cleared", "cleared_at": str(int(time.time()))},
        )
    await redis.srem("debt:orders", request_id)  # type: ignore[misc]


async def set_debt_block(user_id: int) -> None:
    """写入欠费熔断名单 ``debt:{user_id}``（至欠费清偿时由 outbox 清偿路径 DEL）。"""
    redis = await get_redis()
    await redis.set(f"debt:{user_id}", "1")


async def is_debt_blocked(user_id: int) -> bool:
    """熔断名单检查：提交类端点 402 拒绝、查询类放行（W1 消费，§3.6/§5.6）。"""
    redis = await get_redis()
    return bool(await redis.exists(f"debt:{user_id}"))


async def clear_debt_block(user_id: int) -> None:
    """欠费清偿后解除熔断名单。"""
    redis = await get_redis()
    await redis.delete(f"debt:{user_id}")


# ---------- 内部助手 ----------


async def _load_task_user_id(session: AsyncSession, task_id: str) -> int | None:
    row = (
        await session.execute(
            text(f"SELECT user_id FROM tasks WHERE task_id = :tid"
                 f" AND platform LIKE '{GW_PLATFORM_LIKE}'"),
            {"tid": task_id},
        )
    ).mappings().first()
    return int(row["user_id"]) if row and row["user_id"] is not None else None


async def _load_gateway_pdata(session: AsyncSession, task_id: str) -> dict[str, Any]:
    row = (
        await session.execute(
            text(f"SELECT private_data FROM tasks WHERE task_id = :tid"
                 f" AND platform LIKE '{GW_PLATFORM_LIKE}'"),
            {"tid": task_id},
        )
    ).mappings().first()
    pdata = row.get("private_data") if row else None
    if isinstance(pdata, str):
        pdata = json.loads(pdata)
    return (pdata or {}).get("gateway") or {}
