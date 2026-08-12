"""每日对账任务入口（SPEC §3.11.6，架构 §5.5 五项；零自有表改造，决策 A-8）。

五项职责：

1. **frozen>24h 任务对账轮询**：网关侧 ``billing_state='frozen'`` 超 24h 的任务，
   逐个 ``GET /billing/freeze/{request_id}`` 校验计费服务端冻结单仍存活
   （request_id 取当前活跃分片 ``{task_id}:{freeze_shard_seq}``）。服务端已
   解冻/已结算且任务已终态 = **漏结算** → 重新入队 outbox（Redis ``obx``
   队列）兜底收敛（决策 A-4 对账兜底职责）；
2. **终态任务 billing_state 收敛性比对**：窗口内终态任务 SUCCESS 但
   ``billing_state`` 未收敛为 settled/charged、FAILURE 但未 cancelled 的行
   即差异（零自有表后 outbox 入队失败只告警，本项是确定性收敛兜底）——
   差异行**重新入队** outbox；
3. **kling 3.0 三方对账**：上游实收 ``usage_actual.upstream_amount`` vs
   计费服务资金流水（``GET /billing/logs?request_id=``，决策 A-5 后计费
   审计以计费服务真实流水为准）的 settle 实收金额，差异超阈值（$0.01）告警
   （用户收单 / 我方台账 / 上游实收）；
4. **报告输出**（决策 A-8）：logfire 结构化日志 + **stdout 单行 JSON**
   （定时任务采集；``gateway_reconciliation_reports`` 表已删除）；
5. **tasks 滞留巡检**：``platform LIKE 'gw\\_%'`` 未完成但 ``submit_time``
   已越过网关 deadline 的行存量 >0 告警（应已被网关 deadline 收敛，
   防滞留行驱动 new-api 轮询器空转，简报 C §四.10）。

入口：``python -m app.billing.reconcile``（外层定时调度，如宿主机 cron 或
编排层定时任务，架构 §12.1）。
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import logfire
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.billing.client import BillingServiceClient
from app.billing.outbox import enqueue_outbox
from app.billing.pricing import PricingEvaluator
from app.billing.renewer import _session_sk
from app.config import settings
from app.tasks.models import GW_PLATFORM_LIKE

_FROZEN_AGE_SECONDS = 86_400  # frozen 超 24h 进入对账轮询
_THREE_WAY_THRESHOLD_USD = Decimal("0.01")  # 三方对账差异阈值


async def run_daily_reconciliation(
    session_factory: async_sessionmaker[AsyncSession],
    billing: BillingServiceClient,
    pricing: PricingEvaluator,
) -> dict[str, Any]:
    """执行五项对账，输出 logfire + stdout JSON 报告并返回报告 dict；
    单项失败不阻断其余项（各自告警）。"""
    del pricing  # 预留：重估口径对账；当前五项不依赖表达式求值
    now = int(datetime.now(UTC).timestamp())
    window_start = now - 86_400
    detail: dict[str, Any] = {
        "frozen_over_24h": [],
        "ledger_mismatches": [],
        "three_way_mismatches": [],
        "stuck_tasks": [],
        "reenqueued": [],
        "errors": [],
    }
    total_count = 0
    mismatch_count = 0
    mismatch_amount = Decimal("0")

    # ---- 1. frozen>24h 对账轮询（漏结算 → 重新入队 outbox，决策 A-4 兜底） ----
    try:
        async with session_factory() as session:
            rows = (
                await session.execute(
                    text(
                        "SELECT task_id, user_id, status, private_data FROM tasks"
                        f" WHERE platform LIKE '{GW_PLATFORM_LIKE}'"
                        " AND JSON_UNQUOTE(JSON_EXTRACT(private_data,"
                        " '$.gateway.billing_state')) = 'frozen'"
                        " AND submit_time < :cutoff"
                    ),
                    {"cutoff": now - _FROZEN_AGE_SECONDS},
                )
            ).mappings().all()
        for t in rows:
            total_count += 1
            entry = await _check_frozen_row(billing, t)
            if entry is not None:
                mismatch_count += 1
                detail["frozen_over_24h"].append(entry)
                if entry.get("reenqueue"):
                    outbox_id = await _reenqueue_outbox(t)
                    if outbox_id:
                        detail["reenqueued"].append(
                            {"task_id": entry["task_id"], "outbox_id": outbox_id,
                             "reason": "frozen_row_server_not_frozen"}
                        )
    except Exception as exc:
        logfire.exception("reconcile: frozen>24h check failed")
        detail["errors"].append(f"frozen_over_24h: {exc}")

    # ---- 2. 终态任务 billing_state 收敛性比对（差异 → 重新入队 outbox） ----
    try:
        async with session_factory() as session:
            rows = (
                await session.execute(
                    text(
                        "SELECT task_id, user_id, status, private_data,"
                        " JSON_UNQUOTE(JSON_EXTRACT(private_data,"
                        " '$.gateway.billing_state')) AS billing_state"
                        " FROM tasks"
                        f" WHERE platform LIKE '{GW_PLATFORM_LIKE}'"
                        " AND status IN ('SUCCESS','FAILURE')"
                        " AND finish_time >= :ws"
                    ),
                    {"ws": window_start},
                )
            ).mappings().all()
        for r in rows:
            total_count += 1
            state = r["billing_state"]
            expected = "settled" if r["status"] == "SUCCESS" else "cancelled"
            if state not in (expected, "charged", "none"):
                mismatch_count += 1
                detail["ledger_mismatches"].append(
                    {"task_id": r["task_id"], "status": r["status"],
                     "billing_state": state, "expected": expected}
                )
                outbox_id = await _reenqueue_outbox(r)
                if outbox_id:
                    detail["reenqueued"].append(
                        {"task_id": r["task_id"], "outbox_id": outbox_id,
                         "reason": "billing_state_not_converged"}
                    )
    except Exception as exc:
        logfire.exception("reconcile: ledger comparison failed")
        detail["errors"].append(f"ledger: {exc}")

    # ---- 3. kling 3.0 三方对账（上游实收 vs 计费服务 settle 流水，决策 A-5） ----
    try:
        async with session_factory() as session:
            rows = (
                await session.execute(
                    text(
                        "SELECT task_id, user_id, private_data,"
                        " JSON_UNQUOTE(JSON_EXTRACT(private_data,"
                        " '$.gateway.usage_actual.upstream_amount')) AS upstream_amount"
                        " FROM tasks"
                        " WHERE platform = 'gw_kling' AND status = 'SUCCESS'"
                        " AND finish_time >= :ws"
                        " AND JSON_EXTRACT(private_data,"
                        " '$.gateway.usage_actual.upstream_amount') IS NOT NULL"
                    ),
                    {"ws": window_start},
                )
            ).mappings().all()
        for r in rows:
            total_count += 1
            entry = await _check_three_way_row(billing, r)
            if entry is not None:
                mismatch_count += 1
                if "diff_usd" in entry:
                    mismatch_amount += Decimal(entry["diff_usd"])
                detail["three_way_mismatches"].append(entry)
    except Exception as exc:
        logfire.exception("reconcile: three-way check failed")
        detail["errors"].append(f"three_way: {exc}")

    # ---- 5. tasks 滞留巡检 ----
    deadline_span = min(
        settings.default_task_ttl_seconds,
        settings.newapi_task_timeout_minutes * 60 - settings.newapi_sweep_margin_seconds,
    )
    try:
        async with session_factory() as session:
            stuck = (
                await session.execute(
                    text(
                        "SELECT task_id, status, submit_time FROM tasks"
                        f" WHERE platform LIKE '{GW_PLATFORM_LIKE}'"
                        " AND status IN ('SUBMITTED','QUEUED','IN_PROGRESS')"
                        " AND submit_time < :cutoff"
                    ),
                    {"cutoff": now - deadline_span},
                )
            ).mappings().all()
        for r in stuck:
            detail["stuck_tasks"].append(
                {"task_id": r["task_id"], "status": r["status"],
                 "submit_time": int(r["submit_time"])}
            )
        if stuck:
            logfire.error("reconcile: gateway tasks stuck past deadline",
                          count=len(stuck))
    except Exception as exc:
        logfire.exception("reconcile: stuck task scan failed")
        detail["errors"].append(f"stuck: {exc}")

    # ---- 4. 报告输出（决策 A-8）：logfire + stdout 单行 JSON（定时任务采集） ----
    report = {
        "report_date": date.today().isoformat(),
        "total_count": total_count,
        "mismatch_count": mismatch_count,
        "mismatch_amount_usd": str(mismatch_amount),
        "detail": detail,
    }
    if mismatch_count or detail["stuck_tasks"] or detail["errors"]:
        logfire.error("daily reconciliation found mismatches",
                      report_date=report["report_date"], total=total_count,
                      mismatches=mismatch_count,
                      mismatch_amount_usd=str(mismatch_amount),
                      reenqueued=len(detail["reenqueued"]))
    else:
        logfire.info("daily reconciliation clean",
                     report_date=report["report_date"], total=total_count)
    print(json.dumps({"event": "reconciliation_report", **report},
                     ensure_ascii=False, default=str), flush=True)
    return report


async def _gateway_pdata(t: Any) -> dict[str, Any]:
    pdata = t["private_data"]
    if isinstance(pdata, str):
        pdata = json.loads(pdata)
    return (pdata or {}).get("gateway") or {}


async def _check_frozen_row(billing: BillingServiceClient, t: Any) -> dict[str, Any] | None:
    """单行 frozen>24h 校验：拉计费服务冻结单比对状态；异常/不一致 → 差异条目。

    服务端已非 frozen 且任务已终态 → ``reenqueue=True``（漏结算，由调用方
    重新入队 outbox 收敛）。
    """
    task_id = str(t["task_id"])
    gateway = await _gateway_pdata(t)
    seq = int(gateway.get("freeze_shard_seq", 0))
    request_id = f"{task_id}:{seq}"
    sk = await _session_sk(task_id)
    if not sk:
        logfire.error("reconcile: user_sk unavailable", task_id=task_id,
                      user_id=int(t["user_id"]))
        return {"task_id": task_id, "request_id": request_id,
                "issue": "user_sk_unavailable"}
    try:
        freeze = await billing.get_freeze(request_id=request_id, user_sk=sk)
    except Exception as exc:
        return {"task_id": task_id, "request_id": request_id,
                "issue": f"freeze_lookup_failed: {exc}"}
    state = str(freeze.get("status") or freeze.get("state") or "")
    if state != "frozen":
        # 服务端已解冻/已结算但网关侧仍 frozen：确定性漏扣风险
        return {"task_id": task_id, "request_id": request_id,
                "issue": f"server_state={state}",
                "reenqueue": str(t["status"]) in ("SUCCESS", "FAILURE")}
    return None


async def _check_three_way_row(
    billing: BillingServiceClient, r: Any
) -> dict[str, Any] | None:
    """单行三方对账：上游实收 vs 计费服务资金流水 settle 实收（决策 A-5）。

    计费服务流水查询失败 → 差异条目（issue）；无 settle 流水 → 差异条目。
    """
    task_id = str(r["task_id"])
    gateway = await _gateway_pdata(r)
    seq = int(gateway.get("freeze_shard_seq", 0))
    request_id = f"{task_id}:{seq}"
    upstream = Decimal(str(r["upstream_amount"]))
    sk = await _session_sk(task_id)
    if not sk:
        return {"task_id": task_id, "request_id": request_id,
                "issue": "user_sk_unavailable"}
    try:
        logs = await billing.get_billing_logs(request_id=request_id, user_sk=sk)
    except Exception as exc:
        return {"task_id": task_id, "request_id": request_id,
                "issue": f"billing_logs_lookup_failed: {exc}"}
    settle_entries = [
        e for e in logs
        if str(e.get("op") or e.get("action") or e.get("type") or "") == "settle"
    ]
    if not settle_entries:
        return {"task_id": task_id, "request_id": request_id,
                "issue": "no_settle_log", "upstream_amount_usd": str(upstream)}
    settled = Decimal(str(
        settle_entries[-1].get("amount_usd")
        or settle_entries[-1].get("actual_amount") or "0"
    ))
    diff = abs(upstream - settled)
    if diff > _THREE_WAY_THRESHOLD_USD:
        return {"task_id": task_id, "request_id": request_id,
                "upstream_amount_usd": str(upstream),
                "settled_amount_usd": str(settled), "diff_usd": str(diff)}
    return None


async def _reenqueue_outbox(t: Any) -> str | None:
    """漏结算兜底：按任务终态重新入队 outbox（Redis ``obx``，决策 A-4）。

    payload 与终态副作用同构（request_id 原样 = 服务端幂等键，重放安全；
    settle 金额置 reevaluate=true 由 outbox worker 重估）。
    """
    task_id = str(t["task_id"])
    try:
        gateway = await _gateway_pdata(t)
        seq = int(gateway.get("freeze_shard_seq", 0))
        op = "settle" if str(t["status"]) == "SUCCESS" else "cancel"
        payload: dict[str, Any] = {
            "request_id": f"{task_id}:{seq}",
            "user_id": int(t["user_id"]),
            "cancel_prev_shards": [f"{task_id}:{i}" for i in range(seq)],
        }
        if op == "settle":
            payload["actual_amount"] = None
            payload["reevaluate"] = True
        outbox_id = await enqueue_outbox(task_id=task_id, op=op, payload=payload)
        logfire.warning("reconcile re-enqueued outbox for missed settlement",
                        task_id=task_id, op=op, outbox_id=outbox_id)
        return outbox_id
    except Exception as exc:
        logfire.error("reconcile re-enqueue failed", task_id=task_id, error=str(exc))
        return None


def main() -> None:
    """对账任务入口：``python -m app.billing.reconcile``。"""
    from app.db import get_session_factory

    session_factory = get_session_factory()
    asyncio.run(
        run_daily_reconciliation(
            session_factory, BillingServiceClient(), PricingEvaluator(session_factory)
        )
    )


if __name__ == "__main__":
    main()
