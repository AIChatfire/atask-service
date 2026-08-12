"""TaskManager：提交全链路 + status-CAS 唯一状态仲裁点（SPEC §3.10.1 / 架构 §13.4）。

职责：
- ``submit_task``：幂等提交——顶格预估 → freeze 预冻 → 上游提交 → tasks 表
  自有行 INSERT（SPEC §4.1 三件套纪律）→ Redis 台账（cb:cap / freeze:shard /
  **tidx 回调反查索引**，决策 A-2）。任何一步失败 cancel 当前冻结分片补偿，
  保证资金与任务一致。
- ``transition``：轮询/callback/sweep 三通道收敛的唯一仲裁点。行锁读取 +
  终态不可逆 + rank 乱序防护 + status-CAS（对齐 new-api ``UpdateWithStatus``
  习惯）。终态副作用（outbox / delivery）原与状态 UPDATE 同事务；零自有表
  （决策 A）后改为：**commit 后立即入队 Redis 延迟队列**（dlv/obx，AOF
  everysec），入队失败只告警——由对账任务漏结算重入队兜底收敛。
- ``track_passthrough_task``：透传形态实际创建上游异步任务时落 tracked 行
  （``billing_state='none'``，不 freeze，SPEC §5.6.4）+ tidx 索引写入。
- ``current_freeze_shard``：freeze 分片序号两级读取（Redis 热台账 → tasks 行
  持久真相源），W3 outbox worker 与 renewer 共用。

状态映射一律使用 ``app.tasks.models`` 的 ``db_status/db_progress/db_to_internal``；
fail_reason 唯一生成点是本模块 ``_fail_reason``（``timeout: ``/``canceled: ``/
``failed: `` 前缀单一口径，SPEC §4.1 注）。
"""

from __future__ import annotations

import json
import os
import secrets
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import logfire
import ulid
from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters import get_adapter
from app.adapters.base import (
    CanonicalTaskRequest,
    SubmitContext,
    TaskSnapshot,
    TaskStatus,
)
from app.config import settings
from app.redis_client import get_redis
from app.tasks.models import (
    GW_PLATFORM_LIKE,
    db_progress,
    db_status,
    db_to_internal,
    new_task_id,
    platform_for,
    to_video_status,
)

if TYPE_CHECKING:
    from app.auth import TokenInfo  # 运行时仅需 user_id/sk_hash/raw/group/is_system 属性
    from app.billing.client import BillingServiceClient
    from app.billing.pricing import PricingEvaluator
    from app.registry import BizConfig

# W3 交付前的并行开发兜底：SPEC §3.11 模块就绪后自动切换到真实类。
# （骨架期 app/billing/*.py 仅占位，from-import 会 ImportError。）
try:
    from app.billing.client import InsufficientBalance
except ImportError:  # pragma: no cover - W3 合入后不再触发

    class InsufficientBalance(Exception):  # type: ignore[no-redef]
        """SPEC §3.11.1 同名异常兜底（W3 未就绪时仅用于类型对齐）。"""


try:
    from app.billing.pricing import PricingEvalError
except ImportError:  # pragma: no cover - W3 合入后不再触发

    class PricingEvalError(RuntimeError):  # type: ignore[no-redef]
        """SPEC §3.11.2 同名异常兜底（W3 未就绪时仅用于类型对齐）。"""


FREEZE_SHARD_TTL: int = settings.freeze_shard_ttl_seconds
RENEW_WINDOW: int = settings.freeze_renew_window_seconds


class PaymentRequired(Exception):
    """freeze 402（余额不足）→ W1 转 HTTP 402；任务不落库，幂等键释放。"""


def _now_unix() -> int:
    return int(datetime.now(UTC).timestamp())


def _rank(s: TaskStatus) -> int:
    """状态推进秩（乱序防护）：queued < running < 终态；rank 不回退。"""
    return {TaskStatus.QUEUED: 0, TaskStatus.RUNNING: 1}.get(s, 2)


def _fail_reason(target: TaskStatus, snapshot: TaskSnapshot) -> str:
    """fail_reason 唯一生成点（SPEC §3.10.1）：timeout:/canceled:/failed: 前缀。"""
    msg = ((snapshot.error or {}).get("message") or "").strip()
    if target is TaskStatus.TIMEOUT:
        return f"timeout: {msg or 'deadline exceeded'}"
    if target is TaskStatus.CANCELED:
        return f"canceled: {msg or 'user request'}"
    return f"failed: {msg or 'upstream error'}"


def _load_json(value: Any) -> dict[str, Any]:
    """JSON 列读出归一：asyncmy 返回文本，容错已解析的 dict。"""
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    return json.loads(value)


def _env_case_insensitive(name: str) -> str | None:
    """环境变量读取（大小写键兼容）：先精确匹配，再按大写归一扫描兜底。"""
    value = os.environ.get(name)
    if value is not None:
        return value
    upper = name.upper()
    for key, val in os.environ.items():
        if key.upper() == upper:
            return val
    return None


async def resolve_submit_secrets(cfg: BizConfig) -> dict[str, str]:
    """构造 SubmitContext.secrets 的唯一凭证来源（SPEC §3.12/§3.13）。

    先 ``KeyProvider.acquire(cfg.adapter)``（keys 轮询微服务）；未配置/
    服务不可用/字段不齐 → 回退 :func:`_resolve_submit_secrets_env`（env
    静态密钥兜底）。keys 凭证字段与 env 形态对齐：aksk_jwt 需 ak+sk，
    bearer 需 api_key，缺失视为 acquire 无效回退 env。
    """
    from app.keys import key_provider

    lease = await key_provider.acquire(cfg.adapter)
    if lease is not None:
        creds = {k.lower(): v for k, v in lease.credentials.items()}
        if cfg.auth_type == "aksk_jwt" and creds.get("ak") and creds.get("sk"):
            return {"ak": creds["ak"], "sk": creds["sk"]}
        if cfg.auth_type != "aksk_jwt" and creds.get("api_key"):
            return {"api_key": creds["api_key"]}
        logfire.warning("keys lease credentials incomplete, env fallback",
                        biz=cfg.biz, provider=cfg.adapter)
    return _resolve_submit_secrets_env(cfg)


def _resolve_submit_secrets_env(cfg: BizConfig) -> dict[str, str]:
    """env 静态密钥兜底（keys 服务不可用时的 fallback，SPEC §3.13 例外
    条款允许直读 os.environ）。

    - ``aksk_jwt``（kling）：``{ref}_AK`` / ``{ref}_SK`` → ``{"ak", "sk"}``；
    - 其余（bearer_key，seedance/方舟）：``os.environ[ref]`` → ``{"api_key"}``；
    - 环境键名大小写兼容；缺失即抛 RuntimeError（明确报错路径，绝不空凭证
    放行——kling 空 secrets 必抛、seedance 空 key 必 401）。
    """
    ref = cfg.auth_secret_ref
    if cfg.auth_type == "aksk_jwt":
        ak = _env_case_insensitive(f"{ref}_AK")
        sk = _env_case_insensitive(f"{ref}_SK")
        if not ak or not sk:
            raise RuntimeError(
                f"upstream credentials missing: env {ref}_AK/{ref}_SK"
                f" (biz={cfg.biz}, auth_type=aksk_jwt)"
            )
        return {"ak": ak, "sk": sk}
    api_key = _env_case_insensitive(ref)
    if not api_key:
        raise RuntimeError(
            f"upstream credential missing: env {ref}"
            f" (biz={cfg.biz}, auth_type={cfg.auth_type})"
        )
    return {"api_key": api_key}


def _actual_context(
    snapshot: TaskSnapshot, request_snapshot: dict[str, Any]
) -> dict[str, float | str]:
    """终态实收求值上下文（§5.3 变量名契约；W5 estimate_usage 同名键）。

    请求基底（mode/generate_audio/has_image_input/quantity）由 request_snapshot
    提供，实收信号（completion_tokens→usage_tokens、actual_duration→duration、
    resolution、upstream_amount）覆盖之。
    """
    usage = snapshot.usage or {}
    ctx: dict[str, float | str] = {
        "duration": float(usage.get("actual_duration") or request_snapshot.get("duration") or 0.0),
        "resolution": str(usage.get("resolution") or request_snapshot.get("resolution") or ""),
        "mode": str(request_snapshot.get("mode") or ""),
        "quantity": float(request_snapshot.get("n") or 1),
        "usage_tokens": float(usage.get("completion_tokens") or 0.0),
        "generate_audio": 1.0 if request_snapshot.get("generate_audio") else 0.0,
        "has_image_input": 1.0 if request_snapshot.get("image") else 0.0,
        # service_tier 属供应商扩展字段，口径与 outbox settle 重估一致：
        # 取 request_snapshot.extra（seedance estimate_usage 同源），顶层恒无
        "service_tier": str(
            (request_snapshot.get("extra") or {}).get("service_tier") or "default"
        ),
    }
    if usage.get("upstream_amount") is not None:
        ctx["upstream_amount"] = float(usage["upstream_amount"])
    return ctx


class TaskManager:
    """任务状态机唯一仲裁与提交全链路（SPEC §3.10.1）。"""

    def __init__(self, billing: BillingServiceClient, pricing: PricingEvaluator) -> None:
        self._billing = billing
        self._pricing = pricing

    # ---------- 提交：求值 → freeze → submit → 落库 → Redis 台账 ----------

    async def submit_task(
        self,
        session: AsyncSession,
        *,
        biz_cfg: BizConfig,
        req: CanonicalTaskRequest,
        token: TokenInfo,
        form: str,
        idem_key: str | None,
    ) -> dict[str, Any]:
        """§13.4 语义为准，顺序不可换：

        ① 求值顶格预估（phase="freeze"）→ ② freeze(request_id={task_id}:0,
        ttl=min(任务TTL, FREEZE_SHARD_TTL))，InsufficientBalance → PaymentRequired；
        ③ capability + adapter.submit，失败 cancel 当前分片后重抛；
        ④ INSERT tasks 自有行（§4.1 字段纪律）+ gateway_task_upstream_index
        同事务 commit，失败同样 cancel；⑤ 写 cb:cap / freeze:shard Redis。
        """
        adapter = get_adapter(biz_cfg.adapter)
        # 上游凭证先解析（缺失即明确报错，绝不 freeze 后再失败/空凭证提交）
        submit_secrets = await resolve_submit_secrets(biz_cfg)
        # 系统跳过开关（X-Skip-Auth-Billing）：计费动作一律 no-op（§3.9.7）
        skip_billing = token.is_system
        task_id = new_task_id()
        now_unix = _now_unix()
        ttl = settings.default_task_ttl_seconds
        # 网关 deadline 必须早于 new-api 超时清扫器（§4.1 三件套第 3 条）
        deadline_unix = now_unix + min(
            ttl,
            settings.newapi_task_timeout_minutes * 60 - settings.newapi_sweep_margin_seconds,
        )

        # ① 计费逻辑求值（顶格预估上下文由适配器产出，金额由定价表达式求值）
        estimate = adapter.estimate_usage(req)
        logic = await self._pricing.get_logic(biz_cfg.biz, req.model, req.action)
        estimate_usd = await self._pricing.evaluate(logic, estimate.context, phase="freeze")

        # ② freeze 预冻（分片冻结 §5.4.1：首片 seq=0）；skip 路径 no-op
        shard_ttl = min(ttl, FREEZE_SHARD_TTL)
        shard_request_id = f"{task_id}:0"
        if skip_billing:
            logfire.info("billing skipped (system identity)",
                         event="skip_auth_billing", task_id=task_id, op="freeze")
        else:
            try:
                await self._billing.freeze(
                    request_id=shard_request_id,
                    biz_type=biz_cfg.billing_keys["biz_type"],
                    metric=biz_cfg.billing_keys.get("metric", "call"),
                    amount_usd=estimate_usd,
                    ttl_seconds=shard_ttl,
                    user_sk=token.raw,
                    attrs={"biz": biz_cfg.biz, "model": req.model, "action": req.action},
                )
            except InsufficientBalance as exc:
                raise PaymentRequired("insufficient balance") from exc

        # ③ 上游提交；失败即 cancel 解冻当前分片（资金与任务一致）
        capability = secrets.token_urlsafe(24) if adapter.callback_capability else None
        cb_url = (
            f"{settings.gateway_public_base_url}"
            f"/callbacks/{biz_cfg.biz}/{adapter.name}/{capability}"
            if capability
            else ""
        )
        ctx = SubmitContext(
            biz=biz_cfg.biz,
            task_id=task_id,
            gateway_callback_url=cb_url,
            upstream_base_url=biz_cfg.upstream_base_url,
            secrets=submit_secrets,
            action=req.action,
        )
        try:
            result = await adapter.submit(req, ctx)
        except Exception:
            if not skip_billing:
                await self._cancel_shard_quiet(shard_request_id, token.raw, task_id)
            raise

        # ④ 落库 tasks 自有行 + 回调反查索引（同事务 commit）。
        #    纪律（§4.1）：platform=gw_{adapter} 自定义命名空间；quota 恒 0；
        #    全部 bigint 时间列显式自填（start_time/finish_time 填 0，绝不 NULL）；
        #    fail_reason 显式空串；private_data.gateway 键清单全量（SPEC §3.10.1）。
        platform = platform_for(adapter.name)
        private_data = {
            "upstream_task_id": result.upstream_task_id,
            "gateway": {
                "biz": biz_cfg.biz,
                "form": form,
                "sk_hash": token.sk_hash,
                "idempotency_key": idem_key,
                "callback_url": req.callback_url,
                # skip 路径无冻结单：billing_state=none（终态不再入队计费 outbox）
                "billing_state": "none" if skip_billing else "frozen",
                "skip_billing": skip_billing,
                "deadline_unix": deadline_unix,
                "freeze_shard_seq": 0,
                "freeze_shard_amount_usd": str(estimate_usd),
                "freeze_shard_expires_at": now_unix + shard_ttl,
                "next_poll_at": 0,
                "usage_actual": None,
                "request_snapshot": asdict(req),
            },
        }
        properties = {
            "input": (req.prompt or "")[:200],
            "upstream_model_name": req.model,
            "origin_model_name": req.model,
        }
        try:
            await session.execute(
                text(
                    """
                    INSERT INTO tasks (task_id, platform, user_id, `group`, channel_id,
                        quota, action, status, progress, properties, private_data, data,
                        fail_reason, submit_time, start_time, finish_time,
                        created_at, updated_at)
                    VALUES (:task_id, :platform, :user_id, :group, :channel_id, 0,
                        :action, :status, :progress, :properties, :private_data, :data,
                        '', :now, 0, 0, :now, :now)
                    """
                ),
                {
                    "task_id": task_id,
                    "platform": platform,
                    "user_id": token.user_id,
                    "group": token.group,
                    "channel_id": biz_cfg.newapi_channel_id,
                    "action": req.action,
                    "status": db_status(TaskStatus.QUEUED),
                    "progress": db_progress(TaskStatus.QUEUED),
                    "properties": json.dumps(properties),
                    "private_data": json.dumps(private_data),
                    "data": json.dumps(result.raw),
                    "now": now_unix,
                },
            )
            await session.commit()
        except Exception:
            await session.rollback()
            if not skip_billing:
                await self._cancel_shard_quiet(shard_request_id, token.raw, task_id)
            raise

        # ⑤ Redis 台账（落库成功后；失败不补偿任务行——行已是事实）
        redis = await get_redis()
        if capability is not None:
            await redis.set(f"cb:cap:{task_id}", capability, ex=ttl)  # §7.1；GET 不 GETDEL
        await redis.hset(  # type: ignore[misc]  # redis-py 5.x stubs 历史噪音：异步方法返回 Awaitable|T 联合
            f"freeze:shard:{task_id}",
            mapping={
                "seq": "0",
                "amount_usd": str(estimate_usd),
                "expires_at": str(now_unix + shard_ttl),
            },
        )
        await redis.expire(f"freeze:shard:{task_id}", ttl)
        if not skip_billing:
            # sksess：后台流程（renewer/outbox/对账）user_sk 取回凭据，终态 DEL
            from app.auth import store_user_sk

            await store_user_sk(task_id, token.raw, deadline_unix)
        # 回调反查索引（决策 A-2）：tidx:{biz}:{upstream_task_id} → task_id，7d
        await redis.set(
            f"tidx:{biz_cfg.biz}:{result.upstream_task_id}",
            task_id,
            ex=settings.upstream_index_ttl_seconds,
        )
        logfire.info(
            "task submitted",
            task_id=task_id,
            biz=biz_cfg.biz,
            form=form,
            estimate_usd=str(estimate_usd),
        )
        return {"task_id": task_id, "status": "queued", "created_at": now_unix}

    # ---------- 状态迁移：唯一仲裁点（status-CAS + 事务性 outbox） ----------

    async def transition(
        self,
        session: AsyncSession,
        *,
        task_id: str,
        snapshot: TaskSnapshot,
        channel: str,
    ) -> bool:
        """唯一状态仲裁点（§4.3/§13.4）。

        SELECT ... FOR UPDATE（WHERE 必含 platform LIKE 'gw\\_%'）→ 终态不可逆 +
        rank 乱序防护 → SUCCESS 时先事务外求值（phase="settle"，PricingEvalError
        → settle 金额 None + outbox reevaluate 标记）→ CAS UPDATE（WHERE
        status=旧值 AND status NOT IN ('SUCCESS','FAILURE')；rowcount!=1 →
        rollback + False）→ 终态 _finalize_in_tx（outbox 行 + delivery 行与状态
        UPDATE 同一事务 commit）。
        True=赢得迁移；False=竞态落败/乱序/不存在（正常路径非异常）。
        channel ∈ "poll"|"callback"|"sweep"（审计用）。
        """
        row = (
            await session.execute(
                text(
                    "SELECT status, private_data, user_id FROM tasks "
                    "WHERE task_id = :task_id AND platform LIKE :gw_like FOR UPDATE"
                ),
                {"task_id": task_id, "gw_like": GW_PLATFORM_LIKE},
            )
        ).mappings().first()
        if row is None:
            return False
        current = db_to_internal(row["status"])
        if current.is_terminal:
            return False  # 终态不可逆：迟到快照丢弃
        target = snapshot.status
        if _rank(target) <= _rank(current):
            return False  # 乱序防护：不回退

        pdata = _load_json(row["private_data"])
        gateway = pdata.setdefault("gateway", {})
        request_snapshot = gateway.get("request_snapshot") or {}

        # 结算金额需在事务内的 CAS 前求值（沙箱是外部调用，不进 DB 事务窗口）。
        settle_amount: Decimal | None = None
        reevaluate = False
        if target is TaskStatus.SUCCEEDED:
            try:
                logic = await self._pricing.get_logic_for_task(session, task_id)
                settle_amount = await self._pricing.evaluate(
                    logic,
                    _actual_context(snapshot, request_snapshot),
                    phase="settle",
                )
            except PricingEvalError:
                # 求值失败绝不静默顶格（§13.3）：outbox reevaluate 标记，
                # 补偿 worker 延迟重估，超限进死信人工处理
                reevaluate = True
                logfire.error(
                    "settle eval failed, outbox pending re-eval", task_id=task_id
                )

        gateway["usage_actual"] = snapshot.usage
        result_url = (snapshot.result or {}).get("url") if target is TaskStatus.SUCCEEDED else None
        if result_url:
            pdata["result_url"] = result_url  # 对齐 new-api private_data 语义

        now_unix = _now_unix()
        is_terminal = target.is_terminal
        is_failing = target in (
            TaskStatus.FAILED,
            TaskStatus.TIMEOUT,
            TaskStatus.CANCELED,
        )
        # DML 语句实际返回 CursorResult（带 rowcount）；execute() 标注为 Result 需收窄
        res = cast("CursorResult[Any]", await session.execute(
            text(
                """
                UPDATE tasks SET status = :status, progress = :progress,
                    updated_at = :now,
                    start_time = CASE WHEN :mark_in_progress = 1 AND start_time = 0
                                      THEN :now ELSE start_time END,
                    finish_time = CASE WHEN :mark_terminal = 1
                                       THEN :now ELSE finish_time END,
                    fail_reason = CASE WHEN :mark_failing = 1
                                       THEN :reason ELSE fail_reason END,
                    private_data = :private_data,
                    data = CASE WHEN :data IS NULL THEN data ELSE :data END
                WHERE task_id = :task_id AND platform LIKE :gw_like
                  AND status = :old_status AND status NOT IN ('SUCCESS', 'FAILURE')
                """
            ),
            {
                "status": db_status(target),
                "progress": db_progress(target),
                "now": now_unix,
                "mark_in_progress": 1 if db_status(target) == "IN_PROGRESS" else 0,
                "mark_terminal": 1 if is_terminal else 0,
                "mark_failing": 1 if is_failing else 0,
                "reason": _fail_reason(target, snapshot),
                "private_data": json.dumps(pdata),
                "data": json.dumps(snapshot.raw) if snapshot.raw is not None else None,
                "task_id": task_id,
                "gw_like": GW_PLATFORM_LIKE,
                "old_status": row["status"],
            },
        ))
        if res.rowcount != 1:
            await session.rollback()
            return False  # 竞态落败：另一通道已推进（正常路径，非异常）

        finalize_plan: dict[str, Any] | None = None
        if is_terminal:
            finalize_plan = await self._build_finalize_plan(
                session,
                task_id=task_id,
                row=row,
                pdata=pdata,
                target=target,
                snapshot=snapshot,
                settle_amount=settle_amount,
                reevaluate=reevaluate,
            )
        await session.commit()  # 状态 CAS 落盘（§4.7）
        if finalize_plan is not None:
            await self._dispatch_finalize(finalize_plan)
            # 终态：sksess 敏感数据最小驻留（user_sk 已随 outbox payload 携带）
            from app.auth import clear_user_sk

            await clear_user_sk(task_id)
        logfire.info(
            "task transition", task_id=task_id, to=str(target), channel=channel
        )
        return True

    async def _build_finalize_plan(
        self,
        session: AsyncSession,
        *,
        task_id: str,
        row: Any,
        pdata: dict[str, Any],
        target: TaskStatus,
        snapshot: TaskSnapshot,
        settle_amount: Decimal | None,
        reevaluate: bool,
    ) -> dict[str, Any]:
        """终态副作用计划（状态 UPDATE commit 前计算，commit 后入队 Redis）。

        计费 settle/cancel → obx 队列（含 cancel_prev_shards 历史分片统一
        收口）；用户回调 → dlv 队列。billing_state 的 settled/cancelled 由
        outbox worker 计费成功后回写（§4.7）。envelope 与
        ``app.schemas.CallbackEventEnvelope`` 同形；delivery ``id`` 即对外
        事件 ID（evt_+ulid，重试不变，接收方幂等依据）。
        """
        shard = await current_freeze_shard(session, task_id)  # 两级台账（§5.4.1）
        cancel_prev = [f"{task_id}:{i}" for i in range(shard)]
        user_id = row["user_id"]
        payload: dict[str, Any] = {
            "request_id": f"{task_id}:{shard}",
            "user_id": user_id,
            "cancel_prev_shards": cancel_prev,
        }
        # skip 路径（billing_state=none）无冻结单：终态不入队计费 outbox；
        # 否则把 sksess 中的 user_sk 随 payload 携带（终态后 sksess 即 DEL，
        # outbox 重放期间无法再取回——Redis outbox 与 sksess 同属敏感面）
        gateway = pdata.get("gateway") or {}
        billing_noop = gateway.get("billing_state") == "none"
        if not billing_noop:
            from app.auth import get_user_sk_for_task

            user_sk = await get_user_sk_for_task(task_id)
            if user_sk:
                payload["user_sk"] = user_sk
        op: str | None
        if target is TaskStatus.SUCCEEDED:
            op = "settle"
            payload["actual_amount"] = (
                str(settle_amount) if settle_amount is not None else None
            )
            payload["reevaluate"] = reevaluate
        else:  # failed/timeout/canceled → 全额解冻
            op = "cancel"
        if billing_noop:
            op = None  # skip 路径：计费动作 no-op（仅保留用户回调 delivery）

        delivery: dict[str, Any] | None = None
        callback_url = (pdata.get("gateway") or {}).get("callback_url")
        if callback_url:
            delivery_id = f"evt_{ulid.new()}"
            event_type = f"task.{target.value}"
            envelope = {
                "id": delivery_id,
                "type": event_type,
                "created_at": _now_unix(),
                "subject": f"task/{task_id}",
                "data": {
                    "task_id": task_id,
                    "status": to_video_status(db_status(target)),
                    "url": pdata.get("result_url"),
                    "metadata": {"usage": snapshot.usage},
                    "error": snapshot.error,
                },
            }
            delivery = {
                "delivery_id": delivery_id,
                "task_id": task_id,
                "user_id": user_id,
                "url": callback_url,
                "event_type": event_type,
                "envelope": envelope,
            }
        return {"task_id": task_id, "op": op, "payload": payload, "delivery": delivery}

    async def _dispatch_finalize(self, plan: dict[str, Any]) -> None:
        """commit 后副作用入队（Redis 延迟队列，决策 A-3/A-4）。

        入队失败只告警不抛出——状态已落库是事实，资金/投递由对账任务
        （漏结算重入队）与轮询兜底通道收敛（§4.7 零自有表口径）。
        skip 路径（op=None）计费 no-op：不入队 outbox，仅保留 delivery。
        """
        from app.billing.outbox import enqueue_outbox
        from app.callbacks.dispatcher import enqueue_delivery

        try:
            if plan["op"] is not None:
                await enqueue_outbox(
                    task_id=plan["task_id"], op=plan["op"], payload=plan["payload"]
                )
            if plan["delivery"] is not None:
                await enqueue_delivery(**plan["delivery"])
        except Exception:
            logfire.error(
                "finalize side-effect enqueue failed, reconcile will recover",
                task_id=plan["task_id"], op=plan["op"],
            )

    # ---------- 透传 tracked 行（§5.6.4） ----------

    async def track_passthrough_task(
        self,
        session: AsyncSession,
        *,
        biz_cfg: BizConfig,
        token: TokenInfo,
        upstream_task_id: str,
        action: str,
        request_snapshot: dict[str, Any],
        raw_response: dict[str, Any],
    ) -> str:
        """透传 tracked 行（§5.6.4）：form='passthrough_tracked'、
        billing_state='none'、不 freeze；其余字段纪律同 submit_task 第④步。

        触发条件（W1 调用方判定）：POST 透传响应体含上游 task_id（透传实际
        创建了一个异步任务）。纯同步透传不落行。返回网关 task_id。
        """
        adapter = get_adapter(biz_cfg.adapter)
        task_id = new_task_id()
        now_unix = _now_unix()
        deadline_unix = now_unix + min(
            settings.default_task_ttl_seconds,
            settings.newapi_task_timeout_minutes * 60 - settings.newapi_sweep_margin_seconds,
        )
        platform = platform_for(adapter.name)
        private_data = {
            "upstream_task_id": upstream_task_id,
            "gateway": {
                "biz": biz_cfg.biz,
                "form": "passthrough_tracked",
                "sk_hash": token.sk_hash,
                "idempotency_key": None,
                "callback_url": request_snapshot.get("callback_url"),
                "billing_state": "none",
                "deadline_unix": deadline_unix,
                "freeze_shard_seq": 0,
                "next_poll_at": 0,
                "usage_actual": None,
                "request_snapshot": request_snapshot,
            },
        }
        properties = {
            "input": str(request_snapshot.get("prompt") or "")[:200],
            "upstream_model_name": request_snapshot.get("model"),
            "origin_model_name": request_snapshot.get("model"),
        }
        await session.execute(
            text(
                """
                INSERT INTO tasks (task_id, platform, user_id, `group`, channel_id,
                    quota, action, status, progress, properties, private_data, data,
                    fail_reason, submit_time, start_time, finish_time,
                    created_at, updated_at)
                VALUES (:task_id, :platform, :user_id, :group, :channel_id, 0,
                    :action, :status, :progress, :properties, :private_data, :data,
                    '', :now, 0, 0, :now, :now)
                """
            ),
            {
                "task_id": task_id,
                "platform": platform,
                "user_id": token.user_id,
                "group": token.group,
                "channel_id": biz_cfg.newapi_channel_id,
                "action": action,
                "status": db_status(TaskStatus.QUEUED),
                "progress": db_progress(TaskStatus.QUEUED),
                "properties": json.dumps(properties),
                "private_data": json.dumps(private_data),
                "data": json.dumps(raw_response),
                "now": now_unix,
            },
        )
        await session.commit()
        # 回调反查索引（决策 A-2）：与 submit_task 同口径
        redis = await get_redis()
        await redis.set(
            f"tidx:{biz_cfg.biz}:{upstream_task_id}",
            task_id,
            ex=settings.upstream_index_ttl_seconds,
        )
        logfire.info(
            "passthrough task tracked",
            task_id=task_id,
            biz=biz_cfg.biz,
            upstream_task_id=upstream_task_id,
        )
        return task_id

    # ---------- 内部 ----------

    async def _cancel_shard_quiet(self, request_id: str, user_sk: str, task_id: str) -> None:
        """提交失败路径的补偿解冻：cancel 失败只告警，不掩盖原始异常。"""
        try:
            await self._billing.cancel(request_id=request_id, user_sk=user_sk)
            logfire.info(
                "upstream/insert failed, freeze cancelled",
                task_id=task_id,
                request_id=request_id,
            )
        except Exception:
            # 资金链路告警（§4.9）：分片残留由对账/人工介入收口
            logfire.error(
                "compensation freeze cancel failed",
                task_id=task_id,
                request_id=request_id,
            )


async def current_freeze_shard(session: AsyncSession, task_id: str) -> int:
    """当前活跃分片序号，两级读取（§13.4 _current_freeze_shard 语义）：

    ① Redis 热台账 ``freeze:shard:{task_id}``；
    ② 持久真相源 tasks 行 ``private_data.gateway.freeze_shard_seq``（W3
    renewer 每次续期成功后回写）——Redis 丢失时回读它并顺带重建 Redis，
    保证 settle 永远打向真实活跃分片而非已 cancel 的 ``{task_id}:0``；
    双失回退 0 + 告警（历史分片列表兜底 cancel 仍幂等安全）。
    W3 outbox worker 与 renewer 共用。
    """
    redis = await get_redis()
    seq = await redis.hget(f"freeze:shard:{task_id}", "seq")  # type: ignore[misc]  # redis-py 5.x stubs 历史噪音：异步方法返回 Awaitable|T 联合
    if seq is not None:
        return int(seq)
    row = (
        await session.execute(
            text(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(private_data,"
                " '$.gateway.freeze_shard_seq')) AS seq FROM tasks"
                " WHERE task_id = :task_id AND platform LIKE :gw_like"
            ),
            {"task_id": task_id, "gw_like": GW_PLATFORM_LIKE},
        )
    ).mappings().first()
    if row is not None and row["seq"] is not None:
        seq_int = int(row["seq"])
        # 顺带重建 Redis 热台账（金额/到期时间 DB 不可得，由 Renewer 下轮补齐）
        await redis.hset(f"freeze:shard:{task_id}", "seq", str(seq_int))  # type: ignore[misc]  # redis-py 5.x stubs 历史噪音：异步方法返回 Awaitable|T 联合
        return seq_int
    logfire.error(
        "freeze shard ledger lost in both redis and db, fallback 0", task_id=task_id
    )
    return 0


__all__ = [
    "FREEZE_SHARD_TTL",
    "RENEW_WINDOW",
    "PaymentRequired",
    "TaskManager",
    "current_freeze_shard",
    "resolve_submit_secrets",
]
