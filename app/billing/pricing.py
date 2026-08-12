"""计费逻辑获取（三级缓存 + fail-closed 降级）与求值（SPEC §3.11.2，架构 §5.2/§5.3）。

三级缓存（拉取维度 ``(biz, model, action)``）：

- L1 进程内 dict，TTL ``settings.pricing_l1_ttl_seconds``（默认 5min），含 version；
- L2 Redis ``pricing:{biz}:{model}:{action}`` JSON
  ``{expr, expr_type, version, fallback_amount, updated_at}``（无 TTL，靠版本失效）；
- L3 计费逻辑服务 ``GET /api/v1/pricing/logic?biz&model&action``
  （超时 3s、重试 1 次；响应字段名为设计约定 V4，**集中常量化**便于对齐实际契约）。

降级策略（逻辑服务故障，**fail-closed 绝不放行免费请求**）：

- L1/L2 有缓存 → 继续用旧版（价格变更频率低，可接受）；
- 缓存全 miss → 用 biz 注册表 ``default_freeze_amount_usd`` 构造固定金额逻辑并告警；
- 连兜底价都没有 → 抛 :class:`PricingEvalError`（请求失败，不放行）。

求值（``evaluate``）：

- ``asteval`` → 子进程沙箱（``app.billing.sandbox``，总超时
  ``settings.pricing_eval_timeout_seconds``）；
- ``json_logic`` → 无代码执行面求值（本模块内置安全子集）；
- ``python_func`` → **不实现 exec 路径**（V5 未定），按求值失败处理；
- 结果 ``quantize`` 6 位小数 ``ROUND_HALF_UP``；
- 失败处置分两相位：``phase="freeze"`` → 返回 ``fallback_amount_usd`` +
  warning（fail-closed 顶格预冻）；``phase="settle"`` → 抛
  :class:`PricingEvalError`（**绝不静默顶格**，多扣即资金事故，转 outbox
  人工/延迟重估，§13.3/§13.4）。

求值上下文变量名契约（SPEC §3.11.2 §5.3，V8）：``duration``(float)、
``resolution``(str)、``mode``(str)、``quantity``(float)、``usage_tokens``(float)、
``generate_audio``(float 0/1)、``has_image_input``(float 0/1)、``service_tier``(str)。
"""

from __future__ import annotations

import asyncio
import itertools
import json
import time
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import httpx
import logfire
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.billing.sandbox import MAX_EXPR_LEN, ast_precheck, eval_expr_subprocess, get_eval_pool
from app.config import settings
from app.http_clients import pricing_client
from app.redis_client import get_redis
from app.registry import registry
from app.tasks.models import GW_PLATFORM_LIKE

# ---- L3 逻辑服务响应字段名（V4 设计约定，集中常量化便于对齐实际契约后单点修改） ----
FIELD_EXPR = "expr"
FIELD_EXPR_TYPE = "expr_type"
FIELD_VERSION = "version"
FIELD_FALLBACK = "fallback_amount"

_QUANT = Decimal("0.000001")
_L3_MAX_ATTEMPTS = 2  # 超时 3s 重试 1 次（§5.2）
_VALID_EXPR_TYPES = frozenset({"asteval", "python_func", "json_logic"})


@dataclass
class PricingLogic:
    """一条计费逻辑（缓存单元；表达式长度恒 ≤1024，入库与拉取双重校验）。"""

    expr: str
    expr_type: str  # 'asteval' | 'python_func' | 'json_logic'
    version: int
    fallback_amount_usd: Decimal


class PricingEvalError(RuntimeError):
    """结算阶段求值失败——不可静默顶格（会多扣），进 outbox 人工/延迟重估（§5.5）。"""


def _redis_key(biz: str, model: str, action: str) -> str:
    return f"pricing:{biz}:{model}:{action}"


def _parse_logic_blob(blob: dict[str, Any]) -> PricingLogic:
    """校验并构造 PricingLogic（L2/L3 共用入口；非法一律 ValueError → fail-closed）。"""
    expr = str(blob.get(FIELD_EXPR) or "")
    if not expr or len(expr) > MAX_EXPR_LEN:
        raise ValueError("pricing expr missing or too long")
    expr_type = str(blob.get(FIELD_EXPR_TYPE) or "")
    if expr_type not in _VALID_EXPR_TYPES:
        raise ValueError(f"unknown expr_type: {expr_type!r}")
    version = int(blob.get(FIELD_VERSION) or 0)
    fallback_raw = blob.get(FIELD_FALLBACK)
    if fallback_raw is None:
        raise ValueError("fallback_amount missing")
    fallback = Decimal(str(fallback_raw))
    if fallback < 0:
        raise ValueError("fallback_amount negative")
    return PricingLogic(expr=expr, expr_type=expr_type, version=version,
                        fallback_amount_usd=fallback)


class PricingEvaluator:
    """表达式缓存 + 子进程池求值 + fallback 降级（§5.2/§5.3）。"""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory
        # L1：key=(biz,model,action) → (monotonic 写入时刻, logic)
        self._l1: dict[tuple[str, str, str], tuple[float, PricingLogic]] = {}

    # ---------- 逻辑获取：L1 → L2 → L3 → fail-closed ----------

    async def get_logic(self, biz: str, model: str, action: str) -> PricingLogic:
        """三级缓存获取；全 miss → fail-closed 固定金额逻辑（绝不放行免费请求）。"""
        key = (biz, model, action)

        hit = self._l1.get(key)
        if hit is not None and time.monotonic() - hit[0] < settings.pricing_l1_ttl_seconds:
            return hit[1]

        logic = await self._get_l2(biz, model, action)
        if logic is None:
            logic = await self._get_l3(biz, model, action)
        if logic is not None:
            self._l1[key] = (time.monotonic(), logic)
            return logic

        return await self._fail_closed_logic(biz, model, action)

    async def get_logic_for_task(self, session: AsyncSession, task_id: str) -> PricingLogic:
        """settle 阶段用：从 tasks 行取回 (biz, model, action) 后委托 get_logic，
        保证与 freeze 同维度。行不存在或快照缺失 → PricingEvalError。"""
        row = (
            await session.execute(
                text(
                    "SELECT private_data FROM tasks WHERE task_id = :tid"
                    f" AND platform LIKE '{GW_PLATFORM_LIKE}'"
                ),
                {"tid": task_id},
            )
        ).mappings().first()
        pdata = row.get("private_data") if row else None
        if isinstance(pdata, str):
            pdata = json.loads(pdata)
        gateway = (pdata or {}).get("gateway") or {}
        snapshot = gateway.get("request_snapshot") or {}
        biz = gateway.get("biz")
        model = snapshot.get("model")
        action = snapshot.get("action")
        if not (biz and model and action):
            raise PricingEvalError(f"task {task_id} missing pricing dims in request_snapshot")
        return await self.get_logic(str(biz), str(model), str(action))

    async def _get_l2(self, biz: str, model: str, action: str) -> PricingLogic | None:
        try:
            redis = await get_redis()
            blob = await redis.get(_redis_key(biz, model, action))
        except Exception as exc:  # Redis 故障不阻断（L3 仍可回源）
            logfire.warning("pricing L2 redis error", error=str(exc))
            return None
        if not blob:
            return None
        try:
            return _parse_logic_blob(json.loads(blob))
        except Exception as exc:
            logfire.warning("pricing L2 blob invalid, treat as miss",
                            biz=biz, model=model, action=action, error=str(exc))
            return None

    async def _get_l3(self, biz: str, model: str, action: str) -> PricingLogic | None:
        client = pricing_client()
        last_exc: Exception | None = None
        for attempt in range(_L3_MAX_ATTEMPTS):
            try:
                resp = await client.get(
                    "/api/v1/pricing/logic",
                    params={"biz": biz, "model": model, "action": action},
                )
                if resp.status_code >= 500 and attempt + 1 < _L3_MAX_ATTEMPTS:
                    continue
                if resp.status_code != 200:
                    logfire.warning("pricing logic service non-200",
                                    status=resp.status_code, biz=biz, model=model)
                    return None
                payload = resp.json()
                data = payload.get("data", payload) if isinstance(payload, dict) else {}
                logic = _parse_logic_blob(data)
                await self._fill_l2(biz, model, action, data)
                return logic
            except (httpx.HTTPError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
                last_exc = exc
                if attempt + 1 < _L3_MAX_ATTEMPTS:
                    continue
        logfire.warning("pricing logic service fetch failed",
                        biz=biz, model=model, action=action,
                        error=str(last_exc) if last_exc else "unknown")
        return None

    async def _fill_l2(self, biz: str, model: str, action: str, data: dict[str, Any]) -> None:
        blob = json.dumps(
            {
                FIELD_EXPR: data.get(FIELD_EXPR),
                FIELD_EXPR_TYPE: data.get(FIELD_EXPR_TYPE),
                FIELD_VERSION: data.get(FIELD_VERSION),
                FIELD_FALLBACK: data.get(FIELD_FALLBACK),
                "updated_at": int(time.time()),
            },
            ensure_ascii=False,
        )
        try:
            redis = await get_redis()
            await redis.set(_redis_key(biz, model, action), blob)  # 无 TTL，靠版本失效
        except Exception as exc:
            logfire.warning("pricing L2 backfill failed", error=str(exc))

    async def _fail_closed_logic(self, biz: str, model: str, action: str) -> PricingLogic:
        """缓存全 miss：用 biz 注册表 default_freeze_amount_usd 构造固定金额逻辑
        并告警；连兜底价都没有 → PricingEvalError（fail-closed，绝不免费放行）。"""
        try:
            async with self._sf() as session:
                cfg = await registry.get(biz, session)
        except Exception as exc:
            logfire.error("pricing fail-closed: biz registry unavailable",
                          biz=biz, error=str(exc))
            raise PricingEvalError(
                f"pricing logic unavailable and registry lookup failed for biz={biz}"
            ) from exc
        if cfg.default_freeze_amount_usd is None:
            logfire.error("pricing fail-closed: no default_freeze_amount_usd",
                          biz=biz, model=model, action=action)
            raise PricingEvalError(
                f"pricing logic unavailable and no default freeze amount for biz={biz}"
            )
        amount = Decimal(cfg.default_freeze_amount_usd)
        logfire.warning("pricing logic all-miss, using default freeze amount",
                        biz=biz, model=model, action=action, amount_usd=str(amount))
        # 常量表达式：沙箱可直接求值，fallback 同值
        return PricingLogic(expr=str(amount), expr_type="asteval", version=0,
                            fallback_amount_usd=amount)

    # ---------- 求值：两相位失败处置 ----------

    async def evaluate(
        self,
        logic: PricingLogic,
        context: dict[str, float | str],
        *,
        phase: str = "freeze",
    ) -> Decimal:
        """求值 → Decimal 美元（6 位小数 HALF_UP）。

        失败：``phase='settle'`` → 抛 PricingEvalError（绝不静默顶格）；
        其余（freeze）→ 返回 fallback_amount_usd + warning（fail-closed 顶格预冻）。
        """
        try:
            if logic.expr_type == "asteval":
                value = await self._eval_asteval(logic.expr, context)
            elif logic.expr_type == "json_logic":
                value = _eval_json_logic(json.loads(logic.expr), context, depth=0)
            else:
                # python_func：V5 未定，不实现 exec 路径，按求值失败处理
                raise ValueError(f"unsupported expr_type: {logic.expr_type}")
            result = Decimal(str(value)).quantize(_QUANT, rounding=ROUND_HALF_UP)
            if result < 0:
                raise ValueError(f"pricing result negative: {result}")
            return result
        except Exception as exc:
            if phase == "settle":
                # 结算阶段：静默 fallback 顶格价 = 多扣用户钱 → 抛出，
                # 由调用方写 outbox（payload.reevaluate=true）待人工/延迟重估
                logfire.error("pricing eval failed at settle phase",
                              error=str(exc), version=logic.version, expr_type=logic.expr_type)
                raise PricingEvalError(str(exc)) from exc
            # freeze 阶段：fail-closed 用顶格兜底价，绝不放行免费请求（§5.2）
            logfire.warning("pricing eval failed, fallback to ceiling",
                            error=str(exc), version=logic.version, expr_type=logic.expr_type,
                            fallback_usd=str(logic.fallback_amount_usd))
            return logic.fallback_amount_usd

    async def _eval_asteval(self, expr: str, context: dict[str, float | str]) -> float:
        ast_precheck(expr)  # 主进程先拦（拉取时已校验，纵深防御；异常→失败路径）
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(
                get_eval_pool(), eval_expr_subprocess, expr, dict(context)
            ),
            timeout=settings.pricing_eval_timeout_seconds,
        )


# ---------- json_logic 安全子集（无代码执行面，简报 B §4 推荐方向） ----------

_JSON_LOGIC_MAX_DEPTH = 32


def _eval_json_logic(rule: Any, ctx: dict[str, float | str], depth: int) -> Any:
    """json-logic 安全子集求值：var/比较/算术/if/and/or/!/min/max。

    规则即 JSON 数据，无代码执行面；未知操作符/深度超限一律 ValueError
    （由调用方按求值失败处理）。
    """
    if depth > _JSON_LOGIC_MAX_DEPTH:
        raise ValueError("json_logic nesting too deep")
    if not isinstance(rule, dict) or len(rule) != 1:
        return rule  # 字面量
    op, args = next(iter(rule.items()))
    if not isinstance(args, list):
        args = [args]

    def val(i: int) -> Any:
        return _eval_json_logic(args[i], ctx, depth + 1)

    if op == "var":
        name = str(args[0])
        if name not in ctx:
            raise ValueError(f"json_logic var missing: {name}")
        return ctx[name]
    if op == "if" or op == "?:":
        i = 0
        while i + 1 < len(args):
            if val(i):
                return val(i + 1)
            i += 2
        return val(i) if i < len(args) else None
    if op in ("==", "==="):
        return val(0) == val(1)
    if op in ("!=", "!=="):
        return val(0) != val(1)
    if op in ("<", "<=", ">", ">="):
        operands = [float(_eval_json_logic(a, ctx, depth + 1)) for a in args]
        return all(_compare(op, a, b) for a, b in itertools.pairwise(operands))
    if op == "+":
        return sum(float(_eval_json_logic(a, ctx, depth + 1)) for a in args)
    if op == "-":
        if len(args) == 1:
            return -float(val(0))
        diff = float(val(0))
        for a in args[1:]:
            diff -= float(_eval_json_logic(a, ctx, depth + 1))
        return diff
    if op == "*":
        product = 1.0
        for a in args:
            product *= float(_eval_json_logic(a, ctx, depth + 1))
        return product
    if op == "/":
        quotient = float(val(0))
        for a in args[1:]:
            quotient /= float(_eval_json_logic(a, ctx, depth + 1))
        return quotient
    if op == "and":
        result: Any = None
        for a in args:
            result = _eval_json_logic(a, ctx, depth + 1)
            if not result:
                return result
        return result
    if op == "or":
        for a in args:
            result = _eval_json_logic(a, ctx, depth + 1)
            if result:
                return result
        return result
    if op == "!":
        return not val(0)
    if op == "min":
        return min(float(_eval_json_logic(a, ctx, depth + 1)) for a in args)
    if op == "max":
        return max(float(_eval_json_logic(a, ctx, depth + 1)) for a in args)
    raise ValueError(f"json_logic unsupported op: {op!r}")


def _compare(op: str, a: float, b: float) -> bool:
    if op == "<":
        return a < b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    return a >= b
