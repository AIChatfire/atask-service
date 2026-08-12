"""原生透传 catch-all（SPEC §3.9.6 / 架构 §13.1/§3.4①/§5.6）。

语义（不可变）：
    registry.get → native_prefixes 白名单（不符 404）→ 鉴权改写
    （摘 Authorization/Host/Content-Length，注入 adapter.auth_headers）→
    POST 且非 allow_user_direct_callback 时 adapter.rewrite_callback_url →
    upstream_client 单例转发（超时 504）→ 响应原样回传 + X-Gateway-Biz 头。

计费闭环（§4.8/§5.6，postpaid + charge）：GET 默认不计费
（billing_keys.charge_on_get 可覆盖）；POST 2xx → 解析用量 → PricingEvaluator
求值 → charge（request_id='pt:...'，§4.3）；charge 402 → 欠费三连（欠费单 +
debt 名单 + 告警），响应仍原样回传 + ``X-Gateway-Billing: debt``；响应含上游
task_id → 落 passthrough_tracked 行（W2 track_passthrough_task，§3.10.1）。

**注册纪律**：本 router 必须在 main.py 中最后一个 include（§4.2 不变量）。
"""

from __future__ import annotations

import hashlib
import json
import posixpath
from typing import TYPE_CHECKING, Annotated, Any

import httpx
import logfire
import ulid
from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app import errors
from app.adapters import get_adapter
from app.auth import TokenInfo, current_token
from app.db import get_session
from app.http_clients import upstream_client
from app.middleware import (
    acquire_upstream_slot,
    check_biz_rate_limit,
    check_debt_block,
    check_user_rate_limit,
    circuit_breaker,
    release_upstream_slot,
)
from app.registry import BizConfig, registry

if TYPE_CHECKING:
    from app.billing.client import BillingServiceClient
    from app.billing.pricing import PricingEvaluator

try:  # W3 未落地时不影响本模块导入（SPEC 依赖纪律：按 §3.11.1 签名注入）
    from app.billing.client import InsufficientBalance
except Exception:  # pragma: no cover - 仅并行开发期生效

    class InsufficientBalance(Exception):  # type: ignore[no-redef]
        """W3 落地前的占位异常；集成后以 app.billing.client.InsufficientBalance 为准。"""


router = APIRouter()

# 透传计费组件（装配期注入；未注入时跳过 charge 并告警——绝不免费放行阻断响应）
_pricing: PricingEvaluator | None = None
_billing: BillingServiceClient | None = None


def set_passthrough_billing(
    pricing: PricingEvaluator | None, billing: BillingServiceClient | None
) -> None:
    """main.py / worker 装配期注入 PricingEvaluator / BillingServiceClient。"""
    global _pricing, _billing
    _pricing, _billing = pricing, billing


# ---------------------------------------------------------------------------
# 透传计费挂钩（§5.6）
# ---------------------------------------------------------------------------


def _charge_request_id_key(user_id: int, req_hash: str) -> str:
    """未带 Idempotency-Key 的透传 request_id 复用键（SPEC §3.6：idem:{user}:{req_hash}）。"""
    return f"idem:{user_id}:{req_hash}"


async def _charge_request_id(
    request: Request, token: TokenInfo, raw_body: bytes
) -> str:
    """charge 幂等 request_id（§4.3 透传规则）：
    带 Idempotency-Key → ``pt:{user_id}:{sha256(key)}``；未带 → ``pt_{ulid}``
    并写 ``idem:{user}:{req_hash}``（24h）供重试命中复用。"""
    idem_key = request.headers.get("Idempotency-Key")
    if idem_key:
        return f"pt:{token.user_id}:{hashlib.sha256(idem_key.encode()).hexdigest()}"
    req_hash = hashlib.sha256(
        request.method.encode() + b" " + request.url.path.encode() + b" " + raw_body
    ).hexdigest()
    from app.redis_client import get_redis

    redis = await get_redis()
    key = _charge_request_id_key(token.user_id, req_hash)
    existing = await redis.get(key)
    if existing:
        return existing
    request_id = f"pt_{ulid.new()}"
    await redis.set(key, request_id, ex=24 * 3600)
    return request_id


def _usage_context(req_json: dict[str, Any], resp_json: dict[str, Any]) -> dict[str, Any]:
    """从透传响应/请求体提取求值上下文（变量名契约 SPEC §3.11.2）。"""
    usage = resp_json.get("usage")
    if not isinstance(usage, dict):
        inner = resp_json.get("data")
        usage = inner.get("usage") if isinstance(inner, dict) else None
    usage = usage if isinstance(usage, dict) else {}
    tokens = usage.get("completion_tokens") or usage.get("total_tokens") or 0.0
    duration = req_json.get("duration") or resp_json.get("duration") or 0.0
    resolution = req_json.get("resolution") or resp_json.get("resolution") or ""
    try:
        tokens_f = float(tokens)
    except (TypeError, ValueError):
        tokens_f = 0.0
    try:
        duration_f = float(duration)
    except (TypeError, ValueError):
        duration_f = 0.0
    return {
        "duration": duration_f,
        "resolution": str(resolution),
        "mode": str(req_json.get("mode") or ""),
        "quantity": float(req_json.get("n") or 1),
        "usage_tokens": tokens_f,
        "generate_audio": 1.0 if req_json.get("generate_audio") else 0.0,
        "has_image_input": 1.0 if req_json.get("image") else 0.0,
        "service_tier": str(resp_json.get("service_tier") or "default"),
    }


def _extract_upstream_task_id(resp_json: dict[str, Any]) -> str | None:
    """响应中的上游 task_id（kling 信封 data.task_id / 顶层 task_id / 顶层 id）。"""
    inner = resp_json.get("data")
    if isinstance(inner, dict) and isinstance(inner.get("task_id"), str):
        return inner["task_id"]
    for key in ("task_id", "id"):
        v = resp_json.get(key)
        if isinstance(v, str) and v:
            return v
    return None


async def _track_passthrough_billing(
    cfg: BizConfig,
    biz: str,
    native_path: str,
    request: Request,
    raw_body: bytes,
    resp_json: dict[str, Any],
    token: TokenInfo,
    session: AsyncSession,
) -> bool:
    """透传计费闭环（§5.6）。返回 True 表示本次进入欠费处置（响应加 debt 头）。

    任何内部失败都不阻断响应（上游已执行）；失败一律落 outbox 补偿 + 告警。
    """
    if token.is_system:
        # skip 路径：charge 闭环 no-op（审计 event 已由 current_token 记录，§3.9.7）
        logfire.info("passthrough charge skipped (system identity)",
                     event="skip_auth_billing", biz=biz)
        return False
    if _pricing is None or _billing is None:
        logfire.warning("passthrough billing skipped: components not wired", biz=biz)
        return False
    try:
        req_json: dict[str, Any] = json.loads(raw_body) if raw_body else {}
        if not isinstance(req_json, dict):
            req_json = {}
    except Exception:
        req_json = {}
    model = (
        req_json.get("model") or req_json.get("model_name")
        or resp_json.get("model") or "-"
    )
    try:
        logic = await _pricing.get_logic(biz, str(model), native_path)
        context = _usage_context(req_json, resp_json)
        # phase="freeze"：求值失败走 fallback 顶格兜底（fail-closed，绝不免费放行）
        amount = await _pricing.evaluate(logic, context, phase="freeze")
    except Exception:
        logfire.exception("passthrough pricing failed", biz=biz)
        return False

    request_id = await _charge_request_id(request, token, raw_body)
    biz_type = str(cfg.billing_keys.get("biz_type") or biz)
    metric = str(cfg.billing_keys.get("metric") or "call")
    try:
        await _billing.charge(
            request_id=request_id, biz_type=biz_type, metric=metric,
            amount_usd=amount, user_sk=token.raw,
        )
        return False
    except InsufficientBalance:
        # 欠费三连（§4.8/§5.6）：① 欠费单 + outbox charge 行（同事务）
        # ② debt:{user_id} 熔断名单 ③ 告警；响应原样回传 + X-Gateway-Billing: debt
        logfire.error("passthrough charge 402, entering debt flow",
                      biz=biz, user_id=token.user_id, amount_usd=str(amount))
        await _record_debt(token, request_id, amount,
                           biz=biz, biz_type=biz_type, metric=metric)
        return True
    except Exception as exc:
        # 计费服务不可用：异步入 outbox 由 W3 补偿（幂等 request_id 安全重放）
        logfire.exception("passthrough charge failed, queued to outbox",
                          biz=biz, request_id=request_id)
        await _enqueue_charge_outbox(
            token, request_id, amount,
            biz=biz, biz_type=biz_type, metric=metric, debt=False,
            last_error=f"{type(exc).__name__}: {exc}"[:500],
        )
        return False


async def _verify_only_precheck(
    cfg: BizConfig,
    biz: str,
    native_path: str,
    request: Request,
    raw_body: bytes,
    token: TokenInfo,
) -> str | None:
    """verify_only 预检（决策 A-9）：上游调用前 charge(verify_only=true) 验余额。

    - 402 → 直接拒绝（errors.payment_required），**对上游零成本**；
    - 计费服务不可用/内部异常 → 放行（best-effort：正式 charge 闭环在响应
      后照常执行，失败有 outbox 补偿）；
    - biz 配置可关：``billing_keys.verify_only_precheck=false``；
    - 返回正式 charge 复用的 request_id（同一幂等键）；未执行预检返回 None。
    """
    if _billing is None or _pricing is None:
        return None
    if not cfg.billing_keys.get("verify_only_precheck", True):
        return None
    request_id = await _charge_request_id(request, token, raw_body)
    try:
        req_json: dict[str, Any] = json.loads(raw_body) if raw_body else {}
        if not isinstance(req_json, dict):
            req_json = {}
        model = str(req_json.get("model") or req_json.get("model_name") or "-")
        logic = await _pricing.get_logic(biz, model, native_path)
        # 请求侧顶格（usage 未知按 0，phase="freeze" 求值失败走 fallback 顶格）
        amount = await _pricing.evaluate(logic, _usage_context(req_json, {}),
                                         phase="freeze")
        await _billing.charge(
            request_id=f"{request_id}:vo",
            biz_type=str(cfg.billing_keys.get("biz_type") or biz),
            metric=str(cfg.billing_keys.get("metric") or "call"),
            amount_usd=amount, user_sk=token.raw, verify_only=True,
        )
        return request_id
    except InsufficientBalance as exc:
        logfire.info("verify_only precheck rejected: insufficient balance",
                     biz=biz, user_id=token.user_id)
        raise errors.payment_required() from exc
    except Exception as exc:
        logfire.warning("verify_only precheck failed, proceeding",
                        biz=biz, error=str(exc))
        return request_id


async def _enqueue_charge_outbox(
    token: TokenInfo, request_id: str, amount: Any, *,
    biz: str, biz_type: str, metric: str, debt: bool, last_error: str | None = None,
) -> None:
    """入 Redis outbox charge 条（决策 A-4；payload.request_id 原样，幂等重放）。

    payload 键契约与 outbox.py 消费方严格对齐（§3.11.4）：金额键为 ``amount``
    （str），并携带 ``user_id``/``biz`` —— 402 欠费清偿成功路径据此解除
    ``debt:{user_id}`` 熔断名单并写计费审计日志。
    """
    from app.billing.outbox import enqueue_outbox

    payload: dict[str, Any] = {
        "request_id": request_id,
        "biz_type": biz_type,
        "metric": metric,
        "amount": str(amount),
        "user_sk": token.raw,
        "user_id": token.user_id,
        "biz": biz,
        "debt": debt,
    }
    try:
        await enqueue_outbox(
            task_id=request_id, op="charge", payload=payload, last_error=last_error
        )
    except Exception:
        logfire.exception("charge outbox enqueue failed", request_id=request_id)


async def _record_debt(
    token: TokenInfo, request_id: str, amount: Any, *,
    biz: str, biz_type: str, metric: str,
) -> None:
    """欠费单（Redis）+ outbox 持续追扣条 + debt 熔断名单（§5.6 处置三连①②）。"""
    from app.billing.outbox import write_debt_order

    try:
        await write_debt_order(
            user_id=token.user_id, task_id=None, request_id=request_id,
            amount_usd=amount, biz=biz,
        )
        await _enqueue_charge_outbox(
            token, request_id, amount,
            biz=biz, biz_type=biz_type, metric=metric, debt=True,
        )
        from app.redis_client import get_redis

        redis = await get_redis()
        await redis.set(f"debt:{token.user_id}", request_id)
    except Exception:
        logfire.exception("debt record failed", request_id=request_id)


async def _track_passthrough_task(
    cfg: BizConfig, native_path: str, req_json: dict[str, Any],
    resp_json: dict[str, Any], token: TokenInfo, session: AsyncSession,
) -> None:
    """响应含上游 task_id → 落 passthrough_tracked 自有行（§5.6.4；W2 提供实现）。

    失败仅告警——不影响响应回传（回调/轮询兜底收敛）。
    """
    upstream_task_id = _extract_upstream_task_id(resp_json)
    if not upstream_task_id:
        return
    from app.routing import videos  # 单点注入的 TaskManager 与 videos 形态共享

    tm = videos.task_manager
    if tm is None:
        logfire.warning("passthrough tracked skipped: task manager not wired",
                        upstream_task_id=upstream_task_id)
        return
    try:
        await tm.track_passthrough_task(
            session, biz_cfg=cfg, token=token, upstream_task_id=upstream_task_id,
            action=native_path,
            request_snapshot={"native_path": native_path, "body": req_json},
            raw_response=resp_json,
        )
    except Exception:
        logfire.exception("track passthrough task failed",
                          upstream_task_id=upstream_task_id)


# ---------------------------------------------------------------------------
# native_path 白名单校验（含 dot-segment 穿越防护）
# ---------------------------------------------------------------------------


def _check_native_path_allowed(cfg: BizConfig, native_path: str) -> None:
    """native_prefixes 前缀白名单 + ``..`` 穿越拒绝（安全不变量）。

    出站转发用 httpx 拼接 ``{base}/{native_path}``，httpx 会做 dot-segment
    归一化——``v1/videos/../../admin`` 能通过朴素前缀校验却被归一化成
    ``/admin``，带着注入的上游凭证打任意路径。故：含 ``..`` 段直接拒绝，
    并对归一化后的路径复检前缀（双保险），转发仍用原始 native_path。
    """
    if ".." in native_path.split("/"):
        raise errors.not_found("path not allowed for biz")
    normalized = posixpath.normpath(native_path)
    if not any(
        native_path == p or native_path.startswith(p.rstrip("/") + "/")
        for p in cfg.native_prefixes
    ) or not any(
        normalized == p or normalized.startswith(p.rstrip("/") + "/")
        for p in cfg.native_prefixes
    ):
        raise errors.not_found("path not allowed for biz")


# ---------------------------------------------------------------------------
# catch-all 端点（必须最后注册，§4.2）
# ---------------------------------------------------------------------------


@router.api_route(
    "/{biz}/{native_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"]
)
async def passthrough(
    biz: str,
    native_path: str,
    request: Request,
    token: Annotated[TokenInfo, Depends(current_token)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    """原生透传（架构 §13.1 语义为准；详见模块 docstring）。"""
    cfg = await registry.get(biz, session)
    _check_native_path_allowed(cfg, native_path)

    if request.method == "POST" and not token.is_system:
        # 欠费名单：提交类 402，查询类放行；skip 路径不做欠费检查（§3.9.7）
        await check_debt_block(token)
    await check_user_rate_limit(token, cfg)
    await check_biz_rate_limit(cfg)

    circuit = f"upstream:{biz}"
    if not await circuit_breaker.allow(circuit):
        raise errors.backpressure(retry_after=5, message="upstream circuit open")
    if not await acquire_upstream_slot(cfg):
        raise errors.backpressure(retry_after=5, message="upstream concurrency limit")

    try:
        adapter = get_adapter(cfg.adapter)
        raw_body = await request.body()
        upstream_url = f"{cfg.upstream_base_url.rstrip('/')}/{native_path}"

        # 鉴权改写：摘用户 Bearer/Host/Content-Length，注入上游凭证（§3.4①）
        headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in ("authorization", "host", "content-length")
        }
        headers.update(dict(adapter.auth_headers(cfg)))

        # callback_url 改写：默认收敛回网关（用户回调由 §7.2 透传）
        if (
            request.method == "POST" and raw_body
            and not cfg.billing_keys.get("allow_user_direct_callback")
        ):
            raw_body = adapter.rewrite_callback_url(raw_body, cfg)

        # verify_only 预检（决策 A-9）：POST 在上游调用前先
        # charge(verify_only=true) 验余额，402 直接拒绝——对上游零成本。
        # biz 配置可关（billing_keys.verify_only_precheck=false）；计费服务
        # 异常放行（best-effort，正式 charge 闭环在响应后照常执行）。
        if request.method == "POST" and not token.is_system:
            # skip 路径：verify_only 预检 no-op（计费全跳，§3.9.7）
            await _verify_only_precheck(
                cfg, biz, native_path, request, raw_body, token
            )

        with logfire.span("upstream passthrough", biz=biz, path=native_path):
            try:
                resp = await upstream_client().request(
                    request.method, upstream_url,
                    params=httpx.QueryParams(request.query_params),
                    content=raw_body or None, headers=headers,
                )
            except httpx.TimeoutException as exc:
                await circuit_breaker.on_failure(circuit)
                raise errors.upstream_error(
                    "upstream timeout", status_code=504
                ) from exc
            except httpx.HTTPError as exc:
                await circuit_breaker.on_failure(circuit)
                raise errors.upstream_error(f"upstream transport error: {exc}") from exc

        # 熔断记账：5xx 计失败；2xx/4xx（含 429——上游健康只是太快）计成功（§8.2）
        if resp.status_code >= 500:
            await circuit_breaker.on_failure(circuit)
        else:
            await circuit_breaker.on_success(circuit)

        debt = False
        resp_json: dict[str, Any] | None = None
        chargeable = request.method == "POST" or (
            request.method == "GET" and bool(cfg.billing_keys.get("charge_on_get"))
        )
        if chargeable and 200 <= resp.status_code < 300:
            try:
                parsed = json.loads(resp.content) if resp.content else None
                resp_json = parsed if isinstance(parsed, dict) else None
            except Exception:
                resp_json = None
            if resp_json is not None:
                debt = await _track_passthrough_billing(
                    cfg, biz, native_path, request, raw_body, resp_json, token, session
                )
                if request.method == "POST":
                    req_json: dict[str, Any] = {}
                    try:
                        parsed_req = json.loads(raw_body) if raw_body else {}
                        if isinstance(parsed_req, dict):
                            req_json = parsed_req
                    except Exception:
                        req_json = {}
                    await _track_passthrough_task(
                        cfg, native_path, req_json, resp_json, token, session
                    )

        resp_headers = {
            k: v for k, v in resp.headers.items() if k.lower().startswith("content-")
        }
        resp_headers["X-Gateway-Biz"] = biz
        if debt:
            resp_headers["X-Gateway-Billing"] = "debt"
        return Response(
            content=resp.content, status_code=resp.status_code, headers=resp_headers
        )
    finally:
        await release_upstream_slot(biz)
