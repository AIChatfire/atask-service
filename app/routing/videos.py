"""new-api 兼容 videos 形态四端点（SPEC §3.9.5 / 架构 §3.4③/§11.2）。

编排顺序（不可换，SPEC §3.9.5 docstring 语义）：
    registry.get → 幂等 guard → check_debt_block → CanonicalTaskRequest
    组装 → TaskManager.submit_task → 201 响应。
402/429/503 经 errors 工厂；上游提交失败由 manager 内 cancel 解冻后向上抛 502。

依赖装配：模块级 ``task_manager`` 由 main.py / worker 经 ``set_task_manager()``
注入（与 §13.5 receiver 同款模式）；W2 未落地时保持 None，提交类端点 503 背压。
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Annotated, Any

import logfire
from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app import errors
from app.adapters.base import CanonicalTaskRequest
from app.auth import TokenInfo, current_token, get_owned_task
from app.db import get_session, get_session_factory
from app.errors import GatewayError
from app.middleware import (
    check_biz_rate_limit,
    check_debt_block,
    check_user_rate_limit,
    idempotency_complete,
    idempotency_guard,
    idempotency_release,
)
from app.registry import registry
from app.schemas import (
    VideoError,
    VideoRemixRequest,
    VideoStatus,
    VideoStatusResponse,
    VideoSubmitRequest,
    VideoSubmitResponse,
)
from app.tasks.models import to_video_status

if TYPE_CHECKING:
    from app.tasks.manager import TaskManager

try:  # W2 未落地时不影响本模块导入（SPEC 依赖纪律：按 §3.10.1 签名 mock/注入）
    from app.tasks.manager import PaymentRequired
except Exception:  # pragma: no cover - 仅并行开发期生效

    class PaymentRequired(Exception):  # type: ignore[no-redef]
        """W2 落地前的占位异常；集成后以 app.tasks.manager.PaymentRequired 为准。"""


try:  # W3 未落地时不影响本模块导入（SPEC §3.11.1 契约）
    from app.billing.client import BillingLockBusy
except Exception:  # pragma: no cover - 仅并行开发期生效

    class BillingLockBusy(Exception):  # type: ignore[no-redef]
        """W3 落地前的占位异常；集成后以 app.billing.client.BillingLockBusy 为准。"""

        retry_after_ms: int = 1000


def _billing_lock_busy_503(exc: BillingLockBusy) -> GatewayError:
    """计费锁 409（客户端内已重试仍忙）→ 503 背压 + Retry-After（绝不冒泡 500）。"""
    retry_after = max(1, -(-int(exc.retry_after_ms) // 1000))  # ceil(ms/1000)
    return errors.backpressure(retry_after=retry_after,
                               message="billing lock busy, retry later")


router = APIRouter()

task_manager: TaskManager | None = None  # 装配注入点（SPEC §3.9.5）


def set_task_manager(tm: TaskManager) -> None:
    """main.py / worker 装配期注入 TaskManager 单例。"""
    global task_manager
    task_manager = tm


def _require_task_manager() -> TaskManager:
    if task_manager is None:
        raise errors.backpressure(retry_after=5, message="task manager not ready")
    return task_manager


# ---------------------------------------------------------------------------
# 请求/响应组装助手
# ---------------------------------------------------------------------------

# new-api 动作枚举（简报 C §三 / SPEC §5.1）
_ACTIONS = frozenset(
    {"generate", "textGenerate", "firstTailGenerate", "referenceGenerate", "remixGenerate"}
)

# metadata 中被 CanonicalTaskRequest 显式消费的键；其余进 extra（供应商扩展）
_META_CONSUMED = frozenset({"callback_url", "resolution", "mode", "generate_audio", "action"})


def _derive_action(body: VideoSubmitRequest, metadata: dict[str, Any]) -> str:
    """action 由 image / metadata.action 推导（SPEC §3.9.5，对齐 new-api 枚举）。"""
    explicit = metadata.get("action")
    if explicit is not None:
        if isinstance(explicit, str) and explicit in _ACTIONS:
            return explicit
        raise GatewayError(
            f"unsupported action: {explicit}", status_code=400,
            error_type="invalid_request_error", param="metadata.action",
        )
    return "firstTailGenerate" if body.image else "textGenerate"


def _build_canonical(body: VideoSubmitRequest) -> CanonicalTaskRequest:
    """VideoSubmitRequest → CanonicalTaskRequest（metadata 提取扩展参数，§3.4③）。"""
    metadata = dict(body.metadata or {})
    extra: dict[str, Any] = {k: v for k, v in metadata.items() if k not in _META_CONSUMED}
    for opt in ("size", "width", "height", "fps", "seed"):
        v = getattr(body, opt)
        if v is not None:
            extra[opt] = v
    callback_url = metadata.get("callback_url")
    if callback_url is not None and not isinstance(callback_url, str):
        raise GatewayError(
            "callback_url must be a string", status_code=400,
            error_type="invalid_request_error", param="metadata.callback_url",
        )
    return CanonicalTaskRequest(
        model=body.model,
        prompt=body.prompt,
        action=_derive_action(body, metadata),
        duration=body.duration,
        resolution=metadata.get("resolution") or body.size,
        mode=metadata.get("mode"),
        image=body.image,
        n=body.n,
        generate_audio=bool(metadata.get("generate_audio", False)),
        callback_url=callback_url,
        extra=extra or None,
    )


def _asdict(value: Any) -> dict[str, Any]:
    """JSON 列兼容：dict 原样 / JSON 文本解析 / 其余空 dict。"""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _parse_error(fail_reason: str | None) -> VideoError | None:
    """fail_reason → VideoError（timeout:/canceled: 前缀归一为 code，SPEC §3.9.5）。"""
    if not fail_reason:
        return VideoError(code="failed", message="task failed")
    for prefix in ("timeout:", "canceled:", "failed:"):
        if fail_reason.startswith(prefix):
            return VideoError(
                code=prefix.rstrip(":"), message=fail_reason[len(prefix):].strip()
            )
    return VideoError(code="failed", message=fail_reason)


def _status_response(row: dict[str, Any]) -> VideoStatusResponse:
    """tasks 自有行 → VideoStatusResponse（状态一律经 to_video_status，SPEC §3.4.3）。"""
    status = to_video_status(row["status"])
    private_data = _asdict(row.get("private_data"))
    gateway = _asdict(private_data.get("gateway"))
    snapshot = _asdict(gateway.get("request_snapshot"))
    data = _asdict(row.get("data"))

    url: str | None = None
    if status == "completed":
        url = private_data.get("result_url")

    metadata: dict[str, Any] = {}
    for key in ("duration", "fps", "width", "height", "seed", "resolution"):
        if snapshot.get(key) is not None:
            metadata[key] = snapshot[key]
    usage = gateway.get("usage_actual")
    if usage:
        metadata["usage"] = usage

    error = _parse_error(row.get("fail_reason")) if status == "failed" else None
    fmt = data.get("format") or snapshot.get("format")
    return VideoStatusResponse(
        task_id=row["task_id"], status=VideoStatus(status), url=url,
        format=fmt if isinstance(fmt, str) else None,
        metadata=metadata, error=error,
    )


def _submit_response(result: dict[str, Any], model: str) -> VideoSubmitResponse:
    return VideoSubmitResponse(
        id=result["task_id"], model=model,
        created_at=int(result["created_at"]), task_id=result["task_id"],
    )


# ---------------------------------------------------------------------------
# 端点
# ---------------------------------------------------------------------------


@router.post("/{biz}/v1/videos", status_code=201)
async def videos_submit(
    biz: str,
    body: VideoSubmitRequest,
    request: Request,
    token: Annotated[TokenInfo, Depends(current_token)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> VideoSubmitResponse:
    """提交视频任务（编排顺序见模块 docstring；SPEC §3.9.5）。"""
    cfg = await registry.get(biz, session)
    idem_key = await idempotency_guard(request, token)
    try:
        if not token.is_system:                      # skip 路径不做欠费检查（§3.9.7）
            await check_debt_block(token)
        await check_user_rate_limit(token, cfg)
        await check_biz_rate_limit(cfg)
        req = _build_canonical(body)
        tm = _require_task_manager()
        result = await tm.submit_task(
            session, biz_cfg=cfg, req=req, token=token, form="videos", idem_key=idem_key
        )
    except BillingLockBusy as exc:
        if idem_key:
            await idempotency_release(token, idem_key)
        raise _billing_lock_busy_503(exc) from exc
    except PaymentRequired as exc:
        if idem_key:
            await idempotency_release(token, idem_key)   # 任务不落库，幂等键释放
        raise errors.payment_required(str(exc) or "insufficient balance") from exc
    except Exception:
        if idem_key:
            await idempotency_release(token, idem_key)
        raise
    resp = _submit_response(result, body.model)
    if idem_key:
        await idempotency_complete(
            token, idem_key, status_code=201, body=json.loads(resp.model_dump_json())
        )
    return resp


@router.get("/{biz}/v1/videos/{task_id}")
async def videos_get(
    biz: str,
    task_id: str,
    token: Annotated[TokenInfo, Depends(current_token)],
) -> VideoStatusResponse:
    """查询任务（get_owned_task 404 防 IDOR；鉴权上下文从 tasks 行取回，§6.4）。"""
    del biz  # biz 维度已含在 task 行 platform 中；归属校验以 user_id 为准
    row = await get_owned_task(task_id, token)
    return _status_response(row)


@router.get("/{biz}/v1/videos/{task_id}/content")
async def videos_content(
    biz: str,
    task_id: str,
    token: Annotated[TokenInfo, Depends(current_token)],
) -> Response:
    """产物内容：默认 302 重定向到 result_url；``billing_keys.proxy_content``
    开启时经网关代理流式回传（对齐 new-api result_url 代理语义）。"""
    row = await get_owned_task(task_id, token)
    private_data = _asdict(row.get("private_data"))
    result_url = private_data.get("result_url")
    if not isinstance(result_url, str) or not result_url:
        raise GatewayError(
            "task result not ready", status_code=409,
            error_type="invalid_request_error", code="task_not_completed",
        )

    proxy = False
    try:
        async with get_session_factory()() as session:
            cfg = await registry.get(biz, session)
        proxy = bool(cfg.billing_keys.get("proxy_content"))
    except GatewayError:
        raise
    except Exception:
        logfire.exception("content proxy config lookup failed", biz=biz)

    if not proxy:
        return RedirectResponse(url=result_url, status_code=302)

    # 代理流式回传（复用出站单例，禁止每请求新建 client）
    from app.http_clients import upstream_client

    client = upstream_client()
    try:
        upstream_resp = await client.get(result_url)
    except Exception as exc:
        raise errors.upstream_error("failed to fetch task result") from exc
    headers = {}
    if ctype := upstream_resp.headers.get("content-type"):
        headers["content-type"] = ctype
    return Response(
        content=upstream_resp.content, status_code=upstream_resp.status_code,
        headers=headers,
    )


@router.post("/{biz}/v1/videos/{video_id}/remix", status_code=201)
async def videos_remix(
    biz: str,
    video_id: str,
    body: VideoRemixRequest,
    request: Request,
    token: Annotated[TokenInfo, Depends(current_token)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> VideoSubmitResponse:
    """remix（SPEC §3.9.5 / 架构 §13.4 remix 桩语义）：
    原任务归属 404 → 非 SUCCESS 422 → request_snapshot 基底 + 白名单覆盖 →
    action="remixGenerate" 新 task_id 独立 freeze，form="videos_remix"。"""
    origin = await get_owned_task(video_id, token)
    if origin["status"] != "SUCCESS":
        raise GatewayError(
            "remix source must be a succeeded task", status_code=422,
            error_type="invalid_request_error", code="remix_source_not_succeeded",
        )
    cfg = await registry.get(biz, session)
    idem_key = await idempotency_guard(request, token)
    try:
        if not token.is_system:                      # skip 路径不做欠费检查（§3.9.7）
            await check_debt_block(token)
        gateway = _asdict(_asdict(origin.get("private_data")).get("gateway"))
        base = _asdict(gateway.get("request_snapshot"))
        if not base:
            raise GatewayError(
                "origin task has no request snapshot", status_code=422,
                error_type="invalid_request_error", code="remix_source_invalid",
            )
        merged: dict[str, Any] = {**base}
        if body.prompt is not None:                       # 白名单覆盖：prompt
            merged["prompt"] = body.prompt
        metadata = dict(body.metadata or {})              # 白名单覆盖：metadata
        for key in ("callback_url", "resolution", "mode", "generate_audio"):
            if key in metadata:
                merged[key] = metadata[key]
        extra = dict(merged.get("extra") or {})
        extra.update({k: v for k, v in metadata.items() if k not in _META_CONSUMED})
        merged["extra"] = extra or None
        merged.pop("action", None)                        # 避免与显式 action 冲突
        req = CanonicalTaskRequest(**merged, action="remixGenerate")
        await check_user_rate_limit(token, cfg)
        await check_biz_rate_limit(cfg)
        tm = _require_task_manager()
        result = await tm.submit_task(
            session, biz_cfg=cfg, req=req, token=token,
            form="videos_remix", idem_key=idem_key,
        )
    except PaymentRequired as exc:
        if idem_key:
            await idempotency_release(token, idem_key)
        raise errors.payment_required(str(exc) or "insufficient balance") from exc
    except Exception:
        if idem_key:
            await idempotency_release(token, idem_key)
        raise
    resp = _submit_response(result, req.model)
    if idem_key:
        await idempotency_complete(
            token, idem_key, status_code=201, body=json.loads(resp.model_dump_json())
        )
    logfire.info("videos remix submitted", biz=biz, origin=video_id)
    return resp


__all__ = ["router", "set_task_manager", "task_manager"]
