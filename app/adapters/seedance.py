"""Seedance（火山方舟 Ark）上游适配器（SPEC §3.13.2；事实依据：简报 A §4）。

- 端点：创建 ``POST {base}/api/v3/contents/generations/tasks``，查询
  ``GET {base}/api/v3/contents/generations/tasks/{id}``；
  鉴权 ``Authorization: Bearer $ARK_API_KEY``（静态 key，无需 JWT）。
- 请求 ``content`` 数组：``{"type":"text","text":...}`` /
  ``{"type":"image_url","image_url":{"url":...},"role":"first_frame"}``。
- 创建响应仅 ``{"id":"cgt-..."}``；查询响应含 ``status`` /
  ``content.video_url`` / ``usage.completion_tokens`` / 实际 ``resolution``。
- 状态机 ``queued → running → succeeded / failed / expired``
  （``expired`` = 超 ``execution_expires_after``，默认 48h）。
- Webhook：创建时传顶层 ``callback_url``；**回调体与查询响应体一致**
  （简报 A §4 已确认），``parse_callback`` 直接复用查询响应解析。

**反查语义**：方舟回调只带 ``cgt-`` 前缀上游 id、**不回显**网关注入的
external id（``echoes_external_task_id=False``），W4 回调反查必须走
``gateway_task_upstream_index`` 索引表（SPEC §4.6）。

**幂等提交语义**：方舟创建接口无客户端幂等键；网络抖动/超时后网关**不重发**
（submit 异常向上抛后由 W2 cancel 解冻并终止该任务，用户经 Idempotency-Key
重放会得到首个响应或重新提交为新任务）。5xx/超时重试决策在网关层
（tenacity 只重试可重试错误，同一报文体重发对方舟是新建任务语义，故
适配器内不做自动重试）。
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, ClassVar

import httpx

from app.adapters.base import (
    CanonicalTaskRequest,
    SubmitContext,
    SubmitResult,
    TaskSnapshot,
    TaskStatus,
    UpstreamBizError,
    UpstreamRateLimitError,
    UsageEstimate,
    register,
)
from app.http_clients import upstream_client

__all__ = ["SeedanceAdapter"]

_TASKS_PATH = "api/v3/contents/generations/tasks"

# 透传字段白名单（req.extra → 方舟请求体顶层，简报 A §4 创建请求字段）
_EXTRA_PASSTHROUGH: tuple[str, ...] = (
    "ratio",
    "watermark",
    "seed",
    "service_tier",
    "return_last_frame",
    "draft",
    "execution_expires_after",
)


def _retry_after(resp: httpx.Response) -> float | None:
    raw = resp.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _check_http(resp: httpx.Response, provider: str) -> None:
    """错误分级（SPEC §3.2.3 / §8.2）：429 → RateLimit（可重试、不计熔断）；
    其他 4xx → BizError（绝不重试）；5xx → HTTPStatusError（可重试、计熔断）。"""
    if resp.status_code < 400:
        return
    if resp.status_code in (401, 403):
        # 上游鉴权失败：回报 keys 轮询微服务剔除坏 key（无租约时 no-op）
        from app.keys import key_provider

        key_provider.report_auth_failure(provider, resp.status_code)
    if resp.status_code == 429:
        raise UpstreamRateLimitError(
            f"{provider} rate limited (429)", retry_after=_retry_after(resp)
        )
    if resp.status_code >= 500:
        resp.raise_for_status()
    message = f"{provider} HTTP {resp.status_code}"
    code: int | str | None = resp.status_code
    try:
        body = resp.json()
    except json.JSONDecodeError:
        body = None
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        err = body["error"]
        message = str(err.get("message") or message)
        code = err.get("code") or code
    raise UpstreamBizError(message, code=code)


class SeedanceAdapter:
    """seedance/方舟适配器（callback 支持；回调不回显 external id）。"""

    name: ClassVar[str] = "seedance"
    callback_capability: ClassVar[bool] = True
    echoes_external_task_id: ClassVar[bool] = False

    _STATUS_MAP: ClassVar[dict[str, TaskStatus]] = {
        "queued": TaskStatus.QUEUED,
        "running": TaskStatus.RUNNING,
        "succeeded": TaskStatus.SUCCEEDED,
        "failed": TaskStatus.FAILED,
        "expired": TaskStatus.TIMEOUT,
    }

    # ------------------------------------------------------------------
    # 状态映射 / 鉴权
    # ------------------------------------------------------------------

    def map_status(self, upstream_status: str) -> TaskStatus:
        # 未知状态映射为 RUNNING（不推进终态，等下一轮）
        return self._STATUS_MAP.get(upstream_status, TaskStatus.RUNNING)

    def auth_headers(self, cfg: Any) -> Mapping[str, str]:
        if isinstance(cfg, SubmitContext):
            key = (
                cfg.secrets.get("api_key")
                or cfg.secrets.get("API_KEY")
                or cfg.secrets.get("ark_api_key")
                or cfg.secrets.get("ARK_API_KEY", "")
            )
        else:
            key = os.environ[cfg.auth_secret_ref]
        return {"Authorization": f"Bearer {key}"}

    # ------------------------------------------------------------------
    # 提交 / 轮询 / 回调
    # ------------------------------------------------------------------

    async def submit(self, req: CanonicalTaskRequest, ctx: SubmitContext) -> SubmitResult:
        base = ctx.upstream_base_url.rstrip("/")
        content: list[dict[str, Any]] = [{"type": "text", "text": req.prompt}]
        if req.image:
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": req.image},
                    "role": "first_frame",
                }
            )
        body: dict[str, Any] = {
            "model": req.model,
            "content": content,
            "callback_url": ctx.gateway_callback_url,
        }
        if req.duration:
            body["duration"] = int(req.duration)
        if req.resolution:
            body["resolution"] = req.resolution
        if req.generate_audio:
            body["generate_audio"] = True
        extra = req.extra or {}
        for key in _EXTRA_PASSTHROUGH:  # execution_expires_after 等透传
            if key in extra:
                body[key] = extra[key]
        resp = await upstream_client().post(
            f"{base}/{_TASKS_PATH}", json=body, headers=self.auth_headers(ctx)
        )
        _check_http(resp, self.name)
        data = resp.json()
        upstream_task_id = data.get("id")
        if not upstream_task_id:
            raise UpstreamBizError("seedance submit response missing task id")
        return SubmitResult(upstream_task_id=str(upstream_task_id), raw=data)

    async def poll(self, upstream_task_id: str, ctx: SubmitContext) -> TaskSnapshot:
        base = ctx.upstream_base_url.rstrip("/")
        resp = await upstream_client().get(
            f"{base}/{_TASKS_PATH}/{upstream_task_id}",
            headers=self.auth_headers(ctx),
        )
        _check_http(resp, self.name)
        return self._parse_task(resp.json())

    def parse_callback(self, raw_body: bytes, headers: Mapping[str, str]) -> TaskSnapshot:
        # 回调体 = 查询任务响应体（简报 A §4 已确认），直接复用查询解析；
        # 验签不在此做（W4 职责）。
        try:
            data = json.loads(raw_body)
        except ValueError as exc:  # JSONDecodeError / UnicodeDecodeError
            raise ValueError(f"seedance callback body is not JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError("seedance callback body is not a JSON object")
        return self._parse_task(data)

    # ------------------------------------------------------------------
    # 响应解析（poll 与 parse_callback 共用）
    # ------------------------------------------------------------------

    def _parse_task(self, data: dict[str, Any]) -> TaskSnapshot:
        status_raw = str(data.get("status") or "")
        status = self.map_status(status_raw)

        result: dict[str, Any] | None = None
        content = data.get("content") or {}
        if status is TaskStatus.SUCCEEDED and content.get("video_url"):
            result = {"url": content["video_url"]}
            if content.get("last_frame_url"):
                result["last_frame_url"] = content["last_frame_url"]
            if data.get("duration") is not None:
                result["duration"] = float(data["duration"])
            if data.get("resolution"):
                result["resolution"] = data["resolution"]

        # 实收信号（SPEC §3.2.2 键约定）：completion_tokens + 实际 resolution 档
        usage: dict[str, Any] = {}
        raw_usage = data.get("usage") or {}
        if raw_usage.get("completion_tokens") is not None:
            usage["completion_tokens"] = float(raw_usage["completion_tokens"])
        if data.get("resolution"):
            usage["resolution"] = data["resolution"]
        if data.get("duration") is not None:
            usage["actual_duration"] = float(data["duration"])
        if data.get("generate_audio") is not None:
            usage["generate_audio"] = 1.0 if data["generate_audio"] else 0.0

        error: dict[str, Any] | None = None
        raw_error = data.get("error")
        if status is TaskStatus.FAILED:
            if isinstance(raw_error, dict):
                error = {"code": raw_error.get("code"), "message": raw_error.get("message", "")}
            else:
                error = {"code": None, "message": str(raw_error or "")}

        return TaskSnapshot(
            upstream_status=status_raw,
            status=status,
            result=result,
            usage=usage or None,
            error=error,
            event_id=f"seedance:{data.get('id', '')}:{status_raw}:{data.get('updated_at', '')}",
            raw=data,
        )

    # ------------------------------------------------------------------
    # 用量估算 / 透传改写
    # ------------------------------------------------------------------

    def estimate_usage(self, req: CanonicalTaskRequest) -> UsageEstimate:
        # 顶格预估（SPEC §3.2.1）：未指定的参数按最高档估。
        # 变量名契约见 SPEC §3.11.2（与 W3 求值上下文同名）。
        extra = req.extra or {}
        return UsageEstimate(
            amount_usd=Decimal("0"),  # 金额由 W3 PricingEvaluator 求值，恒 0 占位
            context={
                "duration": float(req.duration) if req.duration else 10.0,
                "resolution": req.resolution or "1080p",
                "mode": "default",
                "quantity": float(req.n),
                "usage_tokens": 0.0,  # 实收信号由 TaskSnapshot.usage 提供
                "generate_audio": 1.0 if req.generate_audio else 0.0,
                "has_image_input": 1.0 if req.image else 0.0,
                "service_tier": str(extra.get("service_tier") or "default"),
            },
        )

    def rewrite_callback_url(self, raw_body: bytes, cfg: Any) -> bytes:
        """透传形态摘除用户自带 callback_url（顶层字段）。"""
        try:
            body = json.loads(raw_body)
        except ValueError:  # JSONDecodeError / UnicodeDecodeError
            return raw_body  # 非 JSON 原样返回
        if not isinstance(body, dict):
            return raw_body
        body.pop("callback_url", None)
        return json.dumps(body, ensure_ascii=False).encode()


register(SeedanceAdapter())
