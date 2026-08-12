"""Kling 上游适配器（SPEC §3.13.1；事实依据：简报 A §3，架构 §3.3/§13.2）。

两代并存（代际由 model 名判断：``kling-v3*`` 或含 ``3.0``）：

- **旧版 v1 形态**：``POST /v1/videos/text2video|image2video``、查询
  ``GET /v1/videos/{action}/{task_id}``；``duration`` 为**字符串**；
  成功态拼写 ``succeed``；响应信封 ``{code, message, data:{task_id,
  task_status, task_result.videos[]}}``。
- **3.0 形态**：``POST /text-to-video/kling-3.0 | /image-to-video/kling-3.0``
  （settings/options 信封）；成功态 ``succeeded``；查询 ``GET /tasks?task_ids=``，
  实收信号 ``billing[].amount``。

鉴权：AK/SK 签 HS256 JWT（payload ``{iss, exp=now+1800, nbf=now-5}``），
进程内缓存（key 含 AK，过期前 60s 重签）；也支持静态 Bearer Key
（``auth_type == "bearer_key"``）。

**幂等提交语义**：上游以 ``external_task_id``（= 网关 task_id）作为单用户下
唯一键；网络抖动/超时后的重发携带同一 ``external_task_id`` 与同一报文体，
上游按唯一约束去重或拒绝重复（旧版支持按 external_task_id 反查），不会产生
第二个计费任务。网关层另有 Idempotency-Key guard（W1）兜底。
"""

from __future__ import annotations

import json
import os
import time as _time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, ClassVar

import httpx
import jwt

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

__all__ = ["KlingAdapter"]

# 旧版 v1 原生 action → 路径段（简报 A §3a）
_LEGACY_ACTIONS: dict[str, str] = {
    "text2video": "text2video",
    "image2video": "image2video",
}

# new-api 动作枚举（W1 推导）→ 旧版 v1 原生 action。
# textGenerate=文生视频；firstTailGenerate（首尾帧）/referenceGenerate（参考图）/
# generate（带图）均走 image2video。
_NEWAPI_ACTION_MAP: dict[str, str] = {
    "generate": "image2video",
    "textGenerate": "text2video",
    "firstTailGenerate": "image2video",
    "referenceGenerate": "image2video",
    "remixGenerate": "image2video",
}


def _retry_after(resp: httpx.Response) -> float | None:
    raw = resp.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _check_http(resp: httpx.Response, provider: str) -> None:
    """错误分级（SPEC §3.2.3 / §8.2）：

    - 429 → ``UpstreamRateLimitError``（消费 Retry-After，不计熔断，可重试）；
    - 其他 4xx → ``UpstreamBizError``（绝不重试，默认计熔断）；
    - 5xx → ``httpx.HTTPStatusError`` 向上抛（调用方 tenacity 重试 + 计熔断）；
    - 超时/传输异常不捕获，原样上抛（可重试）。
    """
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
    try:
        body = resp.json()
    except json.JSONDecodeError:
        body = None
    if isinstance(body, dict):
        message = str(body.get("message") or body.get("error") or message)
    raise UpstreamBizError(message, code=resp.status_code)


class KlingAdapter:
    """kling 旧版 v1 + 3.0 两代并存适配器（代际由 model 名判断，对外接口不变）。

    扩展点（V6）：kling 3.0 支持批量查询 ``GET /tasks?task_ids=a,b,c``（逗号
    分隔，一次多任务）。轮询 worker 按 biz 分组后可走 ``poll_batch`` 显著降
    QPS；当前 ``poll()`` 逐任务查询，协议级批量方法留待 W2/W5 协商后加入
    （可选方法，不改既有 ``poll`` 契约）。
    """

    name: ClassVar[str] = "kling"
    callback_capability: ClassVar[bool] = True
    echoes_external_task_id: ClassVar[bool] = True

    _STATUS_MAP: ClassVar[dict[str, TaskStatus]] = {
        # 旧版 'succeed' 与 3.0 'succeeded' 拼写差异在此吃掉
        "submitted": TaskStatus.QUEUED,
        "processing": TaskStatus.RUNNING,
        "succeed": TaskStatus.SUCCEEDED,
        "succeeded": TaskStatus.SUCCEEDED,
        "failed": TaskStatus.FAILED,
    }

    # JWT 进程内缓存：AK -> (token, exp_epoch)。key 必含 AK（SPEC §3.2.1）。
    _jwt_cache: ClassVar[dict[str, tuple[str, int]]] = {}

    # ------------------------------------------------------------------
    # 状态映射 / 鉴权
    # ------------------------------------------------------------------

    def map_status(self, upstream_status: str) -> TaskStatus:
        # 未知状态映射为 RUNNING（不推进终态，等下一轮）
        return self._STATUS_MAP.get(upstream_status, TaskStatus.RUNNING)

    @staticmethod
    def _resolve_credentials(cfg: Any) -> tuple[str, str]:
        """按 auth_secret_ref 解析 AK/SK（SPEC §3.1 例外条款允许直读 os.environ）。

        支持传入 ``SubmitContext``（使用其 secrets 映射，key 为 ``ak``/``sk``，
        兼容大写）或 biz 配置对象（``auth_secret_ref`` 指向
        ``{ref}_AK`` / ``{ref}_SK`` 环境变量）。
        """
        if isinstance(cfg, SubmitContext):
            secrets = {k.lower(): v for k, v in cfg.secrets.items()}
            ak = secrets.get("ak")
            sk = secrets.get("sk")
            if ak is None or sk is None:
                raise UpstreamBizError("kling credentials missing in SubmitContext.secrets")
            return ak, sk
        ref = cfg.auth_secret_ref
        return os.environ[f"{ref}_AK"], os.environ[f"{ref}_SK"]

    def auth_headers(self, cfg: Any) -> Mapping[str, str]:
        auth_type = getattr(cfg, "auth_type", "aksk_jwt")
        if auth_type == "bearer_key":
            if isinstance(cfg, SubmitContext):
                key = cfg.secrets.get("api_key") or cfg.secrets.get("API_KEY", "")
            else:
                key = os.environ[cfg.auth_secret_ref]
            return {"Authorization": f"Bearer {key}"}
        ak, sk = self._resolve_credentials(cfg)
        now = int(_time.time())
        cached = self._jwt_cache.get(ak)
        if cached and cached[1] - 60 > now:  # 未进入过期前 60s 窗口，直接复用
            return {"Authorization": f"Bearer {cached[0]}"}
        exp = now + 1800
        token = jwt.encode({"iss": ak, "exp": exp, "nbf": now - 5}, sk, algorithm="HS256")
        self._jwt_cache[ak] = (token, exp)
        return {"Authorization": f"Bearer {token}"}

    # ------------------------------------------------------------------
    # 代际判断与路径
    # ------------------------------------------------------------------

    @staticmethod
    def _is_v3(model: str) -> bool:
        return model.startswith("kling-v3") or "3.0" in model

    @staticmethod
    def _legacy_action(action: str, image: str | None = None) -> str:
        """内部/new-api 动作 → 旧版 v1 原生 action（text2video/image2video）。"""
        if action in _LEGACY_ACTIONS:
            return action
        if action in _NEWAPI_ACTION_MAP:
            return _NEWAPI_ACTION_MAP[action]
        # 兜底：有图走 image2video，否则 text2video
        return "image2video" if image else "text2video"

    # ------------------------------------------------------------------
    # 提交 / 轮询 / 回调
    # ------------------------------------------------------------------

    async def submit(self, req: CanonicalTaskRequest, ctx: SubmitContext) -> SubmitResult:
        base = ctx.upstream_base_url.rstrip("/")
        if self._is_v3(req.model):
            action = (
                "text" if self._legacy_action(req.action, req.image) == "text2video" else "image"
            )
            path = f"{action}-to-video/kling-3.0"
            settings: dict[str, Any] = {}
            if req.resolution:
                settings["resolution"] = req.resolution
            if req.duration:
                settings["duration"] = req.duration
            if req.generate_audio:
                settings["audio"] = "native"
            body: dict[str, Any] = {
                "prompt": req.prompt,
                "settings": settings,
                "options": {
                    "callback_url": ctx.gateway_callback_url,
                    "external_task_id": ctx.task_id,
                },
            }
            if req.image:
                body["image"] = req.image
            if req.extra:
                body.update(req.extra)
        else:
            path = f"v1/videos/{self._legacy_action(req.action, req.image)}"
            body = {
                "model_name": req.model,
                "prompt": req.prompt,
                "mode": req.mode or "std",
                "duration": str(int(req.duration or 5)),  # 旧版 duration 是字符串！
                "callback_url": ctx.gateway_callback_url,
                "external_task_id": ctx.task_id,
            }
            if req.image:
                body["image"] = req.image
            if req.extra:
                body.update(req.extra)
        resp = await upstream_client().post(
            f"{base}/{path}", json=body, headers=self.auth_headers(ctx)
        )
        _check_http(resp, self.name)
        data = resp.json()
        code = data.get("code", 0)
        if code != 0:  # 信封 code != 0 → 业务错误（不重试）
            raise UpstreamBizError(data.get("message", "kling submit failed"), code=code)
        inner = data.get("data") or {}
        upstream_task_id = inner.get("task_id") or inner.get("id")
        if not upstream_task_id:
            raise UpstreamBizError("kling submit response missing task id", code=code)
        return SubmitResult(upstream_task_id=str(upstream_task_id), raw=data)

    async def poll(self, upstream_task_id: str, ctx: SubmitContext) -> TaskSnapshot:
        base = ctx.upstream_base_url.rstrip("/")
        action = self._legacy_action(ctx.action)
        resp = await upstream_client().get(
            f"{base}/v1/videos/{action}/{upstream_task_id}",
            headers=self.auth_headers(ctx),
        )
        _check_http(resp, self.name)
        data = resp.json()
        code = data.get("code", 0)
        if code != 0:
            raise UpstreamBizError(data.get("message", "kling poll failed"), code=code)
        return self._parse_envelope(data)

    def parse_callback(self, raw_body: bytes, headers: Mapping[str, str]) -> TaskSnapshot:
        # 回调 schema V1 未确认（简报 A §5）：按行业惯例「回调体 ≈ 任务查询
        # 响应」复用信封解析，字段缺失容错；验签不在此做（W4 职责）。
        try:
            data = json.loads(raw_body)
        except ValueError as exc:  # JSONDecodeError / UnicodeDecodeError
            raise ValueError(f"kling callback body is not JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError("kling callback body is not a JSON object")
        return self._parse_envelope(data)

    # ------------------------------------------------------------------
    # 信封解析（poll 与 parse_callback 共用；v1/v3 字段兼容）
    # ------------------------------------------------------------------

    def _parse_envelope(self, data: dict[str, Any]) -> TaskSnapshot:
        inner: Any = data.get("data") or {}
        if isinstance(inner, list):  # v3 GET /tasks?task_ids= 批量响应取首项
            inner = inner[0] if inner else {}
        if not isinstance(inner, dict):
            raise ValueError("kling response data is not an object")
        status_raw = str(inner.get("task_status") or inner.get("status") or "")
        status = self.map_status(status_raw)
        videos = (inner.get("task_result") or {}).get("videos") or inner.get("outputs") or []
        first = videos[0] if videos and isinstance(videos[0], dict) else {}
        billing = inner.get("billing") or []

        result: dict[str, Any] | None = None
        if status is TaskStatus.SUCCEEDED and first.get("url"):
            result = {"url": first["url"]}
            if first.get("duration") is not None:
                result["duration"] = float(first["duration"])

        usage: dict[str, Any] = {}
        if first.get("duration") is not None:
            usage["actual_duration"] = float(first["duration"])
        if billing and isinstance(billing[0], dict) and billing[0].get("amount") is not None:
            usage["upstream_amount"] = str(billing[0]["amount"])  # 金额字符串防浮点

        error: dict[str, Any] | None = None
        if status is TaskStatus.FAILED:
            error = {
                "code": inner.get("code") or data.get("code"),
                "message": inner.get("task_status_msg") or data.get("message") or "",
            }

        upstream_id = inner.get("task_id") or inner.get("id") or ""
        updated = inner.get("updated_at") or inner.get("update_time") or ""
        return TaskSnapshot(
            upstream_status=status_raw,
            status=status,
            result=result,
            usage=usage or None,
            error=error,
            event_id=f"kling:{upstream_id}:{status_raw}:{updated}",
            raw=data,
        )

    # ------------------------------------------------------------------
    # 用量估算 / 透传改写
    # ------------------------------------------------------------------

    def estimate_usage(self, req: CanonicalTaskRequest) -> UsageEstimate:
        # 顶格预估（SPEC §3.2.1）：未指定的参数按最高档估。
        # 变量名契约见 SPEC §3.11.2（与 W3 求值上下文同名）。
        return UsageEstimate(
            amount_usd=Decimal("0"),  # 金额由 W3 PricingEvaluator 求值，恒 0 占位
            context={
                "duration": float(req.duration) if req.duration else 10.0,
                "resolution": req.resolution or "1080p",
                "mode": req.mode or "pro",
                "quantity": float(req.n),
                "usage_tokens": 0.0,
                "generate_audio": 1.0 if req.generate_audio else 0.0,
                "has_image_input": 1.0 if req.image else 0.0,
                "service_tier": "default",
            },
        )

    def rewrite_callback_url(self, raw_body: bytes, cfg: Any) -> bytes:
        """透传形态摘除用户自带 callback_url（顶层与 v3 options 信封）。"""
        try:
            body = json.loads(raw_body)
        except ValueError:  # JSONDecodeError / UnicodeDecodeError
            return raw_body  # 非 JSON 原样返回
        if not isinstance(body, dict):
            return raw_body
        body.pop("callback_url", None)
        if isinstance(body.get("options"), dict):
            body["options"].pop("callback_url", None)
        return json.dumps(body, ensure_ascii=False).encode()


register(KlingAdapter())
