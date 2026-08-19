"""keypool-service 实现（https://github.com/AIChatfire/keypool-service）。

服务级 Bearer token 认证（GW_KEY_SVC_TOKEN）；统一响应包络
``{"code":0,"message":"ok","data":...}``。

- ``POST {BASE}/v1/keys/select``：按 ``group+model`` 选渠道与 key（或
  ``channel_id`` 直达）；``include_channel=true`` 让响应附带渠道全量元数据
  （model_mapping/param_override/header_override/status_code_mapping/
  setting.proxy/openai_organization/base_url）——网关零配置消费渠道差异。
  ``retry`` 为**服务侧内部重试深度**（选 key 失败时 keypool 内部换 key 重试
  的次数）：默认 1 即可——网关自身另有租约失败重试与探测重投，两层语义
  互不冲突，该值对网关无影响。
- ``POST {BASE}/v1/keys/report``：上报调用结果驱动自动禁启；
  ``Idempotency-Key`` 头幂等，重复上报 409 视为成功；fire-and-forget。

错误包络 code：40001=无可用 key（503，data.retry_after_ms 给出建议）、
40002=渠道不存在（404）、40010=参数错误（400）、40100=未鉴权（401）。
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from app.config import settings
from app.logging import log
from app.schemas import KeyLease
from app.services import httpc
from app.services.providers import KeyLeaseError

#: select 内部重试深度（keypool 服务侧换 key 重试次数；网关自身有租约失败
#: 重试与探测重投，默认 1 对网关无影响，见模块 docstring）
_SELECT_RETRY = 1

# fire-and-forget 上报任务引用集（防 GC 提前回收，RUF006）
_BACKGROUND: set[asyncio.Task] = set()


class KeypoolProvider:
    def _headers(self, extra: dict | None = None) -> dict:
        return {"Authorization": f"Bearer {settings.key_svc_token}", **(extra or {})}

    @staticmethod
    def _unwrap(resp_json: dict, ctx: str) -> dict:
        """解统一包络；code != 0 按错误码语义抛 KeyLeaseError。"""
        code = resp_json.get("code", 0)
        if code == 0:
            return resp_json.get("data") or {}
        message = str(resp_json.get("message") or "keypool error")
        if code == 40001:
            raw_ms = (resp_json.get("data") or {}).get("retry_after_ms")
            retry_ms = int(raw_ms) if isinstance(raw_ms, int | float) else None
            raise KeyLeaseError(
                f"no available key ({ctx}); retry_after_ms={retry_ms}",
                retry_after_ms=retry_ms,
            )
        raise KeyLeaseError(f"keypool {ctx} failed: code={code} {message}")

    async def lease(self, biz: str, model: str = "", key_id: int | None = None,
                    group: str = "") -> KeyLease:
        """取一个可用 key 与渠道覆盖配置。

        ``key_id`` 非空 → ``channel_id`` 直达（探测时钉回原渠道）；否则按
        ``group+model`` 经 abilities 分档加权选择。``group`` 缺省取
        ``GW_KEY_GROUP``（统一分组，默认 "keypool"）。
        """
        body: dict[str, Any] = {"retry": _SELECT_RETRY, "include_channel": True}
        if key_id:
            body["channel_id"] = key_id
        else:
            body["group"] = group or settings.key_group
            body["model"] = model
        # 共享客户端（连接池 keep-alive）：每次新建 client 会让 select 多付
        # 一次 TCP+TLS 握手，preflight 提交链路会逐请求放大该开销
        client = httpc.shared_client(timeout=settings.http_timeout)
        resp = await client.post(
            f"{settings.key_svc_url}/v1/keys/select",
            headers=self._headers(),
            json=body,
        )
        if resp.status_code == 401:
            raise KeyLeaseError("keypool auth failed (check GW_KEY_SVC_TOKEN)")
        if resp.status_code != 200:
            # 错误响应仍是统一包络（如 503 + code=40001 无可用 key）——解出
            # data.retry_after_ms 作为结构化退避 hint（探测重投 / Retry-After 头）
            retry_ms: int | None = None
            try:
                err_env = resp.json()
            except Exception:
                err_env = {}
            if isinstance(err_env, dict):
                raw_ms = (err_env.get("data") or {}).get("retry_after_ms")
                if isinstance(raw_ms, int | float):
                    retry_ms = int(raw_ms)
            raise KeyLeaseError(
                f"keypool select failed: {resp.status_code} {resp.text[:200]}",
                retry_after_ms=retry_ms,
            )
        data = self._unwrap(resp.json(), "select")

        key = data.get("key") or ""
        if not key:
            raise KeyLeaseError(f"keypool returned no key: {str(data)[:200]}")
        channel = data.get("channel") or {}
        log.debug(
            "key lease acquired: biz={} channel_id={} key_index={} base_url={}",
            biz, data.get("channel_id") or channel.get("id"), data.get("key_index"),
            data.get("base_url") or channel.get("base_url"),
        )
        setting = channel.get("setting") or {}
        # header_override 里允许嵌套配置块（upstream 网关配置，见 registry）：
        # 装配 KeyLease 时剥离一切非标量值，保证下游拿到的是纯 HTTP 头
        header_override = {
            str(k): str(v)
            for k, v in (channel.get("header_override") or {}).items()
            if not isinstance(v, dict | list)
        }
        return KeyLease(
            key_id=int(data.get("channel_id") or channel.get("id") or 0),
            key_index=int(data.get("key_index") or 0),
            key=key,
            base_url=data.get("base_url") or channel.get("base_url") or None,
            epoch=str(data.get("epoch") or ""),
            lease_id=str(data.get("lease_id") or ""),
            model_mapping=dict(channel.get("model_mapping") or {}),
            header_override=header_override,
            param_override=dict(channel.get("param_override") or {}),
            status_code_mapping=dict(channel.get("status_code_mapping") or {}),
            proxy=setting.get("proxy") or None,
            openai_organization=channel.get("openai_organization") or None,
            channel=channel,
        )

    async def report(self, key: KeyLease, ok: bool, status_code: int = 0,
                     latency_ms: int = 0, error: str = "",
                     usage: dict[str, Any] | None = None) -> None:
        """fire-and-forget：上报失败不阻塞主链路；重复上报（409）视为成功。"""

        async def _send() -> None:
            body: dict[str, Any] = {
                "channel_id": key.key_id,
                "key_index": key.key_index,
                "success": ok,
            }
            if key.epoch:
                body["epoch"] = key.epoch
            if key.lease_id:
                body["lease_id"] = key.lease_id
            if not ok:
                body["status_code"] = status_code
                body["error_message"] = error[:200]
            if usage:
                body["usage"] = usage
            try:
                client = httpc.shared_client(timeout=5)
                await client.post(
                    f"{settings.key_svc_url}/v1/keys/report",
                    headers=self._headers({"Idempotency-Key": uuid.uuid4().hex}),
                    json=body,
                )
            except Exception:
                log.opt(exception=True).debug("key report failed")

        task = asyncio.create_task(_send())
        _BACKGROUND.add(task)
        task.add_done_callback(_BACKGROUND.discard)
