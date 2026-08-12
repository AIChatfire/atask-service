"""Callback Hub：接收上游 webhook。
入站 HMAC 验签 → Redis 去重 → 状态机推进 → 结算/通知事件。
"""

import hashlib
import hmac
import json
import logging

from fastapi import APIRouter, Request

from app.config import settings
from app.redis import K_CB, r
from app.schemas import ACTIVE, TERMINAL
from app.services import flow, statelog, statusmap, taskstore, upstream
from app.services.registry import registry

log = logging.getLogger("gateway.callback")
router = APIRouter()


def _verify(secret: str | None, sig_header: str, headers, raw: bytes) -> bool:
    if not secret:
        log.warning("callback without secret configured, skip verification")
        return True
    provided = headers.get(sig_header, "")
    provided = provided.removeprefix("sha256=").strip()
    if not provided:
        return False
    expected = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, provided)


@router.post("/callback/{biz}/{task_id}")
async def receive_callback(biz: str, task_id: str, request: Request):
    route = registry.get(biz)
    if route is None:
        return {"status": "ignored"}

    raw = await request.body()
    if not _verify(route.callback_secret, route.callback_sig_header, request.headers, raw):
        return JSONResponse_401()
    try:
        payload = json.loads(raw or b"{}")
    except json.JSONDecodeError:
        return {"status": "bad_payload"}

    # 去重：优先上游事件 ID，否则报文哈希
    event_id = request.headers.get("X-Event-Id") or hashlib.sha256(raw).hexdigest()
    if not await r.set(K_CB.format(biz=biz, event_id=event_id), "1", ex=settings.cb_dedup_ttl, nx=True):
        return {"status": "duplicate"}

    task = await taskstore.get(task_id)
    if not task:
        return {"status": "ignored"}          # 不认识的任务直接丢弃

    upstream_status = upstream.extract_path(payload, route.status_path)
    mapped = statusmap.map_status(route, upstream_status)

    if mapped is not None:
        await statelog.record_if_changed(task_id, mapped, detail="callback")

    if mapped is None:
        return {"status": "ignored_unknown_status"}   # 已记日志，待字典扩展
    if mapped in TERMINAL:
        await flow.finalize_task(
            task, mapped, payload,
            fail_reason=str(upstream.extract_path(payload, "error") or upstream.extract_path(payload, "fail_reason") or ""),
        )
    elif mapped in ACTIVE:
        await taskstore.patch_data(task_id, {"upstream_status": upstream_status}, status=mapped)

    return {"status": "ok"}


def JSONResponse_401():
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=401, content={"status": "invalid_signature"})
