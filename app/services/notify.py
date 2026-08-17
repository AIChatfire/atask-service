"""推送结果到用户的 callback_url，HMAC-SHA256 签名。
至少一次投递（失败由 events 重投），用户侧按 task_id+status 去重。
"""

import hashlib
import hmac
import json
import time

import httpx

from app.config import settings
from app.logging import log


class NotifyError(Exception):
    pass


def sign(body: bytes) -> tuple[int, str]:
    ts = int(time.time())
    mac = hmac.new(settings.callback_sign_secret.encode(), f"{ts}.".encode() + body, hashlib.sha256)
    return ts, mac.hexdigest()


async def push(url: str, payload: dict) -> None:
    if not url:
        return
    body = json.dumps(payload, ensure_ascii=False).encode()
    ts, sig = sign(body)
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            url,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Gateway-Signature": f"t={ts},v1={sig}",
            },
        )
    if resp.status_code >= 300:
        raise NotifyError(f"notify {url}: {resp.status_code}")
    log.debug("notify pushed: {} -> {}", url, resp.status_code)
