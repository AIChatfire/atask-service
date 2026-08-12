"""用户回调可靠投递（SPEC §3.12.2 / 架构 §7.2/§7.3/§13.6，简报 B §7）。

投递状态机：``pending → delivering(lease) → delivered | dead``，
``dead → pending`` 仅经人工重放入口（``replay_dead_delivery``）。
**零自有表（决策 A-3）**：队列载体为 Redis 延迟队列（``app/redis_queue.py``
``dlv`` 命名空间：``dlv:due`` ZSET + ``dlv:{id}`` HASH + ``dlv:lease`` ZSET +
``dlv:dead`` ZSET），语义与原 ``gateway_callback_deliveries`` 表完全一致。

- **至少一次投递** + 接收方按 envelope 稳定 ``id``（= delivery id，重试不变）
  幂等去重，是业界共识（简报 B §7）；
- **领取**：Lua 原子脚本（取到期项 → 置 delivering+lease → 移入 ``dlv:lease``
  ZSET；调度器每轮先回收过期 lease 回 due），多副本互斥、线性分摊（§7.3）；
  lease（默认 60s）防副本死亡卡死；
- **退避**：``[1m, 5m, 30m, 2h, 6h]`` + 0–30s jitter，总窗口约 3 天
  （对齐 Stripe 系实践）；attempts 超上限 → dead + 告警；
- **响应语义**：2xx 已收（停重试）；``410 Gone`` 与其余 4xx（除 408/429）
  为终态进死信；5xx/超时退避重排；
- **域级熔断** ``user-callback:{domain}``（§8.2）：open 时直接重排不投递
  （不计 attempts——熔断不是接收方响应），避免坏地址拖垮全队列；投递结果
  在本模块接 ``on_success``/``on_failure`` 记账——收到 HTTP 响应（含 4xx）
  计成功，5xx/超时/传输错误计失败；
- **签名**：``X-Signature: t={ts},v1={hmac}``（Stripe 形制，**对原始字节**
  计算，轮换期双密钥并存携带 ``v1_old``）+ ``X-Delivery-Id`` = delivery id。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import random
import time
from typing import Any
from urllib.parse import urlparse

import httpx
import logfire

from app import redis_queue
from app.config import settings
from app.http_clients import delivery_client
from app.redis_client import get_redis

NS = "dlv"  # Redis 队列命名空间（dlv:due / dlv:{id} / dlv:lease / dlv:dead）

BACKOFF_SECONDS: list[int] = list(settings.delivery_backoff_seconds)  # §7.2: 约 3 天窗口
LEASE_SECONDS: int = settings.delivery_lease_seconds
JITTER_SECONDS = 30                                   # 重排抖动 0–30s（§7.2）
CIRCUIT_OPEN_RESCHEDULE_SECONDS = 30                  # 域级熔断 open 时的重排间隔
BATCH_SIZE = 50                                       # 单批领取上限
DELIVERY_TIMEOUT_SECONDS = 10.0                       # 投递超时（§7.3）
WORKER_ID = os.getenv("HOSTNAME", "dispatcher-0")


def sign_headers(user_id: int, raw_body: bytes, delivery_id: str) -> dict[str, str]:
    """``X-Signature: t={ts},v1={hmac_hex}``（Stripe 形制，§13.6）。

    - HMAC-SHA256 over ``"{ts}.{raw_body}"``，**对原始字节**计算；
    - 轮换期双密钥并存：配置旧密钥时头中同时携带 ``v1`` 与 ``v1_old``；
    - ``X-Delivery-Id`` 用 delivery/event id（对外事件 ID，重试不变），
      供接收方幂等去重与对账；
    - ``user_id`` 为每用户独立密钥演进预留（§7.2：secret 每用户独立，
      当前 V1 用全局密钥，签名串不含 user_id 故参数暂不参与计算）。
    """
    del user_id
    ts = int(time.time())
    signed = f"{ts}.".encode() + raw_body
    cur = settings.callback_signing_secret_current.encode()
    v1 = hmac.new(cur, signed, hashlib.sha256).hexdigest()
    sig = f"t={ts},v1={v1}"
    old = settings.callback_signing_secret_old
    if old:
        v1_old = hmac.new(old.encode(), signed, hashlib.sha256).hexdigest()
        sig += f",v1_old={v1_old}"
    return {"X-Signature": sig, "X-Delivery-Id": delivery_id}


async def enqueue_delivery(
    *,
    delivery_id: str,
    task_id: str,
    user_id: int,
    url: str,
    event_type: str,
    envelope: dict[str, Any],
) -> None:
    """生产侧入队（TaskManager 终态副作用调用；payload 存紧凑 JSON 原始字节）。

    ``delivery_id`` 即对外事件 ID（evt_+ulid，重试不变，接收方幂等依据）。
    """
    redis = await get_redis()
    await redis_queue.enqueue(
        redis,
        NS,
        delivery_id,
        {
            "task_id": task_id,
            "user_id": user_id,
            "url": url,
            "event_type": event_type,
            "payload": json.dumps(envelope, ensure_ascii=False),
        },
    )


async def replay_dead_delivery(delivery_id: str) -> bool:
    """死信人工重放入口（§7.2 状态图 ``dead → pending``，控制台/API 调用）。

    仅 ``dead`` 状态可重放（Lua 原子条件，防并发竞态）：重置 attempts 并
    重新进入 due 队列。返回是否命中。
    """
    redis = await get_redis()
    ok = await redis_queue.replay_dead(redis, NS, delivery_id)
    if ok:
        logfire.info("dead delivery replayed", delivery_id=delivery_id)
    return ok


class DeliveryDispatcher:
    """至少一次投递 + 接收方按 event.id 幂等（§7.2）；lease 防副本死亡卡死（§7.3）。"""

    async def run_forever(self) -> None:
        while True:
            try:
                claimed = await self._claim_batch(BATCH_SIZE)
                if claimed:
                    await asyncio.gather(
                        *(self._deliver(d) for d in claimed), return_exceptions=True
                    )
                else:
                    await asyncio.sleep(2)
            except asyncio.CancelledError:
                raise                                   # 优雅停机（§8.5）由 worker 编排
            except Exception:
                logfire.exception("dispatcher loop error")
                await asyncio.sleep(5)

    async def _claim_batch(self, limit: int) -> list[dict[str, Any]]:
        """Lua 原子领取（先回收过期 lease → 取到期项置 delivering+lease）。

        多副本互斥语义与原 MySQL ``FOR UPDATE SKIP LOCKED`` 等价（§7.3）。
        """
        redis = await get_redis()
        await redis_queue.reclaim_expired_leases(redis, NS, limit=limit)
        ids = await redis_queue.claim(redis, NS, limit=limit, lease_seconds=LEASE_SECONDS)
        items: list[dict[str, Any]] = []
        for delivery_id in ids:
            data = await redis_queue.get_item(redis, NS, delivery_id)
            if data is None:
                continue                          # 并发下条目已被收口，跳过
            items.append(
                {
                    "id": delivery_id,
                    "task_id": data.get("task_id", ""),
                    "user_id": int(data.get("user_id") or 0),
                    "url": data.get("url", ""),
                    "event_type": data.get("event_type", ""),
                    "payload_json": data.get("payload", ""),
                    "attempts": int(data.get("attempts") or 0),
                }
            )
        return items

    async def _deliver(self, d: dict[str, Any]) -> None:
        redis = await get_redis()
        # 域级熔断（§8.2）：open 时直接重排不投递（§3.12.2），不计 attempts——
        # 熔断期间的失败不是接收方响应，不应消耗退避预算或进死信。
        domain = urlparse(d["url"]).netloc
        if await _domain_circuit_open(domain):
            await redis_queue.reschedule(
                redis, NS, d["id"],
                delay_seconds=CIRCUIT_OPEN_RESCHEDULE_SECONDS
                + random.uniform(0, JITTER_SECONDS),
            )
            return

        # 原始字节签名（§7.2）：payload 以紧凑 JSON 文本存储，签名与投递
        # 共用同一份字节。
        payload = d["payload_json"]
        raw_body = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
        headers = {
            "Content-Type": "application/json",
            **sign_headers(d["user_id"], raw_body, d["id"]),
        }
        status_code: int | None = None
        try:
            resp = await delivery_client().post(
                d["url"], content=raw_body, headers=headers,
                timeout=DELIVERY_TIMEOUT_SECONDS,
            )
            status_code = resp.status_code
            # 域级熔断记账（§8.2）：5xx 计失败；收到 HTTP 响应（含 4xx/429——
            # 接收方在线只是拒绝或限速）计成功，口径与 upstream 熔断一致
            if status_code >= 500:
                await _domain_on_failure(domain)
            else:
                await _domain_on_success(domain)
            if status_code < 300:
                await redis_queue.mark_done(redis, NS, d["id"])
                return                                  # 2xx 已收：停重试
            if status_code == 410 or (
                400 <= status_code < 500 and status_code not in (408, 429)
            ):
                # 410 Gone = 接收方约定「不再投递」；其余 4xx（除 408/429）终态
                await redis_queue.dead_letter(
                    redis, NS, d["id"], reason="receiver terminal",
                    fields={"last_status_code": status_code},
                )
                logfire.warning(
                    "delivery dead-lettered by receiver",
                    delivery_id=d["id"],
                    task_id=d["task_id"],
                    status_code=status_code,
                )
                return
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            await _domain_on_failure(domain)
            logfire.info(
                "delivery transport error", delivery_id=d["id"], error=str(exc)
            )

        # 5xx / 408 / 429 / 超时：指数退避重排；attempts 超上限 → 死信+告警
        attempts = d["attempts"] + 1
        fields = {"last_status_code": status_code} if status_code is not None else None
        if attempts > len(BACKOFF_SECONDS):
            await redis_queue.dead_letter(
                redis, NS, d["id"], reason="max attempts", attempts=attempts,
                fields=fields,
            )
            logfire.warning(
                "delivery dead-lettered", delivery_id=d["id"], task_id=d["task_id"]
            )
        else:
            delay = BACKOFF_SECONDS[attempts - 1] + random.uniform(0, JITTER_SECONDS)
            await redis_queue.reschedule(
                redis, NS, d["id"], delay_seconds=delay, attempts=attempts,
                fields=fields,
            )


async def _domain_circuit_open(domain: str) -> bool:
    """域级熔断 ``user-callback:{domain}``（§8.2）只读检查。

    熔断状态由 W1 ``CircuitBreaker`` 维护；本模块在投递结果处经
    ``_domain_on_success``/``_domain_on_failure`` 记账。W1 未装配（并行
    开发期）时降级放行。
    """
    try:
        from app.middleware import circuit_breaker
    except ImportError:
        return False
    return not await circuit_breaker.allow(f"user-callback:{domain}")


async def _domain_on_success(domain: str) -> None:
    """投递拿到 HTTP 响应（2xx/4xx）→ 域级熔断计成功（闭环/释放探针）。

    记账失败绝不阻断投递主流程（best-effort，熔断状态由后续投递继续收敛）。
    """
    try:
        from app.middleware import circuit_breaker
        await circuit_breaker.on_success(f"user-callback:{domain}")
    except Exception:
        logfire.exception("domain circuit on_success failed", domain=domain)


async def _domain_on_failure(domain: str) -> None:
    """投递 5xx/超时/传输错误 → 域级熔断计失败（429 绝不走这里，§8.2）。"""
    try:
        from app.middleware import circuit_breaker
        await circuit_breaker.on_failure(f"user-callback:{domain}")
    except Exception:
        logfire.exception("domain circuit on_failure failed", domain=domain)
