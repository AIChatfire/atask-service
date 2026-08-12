"""上游回调接收 + 队列消费（SPEC §3.12.1 / 架构 §7.1/§13.5，简报 B §7）。

接收端点 ``POST /callbacks/{biz}/{provider}/{capability}``（固定前缀路由，
注册顺序先于 catch-all 的不变量由 W1 装配保证，SPEC §4.2）。处理顺序
**不可颠倒**（§7.1）：

原始字节读取 → 端点级防刷限流 → capability 校验（验签第 1 层）→
HMAC 框架（第 2 层，预留）→ adapter.parse_callback → 事件幂等去重 →
RPUSH 入队 → **立即 202**。业务处理（状态机/结算）全部异步——上游对
5xx/超时会重试，端点必须快。本端点不过 Bearer 中间件。

验签背景（架构 §7.1，简报 A §5：各上游签名机制均未确认）：

- **V1（kling 旧版）/V2（kling 3.0）**：回调是否带签名头未确认
  （3.0「Callback 协议」细节页未抓到，【建议验证】）；
- **V3（seedance/方舟）**：官方文档无回调签名——capability token 为唯一
  强制校验，HMAC 框架预留不强制。

消费侧 ``process_upstream_callback``：回调可能先于 tasks 行 commit 可见
（提交即回调），not-found 按 ``NOT_FOUND_RETRY_DELAYS`` 延迟重试后再丢弃
+告警——丢弃不丢正确性，由轮询兜底通道收敛（§4.3）。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
from typing import TYPE_CHECKING, Annotated, Any

import logfire
from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters import get_adapter
from app.db import get_session, get_session_factory
from app.redis_client import get_redis

if TYPE_CHECKING:
    from app.tasks.manager import TaskManager

router = APIRouter()

REPLAY_WINDOW_SECONDS = 300                       # HMAC 时间戳重放窗（±5min，§7.1）
NOT_FOUND_RETRY_DELAYS = [2, 5, 15, 30, 60]       # 回调先于 tasks 行 commit 的补偿窗口
CALLBACK_RATE_LIMIT_PER_MINUTE = 600              # 端点级防刷固定窗口（§8.3 端点级）
DEDUP_TTL_SECONDS = 86400                         # wh:seen:{event_id} TTL（SPEC §3.6）

QUEUE_KEY = "queue:upstream_callbacks"
PROCESSING_QUEUE_KEY = f"{QUEUE_KEY}:processing"

# ---------------------------------------------------------------------------
# 装配（SPEC §3.12.1 模块级 task_manager + set_task_manager，同 videos.py 模式；
# 由 app/worker.py 在构造 TaskManager 后注入）
# ---------------------------------------------------------------------------

task_manager: TaskManager | None = None
_session_factory: Any = None  # worker 注入的会话工厂；缺省回退 app.db.get_session_factory()


def set_task_manager(tm: TaskManager) -> None:
    global task_manager
    task_manager = tm


def set_session_factory(sf: Any) -> None:
    global _session_factory
    _session_factory = sf


def _open_session() -> AsyncSession:
    sf = _session_factory or get_session_factory()
    return sf()


# ---------------------------------------------------------------------------
# 接收端点
# ---------------------------------------------------------------------------


@router.post("/callbacks/{biz}/{provider}/{capability}")
async def recv_upstream_callback(
    biz: str,
    provider: str,
    capability: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    """处理顺序不可颠倒（§7.1）：验签 → 防重放 → 去重 → 入队 → 202。"""
    redis = await get_redis()

    # 0) 端点级防刷固定窗口（§8.3：防刷验签/反查 CPU；key 登记 SPEC §3.6）
    rl_key = f"rl:callback:{provider}"
    count = await redis.incr(rl_key)
    if count == 1:
        await redis.expire(rl_key, 60)
    if count > CALLBACK_RATE_LIMIT_PER_MINUTE:
        return Response(status_code=429)

    raw = await request.body()                        # 原始字节，勿先 json()

    # 1) capability 校验（§7.1 第 1 层，默认启用；先反查网关 task_id 再比对）
    if not await _verify_capability(redis, session, biz, provider, capability, raw):
        logfire.warning("callback rejected: bad capability", provider=provider, biz=biz)
        return Response(status_code=401)

    # 2) HMAC 框架（§7.1 第 2 层，预留：上游无签名头时由 capability 兜底）
    if not _verify_hmac_if_present(provider, request.headers, raw):
        logfire.warning("callback rejected: bad signature", provider=provider, biz=biz)
        return Response(status_code=401)

    # 3) 解析（适配器吃掉各上游差异）；坏报文 400——上游不应重试
    try:
        snapshot = get_adapter(provider).parse_callback(raw, dict(request.headers))
    except Exception:
        logfire.exception("callback parse failed", provider=provider, biz=biz)
        return Response(status_code=400)

    # 4) 事件幂等去重（§7.1：SET NX EX 86400；重复投递 200 幂等 ACK）
    if not await redis.set(
        f"wh:seen:{snapshot.event_id}", 1, nx=True, ex=DEDUP_TTL_SECONDS
    ):
        return Response(
            status_code=200,
            content='{"ok":true,"duplicate":true}',
            media_type="application/json",
        )

    # 5) 入队异步处理（驱动状态机+结算）→ 立即 202（上游重试友好）
    await redis.rpush(  # type: ignore[misc]  # redis-py 5.x stubs 历史噪音：异步方法返回 Awaitable|T 联合
        QUEUE_KEY,
        json.dumps(
            {
                "biz": biz,
                "provider": provider,
                "raw": raw.decode("utf-8", "replace"),
                "event_id": snapshot.event_id,
                "received_at": time.time(),
            }
        ),
    )
    return Response(status_code=202)


# ---------------------------------------------------------------------------
# 验签第 1 层：capability token（提交时注入的一次性随机路径段即凭证）
# ---------------------------------------------------------------------------


async def _verify_capability(
    redis: Any,
    session: AsyncSession,
    biz: str,
    provider: str,
    capability: str,
    raw: bytes,
) -> bool:
    """提交时注入的随机路径段即凭证（§7.1 第 1 层）。

    key 统一为 ``cb:cap:{网关task_id}``（W2 提交时写入，SPEC §3.6）；
    用 **GET 不用 GETDEL**——上游对同一任务多次回调（queued/running/succeeded），
    凭证不可被首次消费。任何一步异常一律判失败（fail-closed）。
    """
    try:
        task_id = await resolve_gateway_task_id(session, biz, provider, raw)
        if task_id is None:
            return False
        stored = await redis.get(f"cb:cap:{task_id}")
        return stored is not None and hmac.compare_digest(
            stored.encode(), capability.encode()
        )
    except Exception:
        logfire.exception("capability verify error", provider=provider, biz=biz)
        return False


async def resolve_gateway_task_id(
    session: AsyncSession, biz: str, provider: str, raw: bytes
) -> str | None:
    """回调体 → 网关 task_id，两条路径（§13.5 ``_resolve_gateway_task_id`` 语义）：

    ① 上游回显 ``external_task_id``/``external_id``（kling 两代，提交时双向
       关联 §3.3）——直接取；
    ② 不回显的上游（seedance/方舟回调体只有上游 ``cgt-`` 前缀 id）——反查索引
       **Redis 优先**（决策 A-2）：``GET tidx:{biz}:{upstream_task_id}``；miss 时
       兜底 SQL ``tasks`` 行（``platform LIKE 'gw\\_%'`` +
       ``private_data.upstream_task_id`` + ``submit_time`` 7d 窗口，submit_time
       有索引限定扫描），命中后回热 Redis。

    Redis 命中后仍回 tasks 表校验 ``platform LIKE 'gw\\_%'`` **双保险**
    （防脏索引串号，简报 C §四.9）。任何不回显 external_task_id 的新上游都
    走路径②。
    """
    del provider  # 反查以 (biz, upstream_task_id) 为键，provider 仅审计维度
    body = json.loads(raw)
    if not isinstance(body, dict):
        return None
    inner = body.get("data")
    if not isinstance(inner, dict):
        inner = body
    echoed = inner.get("external_task_id") or inner.get("external_id")
    if echoed:
        return str(echoed)                                        # 路径①
    upstream_id = inner.get("id") or inner.get("task_id")         # 路径②
    if not upstream_id:
        return None

    redis = await get_redis()
    tidx_key = f"tidx:{biz}:{upstream_id}"
    cached = await redis.get(tidx_key)
    if cached is not None:
        # 回表双保险：确认是网关自有行（§4.4）
        row = (
            await session.execute(
                text(
                    "SELECT task_id FROM tasks "
                    "WHERE task_id=:id AND platform LIKE 'gw\\_%'"
                ),
                {"id": cached},
            )
        ).first()
        return row.task_id if row else None

    # Redis miss：SQL 兜底（submit_time 索引 + 7d 窗口限定扫描，决策 A-2）
    from app.config import settings as _settings

    row = (
        await session.execute(
            text(
                "SELECT task_id FROM tasks "
                "WHERE platform LIKE 'gw\\_%' "
                "AND JSON_UNQUOTE(JSON_EXTRACT(private_data,"
                " '$.upstream_task_id')) = :u "
                "AND submit_time > :since "
                "ORDER BY submit_time DESC LIMIT 1"
            ),
            {"u": str(upstream_id),
             "since": int(time.time()) - _settings.upstream_index_ttl_seconds},
        )
    ).first()
    if row is None:
        return None
    # 命中回热 Redis（后续回调直接命中）
    await redis.set(tidx_key, row.task_id, ex=_settings.upstream_index_ttl_seconds)
    return str(row.task_id)


# ---------------------------------------------------------------------------
# 验签第 2 层：HMAC 框架（预留，上游协议确认后按 provider 注入密钥即启用）
# ---------------------------------------------------------------------------


def _verify_hmac_if_present(provider: str, headers: Any, raw: bytes) -> bool:
    """HMAC 框架（§7.1 第 2 层）：**必须对原始字节计算**（绝不能 parse→
    re-stringify，键序/空白/Unicode 转义差异必失败），时间戳进签名串
    （±300s 重放窗），``hmac.compare_digest`` 防时序攻击。

    支持两种形制：Stripe ``t={ts},v1={hex}`` 与 GitHub ``sha256={hex}``
    （时间戳取 ``X-Timestamp`` 头）。上游无签名头时返回 True（由 capability
    兜底，见模块 docstring 的 V1~V3 背景）；带了签名头但未配置密钥时
    fail-closed 拒绝并告警——说明上游协议已变，须补调研后配置密钥。
    """
    lower = {k.lower(): v for k, v in dict(headers).items()}
    sig = lower.get("x-signature") or lower.get("x-hub-signature-256")
    if not sig:
        return True                                   # 未启用：依赖 capability
    secret = _provider_secret(provider)
    if secret is None:
        logfire.warning(
            "callback signature header present but no secret configured",
            provider=provider,
        )
        return False

    # 解析签名头：Stripe 形制拆为键值对；GitHub 形制取 hex + X-Timestamp
    ts = lower.get("x-timestamp", "")
    presented = ""
    if "=" in sig and not sig.startswith("sha256="):
        parts = dict(
            item.split("=", 1) for item in sig.split(",") if "=" in item
        )
        ts = parts.get("t", ts)
        presented = parts.get("v1", "")
    else:
        presented = sig.removeprefix("sha256=")
    if not ts or not presented:
        return False
    try:
        if abs(time.time() - int(ts)) > REPLAY_WINDOW_SECONDS:
            return False                              # 重放窗拒绝
    except ValueError:
        return False
    expected = hmac.new(secret, f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, presented)


def _provider_secret(provider: str) -> bytes | None:
    """按 provider 取 HMAC 密钥（``UPSTREAM_CALLBACK_SECRET_{PROVIDER}``，
    Secret 注入）。默认 None = 该上游未确认支持签名，框架不启用。

    【建议验证】V1 kling 旧版与 V2 kling 3.0 是否支持签名头；V3 方舟已确认
    无签名（capability 为唯一强制校验）。
    """
    value = os.environ.get(f"UPSTREAM_CALLBACK_SECRET_{provider.upper()}")
    return value.encode() if value else None


# ---------------------------------------------------------------------------
# 消费侧 worker：驱动状态机（§7.1「驱动状态机」段）
# ---------------------------------------------------------------------------


async def process_upstream_callback(msg: dict[str, Any]) -> None:
    """队列消费者：parse → 反查任务 → ``TaskManager.transition``（§4.3 乐观锁
    收敛，channel='callback'）。

    回调可能先于 tasks 行可见（提交事务未 commit 上游即发首回调）：not-found
    按 ``NOT_FOUND_RETRY_DELAYS`` 延迟重试，仍无 → 丢弃+告警，由轮询兜底
    通道收敛（§4.3），不丢正确性。事务边界归 ``transition``（CAS 失败自行
    rollback 返回 False，竞态落败/乱序是正常路径非异常）。
    """
    tm = task_manager
    if tm is None:
        raise RuntimeError("callbacks.receiver.task_manager 未装配（set_task_manager）")
    provider, biz, raw = msg["provider"], msg["biz"], msg["raw"].encode()
    snapshot = get_adapter(provider).parse_callback(raw, {})
    async with _open_session() as session:
        for delay in [0, *NOT_FOUND_RETRY_DELAYS]:
            if delay:
                await asyncio.sleep(delay)
            task_id = await _lookup_task_id(session, biz, provider, raw)
            if task_id is not None:
                break
        else:
            logfire.warning(
                "callback dropped: task not found after retries",
                provider=provider,
                biz=biz,
                event_id=msg.get("event_id"),
            )
            return                                    # 轮询兜底（§4.3）收敛
        await tm.transition(session, task_id=task_id, snapshot=snapshot, channel="callback")


async def _lookup_task_id(
    session: AsyncSession, biz: str, provider: str, raw: bytes
) -> str | None:
    """反查并**确认 tasks 自有行已可见**（回调先于提交事务 commit 的竞态）。

    回显路径不能只信报文里的 task_id——行可能尚未 commit，须回表校验；
    索引路径由 ``resolve_gateway_task_id`` 自带双保险。
    """
    try:
        body = json.loads(raw)
        inner = body.get("data") if isinstance(body, dict) else None
        if not isinstance(inner, dict):
            inner = body if isinstance(body, dict) else {}
        echoed = inner.get("external_task_id") or inner.get("external_id")
    except json.JSONDecodeError:
        echoed = None
    if echoed:
        row = (
            await session.execute(
                text(
                    "SELECT task_id FROM tasks "
                    "WHERE task_id=:id AND platform LIKE 'gw\\_%'"
                ),
                {"id": str(echoed)},
            )
        ).first()
        return row.task_id if row else None
    return await resolve_gateway_task_id(session, biz, provider, raw)


class CallbackQueueConsumer:
    """可靠队列消费者（§8.5）：``BRPOPLPUSH queue:upstream_callbacks`` →
    ``queue:upstream_callbacks:processing``，处理成功后 ``LREM`` 摘除；
    副本死亡时 processing 队列中的消息不丢（恢复路径由 worker 装配负责）。
    处理失败的消息留在 processing 队列待恢复/排查，避免毒消息热循环。
    """

    def __init__(self, task_manager: TaskManager, session_factory: Any) -> None:
        # 装配便捷：注入即接线模块级依赖（process_upstream_callback 按 SPEC
        # 签名读模块级 task_manager / 会话工厂）。
        set_task_manager(task_manager)
        set_session_factory(session_factory)

    async def run_forever(self) -> None:
        redis = await get_redis()
        while True:
            try:
                payload = await redis.brpoplpush(QUEUE_KEY, PROCESSING_QUEUE_KEY, timeout=5)  # type: ignore[misc]  # redis-py 5.x stubs 历史噪音：异步方法返回 Awaitable|T 联合
                if payload is None:
                    continue
                await self._consume_one(redis, payload)
            except asyncio.CancelledError:
                raise                                   # 优雅停机（§8.5）由 worker 编排
            except Exception:
                logfire.exception("callback consumer loop error")
                await asyncio.sleep(5)

    async def _consume_one(self, redis: Any, payload: str) -> None:
        try:
            msg = json.loads(payload)
            await process_upstream_callback(msg)
        except Exception:
            # 留在 processing 队列：恢复路径可重放，轮询兜底保正确性
            logfire.exception("upstream callback consume failed")
            return
        await redis.lrem(PROCESSING_QUEUE_KEY, 1, payload)
