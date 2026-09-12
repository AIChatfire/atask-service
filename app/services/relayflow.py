"""``/batch`` 中继链路的生命周期：受理 / 视图 / 取消 / worker 提交。

本模块是 ADR-010 换向后的**唯一链路**（旧凭证与计费协同链路已整体删除）：

- 鉴权不做内省：用户 token 原样透传上游（本地只算 ``sha256`` 用于限流/幂等/并发键）；
- 计费零资金动作：网关不做任何预扣 / 结算 / 解冻，``tasks.data`` 不写任何计费字段
  （ADR-010 §3）；
- 寻址按请求：``X-Upstream-Base-Url`` 头优先、配置回退，白名单 fail-closed
  （``app.services.upstream_addr``），没有渠道元数据；
- 交互按 new-api 约定硬编码：提交 ``POST {base}{path}``、探测/取消
  ``{base}{path}/{upstream_task_id}``（``app.services.relay``）。

## 安全红线

用户原始 token **只存 Redis 会话**（``app.services.tokensession``），绝不落 tasks
表、绝不进日志、绝不出现在任何响应里。``tasks.data.token_hash`` 是本地身份替身
（限流/并发/幂等键），不是凭证。

## 受理后的形态

受理后的上游提交交 worker（``queue.publish_batch_submit`` → 本模块
``submit_batch_task``），视图由客户端轮询驱动（GET 时按需探测），
后台 sweep 补上「客户端不轮询」的收敛——这正是 ADR-010「探测与回调的取舍
改为固定策略」的落地形态。
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app import queue
from app.config import settings
from app.deps import ratelimit
from app.deps.identity import extract_token
from app.logging import log
from app.redis import K_BATCH_SWEEP_LOCK, LUA_CAS_DELETE, r
from app.schemas import (
    ACTIVE,
    CANCELED,
    FAILURE,
    IN_PROGRESS,
    QUEUED,
    SUBMITTED,
    SUCCESS,
    TERMINAL,
)
from app.services import (
    idem,
    ids,
    nativeapi,
    relay,
    statelog,
    statusmap,
    taskstore,
    tokensession,
    upstream,
)
from app.services.upstream_addr import assert_upstream_allowed, resolve_upstream_base

#: 无上游可问时本地快照的状态词（内置近似；有 ``data.upstream_status`` 原话时优先原话）
_STATUS_WORDS: dict[str, str] = {
    SUBMITTED: "queued",
    QUEUED: "queued",
    IN_PROGRESS: "processing",
    SUCCESS: "succeeded",
    FAILURE: "failed",
    CANCELED: "canceled",
}

#: 幂等重放目标缺失（行被清理）时的统一文案：让客户端摘键重试，绝不放行重建
_REPLAY_MISSING = "idempotent replay target missing; retry without Idempotency-Key"

#: 提交体超过 ``BODY_MAX_BYTES`` 的统一文案（超限一律 413）
_BODY_TOO_LARGE = "request body too large"


# ---------------------------------------------------------------------------
# 路径准入 / 令牌 / 幂等
# ---------------------------------------------------------------------------


def _deny_prefixes() -> tuple[str, ...]:
    """解析 ``BATCH_DENY_PREFIXES``（逗号分隔，容忍空白与缺前导斜杠）。"""
    out: list[str] = []
    for part in (settings.batch_deny_prefixes or "").split(","):
        prefix = part.strip()
        if not prefix:
            continue
        out.append(prefix if prefix.startswith("/") else f"/{prefix}")
    return tuple(out)


def ensure_path_allowed(path: str) -> None:
    """路径准入：命中硬拒前缀 → 403（防把上游管理面/控制台经网关暴露）。"""
    normalized = nativeapi.normalize(path)
    for prefix in _deny_prefixes():
        if normalized.startswith(prefix):
            raise HTTPException(403, f"path not allowed: {normalized}")


async def _rate_and_place(token_hash: str, idem_key: str | None) -> tuple[bool, str | None]:
    """限流 + 幂等占位。

    返回 ``(placeholder_owned, replay_task_id)``；限流拒绝时归还已抢到的占位
    （请求未产生任何副作用）。``idem_key`` 为 None 时只做限流。
    """
    if not idem_key:
        await ratelimit.check_rate(f"tok:{token_hash}")
        return False, None
    rate_res, acq_res = await asyncio.gather(
        ratelimit.check_rate(f"tok:{token_hash}"),
        idem.acquire(token_hash, idem_key),
        return_exceptions=True,
    )
    if isinstance(acq_res, BaseException):
        raise acq_res
    owned, replay_task_id = acq_res
    if isinstance(rate_res, BaseException):
        if owned:
            await idem.release(token_hash, idem_key)
        raise rate_res
    if owned or replay_task_id:
        return owned, replay_task_id
    # 他方占位中（同键真并发）：短轮询等回填，超时按 409（不放行重建）
    replay_task_id = await idem.wait_task_id(token_hash, idem_key)
    if not replay_task_id:
        raise HTTPException(
            409, "Idempotency-Key conflict: another request with the same "
                 "key is in progress; retry with the same key")
    return False, replay_task_id


def _extract_model(body: bytes, content_type: str) -> str:
    """浅解析 body 只为取 ``model``（原文另存 ``request_body``，不改写转发体）。"""
    if not body or "json" not in content_type.lower():
        return ""
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return ""
    if not isinstance(parsed, dict):
        return ""
    value = parsed.get("model") or parsed.get("model_name")
    return str(value) if value else ""


def _local_body(task: dict) -> dict:
    """本地视图（无上游可问 / 幂等重放 / 取消回执）：``{task_id, status}``。

    状态词优先用 ``data.upstream_status``（上游原话），无则回退内置近似词。
    """
    data = task.get("data") or {}
    raw = data.get("upstream_status")
    word = str(raw) if raw else _STATUS_WORDS.get(str(task.get("status")), "queued")
    return {"task_id": task["task_id"], "status": word}


def _rewrite_local_id(payload: dict, upstream_id: str, local_id: str) -> dict:
    """报文里的上游 id 逐字节改写回本地 id（原生报文同构，见 ADR-010）。"""
    if not upstream_id or upstream_id == local_id:
        return payload
    try:
        raw = json.dumps(payload, ensure_ascii=False).encode()
        rewritten = json.loads(nativeapi.rewrite_ids(raw, upstream_id, local_id))
    except (TypeError, ValueError):
        return payload
    return rewritten if isinstance(rewritten, dict) else payload


def _strip_last_segment(path: str) -> str:
    """去掉路径最后一段（本地 task_id 段），用于 ``request_path`` 缺失时兜底。"""
    head = nativeapi.normalize(path).rsplit("/", 1)[0]
    return head or "/"


# ---------------------------------------------------------------------------
# 受理
# ---------------------------------------------------------------------------


async def _read_body_limited(request: Request) -> bytes:
    """读取请求体并强制 ``BODY_MAX_BYTES`` 上限，超限抛 413。

    **为什么不能只看 Content-Length**：该头可缺失（chunked 编码）也可被客户端
    伪造为小值——只信头等于没防。这里先按头快速拒绝（省掉读体），再在
    ``request.stream()`` 读取过程中逐块累加、超过即中止；**绝不**先
    ``await request.body()`` 把整个体读进内存再判长度（那就等于没封顶）。
    """
    limit = settings.body_max_bytes
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            over = int(declared) > limit
        except ValueError:
            over = False               # 非法头：交给下面的流式封顶兜底
        if over:
            raise HTTPException(413, _BODY_TOO_LARGE)

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise HTTPException(413, _BODY_TOO_LARGE)
        chunks.append(chunk)
    return b"".join(chunks)


async def create_batch_task(request: Request, path: str) -> dict:
    """受理 ``POST /batch/{path}``：落库即返回本地 task_id（客户端侧零上游往返）。

    返回对外视图 ``{task_id, status}``；``Location`` 头由路由层按
    ``/batch/{path}/{task_id}`` 组装。上游提交交 worker（``publish_batch_submit``）。
    """
    token = extract_token(request.headers.get("authorization"))
    # 提交体上限：必须在任何副作用之前（限流/幂等占位/并发槽/落库）——被拒的请求
    # 不该留下任何痕迹，否则会污染幂等键（同键重试被误判为已有任务）并泄漏并发槽。
    body = await _read_body_limited(request)
    idem_key = request.headers.get("idempotency-key")
    owned, replay_task_id = await _rate_and_place(token.hash, idem_key)

    # 幂等重放短路：不落库、不入队、不再产生任何副作用（重放语义由
    # idem.wait_task_id 承担——同键请求直接取回已占位任务的 task_id）
    if replay_task_id:
        task = await taskstore.get(replay_task_id)
        if not task:
            raise HTTPException(409, _REPLAY_MISSING)
        return _local_body(task)

    conc_acquired = False
    try:
        ensure_path_allowed(path)

        base = resolve_upstream_base(request)
        if not base:
            raise HTTPException(400, "upstream base url missing")
        assert_upstream_allowed(base)

        await ratelimit.conc_acquire(token.hash)      # 并发上限：键 = token_hash
        conc_acquired = True

        content_type = request.headers.get("content-type", "")
        # 用户回调地址只认 ``X-Callback-Url`` 头（照 stask）；**不读 body 里的
        # callback 字段**——body 原样转发上游，网关不解析、不改写。
        callback_url = str(request.headers.get("x-callback-url") or "").strip()
        data: dict[str, Any] = {
            "source": "batch",
            "model": _extract_model(body, content_type),
            "token_hash": token.hash,
            "request_method": request.method,
            "request_path": nativeapi.normalize(path),
            "request_query": request.url.query or "",
            # 原文保留（转发体基底）；只按 UTF-8 解码，不解析、不重排
            "request_body": body.decode("utf-8", "replace") if body else "",
            "request_content_type": content_type,
            "upstream_base_url": base,
            # 刻意不写任何计费字段（ADR-010 §3：网关零资金动作）
        }
        if callback_url:
            data["callback_url"] = callback_url
        task_id = ids.new_task_id("batch")
        # 令牌只进 Redis 会话（绝不落库/进日志）：用户 token 原文的唯一存放处
        await tokensession.store(task_id, token.raw)
        await taskstore.create(
            task_id=task_id, user_id=0, channel_id=0, action="task", data=data,
        )
        if idem_key:
            await idem.set_task_id(token.hash, idem_key, task_id)
        await queue.publish_batch_submit(task_id)
        log.info("batch task accepted: task_id={} path=/{}", task_id, path)
        return {"task_id": task_id, "status": SUBMITTED}
    except Exception:
        if conc_acquired:
            await ratelimit.conc_release(token.hash)
        if owned and idem_key:
            await idem.release(token.hash, idem_key)
        raise


# ---------------------------------------------------------------------------
# 视图（非终态按需探测；终态零上游往返）
# ---------------------------------------------------------------------------


async def view_batch_task(task_id: str, path: str = "") -> Response:
    """视图：非终态探测上游并推进本地状态；终态回放落库快照（零上游往返）。"""
    task = await taskstore.get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    data = task.get("data") or {}
    upstream_id = str(data.get("upstream_task_id") or "")
    base = str(data.get("upstream_base_url") or "")
    probe_path = str(data.get("request_path") or _strip_last_segment(path))

    if str(task["status"]) in TERMINAL:
        return _terminal_response(task, upstream_id)

    token = await tokensession.get(task_id)
    if not upstream_id or not base or not token:
        # 提交在飞 / 令牌会话缺失：本地排队态直出，绝不 404、零上游往返
        return JSONResponse(content=_local_body(task))

    try:
        status, body, content_type = await relay.call_upstream(
            "GET", base, f"{probe_path}/{upstream_id}", token=token,
        )
    except relay.RelayError as exc:
        log.warning("batch probe unreachable: task_id={} err={}", task_id, exc)
        return JSONResponse(content=_local_body(task))
    except (upstream.BreakerOpenError, HTTPException):
        # 熔断打开 / 寻址护栏拒绝（配置在受理后被改坏）：本地快照兜底，
        # 不打断客户端轮询（探测是尽力而为，本地才是权威）
        log.warning("batch probe unavailable (breaker/guard): task_id={}", task_id)
        return JSONResponse(content=_local_body(task))

    if status >= 400 or not body:
        log.warning("batch probe upstream error: task_id={} status={}", task_id, status)
        return JSONResponse(content=_local_body(task))

    try:
        parsed: object = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        # 非 JSON 探测报文：原样回吐并**沿用上游 Content-Type**（不硬写 JSON）
        return Response(content=body, status_code=200, media_type=content_type)
    if not isinstance(parsed, dict):
        return Response(content=body, status_code=200, media_type=content_type)

    await _advance_from_probe(task, parsed, relay.upstream_status(parsed))
    # drop-in 语义（ADR-010「原生报文同构」）——**不要**改成归一化的
    # ``{task_id, status: <内部态>}``：客户端打 ``/batch/v1/tasks`` 期望拿到的就是
    # new-api 原生报文（id 逐字节改写回本地），status 用上游原话；``map_status``
    # 只驱动本地状态机（判终态/落快照/释并发槽），不改写对外报文。
    return JSONResponse(content=_rewrite_local_id(parsed, upstream_id, task_id))


def _terminal_response(task: dict, upstream_id: str) -> JSONResponse:
    """终态：本地即权威。有快照回放快照（同构），无快照按落库字段构建等价报文。"""
    data = task.get("data") or {}
    snapshot = data.get("upstream_snapshot")
    if isinstance(snapshot, dict) and snapshot:
        return JSONResponse(content=_rewrite_local_id(snapshot, upstream_id, task["task_id"]))
    return JSONResponse(content=_local_body(task))


def _callback_payload(task_id: str, status: str, data: dict,
                      payload: dict | None) -> dict:
    """用户回调体：优先回放上游原生报文（id 逐字节改写回本地 id），
    无报文（本地判死 / 提交被确定性拒绝）时退回 ``{task_id, status}`` 近似词。"""
    upstream_id = str(data.get("upstream_task_id") or "")
    if isinstance(payload, dict) and payload:
        return _rewrite_local_id(payload, upstream_id, task_id)
    raw = data.get("upstream_status")
    word = str(raw) if raw else _STATUS_WORDS.get(status, "queued")
    return {"task_id": task_id, "status": word}


async def _finalize_batch(task_id: str, status: str, data: dict, payload: dict | None,
                          *, from_status: str = "",
                          raw_status: str | None = None,
                          extra_patch: dict | None = None,
                          fail_reason: str = "") -> bool:
    """``/batch`` 链路**唯一终态收口点**：视图探测 / 后台 sweep / worker 提交共用，
    绝不各写一份（四件事的顺序即语义）。

    顺序：CAS 抢推进权 → 记一条状态迁移日志 → 落终态快照 → 释放并发槽 → 投递
    用户回调 → 清令牌会话。

    - CAS 抢不到（终态已被别处推进）→ 整段不执行，返回 False：这是「终态事件
      恰好一次」的唯一保证，重复回调 / 重复释放槽 / **重复状态迁移日志**都由此挡住；
    - 状态迁移日志（``statelog``）**只在 cas 成功分支内**记一条——放到 CAS 之外
      会让重复观察者各记一条；
    - 快照落 ``data.upstream_snapshot``（``nativeapi.capture_snapshot``，≤8KB，
      空报文不落键），供终态零上游往返的回放（ADR-010「原生报文同构」）；
    - 回调**复用既有泛用件**：``queue.publish_notify`` → ``notify.push``（HMAC
      签名 + 重试/死信），本模块不另建投递路径；
    - 令牌会话终态即清（既有纪律），明文 token 不留 Redis。
    """
    patch: dict[str, Any] = dict(extra_patch or {})
    if raw_status:
        patch["upstream_status"] = raw_status
    snapshot = nativeapi.capture_snapshot(
        payload if isinstance(payload, dict) and payload else None)
    if snapshot is not None:
        patch["upstream_snapshot"] = snapshot

    if not await taskstore.cas(task_id, ACTIVE, status, patch=patch, fail_reason=fail_reason):
        return False

    statelog.record_transition(task_id, from_status or None, status,
                               "batch_finalize", detail=fail_reason)
    await ratelimit.conc_release(data.get("token_hash"))
    callback_url = str(data.get("callback_url") or "")
    if callback_url:
        await queue.publish_notify(task_id, callback_url,
                                   _callback_payload(task_id, status, data, payload))
    await tokensession.clear(task_id)
    return True


async def _advance_from_probe(task: dict, payload: dict, raw_status: str | None) -> None:
    """按探测报文推进本地状态（终态走单一收口点；CAS 保护，重复/迟到快照无副作用）。"""
    task_id = task["task_id"]
    data = task.get("data") or {}
    patch: dict[str, Any] = {}
    if raw_status:
        patch["upstream_status"] = raw_status

    mapped = statusmap.map_status(raw_status)
    if mapped is None:
        if patch:
            await taskstore.patch_data(task_id, patch)
        return

    if mapped in TERMINAL:
        await _finalize_batch(task_id, mapped, data, payload,
                              from_status=str(task["status"]), raw_status=raw_status)
        return

    if mapped != str(task["status"]) and mapped in ACTIVE:
        if await taskstore.cas(task_id, ACTIVE, mapped, patch=patch):
            statelog.record_transition(task_id, str(task["status"]), mapped, "batch_probe")
        elif patch:
            await taskstore.patch_data(task_id, patch)
        return
    if patch:
        await taskstore.patch_data(task_id, patch)


# ---------------------------------------------------------------------------
# 取消（本地 CAS 置 CANCELED + 尽力源头止损；不再有解冻）
# ---------------------------------------------------------------------------


async def cancel_batch_task(task_id: str) -> dict:
    """取消：本地 CAS 置 CANCELED；尽力 DELETE 上游（失败只记日志，不影响本地结果）。"""
    task = await taskstore.get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    if str(task["status"]) in TERMINAL:
        return _local_body(task)

    data = task.get("data") or {}
    if not await taskstore.cas(task_id, ACTIVE, CANCELED, fail_reason="canceled by user"):
        fresh = await taskstore.get(task_id)
        return _local_body(fresh or task)

    statelog.record_transition(task_id, str(task["status"]), CANCELED, "batch_cancel")
    await ratelimit.conc_release(data.get("token_hash"))
    await _best_effort_upstream_cancel(task_id, data)
    fresh = await taskstore.get(task_id)
    return _local_body(fresh or task)


async def _best_effort_upstream_cancel(task_id: str, data: dict) -> None:
    """尽力调上游取消端点止损：任何失败只告警，绝不阻塞本地收口。

    **约定推断**：DELETE 形态取 ``DELETE {base}{path}/{upstream_id}``——ADR-010
    只明文规定了探测的 ``GET .../{id}`` 形态，取消形态未明说，此处按对称约定
    实现。若某上游的取消端点不是这个形态，需改代码（ADR-010 已声明「接入不符合
    new-api 约定的上游需要改代码」），别把它当成已验证的事实。
    """
    upstream_id = str(data.get("upstream_task_id") or "")
    base = str(data.get("upstream_base_url") or "")
    probe_path = str(data.get("request_path") or "")
    if not (upstream_id and base and probe_path):
        return
    token = await tokensession.get(task_id)
    if not token:
        return
    try:
        status, _, _ = await relay.call_upstream(
            "DELETE", base, f"{probe_path}/{upstream_id}", token=token,
        )
        if status >= 400:
            log.warning("upstream cancel rejected: task_id={} status={}", task_id, status)
    except Exception as exc:
        log.warning("upstream cancel attempt failed: task_id={} err={}", task_id, exc)


# ---------------------------------------------------------------------------
# worker 提交（queue.batch_submit_task 入口）
# ---------------------------------------------------------------------------


async def submit_batch_task(task_id: str) -> None:
    """上游提交（worker 执行）：按落库的 ``upstream_base_url`` 原样转发。

    分流（ADR-010 后只有三档，无资金分支）：

    - 上游 2xx：回填 ``upstream_task_id`` / ``upstream_status``，置 QUEUED；
      若响应直接给出终态则 CAS 终态并落快照、释放并发槽；
    - 上游 4xx（确定性拒绝）：CAS FAILURE，不重试；
    - 传输错误 / 5xx / 599（模糊失败）：抛给 queue 层退避重试（不判死——上游可能
      已接单，判死会放过真实在跑的单）。
    """
    task = await taskstore.get(task_id)
    if not task or str(task["status"]) in TERMINAL:
        return                                  # 已终态：迟到触发直接短路
    data = task.get("data") or {}
    if data.get("upstream_task_id"):
        return                                  # 已提交：补投/DLQ 重放幂等短路
    if str(task["status"]) not in ACTIVE:
        return

    base = str(data.get("upstream_base_url") or "")
    probe_path = str(data.get("request_path") or "")
    token = await tokensession.get(task_id)
    if not token:
        # 令牌会话丢失（Redis 故障/超 TTL）：无法提交，保持活跃由 ops 观察；
        # 不判死——这是基础设施故障，不是任务失败
        log.error("batch submit skipped, token session missing: {}", task_id)
        return

    body_text = str(data.get("request_body") or "")
    try:
        status, raw, _ = await relay.call_upstream(
            str(data.get("request_method") or "POST"),
            base,
            probe_path,
            token=token,
            query=str(data.get("request_query") or ""),
            body=body_text.encode() if body_text else None,
            content_type=str(data.get("request_content_type") or ""),
        )
    except relay.RelayError:
        raise                                   # 模糊失败：交给 queue 层退避重试

    if status >= 500:
        raise relay.RelayError(status, raw.decode("utf-8", "replace"))
    if status >= 400:
        reason = raw[:400].decode("utf-8", "replace")
        # 走单一收口点：确定性拒绝也要释并发槽/清会话/按需回调（原先此处漏释槽）
        await _finalize_batch(task_id, FAILURE, data, None,
                              from_status=str(task["status"]),
                              fail_reason=f"upstream {status}: {reason}")
        log.warning("batch submit rejected: task_id={} status={}", task_id, status)
        return

    try:
        payload: object = json.loads(raw) if raw else {}
    except (ValueError, UnicodeDecodeError):
        payload = {}

    upstream_id = relay.extract_upstream_task_id(payload)
    if not upstream_id:
        # 约定被违反（2xx 却没有 id/task_id）：立即可见，不静默挂起（照旧链路纪律）
        log.warning("batch submit response missing upstream task id: task_id={}", task_id)
        await _finalize_batch(task_id, FAILURE, data, None,
                              from_status=str(task["status"]),
                              fail_reason="upstream response missing task id")
        return
    raw_status = relay.upstream_status(payload)
    mapped = statusmap.map_status(raw_status)

    if mapped in TERMINAL:
        await _finalize_batch(
            task_id, mapped, data,
            payload if isinstance(payload, dict) else None,
            from_status=str(task["status"]),
            raw_status=raw_status,
            extra_patch={"upstream_task_id": upstream_id},
        )
        return

    patch: dict[str, Any] = {"upstream_task_id": upstream_id}
    if raw_status:
        patch["upstream_status"] = raw_status
    await taskstore.patch_data(task_id, patch, status=QUEUED)
    statelog.record_transition(task_id, str(task["status"]), QUEUED, "batch_submit")
    log.info("batch task submitted: task_id={} upstream_task_id={}", task_id, upstream_id)


async def free_batch_get(path: str, request: Request) -> Response:
    """免费透传（GET 且末段不是本地 task_id）：原样转发上游，不落 tasks 行。

    用用户 token 透传（``Authorization`` 原样），按 IP 限流；上游响应状态、
    Content-Type 与报文原样回吐（可能是图片/二进制产物，绝不硬写 JSON）。
    这是「列表/查询类上游端点」的入口，刻意不产生任何本地任务事实。

    **流式转发**：不解析、不改写 id、不落快照的免费透传直接走
    ``relay.stream_upstream`` + ``StreamingResponse``——产物不整段进内存，
    大文件/二进制也能边收边出。媒体类型用上游的 ``content_type``（不硬写 JSON）。

    **为什么缺 token 直接 401**：``/batch`` 是鉴权中继，令牌的有效性判定
    仍在上游 relay（网关不内省）；但「必须带凭证才能代发」是代发的前提——
    没有 token 既无身份做限流，转发出去也必然被上游拒。这不是网关自建鉴权，
    只是把「无凭证」这一必然失败fail fast。
    """
    await ratelimit.ip_rate_limit(request)

    base = resolve_upstream_base(request)
    if not base:
        raise HTTPException(400, "upstream base url missing")
    assert_upstream_allowed(base)

    token = extract_token(request.headers.get("authorization"))
    try:
        status, content_type, stream = await relay.stream_upstream(
            "GET", base, nativeapi.normalize(path), token=token.raw,
            query=request.url.query or "",
        )
    except (relay.RelayError, upstream.BreakerOpenError) as exc:
        raise HTTPException(502, str(exc)) from exc
    return StreamingResponse(stream, status_code=status, media_type=content_type)


# ---------------------------------------------------------------------------
# 后台收敛（queue.batch_sweep_task 的 cron 入口）
# ---------------------------------------------------------------------------


async def sweep_batch_once(limit: int = 50) -> int:
    """收敛一轮：把长时间未被客户端轮询推进的 ``/batch`` 非终态任务探到终态。

    ``/batch`` 链路本身是纯客户端轮询驱动（``view_batch_task`` 按需探测，无后台
    poller）。后果有二：客户端停止轮询 → 任务永远停在非终态；网关从未观察到
    终态 → 用户回调整条链路是断的。本函数按 ``tasks`` 表事实源补上后台收敛：

    - 候选：``taskstore.stale_batch_active``（``source='batch'``、非终态、超
      ``task_stale_seconds``，只投白名单字段）；
    - 探测：用**落库的** ``upstream_base_url`` + ``request_path`` +
      ``upstream_task_id`` 发 ``GET {base}{path}/{upstream_id}``；用户 token 从
      Redis 会话取（明文 token 绝不落库），取不到就跳过；
    - 状态判定：``statusmap.map_status(raw)``——
      终态 → ``_finalize_batch`` 单一收口点；非终态 → 本轮什么都不做；
    - 令牌会话已过期：跳过并记 DEBUG。会话没了就无法再探测，这是时限/基础设施
      问题而非任务失败，绝不刷 ERROR、绝不误判终态（终态会由客户端轮询时的
      视图路径补上）。

    **重入锁**：一轮可能串行探测 ``limit`` 条 × 单条最长 ``RELAY_TIMEOUT_SECONDS``，
    最坏会超过 1 分钟 cron（多副本更甚）。用 ``K_BATCH_SWEEP_LOCK`` 保证慢轮不
    叠加并发轮。拿不到锁本轮直接返回 0，不报错。

    返回本轮推进到终态的任务数。
    """
    guard = uuid.uuid4().hex
    locked = await r.set(K_BATCH_SWEEP_LOCK, guard, nx=True,
                         ex=settings.batch_sweep_lock_ttl_seconds)
    if not locked:
        log.debug("batch sweep skipped: another round holds the lock")
        return 0
    try:
        return await _sweep_batch_rows(limit)
    finally:
        try:    # CAS 删除：只删自己持有的锁，防误删他人已续期的锁
            await r.eval(LUA_CAS_DELETE, 1, K_BATCH_SWEEP_LOCK, guard)
        except Exception:
            log.opt(exception=True).debug("batch sweep lock release failed")


async def _sweep_batch_rows(limit: int) -> int:
    """``sweep_batch_once`` 的锁内主体（拆出便于锁的 try/finally 收口）。"""
    rows = await taskstore.stale_batch_active(settings.task_stale_seconds, limit=limit)
    advanced = 0
    for row in rows:
        task_id = str(row.get("task_id") or "")
        data = {
            "upstream_base_url": str(row.get("upstream_base_url") or ""),
            "request_path": str(row.get("request_path") or ""),
            "upstream_task_id": str(row.get("upstream_task_id") or ""),
            "callback_url": str(row.get("callback_url") or ""),
            "token_hash": str(row.get("token_hash") or ""),
        }
        upstream_id = data["upstream_task_id"]
        base = data["upstream_base_url"]
        probe_path = data["request_path"]
        if not (task_id and upstream_id and base and probe_path):
            continue                                  # SQL 已挡无 id 行，这里只兜底

        # 先看会话存在性再取令牌：会话过期是常态（TTL 48h），直接 tokensession.get
        # 会对每个过期任务刷 WARNING，超时任务逐轮复现会淹没日志——这里按 DEBUG 静默。
        if not (await tokensession.session_info(task_id)).get("exists"):
            log.debug("batch sweep skip, token session expired: task_id={}", task_id)
            continue
        token = await tokensession.get(task_id)
        if not token:
            log.debug("batch sweep skip, token session vanished: task_id={}", task_id)
            continue

        try:
            status, body, _ = await relay.call_upstream(
                "GET", base, f"{probe_path}/{upstream_id}", token=token,
            )
        except relay.RelayError as exc:
            log.debug("batch sweep probe unreachable: task_id={} err={}", task_id, exc)
            continue
        except (upstream.BreakerOpenError, HTTPException):
            # 熔断打开 / 寻址护栏拒绝：探测是尽力而为，下轮再试
            log.debug("batch sweep probe unavailable (breaker/guard): task_id={}", task_id)
            continue

        if status >= 400 or not body:
            log.debug("batch sweep probe upstream error: task_id={} status={}",
                      task_id, status)
            continue

        try:
            parsed: object = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            continue
        if not isinstance(parsed, dict):
            continue

        raw_status = relay.upstream_status(parsed)
        mapped = statusmap.map_status(raw_status)
        if mapped not in TERMINAL:
            continue                                  # 非终态：下一轮再看，不动任务

        if await _finalize_batch(task_id, mapped, data, parsed,
                                 from_status=str(row.get("status") or ""),
                                 raw_status=raw_status):
            advanced += 1
            log.info("batch sweep advanced to terminal: task_id={} status={}",
                     task_id, mapped)
    return advanced
