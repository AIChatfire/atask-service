"""``/queue`` 中继链路的生命周期：受理 / 视图 / 取消 / 放行 / worker 提交。

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

受理后的上游提交交 worker（``queue.publish_queue_submit`` → 本模块
``submit_queue_task``），视图由客户端轮询驱动（GET 时按需探测），
后台 sweep 补上「客户端不轮询」的收敛——这正是 ADR-010「探测与回调的取舍
改为固定策略」的落地形态。

## 攒批（见 ADR-011）

``BATCH_SIZE >= 2``（或客户端用 ``X-Batch-Size`` 声明）时，受理**不再立刻**投递上游
提交：任务落库为 ``SUBMITTED`` + ``data.batch_state='waiting'``，入批等待，由
「成员数达到 N」或「本批 deadline 到期」触发整批放行——放行才是提交上游的那一刻
（``release_batched_task``）。

三处与「非攒批路径」的关键差别，都是刻意的：

1. **等待期不占并发槽**。占槽点从受理搬到放行：否则等待期也被算进并发额度，一批
   还没放行就把自己的槽耗光，批次永远不可能大于并发上限——攒批就不成立。
   代价是**受理不再因并发满而 429**，改为排队（客户端的重试逻辑要相应调整）。
2. **提交上游多一道闸门**。``queue_submit_task`` 对等待态任务直接短路：补投、
   ``/ops/requeue``、DLQ 重放都走同一个入口，不能绕过放行去提交（那等于绕过并发闸门）。
3. **并发槽的释放改为「谁把掩码置零谁去还」**（``taskstore.claim_slot_release``）。
   等待期未占槽、取消与放行可能真并发，无条件 DECR 会还掉别人的槽。
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app import queue
from app.config import settings
from app.deps import ratelimit
from app.deps.identity import extract_token
from app.errors import error_body
from app.logging import log
from app.redis import K_QUEUE_SWEEP_LOCK, LUA_CAS_DELETE, r
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
    batching,
    callback_addr,
    dynconf,
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

#: 攒批超期兜底的宽限（秒）：``batch_due_at`` 过去这么久仍未放行，就认定「T 触发的
#: 延迟任务丢了 / Redis 索引丢了 / 放行途中崩了」并把它捞回来。
#:
#: 刻意**不做成配置项**：它是内部安全余量而不是运营旋钮，取值只需明显大于「延迟任务
#: 投递 + 一轮放行」的正常耗时；做成配置只会多一个能被配错的地方。120s 同时保证了
#: 「正常在飞的放行（毫秒级）绝不会被误判成卡死」。
_BATCH_RESCUE_GRACE_SECONDS = 120


# ---------------------------------------------------------------------------
# 路径准入 / 令牌 / 幂等 / 攒批决策
# ---------------------------------------------------------------------------


def _deny_prefixes() -> tuple[str, ...]:
    """解析 ``QUEUE_DENY_PREFIXES``（逗号分隔，容忍空白与缺前导斜杠）。"""
    out: list[str] = []
    for part in (settings.queue_deny_prefixes or "").split(","):
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


async def _resolve_batch_plan(request: Request, model: str, token_hash: str) -> batching.Plan:
    """算出本次提交**生效的**攒批决策（客户端头逐字段叠加到服务端配置上）。

    放在受理链路的**校验段**（落库与占槽之前）：坏头必须 400 且不留任何痕迹——留下
    一条永不提交的任务行比直接拒绝更糟，客户端拿到 202 之后会一路轮询到超时。

    非法头在 ``batch_enabled=False`` 时**同样报 400**（``resolve_plan`` 刻意先解析再
    判开关）：否则「关掉攒批时坏头不报错、打开后才报错」会变成一类只在切换开关后
    暴露的客户端 bug。
    """
    try:
        return batching.resolve_plan(
            request.headers,
            model=model,
            token_hash=token_hash,
            enabled=bool(await dynconf.get("batch_enabled")),
            size=int(await dynconf.get("batch_size")),
            wait=int(await dynconf.get("batch_wait_seconds")),
            max_wait=settings.max_batch_wait_seconds,
        )
    except batching.BatchParamError as exc:
        raise HTTPException(
            exc.status,
            error_body(exc.message, "invalid_request_error",
                       code=exc.code, param=exc.param or None),
        ) from exc


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


async def _release_slot(task_id: str, token_hash: str) -> None:
    """按「谁把掩码置零，谁去还槽」的纪律还一次并发槽。

    ``claim_slot_release`` 返回 True 才 DECR：同一任务的槽可能被取消路径与放行路径
    同时来还（两者会真并发），无条件 DECR 会还掉**别人的**槽，而 Lua 只钳 0、发现不了。
    详见 ``taskstore.claim_slot_release``。
    """
    if await taskstore.claim_slot_release(task_id):
        await ratelimit.conc_release(token_hash)


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


async def _join_batch(task_id: str, plan: batching.Plan) -> None:
    """入批并排好两个触发器（见 ADR-011）。

    ``batch_due_at`` 必须落库：Redis 索引丢失后，sweep 的超期兜底只能靠 DB 里的这个
    字段判定「该放行了」。落的是 ``batching.join`` 回读的**真实 ZSCORE**，不是本地算
    出来的值（``ZADD NX`` 只由首个成员写定，NX 命中时两者不同）。

    两处投递失败都**不上抛**：批次已经在 Redis 里，还有另一条触发路径与 sweep 兜底；
    为一次排程抖动让整个提交 500 得不偿失（客户端会以为任务没受理，而它其实已落库）。
    """
    count, due_at, wrote_deadline = await batching.join(
        task_id, plan.key, wait_seconds=plan.wait)
    await taskstore.patch_data(task_id, {"batch_due_at": due_at})

    if wrote_deadline:
        # T 触发：**只由真正写定 deadline 的那个成员排一次**（见 LUA_BATCH_JOIN）。
        # 每个成员都排一次会让一批 N 条排出 N 个延迟任务（N-1 个纯空转），而调度源
        # 每轮都要读全量待派发任务——那是会被放大的浪费。
        try:
            await queue.publish_batch_release(plan.key, "due", due_at=due_at)
        except Exception:
            log.opt(exception=True).warning(
                "batch due-trigger scheduling failed, falls back to sweep: key={}",
                plan.key)

    if count >= plan.size:
        # N 触发：只**投递**一个放行任务就返回，绝不在提交响应里同步放行整批——一批
        # 几百条同步放行会把响应时间拉成秒级，而投递失败本该由 T 与 sweep 兜底。
        try:
            await queue.publish_batch_release(plan.key, "size")
        except Exception:
            log.opt(exception=True).warning(
                "batch size-trigger publish failed, falls back to timeout: "
                "key={} count={}", plan.key, count)


async def create_queue_task(request: Request, path: str) -> dict:
    """受理 ``POST /queue/{path}``：落库即返回本地 task_id（客户端侧零上游往返）。

    返回对外视图 ``{task_id, status}``；攒批时额外回报 ``batch_*``（照 stask 的
    R-20 口径：否则客户端要再查一次库才知道自己在不在批次里）。``Location`` 头由
    路由层按 ``/queue/{path}/{task_id}`` 组装。上游提交交 worker
    （``publish_queue_submit``），或交攒批放行（``publish_batch_release``）。
    """
    token = extract_token(request.headers.get("authorization"))
    # 提交体上限：必须在任何副作用之前（限流/幂等占位/并发槽/落库）——被拒的请求
    # 不该留下任何痕迹，否则会污染幂等键（同键重试被误判为已有任务）并泄漏并发槽。
    body = await _read_body_limited(request)
    idem_key = request.headers.get("idempotency-key")
    owned, replay_task_id = await _rate_and_place(token.hash, idem_key)

    # 幂等重放短路：不落库、不入队、不再产生任何副作用（重放语义由
    # idem.wait_task_id 承担——同键请求直接取回已占位任务的 task_id）。
    # 刻意**不重放批次信息**：AC 口径是「回放不改动任何调度/批次状态」，
    # 批次参数由重放请求自己查库可见，不在这里按新请求的头重算。
    if replay_task_id:
        task = await taskstore.get(replay_task_id)
        if not task:
            raise HTTPException(409, _REPLAY_MISSING)
        return _local_body(task)

    conc_acquired = False
    created = False
    plan = batching.Plan(enabled=False, size=0, wait=0, key="")
    try:
        ensure_path_allowed(path)

        base = resolve_upstream_base(request)
        if not base:
            raise HTTPException(400, "upstream base url missing")
        assert_upstream_allowed(base)

        content_type = request.headers.get("content-type", "")
        model = _extract_model(body, content_type)
        # 分批头校验同样在任何副作用之前（与 body 上限同一条纪律）。
        plan = await _resolve_batch_plan(request, model, token.hash)

        # 用户回调地址：``X-Callback-Url`` 头优先，body 的 ``callback_url`` 兜底
        # （上游 API 文档口径）。与 body 上限、分批头同一条纪律——**必须在任何
        # 副作用之前**：非法地址直接 400，不落库、不占并发槽、不占幂等键
        # （幂等占位由 except 分支回滚，见下方 idem.release）。
        #
        # **透传模式（CALLBACK_PASSTHROUGH_UPSTREAM）下取值与校验一起跳过**：地址
        # 只是转发给上游的普通字段，网关既不投递就**没有立场**判定它可信——用白名单
        # 拦住一个上游本来接受的地址，会把「透传」做成半透传（客户端按上游文档写的
        # 请求被网关 400 打回）。
        callback_url = ""
        if not settings.callback_passthrough_upstream:
            callback_url = callback_addr.callback_url_from(
                request.headers.get(callback_addr.CALLBACK_URL_HEADER), body, content_type)
            if callback_url:
                callback_addr.assert_callback_allowed(callback_url)

        # 并发槽：**只有「收到即提交」才在受理时占**（见模块 docstring 的差别 1）。
        # 攒批路径的占槽点在 release_batched_task（放行的那一刻）。
        if not plan.enabled:
            await ratelimit.conc_acquire(token.hash)      # 并发上限：键 = token_hash
            conc_acquired = True
        data: dict[str, Any] = {
            "source": "queue",
            "model": model,
            "token_hash": token.hash,
            "request_method": request.method,
            "request_path": nativeapi.normalize(path),
            "request_query": request.url.query or "",
            # 原文保留（排障与原文回放用；转发默认也走它，仅当受理时为摘除回调
            # 字段做了重构，才改走 data.submit_body）；只按 UTF-8 解码，不解析、不重排
            "request_body": body.decode("utf-8", "replace") if body else "",
            "request_content_type": content_type,
            "upstream_base_url": base,
            # 是否占着并发槽的唯一依据（放行时置 1，终态/取消时由 claim_slot_release
            # 置 0 并负责 DECR）。缺键的行一律按**已占槽**处理（本特性上线前创建的在途
            # 任务，受理时占槽是当时的唯一路径）——见 taskstore.claim_slot_release。
            "slot_flags": 1 if conc_acquired else 0,
            # 刻意不写任何计费字段（ADR-010 §3：网关零资金动作）
        }
        if callback_url and not settings.callback_passthrough_upstream:
            # 默认「网关接管」：地址落库，终态由 notify.push 签名投递。
            data["callback_url"] = callback_url
            # 转发体摘除该字段，防「上游自己也回调」造成双投递
            # （见 callback_addr.strip_callback_url）。**只有 body 里真的带了该键
            # 才产生重构体**：头传地址时根本不碰 body，原生提交保持原文转发。
            stripped_body = callback_addr.strip_callback_url(body, content_type)
            if stripped_body is not None:
                data["submit_body"] = stripped_body
        if idem_key:
            # 幂等键本身不是凭证（客户端自选的重试标识），记进 data 只为了让
            # /ops/tasks 与 /admin 的 has_idempotency_key 真的可用——该诊断位原先
            # 无人写入，恒为 False（死字段）。
            data["idempotency_key"] = idem_key
        if plan.enabled:
            data["batch_state"] = "waiting"
            data["batch_key"] = plan.key
            # 落**生效值**（客户端头叠加后的结果），不是配置原值：客户端声明 N=3 而
            # 配置写 N=10 时，入批与 N 触发都按 3 走；排障也该看到真正生效的那一套。
            data["batch_size"] = plan.size
            data["batch_wait"] = plan.wait
        task_id = ids.new_task_id("queue")
        # 令牌只进 Redis 会话（绝不落库/进日志）：用户 token 原文的唯一存放处
        await tokensession.store(task_id, token.raw)
        await taskstore.create(
            task_id=task_id, user_id=0, channel_id=0, action="task", data=data,
        )
        created = True
        if idem_key:
            await idem.set_task_id(token.hash, idem_key, task_id)
        if plan.enabled:
            await _join_batch(task_id, plan)
        else:
            await queue.publish_queue_submit(task_id)
        log.info("queue task accepted: task_id={} path=/{} batch={}",
                 task_id, path, plan.key or "-")
        view: dict[str, Any] = {"task_id": task_id, "status": SUBMITTED}
        if plan.enabled:
            # 对外只暴露 waiting / released 两个值（见 batching.public_state）。
            view["batch_key"] = plan.key
            view["batch_state"] = batching.public_state("waiting")
            view["batch_size"] = plan.size
            view["batch_wait"] = plan.wait
        return view
    except Exception:
        if conc_acquired:
            await ratelimit.conc_release(token.hash)
        # 退批是**无条件**的：`create` 成功之后任何一步失败都要退回，而入批本身可能
        # 还没发生。对非成员调用它是安全的（ZREM 空操作；只有批次真的空了才清到期
        # 索引，此时别的成员本来就不存在）——比「猜自己入没入过批」可靠。
        if plan.enabled and created:
            await batching.leave(task_id, plan.key)
        if owned and idem_key:
            await idem.release(token.hash, idem_key)
        raise


# ---------------------------------------------------------------------------
# 攒批放行（受理段与 queue.batch_release_task / queue.queue_release_task 的共同落点）
# ---------------------------------------------------------------------------


async def release_batched_task(task_id: str, *, source: str = "batch") -> str:
    """放行一条攒批任务：抢放行权 → 占并发槽 → 落掩码 → 投递上游提交。

    返回 ``"released"`` / ``"requeued"`` / ``"skipped"``（与 ``batching.release``
    的计数口径一致）。

    顺序即语义：**先抢权再占槽**。反过来的话，抢权失败时已占的槽要靠额外的回滚代码
    还回去，而回滚本身也可能失败。

    占不到并发槽 **不是失败**：退避重排（``batching.requeue``）。客户端此刻早已拿到
    202，报错也没有接收方——它要的是「帮我排队」。

    「已终态 / 已被别的路径放行 / 不在等待态」一律 ``skipped``：绝不重复下发上游，
    这是 ``claim_for_release`` 这道 DB 级幂等之外的**快速路径**（省掉一次抢权 UPDATE）。
    """
    meta = await taskstore.get_batch_meta(task_id)
    if not meta:
        log.warning("batch release: task not found: task_id={}", task_id)
        return "skipped"
    if (str(meta.get("status") or "") not in ACTIVE
            or str(meta.get("batch_state") or "") not in taskstore.BATCH_WAITING_STATES):
        log.info("batch release: not in waiting state, dropped: task_id={} state={}",
                 task_id, meta.get("batch_state"))
        return "skipped"

    # 1) 抢放行权：条件更新，一条任务只可能被一方抢到。Redis 侧的批次 claim 只保证
    #    「整批只被摘一次」，救不了同一成员被两条路径各捞到一次——这一关不能省。
    if not await taskstore.claim_for_release(task_id):
        log.info("batch release: already claimed or terminal: task_id={} source={}",
                 task_id, source)
        return "skipped"

    token_hash = str(meta.get("token_hash") or "")
    batch_key = str(meta.get("batch_key") or "")
    attempts = int(meta.get("requeue_attempts") or 0)

    # 2) 占并发槽。等待期不占，占槽点就在这里——批次因此可以大于并发上限。
    if not await ratelimit.conc_try_acquire(token_hash):
        await taskstore.unclaim_for_release(task_id)
        await batching.requeue(task_id, batch_key, attempts=attempts)
        log.info("batch release: no slot, requeued: task_id={} attempts={}",
                 task_id, attempts + 1)
        return "requeued"

    # 3) 落掩码 + 放行标记。掩码是还槽的唯一依据，必须与占槽同一轮写下。
    #    状态仍是 SUBMITTED —— 提交成功后 submit_queue_task 才 CAS 到 QUEUED。
    try:
        await taskstore.patch_data(task_id, {
            "batch_state": "released",
            "slot_flags": 1,
            "released_at": int(time.time()),
        })
    except Exception:
        # 槽已经占了，但掩码没落库 → 这条行**无法**认领这次还槽（claim_slot_release
        # 读到的还是 0），所以必须直接 DECR，不能走 claim_slot_release。
        await ratelimit.conc_release(token_hash)
        await taskstore.unclaim_for_release(task_id)
        await batching.requeue(task_id, batch_key, attempts=attempts)
        log.opt(exception=True).error("batch release: persist failed: task_id={}", task_id)
        return "requeued"

    # 4) 复核：抢权到落库之间用户可以取消（终态不可逆，而放行是钱花出去的起点）。
    #    已非活跃 → 把刚占的槽还回去，**绝不投递**：否则会为一条用户已经不要的任务
    #    调上游，而上游 relay 会照扣配额，网关零资金动作、无从补救。
    fresh = await taskstore.get_batch_meta(task_id)
    if not fresh or str(fresh.get("status") or "") not in ACTIVE:
        await _release_slot(task_id, token_hash)
        log.info("batch release: canceled while releasing, slot returned: task_id={}",
                 task_id)
        return "skipped"

    try:
        await queue.publish_queue_submit(task_id)
    except Exception:
        await _release_slot(task_id, token_hash)
        await taskstore.cas(
            task_id, (SUBMITTED,), FAILURE,
            patch={"slot_flags": 0, "batch_state": "released"},
            fail_reason="enqueue failed after batch release",
        )
        log.opt(exception=True).error("batch release: enqueue failed: task_id={}", task_id)
        return "skipped"
    log.info("batch release dispatched: task_id={} source={}", task_id, source)
    return "released"


# ---------------------------------------------------------------------------
# 视图（非终态按需探测；终态零上游往返）
# ---------------------------------------------------------------------------


async def view_queue_task(task_id: str, path: str = "") -> Response:
    """视图：非终态探测上游并推进本地状态；终态回放落库快照（零上游往返）。

    攒批等待期的任务**没有** ``upstream_task_id``，会落到下面的「提交在飞」分支直出
    本地排队态——这正是想要的：客户端在等待期看到的是 ``{task_id, status: "queued"}``，
    零上游往返，也不需要为攒批引入新状态。
    """
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
        # 提交在飞 / 攒批等待 / 令牌会话缺失：本地排队态直出，绝不 404、零上游往返
        return JSONResponse(content=_local_body(task))

    try:
        status, body, content_type = await relay.call_upstream(
            "GET", base, f"{probe_path}/{upstream_id}", token=token,
        )
    except relay.RelayError as exc:
        log.warning("queue probe unreachable: task_id={} err={}", task_id, exc)
        return JSONResponse(content=_local_body(task))
    except (upstream.BreakerOpenError, HTTPException):
        # 熔断打开 / 寻址护栏拒绝（配置在受理后被改坏）：本地快照兜底，
        # 不打断客户端轮询（探测是尽力而为，本地才是权威）
        log.warning("queue probe unavailable (breaker/guard): task_id={}", task_id)
        return JSONResponse(content=_local_body(task))

    if status >= 400 or not body:
        log.warning("queue probe upstream error: task_id={} status={}", task_id, status)
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
    # ``{task_id, status: <内部态>}``：客户端打 ``/queue/v1/tasks`` 期望拿到的就是
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


async def _finalize_queue(task_id: str, status: str, data: dict, payload: dict | None,
                          *, from_status: str = "",
                          raw_status: str | None = None,
                          extra_patch: dict | None = None,
                          fail_reason: str = "") -> bool:
    """``/queue`` 链路**唯一终态收口点**：视图探测 / 后台 sweep / worker 提交共用，
    绝不各写一份（四件事的顺序即语义）。

    顺序：CAS 抢推进权 → 记一条状态迁移日志 → 落终态快照 → 释放并发槽 → 投递
    用户回调 → 清令牌会话。

    - CAS 抢不到（终态已被别处推进）→ 整段不执行，返回 False：这是「终态事件
      恰好一次」的唯一保证，重复回调 / 重复释放槽 / **重复状态迁移日志**都由此挡住；
    - 状态迁移日志（``statelog``）**只在 cas 成功分支内**记一条——放到 CAS 之外
      会让重复观察者各记一条；
    - 快照落 ``data.upstream_snapshot``（``nativeapi.capture_snapshot``，≤8KB，
      空报文不落键），供终态零上游往返的回放（ADR-010「原生报文同构」）；
    - **释放并发槽要按掩码认领**（``_release_slot``）：攒批等待期未占槽，取消路径与
      放行路径也可能并发来还——无条件 DECR 会还掉别人的槽（ADR-011）；
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
                               "queue_finalize", detail=fail_reason)
    await _release_slot(task_id, str(data.get("token_hash") or ""))
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
        await _finalize_queue(task_id, mapped, data, payload,
                              from_status=str(task["status"]), raw_status=raw_status)
        return

    if mapped != str(task["status"]) and mapped in ACTIVE:
        if await taskstore.cas(task_id, ACTIVE, mapped, patch=patch):
            statelog.record_transition(task_id, str(task["status"]), mapped, "queue_probe")
        elif patch:
            await taskstore.patch_data(task_id, patch)
        return
    if patch:
        await taskstore.patch_data(task_id, patch)


# ---------------------------------------------------------------------------
# 取消（本地 CAS 置 CANCELED + 退批 + 尽力源头止损；不再有解冻）
# ---------------------------------------------------------------------------


async def cancel_queue_task(task_id: str) -> dict:
    """取消：本地 CAS 置 CANCELED；退批 + 尽力 DELETE 上游（失败只记日志，不影响本地结果）。

    **退批不是可选项**：一批声明 N=100 而其中 5 条被取消，计数就永远差 5 条到不了 N，
    只能干等 T 兜底——等待时长凭空变长，而客户端看不出原因。

    **为什么不复用 `_finalize_queue`**（它自称「唯一终态收口点」）：取消有两处**故意**
    不同的动作——① **不投递用户回调**（取消由客户端自己发起，结果它已经知道，见
    `docs/CALLBACK-CONTRACT.md` §3）；② 多一步**尽力源头止损**（`DELETE` 上游），而它
    必须发生在**清令牌会话之前**（要用会话里的 sk 去发这个请求）。硬套收口点要么给它
    加一个「取消专用」的参数分支，要么把一次网络调用挪到一个不该发请求的位置。

    代价是**终态收口动作的清单不再有单一来源** —— 所以以后新增终态动作时必须同时问
    一句「取消路径要不要」，漏掉就是本仓库 ADR-012 已知限制 ② 那类缺口（明文 sk 残留
    到 `SK_SESSION_TTL_SECONDS` 才被 Redis 回收）。
    """
    task = await taskstore.get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    if str(task["status"]) in TERMINAL:
        return _local_body(task)

    data = task.get("data") or {}
    if not await taskstore.cas(task_id, ACTIVE, CANCELED, fail_reason="canceled by user"):
        fresh = await taskstore.get(task_id)
        return _local_body(fresh or task)

    statelog.record_transition(task_id, str(task["status"]), CANCELED, "queue_cancel")
    batch_key = str(data.get("batch_key") or "")
    if str(data.get("batch_state") or "") in taskstore.BATCH_WAITING_STATES and batch_key:
        await batching.leave(task_id, batch_key)
    await _release_slot(task_id, str(data.get("token_hash") or ""))
    await _best_effort_upstream_cancel(task_id, data)
    # 清令牌会话**必须排在尽力止损之后**：那一步还要用会话里的 sk 调上游 `DELETE`，
    # 先清就取不到 token，函数里 `if not token: return` 会让源头止损**静默失效**。
    # 这是 CANCELED 这条终态原先唯一遗漏的收口动作（另一处「遗漏」是有意的：不投递
    # 用户回调——取消是客户端自己发起的，结果它已经知道，见 CALLBACK-CONTRACT §3）。
    await tokensession.clear(task_id)
    fresh = await taskstore.get(task_id)
    return _local_body(fresh or task)


async def _best_effort_upstream_cancel(task_id: str, data: dict) -> None:
    """尽力调上游取消端点止损：任何失败只告警，绝不阻塞本地收口。

    **约定推断**：DELETE 形态取 ``DELETE {base}{path}/{upstream_id}``——ADR-010
    只明文规定了探测的 ``GET .../{id}`` 形态，取消形态未明说，此处按对称约定
    实现。若某上游的取消端点不是这个形态，需改代码（ADR-010 已声明「接入不符合
    new-api 约定的上游需要改代码」），别把它当成已验证的事实。

    攒批等待期（或占槽失败退避中）的任务没有 ``upstream_task_id``，这里直接返回：
    上游从未收到过这个任务，没有任何东西可取消。
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
# worker 提交（queue.queue_submit_task 入口）
# ---------------------------------------------------------------------------


async def submit_queue_task(task_id: str) -> None:
    """上游提交（worker 执行）：按落库的 ``upstream_base_url`` 原样转发。

    分流（ADR-010 后只有三档，无资金分支）：

    - 上游 2xx：回填 ``upstream_task_id`` / ``upstream_status``，置 QUEUED；
      若响应直接给出终态则 CAS 终态并落快照、释放并发槽；
    - 上游 4xx（确定性拒绝）：CAS FAILURE，不重试；
    - 传输错误 / 5xx / 599（模糊失败）：抛给 queue 层退避重试（不判死——上游可能
      已接单，判死会放过真实在跑的单）。

    **等待态任务在这里被挡下**（见模块 docstring 的差别 2）：补投、``/ops/requeue``、
    DLQ 重放都会经过本函数，而攒批任务的并发槽还没占——它的唯一入口是
    ``release_batched_task``。少了这道闸门，任何一次人工补单都能绕过并发闸门。
    """
    task = await taskstore.get(task_id)
    if not task or str(task["status"]) in TERMINAL:
        return                                  # 已终态：迟到触发直接短路
    data = task.get("data") or {}
    state = str(data.get("batch_state") or "")
    if state in taskstore.BATCH_WAITING_STATES:
        log.warning("queue submit blocked, still batched: task_id={} state={}",
                    task_id, state)
        return
    if data.get("upstream_task_id"):
        return                                  # 已提交：补投/DLQ 重放幂等短路
    if str(task["status"]) not in ACTIVE:
        return

    base = str(data.get("upstream_base_url") or "")
    probe_path = str(data.get("request_path") or "")
    token = await tokensession.get(task_id)
    if not token:
        # 会话可能刚被**终态收口**清掉（取消路径现在也清，见 ``cancel_queue_task``）——
        # 此时「取不到 token」是正常结果，报 ERROR 会制造误导性噪音：运维看到一个
        # 「令牌会话丢失」的 ERROR，而真相是客户端刚取消了。只有「任务仍活跃却取不到
        # 会话」才是基础设施故障（Redis 故障 / 超 TTL）。
        # **必须重读状态**：本函数开头那次读发生在可能已经过期的时刻。
        fresh = await taskstore.get(task_id)
        if fresh and str(fresh["status"]) in TERMINAL:
            log.debug("queue submit skipped, task already terminal: {}", task_id)
            return
        # 令牌会话丢失（Redis 故障/超 TTL）：无法提交，保持活跃由 ops 观察；
        # 不判死——这是基础设施故障，不是任务失败
        log.error("queue submit skipped, token session missing: {}", task_id)
        return

    # 转发体：默认原文（``request_body``）；仅当受理时为摘除回调字段做了重构，
    # 才改走重构体（``submit_body``）——见 ``callback_addr.strip_callback_url``。
    body_text = str(data.get("submit_body") or data.get("request_body") or "")
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
        await _finalize_queue(task_id, FAILURE, data, None,
                              from_status=str(task["status"]),
                              fail_reason=f"upstream {status}: {reason}")
        log.warning("queue submit rejected: task_id={} status={}", task_id, status)
        return

    try:
        payload: object = json.loads(raw) if raw else {}
    except (ValueError, UnicodeDecodeError):
        payload = {}

    upstream_id = relay.extract_upstream_task_id(payload)
    if not upstream_id:
        # 约定被违反（2xx 却没有 id/task_id）：立即可见，不静默挂起（照旧链路纪律）
        log.warning("queue submit response missing upstream task id: task_id={}", task_id)
        await _finalize_queue(task_id, FAILURE, data, None,
                              from_status=str(task["status"]),
                              fail_reason="upstream response missing task id")
        return
    raw_status = relay.upstream_status(payload)
    mapped = statusmap.map_status(raw_status)

    if mapped in TERMINAL:
        await _finalize_queue(
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
    # 回填**必须走带起点的 CAS**，不能裸改状态列：上游提交在飞（最长
    # RELAY_TIMEOUT_SECONDS）期间用户可以取消，而终态不可逆——裸回填会把
    # CANCELED 复活成 QUEUED，留下「QUEUED + progress=100% + finish_time 已写」
    # 的自相矛盾行，随后 sweep 还会给一个已被取消的任务投递「成功」回调。
    # （旧链路 KI-D 修过同一问题，回填改走 CAS；ADR-010 重写时丢了这道守卫。）
    if not await taskstore.cas(task_id, ACTIVE, QUEUED, patch=patch):
        # 抢不到 = 已被取消/判死。上游可能已接单（且已在上游 relay 计费），网关零
        # 资金动作、无从补救——把孤儿 upstream id 记进 data 供运维追溯，
        # **只合并不迁状态**，绝不复活状态列。
        await taskstore.patch_data(task_id, patch)
        log.warning(
            "queue submit landed on a non-active task (orphan upstream task recorded): "
            "task_id={} upstream_task_id={}", task_id, upstream_id)
        return
    statelog.record_transition(task_id, str(task["status"]), QUEUED, "queue_submit")
    log.info("queue task submitted: task_id={} upstream_task_id={}", task_id, upstream_id)


async def free_queue_get(path: str, request: Request) -> Response:
    """免费透传（GET 且末段不是本地 task_id）：原样转发上游，不落 tasks 行。

    用用户 token 透传（``Authorization`` 原样），按 IP 限流；上游响应状态、
    Content-Type 与报文原样回吐（可能是图片/二进制产物，绝不硬写 JSON）。
    这是「列表/查询类上游端点」的入口，刻意不产生任何本地任务事实。

    **流式转发**：不解析、不改写 id、不落快照的免费透传直接走
    ``relay.stream_upstream`` + ``StreamingResponse``——产物不整段进内存，
    大文件/二进制也能边收边出。媒体类型用上游的 ``content_type``（不硬写 JSON）。

    **为什么缺 token 直接 401**：``/queue`` 是鉴权中继，令牌的有效性判定
    仍在上游 relay（网关不内省）；但「必须带凭证才能代发」是代发的前提——
    没有 token 既无身份做限流，转发出去也必然被上游拒。这不是网关自建鉴权，
    只是把「无凭证」这一必然失败 fail fast。
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
# 后台收敛（queue.queue_sweep_task 的 cron 入口）
# ---------------------------------------------------------------------------


async def sweep_queue_once(limit: int = 50) -> int:
    """收敛一轮：① 把长时间未被客户端轮询推进的非终态任务探到终态；
    ② 把攒批里「超期仍未放行」的任务捞回来放行。

    ① ``/queue`` 链路本身是纯客户端轮询驱动（``view_queue_task`` 按需探测，无后台
    poller）。后果有二：客户端停止轮询 → 任务永远停在非终态；网关从未观察到
    终态 → 用户回调整条链路是断的。本函数按 ``tasks`` 表事实源补上后台收敛：

    - 候选：``taskstore.stale_queue_active``（``source='queue'``、非终态、超
      ``task_stale_seconds``，只投白名单字段）；
    - 探测：用**落库的** ``upstream_base_url`` + ``request_path`` +
      ``upstream_task_id`` 发 ``GET {base}{path}/{upstream_id}``；用户 token 从
      Redis 会话取（明文 token 绝不落库），取不到就跳过；
    - 状态判定：``statusmap.map_status(raw)``——
      终态 → ``_finalize_queue`` 单一收口点；非终态 → 本轮什么都不做；
    - 令牌会话已过期：跳过并记 DEBUG。会话没了就无法再探测，这是时限/基础设施
      问题而非任务失败，绝不刷 ERROR、绝不误判终态（终态会由客户端轮询时的
      视图路径补上）。

    ② 攒批的超期兜底（ADR-011）：放行只由 taskiq 延迟任务驱动（T 触发与退避重排），
    延迟任务由 Redis 承载——**它丢了、或者放行途中进程崩了**，这些任务在 DB 里仍是
    「等待放行」，而上面的探测通道要求 ``upstream_task_id`` 非空，天然不会碰它们。
    于是必须有一条只依赖 DB 事实源的通道：``taskstore.stale_batch_waiting`` 按
    ``batch_due_at``（「下一次可放行时刻」）捞出过期仍未放行的行，逐条走
    ``release_batched_task``。卡在 ``releasing`` 的行（放行抢到权就崩了）先
    ``unclaim_for_release`` 退回等待态再放行——不加这一步，它们会永远卡住，客户端
    一路轮询到超时（ADR-010 明令「绝不静默挂起」）。

    **重入锁**：一轮可能串行探测 ``limit`` 条 × 单条最长 ``RELAY_TIMEOUT_SECONDS``，
    最坏会超过 1 分钟 cron（多副本更甚）。用 ``K_QUEUE_SWEEP_LOCK`` 保证慢轮不
    叠加并发轮。拿不到锁本轮直接返回 0，不报错。

    返回本轮**探测推进到终态**的任务数（攒批兜底的数量单独记日志：两个数混在一起
    会让「收敛在动」与「兜底在补」这两件完全不同的事变得不可区分）。
    """
    guard = uuid.uuid4().hex
    locked = await r.set(K_QUEUE_SWEEP_LOCK, guard, nx=True,
                         ex=settings.queue_sweep_lock_ttl_seconds)
    if not locked:
        log.debug("queue sweep skipped: another round holds the lock")
        return 0
    try:
        # 先兜底攒批：它们已经等了至少一个 _BATCH_RESCUE_GRACE_SECONDS，比刚变 stale
        # 的探测候选更急。任一段炸掉不影响另一段（各自内部已按条 try/continue，这里
        # 再包一层是防「查询本身炸掉」把探测通道一起带走）。
        try:
            await _rescue_overdue_batches(limit)
        except Exception:
            log.opt(exception=True).error("queue sweep: batch rescue failed")
        return await _sweep_queue_rows(limit)
    finally:
        try:    # CAS 删除：只删自己持有的锁，防误删他人已续期的锁
            await r.eval(LUA_CAS_DELETE, 1, K_QUEUE_SWEEP_LOCK, guard)
        except Exception:
            log.opt(exception=True).debug("queue sweep lock release failed")


async def _rescue_overdue_batches(limit: int) -> int:
    """攒批超期兜底（``sweep_queue_once`` 的第二个通道）。返回本轮放行成功的条数。"""
    rows = await taskstore.stale_batch_waiting(_BATCH_RESCUE_GRACE_SECONDS, limit=limit)
    rescued = 0
    for row in rows:
        task_id = str(row.get("task_id") or "")
        if not task_id:
            continue
        state = str(row.get("batch_state") or "")
        if state == "releasing":
            # 抢到放行权之后崩了：先退回等待态，否则 claim_for_release 不放行它，
            # 它会永远停在 releasing（既不放行、也不被任何通道捞起）。
            await taskstore.unclaim_for_release(task_id)
        result = await release_batched_task(task_id, source="sweep")
        if result == "released":
            rescued += 1
            log.info("queue sweep rescued overdue batch member: task_id={}", task_id)
    if rescued:
        log.warning("queue sweep rescued {} overdue batch task(s)", rescued)
    return rescued


async def _sweep_queue_rows(limit: int) -> int:
    """``sweep_queue_once`` 的锁内主体（拆出便于锁的 try/finally 收口）。"""
    rows = await taskstore.stale_queue_active(settings.task_stale_seconds, limit=limit)
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
            log.debug("queue sweep skip, token session expired: task_id={}", task_id)
            continue
        token = await tokensession.get(task_id)
        if not token:
            log.debug("queue sweep skip, token session vanished: task_id={}", task_id)
            continue

        try:
            status, body, _ = await relay.call_upstream(
                "GET", base, f"{probe_path}/{upstream_id}", token=token,
            )
        except relay.RelayError as exc:
            log.debug("queue sweep probe unreachable: task_id={} err={}", task_id, exc)
            continue
        except (upstream.BreakerOpenError, HTTPException):
            # 熔断打开 / 寻址护栏拒绝：探测是尽力而为，下轮再试
            log.debug("queue sweep probe unavailable (breaker/guard): task_id={}", task_id)
            continue

        if status >= 400 or not body:
            log.debug("queue sweep probe upstream error: task_id={} status={}",
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

        if await _finalize_queue(task_id, mapped, data, parsed,
                                 from_status=str(row.get("status") or ""),
                                 raw_status=raw_status):
            advanced += 1
            log.info("queue sweep advanced to terminal: task_id={} status={}",
                     task_id, mapped)
    return advanced
