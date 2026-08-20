"""动态透传形态：/{biz}/{原生路径} —— 必须最后注册（通配）。

三种语义，按**渠道配置的路径模板**（零硬编码）分流：

1. **原生提交拦截**（``path == route.submit_path``）：不触上游，preflight →
   落库 → 立即返回，响应体按 ``route.task_id_path`` 塑形为原生形状、值为
   **本地 task_id**（秒级返回，上游提交由 worker 异步执行）。
2. **原生查询/取消拦截**（URL 里带任务 id 且命中 ``probe_path`` /
   ``cancel_path``）：先按 path 里的 id 查 tasks 表（本地 id 主键直查，未命中
   按上游 id 反查），拿 ``channel_id`` 钉回直达租约 —— 精确解，不问 keypool 的
   ``select(group, model)``。查询把 id 换成上游 id 转发探测，响应缓冲后把上游
   id 改写回本地 id（报文其余字节 100% 同构）；上游还没接单时按本地快照直出，
   零上游往返。
3. **其余路径**：维持原透传语义 —— GET/HEAD/OPTIONS 免费（IP 限流），其余方法
   计费透传（preflight + 落 tasks 行），全程流式（大文件不进内存）。

同构约定：计费透传落下的 tasks 行与 flow.create_task **同一份数据形态**
（biz/model/key_index/request_body 全量快照）——终态结算重估、探测钉回渠道、
令牌会话取用对所有入口一视同仁，不为透传形态保留异构兼容路径。

免费路径选渠道纪律：免费请求不带 model，keypool ``select`` 对空 model 直接拒
（40010），所以**永不发起空 model 的 select**——按「path 里的任务 id → 渠道」
（精确）→ Redis biz→channel_id 记忆（``routecache``）→ 进程路由缓存三级钉回
``channel_id`` 直达租约，全落空才 404。
"""

import gzip
import json
import zlib

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app import queue
from app.deps.preflight import preflight
from app.deps.ratelimit import ip_rate_limit
from app.logging import log
from app.schemas import ACTIVE, FAILURE, QUEUED, TERMINAL, KeyLease, RouteConfig
from app.services import (
    flow,
    idem,
    leasing,
    nativeapi,
    polling,
    providers,
    resulturl,
    routecache,
    taskstore,
    upstream,
)
from app.services.providers import KeyLeaseError
from app.services.registry import registry, route_from_lease

router = APIRouter()

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "host",
    "authorization",   # 用户 sk 令牌绝不透传给上游
}
BUFFER_LIMIT = 256 * 1024   # 小响应缓冲上限，用于提取 upstream_task_id
SMALL_BODY_LIMIT = 1_048_576  # 与 preflight JSON 解析上限一致
#: 原生查询拦截的响应缓冲上限：超限则放弃 id 改写、退回流式（探测报文极小，
#: 正常永不触发；触发时告警而不是把内存打爆）
NATIVE_BUFFER_LIMIT = 1_048_576


def _forward_headers(request: Request, extra: dict) -> dict:
    # extra（渠道凭证等注入头）统一小写归一，并按小写名剔除客户端同义头，
    # 保证每个头恰好出现一次（extra 优先），杜绝大小写碰撞产生重复鉴权头
    extra_lc = {k.lower(): v for k, v in extra.items()}
    base = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() not in extra_lc
    }
    # 强制 identity：网关以 aiter_raw 原始字节回填解析（提取 upstream_task_id），
    # 上游若 gzip 会让 json.loads 静默失败、丢轮询挂载；提交响应本身极小，无压缩收益
    return base | extra_lc | {"accept-encoding": "identity"}


async def _request_content(request: Request):
    """转发体三态：小体读全带 Content-Length 直达（避免 chunked 被严格上游拒绝；
    preflight 解析过 JSON 时 starlette 已缓存 body，此处零拷贝）、
    大体流式（文件上传不进内存）、无体 None（GET 不发空 chunked）。"""
    content_length = int(request.headers.get("content-length") or 0)
    if 0 < content_length <= SMALL_BODY_LIMIT:
        return await request.body()
    if content_length > SMALL_BODY_LIMIT or "transfer-encoding" in request.headers:
        return request.stream()
    return None


def _maybe_decompress(body: bytes, content_encoding: str) -> bytes:
    """防御性解码：已强制 accept-encoding: identity，上游仍压缩时兜底"""
    try:
        if content_encoding == "gzip":
            return gzip.decompress(body)
        if content_encoding == "deflate":
            return zlib.decompress(body)
    except Exception:
        pass
    return body


def _target_url(path: str, query: str) -> str:
    """转发目标（相对 base_url）：原样带上客户端查询串（分页/过滤参数不能丢）。"""
    return f"/{path}?{query}" if query else f"/{path}"


def _out_headers(resp_headers) -> dict:
    return {k: v for k, v in resp_headers.items() if k.lower() not in HOP_BY_HOP}


# ---------------------------------------------------------------------------
# 选渠道（永不发起空 model 的 keypool select）
# ---------------------------------------------------------------------------


async def _lease_by_channel(biz: str, channel_id: int, model: str = "",
                            ) -> tuple[KeyLease, RouteConfig] | None:
    """按 channel_id 直达租约并构建路由（keypool 直达分支不校验 model）。"""
    try:
        key = await providers.keys.lease(biz, model=model, key_id=channel_id)
    except KeyLeaseError as exc:
        log.debug("pin-back lease failed: biz={} channel_id={} err={}", biz, channel_id, exc)
        return None
    route = registry.remember(route_from_lease(biz, key))
    return key, route


async def _lease_for_task(biz: str, task: dict) -> tuple[KeyLease, RouteConfig] | None:
    """任务级钉回（精确到 key）：查询/取消必须用创建时那把 key，
    否则同渠道多上游账号时查不到任务（见 app.services.leasing）。"""
    try:
        return await leasing.route_for_task(biz, task.get("data") or {}, task)
    except KeyLeaseError as exc:
        log.debug("task pin-back lease failed: biz={} task_id={} err={}",
                  biz, task.get("task_id"), exc)
        return None


async def _resolve_free_route(biz: str) -> tuple[KeyLease, RouteConfig] | None:
    """免费透传的渠道钉回：Redis biz→channel_id 记忆 → 进程路由缓存。

    两级都落空 → None（调用方 404）。**绝不**用空 model 去问 keypool select
    （必然 40010 失败，白付一次跨服务往返）。
    """
    channel_id = await routecache.get(biz) or registry.channel_id_of(biz)
    if not channel_id:
        return None
    return await _lease_by_channel(biz, channel_id)


async def _resolve_by_path_task(path: str) -> dict | None:
    """按 URL 路径里的任务 id 段反查任务行（精确解，零 keypool 往返）。

    先做零成本形态预筛（``nativeapi.path_task_id_candidates``）：路径里没有
    id 形态的段 → 直接 None，不产生任何查库。本地 id 走主键直查；全部未命中时
    才对**唯一**候选段做一次上游 id 反查（JSON 提取无索引，严格限一次）。
    """
    candidates = nativeapi.path_task_id_candidates(path)
    if not candidates:
        return None
    for candidate in candidates:
        task = await taskstore.get(candidate)
        if task:
            return task
    if len(candidates) == 1:
        return await taskstore.get_by_upstream_id(candidates[0])
    return None


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------


@router.api_route("/{biz}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def dynamic_proxy(biz: str, path: str, request: Request):
    """透传总入口：生命周期拦截（提交/查询/取消）优先，其余维持透传语义。"""
    # ① 路径里带已知任务 id → 一定是「对既有任务的操作」，绝不当新任务计费。
    #    渠道由任务行的 channel_id 钉回（精确解），与创建时同一渠道同一 key 池。
    task = await _resolve_by_path_task(path)
    if task:
        handled = await _lifecycle_intercept(biz, path, request, task)
        if handled is not None:
            return handled

    if request.method in ("GET", "HEAD", "OPTIONS"):
        return await _free_passthrough(biz, path, request)
    return await _billable_passthrough(biz, path, request)


def _lifecycle_kind(route: RouteConfig, method: str, path: str,
                    params: dict) -> str | None:
    """URL 命中的生命周期端点种类：``"query"`` / ``"cancel"`` / None。

    方法收窄（避免把写操作误判成查询）：查询只认 **GET**（POST 形态的查询
    端点罕见且语义不同，不猜）；取消认 GET 之外的写方法（``cancel_path``
    的真实方法由上游定，网关只按 URL 模板识别"这是取消这条任务"）。
    """
    if method == "GET":
        return "query" if nativeapi.probe_id(route, path, params) is not None else None
    return "cancel" if nativeapi.cancel_id(route, path, params) is not None else None


async def _lifecycle_intercept(biz: str, path: str, request: Request, task: dict):
    """既有任务的原生路径操作：命中 probe_path → 查询；cancel_path → 取消。
    两者都不命中返回 None（交回透传语义）。
    """
    method = request.method
    data = task.get("data") or {}
    task_biz = str(data.get("biz") or biz)
    params = dict(request.query_params)

    # 先用进程缓存里的路由做零成本模板判定：不是生命周期端点就立刻交回透传
    # 语义，不为此白付一次 keypool 租约（缓存未命中才必须先租约再判）
    cached = registry.get_cached(task_biz)
    cached_kind = _lifecycle_kind(cached, method, path, params) if cached else None
    if cached is not None and cached_kind is None:
        return None
    if cached is not None and cached_kind == "query" and task.get("status") in TERMINAL:
        # 终态查询本地即权威（快照已落库）：连租约都不必取，零跨服务调用
        await ip_rate_limit(request)
        return JSONResponse(content=nativeapi.snapshot_body(cached, task))

    leased = await _lease_for_task(task_biz, task)
    if leased is None:
        # 渠道已被删/keypool 故障：查询仍可用本地快照兜底（不阻塞客户端轮询），
        # 取消是纯本地资金动作，同样不依赖上游
        if cached is None:
            return None
        key, route = None, cached
    else:
        key, route = leased

    kind = _lifecycle_kind(route, method, path, params)
    if kind is None:
        return None
    await ip_rate_limit(request)     # 生命周期操作免鉴权（task_id 即凭证），按 IP 限流
    if kind == "query":
        return await _native_query(task, route, key, path, params, request)
    log.info("native cancel intercepted: task_id={} biz={}", task["task_id"], task_biz)
    await flow.cancel_task(task["task_id"])
    fresh = await taskstore.get(task["task_id"])
    return JSONResponse(content=nativeapi.snapshot_body(route, fresh or task))


async def _native_query(task: dict, route: RouteConfig, key: KeyLease | None,
                        path: str, params: dict, request: Request):
    """原生查询拦截：报文与上游 100% 同构，**上游 id 与产物直链一律改写**为
    本地 task_id / 网关转存地址（渠道配了 ``result_url_template`` 才改直链）。

    三种情形（前两种零上游往返）：

    - **已终态**：本地即权威（结算/退款已闭环，上游报文快照已落库）→ 回放
      终态快照，不再打上游（上游终态记录有保留期，问了也白付一次公网往返）；
    - **上游还没接单**（异步提交在飞 / HELD 挂起 / 孤儿）或渠道租约不可得
      → 本地快照直出（排队态，绝不 404）；
    - **活跃且已有上游 id**：把 URL 里的 id 换成上游 id 转发探测，缓冲响应后
      逐字节改写 id 与直链；顺带用这份快照推进任务状态（客户端轮询即驱动，
      结果比下一轮 poller 更早可见）。
    """
    local_id = task["task_id"]
    data = task.get("data") or {}
    upstream_task_id = str(data.get("upstream_task_id") or "")
    if task.get("status") in TERMINAL or not upstream_task_id or key is None:
        log.debug("native query answered from local snapshot: task_id={} status={}",
                  local_id, task.get("status"))
        return JSONResponse(content=nativeapi.snapshot_body(route, task))

    fwd_path, fwd_params = nativeapi.swap_id(route.probe_path, path, params, upstream_task_id)
    client = upstream.client_for(route, key)
    fwd_headers = _forward_headers(request, upstream.auth_headers(route, key))
    query = httpx.QueryParams(fwd_params)
    try:
        resp = await client.get(_target_url(fwd_path, str(query)), headers=fwd_headers)
    except httpx.HTTPError as exc:
        await upstream.breaker_report(route.biz, ok=False)
        log.warning("native query upstream unreachable: task_id={} err={}", local_id, exc)
        # 探测失败不影响客户端轮询语义：回本地快照（下一轮再问上游）
        return JSONResponse(content=nativeapi.snapshot_body(route, task))
    await upstream.breaker_report(route.biz, ok=resp.status_code < 500)

    body = _maybe_decompress(resp.content, resp.headers.get("content-encoding", ""))
    headers = _out_headers(resp.headers)
    headers.pop("content-length", None)
    headers.pop("content-encoding", None)
    if len(body) > NATIVE_BUFFER_LIMIT:
        # 异常大的探测报文：放弃 id 改写（宁可透出上游 id 也不吃内存），显式告警
        log.warning("native query response too large to rewrite ids: task_id={} size={}",
                    local_id, len(body))
        return Response(content=body, status_code=resp.status_code, headers=headers)

    url_pairs: list[tuple[str, str]] = []
    if resp.status_code < 400:
        parsed: dict = {}
        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError:
            log.debug("native query response not json: task_id={}", local_id)
        # 客户端轮询驱动状态推进（best-effort）：CAS 保护，重复/迟到快照无副作用
        try:
            await polling.advance_from_probe(task, route, parsed)
        except Exception:
            log.opt(exception=True).debug("native query advance failed: {}", local_id)
        # 产物直链改写：拿这份报文里的原始直链算出对外地址，字节级替换
        url_pairs = resulturl.pairs(
            route, upstream.extract_path(parsed, route.result_path), local_id)

    out = nativeapi.rewrite_ids(body, upstream_task_id, local_id)
    return Response(
        content=resulturl.rewrite_bytes(out, url_pairs),
        status_code=resp.status_code, headers=headers,
        media_type=resp.headers.get("content-type"),
    )


# ---------------------------------------------------------------------------
# 免费透传（GET/HEAD/OPTIONS）
# ---------------------------------------------------------------------------


async def _free_passthrough(biz: str, path: str, request: Request):
    await ip_rate_limit(request)
    leased = await _resolve_free_route(biz)
    if leased is None:
        log.debug("free passthrough with unknown biz: {}", biz)
        return JSONResponse(status_code=404, content={"error": f"unknown biz: {biz}"})
    key, route = leased
    return await _stream_upstream(biz, path, request, route, key, pf=None)


# ---------------------------------------------------------------------------
# 计费路径（POST/PUT/DELETE/PATCH）
# ---------------------------------------------------------------------------


async def _billable_passthrough(biz: str, path: str, request: Request):
    # 直接调用依赖函数（非 Depends 注入）：Header 默认值必须显式传入，
    # 否则拿到的是 Header(None) 声明对象而非真实头值
    pf = await preflight(
        biz, request,
        authorization=request.headers.get("authorization"),
        idempotency_key=request.headers.get("idempotency-key"),
    )
    if pf.replay_task_id:                  # 幂等重放：直接回放首个任务，不重复透传
        task = await taskstore.get(pf.replay_task_id)
        if not task:
            # 重放目标已不存在（行被清理）：preflight 重放短路未做 freeze/route，
            # 不能 fall through（route=None 必撞 assert 500），显式 409 让客户端摘键重试
            return JSONResponse(status_code=409, content={
                "error": "idempotent replay target missing; retry without Idempotency-Key"})
        # 原生提交路径的重放必须回原生形状（客户端只认 task_id_path 那个字段）
        route = await registry.get(biz, model=pf.model)
        if route is not None and nativeapi.match_submit(route, path):
            return JSONResponse(content=nativeapi.submit_body(route, task["task_id"]))
        return JSONResponse(status_code=202, content=flow.public_view(task))

    route = pf.route
    assert route is not None and pf.identity is not None and pf.key is not None

    # 原生提交拦截：走完整异步受理链路（落库即返回本地 task_id，零上游往返），
    # 响应按渠道 task_id_path 塑形为原生报文形状
    if nativeapi.match_submit(route, path):
        view = await flow.create_task(biz, pf.body, pf, action="task", source="native")
        log.info("native submit accepted: task_id={} biz={} path=/{}/{}",
                 view["task_id"], route.biz, biz, path)
        return JSONResponse(content=nativeapi.submit_body(route, view["task_id"]))

    try:
        await taskstore.create(
            task_id=pf.task_id,
            user_id=pf.identity.user_id,
            channel_id=pf.key.key_id,
            action="proxy",
            data={
                # 与 flow.create_task 同构的任务记录：权威 biz 取渠道元数据，
                # model/key_index/request_body 全量落（结算重估与探测钉回的事实源）
                "biz": route.biz,
                "source": "proxy",
                "model": pf.model,
                "token_hash": pf.token.hash,
                "idempotency_key": pf.idem_key,
                # 透传形态不接管用户回调：原始 body 已流式直达上游（含用户
                # 自带 callback_url），网关再 notify 会重复投递
                "callback_url": None,
                "freeze_amount": pf.amount,
                "settled": pf.amount <= 0,
                "freeze_expires_at": pf.freeze_expires_at,
                "key_id": pf.key.key_id,
                "key_index": pf.key.key_index,
                "request_body": pf.body,
                "proxy_path": path,
            },
        )
        if pf.idem_key:
            # 幂等占位回填（同一键 pending → task_id，与 flow.create_task 同时序）
            await idem.set_task_id(pf.token.hash, pf.idem_key, pf.task_id)
    except Exception:
        if pf.idem_key:
            # 落库失败（无任务可回填）：CAS 归还占位，同键重试立即可重建
            await idem.release(pf.token.hash, pf.idem_key)
        raise
    log.info("proxy task created: task_id={} biz={} model={} path=/{}/{}",
             pf.task_id, route.biz, pf.model, biz, path)
    return await _stream_upstream(biz, path, request, route, pf.key, pf=pf)


# ---------------------------------------------------------------------------
# 原样流式透传
# ---------------------------------------------------------------------------


async def _stream_upstream(biz: str, path: str, request: Request,
                           route: RouteConfig, key_lease: KeyLease, pf=None):
    client = upstream.client_for(route, key_lease)
    fwd_headers = _forward_headers(request, upstream.auth_headers(route, key_lease))
    content = await _request_content(request)
    req = client.build_request(
        request.method, _target_url(path, request.url.query),
        headers=fwd_headers, content=content,
    )

    try:
        resp = await client.send(req, stream=True)
    except httpx.HTTPError as exc:
        await upstream.breaker_report(biz, ok=False)
        log.warning("proxy upstream unreachable: biz={} path=/{} err={}", biz, path, exc)
        if pf:
            await taskstore.cas(pf.task_id, ACTIVE, FAILURE, fail_reason=str(exc)[:500])
            if pf.amount > 0:
                await queue.publish_cancel(pf.task_id, pf.token.raw)
        return JSONResponse(status_code=502, content={"error": f"upstream unreachable: {exc}"})
    await upstream.breaker_report(biz, ok=resp.status_code < 500)

    out_headers = _out_headers(resp.headers)
    content_encoding = resp.headers.get("content-encoding", "")
    buf = bytearray()

    async def stream_and_finalize():
        nonlocal buf
        try:
            async for chunk in resp.aiter_raw():
                if len(buf) < BUFFER_LIMIT:
                    buf.extend(chunk[: BUFFER_LIMIT - len(buf)])
                yield chunk
        finally:
            await resp.aclose()
            if pf:
                await _finalize_proxy(biz, route, pf, resp.status_code, bytes(buf),
                                      content_encoding)

    return StreamingResponse(stream_and_finalize(), status_code=resp.status_code,
                             headers=out_headers)


async def _finalize_proxy(biz, route, pf, status_code: int, body: bytes,
                          content_encoding: str = "") -> None:
    """透传结束后的收尾：成功则回填上游任务 ID 并接入状态闭环；失败则取消冻结"""
    body = _maybe_decompress(body, content_encoding)
    if status_code >= 400:
        log.warning("proxy upstream rejected: task_id={} biz={} status={}",
                    pf.task_id, biz, status_code)
        await taskstore.cas(pf.task_id, ACTIVE, FAILURE, fail_reason=f"upstream {status_code}: {body[:400]!r}")
        if pf.amount > 0:
            await queue.publish_cancel(pf.task_id, pf.token.raw)
        return
    upstream_task_id = None
    try:
        parsed = json.loads(body) if body else {}
        upstream_task_id = upstream.extract_path(parsed, route.task_id_path)
    except json.JSONDecodeError:
        parsed = {}
    await taskstore.patch_data(pf.task_id, {"upstream_task_id": upstream_task_id}, status=QUEUED)
    log.info("proxy submitted: task_id={} upstream_task_id={} status={}",
             pf.task_id, upstream_task_id, status_code)
    if not route.supports_callback and upstream_task_id:
        await queue.schedule_poll(pf.task_id, 5)
    # 注意：同步 2xx 不直接 settle —— 视频任务为异步，统一由 callback/poll/sweeper 闭环结算，
    # 避免与终态事件重复（billing 按 request_id 幂等，Sweeper 会兜底漏网之鱼）
