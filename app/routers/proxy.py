"""动态透传形态：/{biz}/{原生路径} —— 必须最后注册（通配）。
- GET/HEAD/OPTIONS：免费透传（按 IP 限流），不产生任务
- 其余方法：计费透传（preflight），同样落 tasks 行，结算走 callback/poll/sweeper 闭环
- 全程流式：大文件不进内存；剥离 hop-by-hop 头

同构约定：计费透传落下的 tasks 行与 flow.create_task **同一份数据形态**
（biz/model/key_index/request_body 全量快照）——终态结算重估、探测钉回
渠道、令牌会话取用对所有入口一视同仁，不为透传形态保留异构兼容路径。
"""

import json

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app import queue
from app.deps.preflight import preflight
from app.deps.ratelimit import ip_rate_limit
from app.logging import log
from app.schemas import ACTIVE, FAILURE, QUEUED
from app.services import flow, providers, taskstore, upstream
from app.services.providers import KeyLeaseError
from app.services.registry import registry, route_from_lease

router = APIRouter()

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "host",
    "authorization",   # 用户 sk 令牌绝不透传给上游
}
BUFFER_LIMIT = 256 * 1024   # 小响应缓冲上限，用于提取 upstream_task_id


def _forward_headers(request: Request, extra: dict) -> dict:
    # extra（渠道凭证等注入头）统一小写归一，并按小写名剔除客户端同义头，
    # 保证每个头恰好出现一次（extra 优先），杜绝大小写碰撞产生重复鉴权头
    extra_lc = {k.lower(): v for k, v in extra.items()}
    base = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() not in extra_lc
    }
    return base | extra_lc


@router.api_route("/{biz}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def dynamic_proxy(biz: str, path: str, request: Request):
    billable = request.method not in ("GET", "HEAD", "OPTIONS")
    pf = None
    if billable:
        # 直接调用依赖函数（非 Depends 注入）：Header 默认值必须显式传入，
        # 否则拿到的是 Header(None) 声明对象而非真实头值
        pf = await preflight(
            biz, request,
            authorization=request.headers.get("authorization"),
            idempotency_key=request.headers.get("idempotency-key"),
        )
        if pf.replay_task_id:                  # 幂等重放：直接回放首个任务，不重复透传
            task = await taskstore.get(pf.replay_task_id)
            if task:
                return JSONResponse(status_code=202, content=flow.public_view(task))
        route = pf.route
        assert route is not None and pf.identity is not None and pf.key is not None
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
        log.info("proxy task created: task_id={} biz={} model={} path=/{}/{}",
                 pf.task_id, route.biz, pf.model, biz, path)
        key_lease = pf.key
        assert key_lease is not None
    else:
        await ip_rate_limit(request)
        try:
            key_lease = await providers.keys.lease(biz, model="")
        except KeyLeaseError:
            # 免费 GET 不带 model，keypool 空 model 返回 40010：回落为按进程缓存
            # 里该 biz 最近使用的渠道 channel_id 直达反查（钉渠道）；缓存未命中才 404
            cached = registry.get_cached(biz)
            if not cached or not cached.channel_id:
                log.debug("free passthrough with unknown biz: {}", biz)
                return JSONResponse(status_code=404, content={"error": f"unknown biz: {biz}"})
            try:
                key_lease = await providers.keys.lease(
                    biz, model="", key_id=cached.channel_id)
            except KeyLeaseError:
                log.debug("free passthrough pin-back failed: biz={} channel_id={}",
                          biz, cached.channel_id)
                return JSONResponse(status_code=404, content={"error": f"unknown biz: {biz}"})
        route = registry.remember(route_from_lease(biz, key_lease))

    client = upstream.client_for(route, key_lease)
    fwd_headers = _forward_headers(request, upstream.auth_headers(route, key_lease))
    req = client.build_request(request.method, f"/{path}", headers=fwd_headers, content=request.stream())

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

    out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}
    buf = bytearray()

    async def stream_and_finalize():
        nonlocal buf
        try:
            async for chunk in resp.aiter_raw():
                if len(buf) < BUFFER_LIMIT:
                    buf.extend(chunk)
                yield chunk
        finally:
            await resp.aclose()
            if pf:
                await _finalize_proxy(biz, route, pf, resp.status_code, bytes(buf))

    return StreamingResponse(stream_and_finalize(), status_code=resp.status_code, headers=out_headers)


async def _finalize_proxy(biz, route, pf, status_code: int, body: bytes) -> None:
    """透传结束后的收尾：成功则回填上游任务 ID 并接入状态闭环；失败则取消冻结"""
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
