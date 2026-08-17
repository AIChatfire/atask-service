"""运维端点：队列观测、任务诊断与补号。
安全：若配置了 GW_ADMIN_TOKEN 则校验 X-Admin-Token；未配置时请在 Ingress 层限制内网访问。
"""

from fastapi import APIRouter, Header, HTTPException

from app import queue
from app.config import settings
from app.logging import log
from app.services import taskstore, tokensession

router = APIRouter()


def _guard(x_admin_token: str | None) -> None:
    if settings.admin_token and x_admin_token != settings.admin_token:
        raise HTTPException(401, "invalid admin token")


@router.get("/ops/queue")
async def queue_stats(x_admin_token: str | None = Header(None)):
    """队列健康快照：pending 积压 / delayed 延迟任务 / dlq 死信 / 任务状态分布"""
    _guard(x_admin_token)
    return await queue.queue_stats()


@router.get("/ops/tasks/{task_id}")
async def task_diagnostics(task_id: str, x_admin_token: str | None = Header(None)):
    """任务内部诊断视图（排障用）。

    new-api 渠道侧轮询不带用户 sk——这里提供 task_id → 令牌会话状态的查询处：
    ``token_session`` 只给出存在性与剩余 TTL，**令牌本体绝不离开 Redis**。
    敏感字段（token_hash 截断、request_body 不回显）。
    """
    _guard(x_admin_token)
    task = await taskstore.get(task_id)
    if not task:
        raise HTTPException(404, "task not found")
    data = task.get("data") or {}
    token_hash = str(data.get("token_hash") or "")
    view = {
        "task_id": task["task_id"],
        "status": task["status"],
        "action": task.get("action"),
        "user_id": task.get("user_id"),
        "channel_id": task.get("channel_id"),
        "submit_time": task.get("submit_time"),
        "updated_at": task.get("updated_at"),
        "finish_time": task.get("finish_time") or 0,
        "fail_reason": task.get("fail_reason") or "",
        "data": {
            "biz": data.get("biz"),
            "source": data.get("source"),
            "model": data.get("model"),
            "freeze_amount": data.get("freeze_amount"),
            "settled": data.get("settled"),
            "settled_amount": data.get("settled_amount"),
            "key_id": data.get("key_id"),
            "key_index": data.get("key_index"),
            "upstream_task_id": data.get("upstream_task_id"),
            "upstream_status": data.get("upstream_status"),
            "has_callback_url": bool(data.get("callback_url")),
            "has_idempotency_key": bool(data.get("idempotency_key")),
            "token_hash_prefix": token_hash[:12],
        },
        "token_session": await tokensession.session_info(task_id),
    }
    log.info("ops task diagnostics: task_id={} status={} token_session={}",
             task_id, view["status"], view["token_session"]["exists"])
    return view


@router.post("/ops/requeue/{task_id}")
async def requeue_task(task_id: str, x_admin_token: str | None = Header(None)):
    """手动补单：立即把任务重新放入探测队列"""
    _guard(x_admin_token)
    await queue.schedule_poll(task_id, 0)
    return {"requeued": task_id}


@router.post("/ops/dlq/replay")
async def dlq_replay(limit: int = 100, x_admin_token: str | None = Header(None)):
    """死信重放（补号）：把死信事件重新入队"""
    _guard(x_admin_token)
    replayed = await queue.replay_dlq(limit)
    return {"replayed": replayed}
