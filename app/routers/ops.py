"""运维端点：队列观测、攒批视图、任务诊断与补号。

安全：鉴权统一走 ``app.deps.admin.require_admin``（``X-Admin-Token``）。
**未配置 ``ADMIN_TOKEN`` 时整个 /ops/* 返回 404（fail-closed）**——
与 ``/admin/*`` 同一语义。``/ops/requeue``、``/ops/dlq/replay`` 是写操作，
更不允许裸奔。

``/ops/batches`` 刻意留在管理面而不是用户面：归组键可能是 ``token:model`` 形式
（``BATCH_GROUP_BY=token_model``）或客户端自定义串（``X-Batch-Key``），
**含 token 指纹**——不能给任意已鉴权调用者看。
"""

from fastapi import APIRouter, Depends, HTTPException

from app import queue
from app.deps.admin import require_admin
from app.logging import log
from app.services import batching, taskstore, tokensession

router = APIRouter()


@router.get("/ops/queue")
async def queue_stats(_: None = Depends(require_admin)):
    """队列健康快照：pending 积压 / delayed 延迟任务 / dlq 死信 / 任务状态分布"""
    return await queue.queue_stats()


@router.get("/ops/batches")
async def batch_stats(_: None = Depends(require_admin)):
    """攒批概览：每个归组键攒了多少条、还有多久到期。

    回答的是 DB 说不清的事——DB 只有逐条的 ``data.batch_due_at``，没有「这一批现在
    几条」。``due_in`` 为负表示**已过期仍在等**（T 触发的延迟任务漏了，等 sweep 的
    超期兜底捞回），那正是排障最需要看到的信号。
    """
    view = await batching.stats()
    log.info("ops batch stats: {} batch(es)", len(view["batches"]))
    return view


@router.get("/ops/tasks/{task_id}")
async def task_diagnostics(task_id: str, _: None = Depends(require_admin)):
    """任务内部诊断视图（排障用）。

    ``token_session`` 只给出存在性与剩余 TTL，**令牌本体绝不离开 Redis**。
    敏感字段（token_hash 截断、request_body 不回显）。
    """
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
        "duration": taskstore.duration_seconds(task),   # 耗时（秒）：终态-创建，单位已归一
        "fail_reason": task.get("fail_reason") or "",
        "data": {
            "source": data.get("source"),
            "model": data.get("model"),
            "upstream_base_url": data.get("upstream_base_url"),
            "request_path": data.get("request_path"),
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
async def requeue_task(task_id: str, _: None = Depends(require_admin)):
    """手动补单：立即把 ``/queue`` 任务重新放入提交队列（worker 执行上游提交）。"""
    await queue.publish_queue_submit(task_id)
    return {"requeued": task_id}


@router.post("/ops/dlq/replay")
async def dlq_replay(limit: int = 100, _: None = Depends(require_admin)):
    """死信重放（补号）：把死信事件重新入队"""
    replayed = await queue.replay_dlq(limit)
    return {"replayed": replayed}
