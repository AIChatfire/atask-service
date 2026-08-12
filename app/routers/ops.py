"""运维端点：队列观测与补号。
安全：若配置了 GW_ADMIN_TOKEN 则校验 X-Admin-Token；未配置时请在 Ingress 层限制内网访问。
"""

from fastapi import APIRouter, Header, HTTPException

from app import queue
from app.config import settings

router = APIRouter()


def _guard(x_admin_token: str | None) -> None:
    if settings.admin_token and x_admin_token != settings.admin_token:
        raise HTTPException(401, "invalid admin token")


@router.get("/ops/queue")
async def queue_stats(x_admin_token: str | None = Header(None)):
    """队列健康快照：pending 积压 / delayed 延迟任务 / dlq 死信 / 任务状态分布"""
    _guard(x_admin_token)
    return await queue.queue_stats()


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
