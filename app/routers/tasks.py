"""通用任务形态：/{biz}/v1/tasks
POST 需鉴权（preflight 内含计费）；GET 免鉴权（task_id 即凭证）。
不提供 list 接口 —— 无鉴权体系下枚举即泄露。
"""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.deps.preflight import Preflight, preflight
from app.deps.ratelimit import ip_rate_limit
from app.services import flow

router = APIRouter()


@router.post("/{biz}/v1/tasks", status_code=202)
async def create_task(biz: str, request: Request, pf: Preflight = Depends(preflight)):
    view = await flow.create_task(biz, pf.body, pf, action="task", source="tasks")
    return JSONResponse(status_code=202, content=view)


@router.get("/{biz}/v1/tasks/{task_id}")
async def get_task(task_id: str, _=Depends(ip_rate_limit)):
    return await flow.view_task(task_id)


@router.post("/{biz}/v1/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, _=Depends(ip_rate_limit)):
    return await flow.cancel_task(task_id)
