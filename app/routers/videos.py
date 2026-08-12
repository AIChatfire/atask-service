"""NewAPI 兼容形态：/{biz}/v1/videos
与 tasks 共用同一套创建/查询流程，仅响应字段按 new-api 视频任务约定封装。
部署时对照你们 new-api 版本的字段（status 枚举大小写、progress 格式等）做契约校准。
"""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.deps.preflight import Preflight, preflight
from app.deps.ratelimit import ip_rate_limit
from app.services import flow

router = APIRouter()


def _video_view(view: dict) -> dict:
    return {
        "task_id": view["task_id"],
        "status": str(view.get("status", "")).lower(),   # new-api 视频任务为小写枚举，按实际版本校准
        "progress": view.get("progress", "0%"),
        "fail_reason": view.get("fail_reason", ""),
        "result": view.get("result"),
    }


@router.post("/{biz}/v1/videos", status_code=202)
async def create_video(biz: str, request: Request, pf: Preflight = Depends(preflight)):
    view = await flow.create_task(biz, pf.body, pf, action="video", source="videos")
    return JSONResponse(status_code=202, content=_video_view(view))


@router.get("/{biz}/v1/videos/{task_id}")
async def get_video(task_id: str, _=Depends(ip_rate_limit)):
    return _video_view(await flow.view_task(task_id))
