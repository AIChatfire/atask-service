"""``/queue`` 中继形态（ADR-010 的唯一对外形态）。

三件套（通配 ``{path:path}``，``{path}`` 是**上游原生路径**，如 ``v1/tasks``）：

| 方法 | 路径 | 语义 |
|---|---|---|
| POST | ``/queue/{path}`` | 受理：落库即返回本地 task_id（202 + Location），上游提交交 worker |
| GET | ``/queue/{path}/{task_id}`` | 查询：末段是本地 task_id 时走视图（非终态按需探测，终态零上游往返） |
| GET | ``/queue/{path}`` | 免费透传：末段不是本地 task_id 时原样转发上游（不落 tasks 行） |
| DELETE | ``/queue/{path}/{task_id}`` | 取消：本地 CAS 置 CANCELED + 尽力源头止损 |

路由层**只做分派与响应塑形**，生命周期语义全在
``app/services/relayflow``（分层约定：路由不碰 DB / 出站）。

**本模块的前缀 ``/queue`` 是「网关自身端点」，不是对外前缀**（两层口径，见本仓库
ADR-010 §1）：

- 对外统一前缀是 ``/async``（与 stask 一致）；nginx 把 ``/async/*`` 反代到本服务并
  重写为 ``/queue/*``（``proxy_pass http://atask:8000/queue/;``），同步类路径先分流给
  stask——**分流表在 nginx，不在本仓库**；
- 本服务内部保留 ``/queue`` 是为了与机制名同源（``data.source='queue'``、``queue_*``
  词根、``task_id`` 前缀 ``queue_``）——对外承诺「异步交付」，内部机制是「排队接管」。

**挂载顺序（见 app/main.py）**：``/queue/{path:path}`` 是唯一的通配路径，必须
**最后注册**——Starlette 按注册顺序首匹配，通配若排在字面前缀路由（``/healthz/*``、
``/ops/*``、``/admin/*``）之前，会把它们整片吞掉且不报错。这条由静态门禁
``tests/test_static_gates.py::test_router_mount_order`` 机械保证（ADR-010 后旧链路
的字面路由已删除，通配只需排在剩余字面路由之后）。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from app.services import nativeapi, relayflow

router = APIRouter()


def _last_segment(path: str) -> str:
    """路径最后一段（任务 id 判定只看它，与旧原生拦截同口径）。"""
    return path.rstrip("/").rsplit("/", 1)[-1]


@router.post("/queue/{path:path}", status_code=202)
async def queue_create(path: str, request: Request):
    """受理上游异步任务：202 + ``{task_id, status}`` + ``Location`` 头。"""
    view = await relayflow.create_queue_task(request, path)
    location = f"/queue/{path.strip('/')}/{view['task_id']}"
    return JSONResponse(status_code=202, content=view, headers={"Location": location})


@router.get("/queue/{path:path}")
async def queue_get(path: str, request: Request):
    """末段是本地 task_id → 视图；否则按免费透传转发上游。"""
    last = _last_segment(path)
    if nativeapi.is_local_id(last):
        return await relayflow.view_queue_task(last, path)
    return await relayflow.free_queue_get(path, request)


@router.delete("/queue/{path:path}")
async def queue_delete(path: str):
    """末段是本地 task_id → 取消；否则无任务可取消（404）。"""
    last = _last_segment(path)
    if not nativeapi.is_local_id(last):
        raise HTTPException(404, "task not found")
    return await relayflow.cancel_queue_task(last)
