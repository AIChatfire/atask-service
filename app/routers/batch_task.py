"""``/batch`` 中继形态（ADR-010 的唯一对外形态）。

三件套（通配 ``{path:path}``，``{path}`` 是**上游原生路径**，如 ``v1/tasks``）：

| 方法 | 路径 | 语义 |
|---|---|---|
| POST | ``/batch/{path}`` | 受理：落库即返回本地 task_id（202 + Location），上游提交交 worker |
| GET | ``/batch/{path}/{task_id}`` | 查询：末段是本地 task_id 时走视图（非终态按需探测，终态零上游往返） |
| GET | ``/batch/{path}`` | 免费透传：末段不是本地 task_id 时原样转发上游（不落 tasks 行） |
| DELETE | ``/batch/{path}/{task_id}`` | 取消：本地 CAS 置 CANCELED + 尽力源头止损 |

路由层**只做分派与响应塑形**，生命周期语义全在
``app/services/relayflow``（分层约定：路由不碰 DB / 出站）。

**为什么前缀是 ``/batch`` 而不是 ``/async``**（不是随意择名）：

1. 本仓库是**异步转异步**——上游本身就是异步任务，网关只是再包一层统一受理并
   持有任务事实源；``/async`` 描述的是「把同步接口异步化」，那正是 **stask 的
   语义**（stask 与 atask 是两个独立服务，见本仓库 ADR-008）。用 ``/batch``
   才不把 atask 误读成 stask。
2. **nginx 前缀分流冲突**（更硬的理由）：``docs/stask-service-design.md`` §7 的
   nginx 方案里 ``location /async/ { proxy_pass http://stask:8000; }``——同域名下
   ``/async/`` 已经归 stask，两个服务不可能共用同一前缀。

**挂载顺序（见 app/main.py）**：``/batch/{path:path}`` 是唯一的通配路径，必须
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


@router.post("/batch/{path:path}", status_code=202)
async def batch_create(path: str, request: Request):
    """受理上游异步任务：202 + ``{task_id, status}`` + ``Location`` 头。"""
    view = await relayflow.create_batch_task(request, path)
    location = f"/batch/{path.strip('/')}/{view['task_id']}"
    return JSONResponse(status_code=202, content=view, headers={"Location": location})


@router.get("/batch/{path:path}")
async def batch_get(path: str, request: Request):
    """末段是本地 task_id → 视图；否则按免费透传转发上游。"""
    last = _last_segment(path)
    if nativeapi.is_local_id(last):
        return await relayflow.view_batch_task(last, path)
    return await relayflow.free_batch_get(path, request)


@router.delete("/batch/{path:path}")
async def batch_delete(path: str):
    """末段是本地 task_id → 取消；否则无任务可取消（404）。"""
    last = _last_segment(path)
    if not nativeapi.is_local_id(last):
        raise HTTPException(404, "task not found")
    return await relayflow.cancel_batch_task(last)
