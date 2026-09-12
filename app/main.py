"""FastAPI 应用装配入口。

- 路由注册顺序即 Starlette 首匹配优先级：healthz → ops → admin →
  ``/queue/{path:path}``（通配，**永远最后**；字面前缀路由必须先于通配注册）。
- 对外形态只有 ``/queue/{上游原生路径}``（ADR-010）：鉴权与计费全部下沉上游，
  网关零资金动作、不持有上游 key。
- 后台异步协同（提交/收敛/通知）由 taskiq 进程承担：
  ``taskiq worker app.queue:broker`` + ``taskiq scheduler app.queue:scheduler``。
- 可观测装配（logfire）收敛到 ``app.observability`` 单点——web 与 worker 只
  传进程形态，配置口径不再两处手工同步。
- 冒烟纪律：``from app.main import app`` 在无 DB/Redis 环境下必须可导入——
  引擎/客户端全部惰性创建。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import observability
from app.db import close_db
from app.errors import register_exception_handlers
from app.logging import setup_logging
from app.services import httpc


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """退出清理：关上游共享连接池 / DB / Redis。"""
    try:
        yield
    finally:
        await httpc.close_all()
        await close_db()
        # Redis 连接池同样要显式关：否则优雅停机期间连接挂在服务端
        # wait_timeout 才回收，滚动发布时会短暂堆高连接数
        from app.redis import r

        await r.aclose()


def create_app() -> FastAPI:
    """应用工厂：日志装配 → 观测初始化 → 异常处理器 → 路由注册（顺序不可换）。"""
    setup_logging()
    app = FastAPI(title="atask-service", lifespan=lifespan)

    observability.setup("web", app=app)
    register_exception_handlers(app)

    from app.healthz import router as health_router
    from app.routers.admin import router as admin_router
    from app.routers.queue_task import router as queue_task_router
    from app.routers.ops import router as ops_router

    app.include_router(health_router)     # /healthz/live /healthz/ready
    app.include_router(ops_router)        # /ops/*（队列观测与补号）
    app.include_router(admin_router)      # /admin/*（看板与运行时热配置）
    # 通配永远最后：``/queue/{path:path}`` 是唯一的可变路径路由，必须排在
    # 字面前缀路由之后，否则会吞掉它们。
    app.include_router(queue_task_router)  # /queue/{path:path}（ADR-010 唯一对外形态）
    return app


app = create_app()
