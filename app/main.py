"""FastAPI 应用装配入口。

- 路由注册顺序即 Starlette 首匹配优先级：healthz → 上游 callback → ops →
  tasks/videos 业务端点 → 动态透传（/{biz}/{path:path} 通配，永远最后）。
- **网关零路由文件**：上游配置（base_url / 提交探测路径 / 渠道覆盖 / 凭证）
  唯一事实源是 keypool 渠道元数据，随租约实时下发（app.services.registry）；
  接入新模型只在 new-api 渠道上配置，网关不改代码、不配文件。
- 后台异步协同（探测/结算/通知/补数）由 taskiq 进程承担：
  ``taskiq worker app.queue:broker`` + ``taskiq scheduler app.queue:scheduler``。
- 冒烟纪律：``from app.main import app`` 在无 DB/Redis 环境下必须可导入——
  引擎/客户端全部惰性创建。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.db import close_db
from app.errors import register_exception_handlers
from app.logging import log, setup_logging
from app.services import httpc, upstream


def _setup_logfire(app: FastAPI) -> None:
    """GW_LOGFIRE_ENABLED=true 时接入 logfire（无 token 走本地，不阻塞启动）。"""
    if not settings.logfire_enabled:
        return
    try:
        import logfire

        logfire.configure(
            service_name="async-gateway",
            service_version=settings.app_version,
            environment=settings.app_env,
            token=settings.logfire_token,
            send_to_logfire="if-token-present",
            scrubbing=logfire.ScrubbingOptions(
                extra_patterns=["api_key", "access_token", "authorization", "sk-"]
            ),
            console=False,
        )
        logfire.instrument_fastapi(app, excluded_urls=settings.logfire_excluded_urls)
    except Exception:
        log.opt(exception=True).warning("logfire setup failed, continue without it")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """退出清理：关上游连接池/DB。

    启动无需加载任何路由文件——上游配置（base_url/路径/覆盖/凭证）全部在
    keypool 渠道元数据里，随租约实时下发（app.services.registry）。
    """
    try:
        yield
    finally:
        await upstream.close_all()
        await httpc.close_all()
        await close_db()


def create_app() -> FastAPI:
    """应用工厂：日志装配 → 观测初始化 → 异常处理器 → 路由注册（顺序不可换）。"""
    setup_logging()
    app = FastAPI(title="atask-service", lifespan=lifespan)

    _setup_logfire(app)
    register_exception_handlers(app)

    from app.healthz import router as health_router
    from app.routers.callback import router as callback_router
    from app.routers.ops import router as ops_router
    from app.routers.proxy import router as proxy_router
    from app.routers.tasks import router as tasks_router
    from app.routers.videos import router as videos_router

    app.include_router(health_router)     # /healthz/live /healthz/ready
    app.include_router(callback_router)   # /callback/{biz}/{task_id}（上游 webhook）
    app.include_router(ops_router)        # /ops/*（队列观测与补号）
    app.include_router(tasks_router)      # /{biz}/v1/tasks（通用任务形态）
    app.include_router(videos_router)     # /{biz}/v1/videos（new-api 兼容形态）
    app.include_router(proxy_router)      # ANY /{biz}/{path:path} —— 永远最后
    return app


app = create_app()
