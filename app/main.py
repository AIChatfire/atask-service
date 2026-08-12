"""FastAPI 入口。API 进程只跑路由配置热更新一个后台循环；
异步任务由独立的 taskiq 进程执行：
  taskiq worker    app.queue:broker --max-async-tasks 100
  taskiq scheduler app.queue:scheduler
路由注册顺序：callback → tasks → videos → proxy（通配必须最后）。
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.db import engine
from app.redis import r
from app.routers import callback, ops, proxy, tasks, videos
from app.services import upstream
from app.services.registry import registry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("gateway.main")


def _init_logfire(app: FastAPI) -> None:
    if not settings.logfire_enabled:
        return
    try:
        import logfire

        # 脱敏只针对凭证类字段；result（上游结果直链）正常记录，便于排查
        logfire.configure(
            token=settings.logfire_token,
            scrubbing=logfire.ScrubbingOptions(extra_patterns=["authorization", "Bearer", "sk-"]),
        )
        # 排除高频轮询路径：GET 状态查询不产生 span，状态变化由 statelog 单独记录
        # （正则按 URL 排除，不影响同路径的 POST 创建 / cancel）
        logfire.instrument_fastapi(
            app,
            excluded_urls=r"^/healthz$|^/readyz$|^/[^/]+/v1/(tasks|videos)/[^/]+$",
        )
        # 不做全局 instrument_httpx：控制面客户端在 services/httpc.py 里按需埋点，
        # 上游探测调用高频不埋点
        logfire.instrument_sqlalchemy(engine=engine.sync_engine)
        log.info("logfire enabled")
    except Exception:
        log.exception("logfire init failed, continue without it")


@asynccontextmanager
async def lifespan(app: FastAPI):
    registry.load_file()
    refresher = asyncio.create_task(registry.refresh_loop())
    log.info("gateway api started (async tasks run in taskiq worker/scheduler processes)")
    try:
        yield
    finally:
        refresher.cancel()
        await asyncio.gather(refresher, return_exceptions=True)
        await upstream.close_all()
        await r.aclose()
        await engine.dispose()


app = FastAPI(title="async-gateway", lifespan=lifespan)
_init_logfire(app)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    from sqlalchemy import text

    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    await r.ping()
    return {"status": "ready"}


# 注册顺序即匹配优先级：固定形态在前，通配透传最后
app.include_router(callback.router)
app.include_router(ops.router)
app.include_router(tasks.router)
app.include_router(videos.router)
app.include_router(proxy.router)
