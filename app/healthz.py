"""健康检查端点（容器探针）。

- ``/healthz/live``：恒 200（零依赖，进程活着即通过）；
- ``/healthz/ready``：Redis ``PING`` + DB ``SELECT 1``，全过 200 否则 503；
  不抛异常，直接构造 Response（探针路径不走 error 处理器）。
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.db import get_session_factory
from app.redis import r

log = logging.getLogger("gateway.healthz")
router = APIRouter()


@router.get("/healthz/live")
async def healthz_live() -> dict[str, str]:
    """liveness 探针：零依赖恒 200。"""
    return {"status": "ok"}


@router.get("/healthz/ready")
async def healthz_ready() -> JSONResponse:
    """readiness 探针：Redis + DB 任一不可用即 503（编排层摘流量，不重启）。"""
    checks: dict[str, str] = {}

    try:
        await r.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"fail: {type(exc).__name__}"
        log.warning("readiness redis check failed: %s", type(exc).__name__)

    try:
        sf = get_session_factory()
        async with sf() as session:
            await session.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as exc:
        checks["db"] = f"fail: {type(exc).__name__}"
        log.warning("readiness db check failed: %s", type(exc).__name__)

    ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"status": "ok" if ok else "unavailable", "checks": checks},
    )
