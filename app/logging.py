"""日志装配：loguru 统一入口（web 与 taskiq worker 进程共用）。

- 业务模块一律 ``from app.logging import log``——loguru logger 单例，
  ``{name}`` 自动带调用方模块名（不再需要 ``logging.getLogger(__name__)``）；
- ``setup_logging()`` 在 web（``app.main.create_app``）与 worker
  （``app.queue.ObservabilityMiddleware.startup``）入口各调用一次：
  重建 stderr sink，并把 stdlib logging（uvicorn / sqlalchemy / taskiq /
  gunicorn hooks）桥接进 loguru，全进程日志同一格式同一出口；
- 级别由 ``GW_LOG_LEVEL`` 控制（默认 INFO；高频探测成功路径一律 DEBUG，
  排障时调到 DEBUG 即可看全链路）。

安全纪律（红线：用户令牌/上游 key 不进日志）：
- ``backtrace=False, diagnose=False``——异常回溯不带局部变量值
  （billing 任务参数含用户 sk，开启 diagnose 会把帧变量打进日志）；
- 业务日志只打 task_id / user_id / channel_id / 金额 / 状态，绝不打
  raw token 与上游 key。
"""

from __future__ import annotations

import logging
import sys

from loguru import logger

from app.config import settings

#: 业务模块统一入口：``from app.logging import log``
log = logger

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


class InterceptHandler(logging.Handler):
    """stdlib logging → loguru 桥接（第三方库日志收编为同一格式）。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        # depth=2：跳过 logging 帧，让 loguru 记录真实的调用位置
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging() -> None:
    """装配 loguru（幂等）：stderr sink + stdlib 桥接。"""
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level.upper(),
        format=_FORMAT,
        backtrace=False,
        diagnose=False,
    )
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
