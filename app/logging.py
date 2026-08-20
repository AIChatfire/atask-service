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
from types import FrameType
from typing import Any

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
        frame: FrameType | None = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


#: loguru→logfire 桥接是否已挂接（``logger.remove()`` 重建 sink 后由
#: ``setup_logging`` 依据它自动补挂；loguru sink 无稳定标记位，用模块态记录）
_logfire_attached: bool = False


def setup_logging() -> None:
    """装配 loguru（幂等）：stderr sink + stdlib 桥接。

    ``logger.remove()`` 会摘掉 logfire 桥接 sink——若本进程已挂接
    （重复调用场景），末尾自动补挂。
    """
    global _logfire_attached
    reattach = _logfire_attached
    _logfire_attached = False
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level.upper(),
        format=_FORMAT,
        backtrace=False,
        diagnose=False,
    )
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # worker 日志压到最干净
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger("taskiq").setLevel(logging.WARNING)

    if reattach:
        attach_logfire_handler()


def attach_logfire_handler() -> None:
    """loguru → logfire 桥接（幂等）：业务日志与 OTel span 同 trace 汇聚，
    logfire 平台不再只有孤零零的 httpx span（排查 4xx 时 body 直接可查）。

    必须在 ``logfire.configure()`` **成功之后**调用——先挂后 configure 时
    启动期日志会被 logfire no-op 吞掉。失败降级为纯 stderr，绝不阻塞启动。
    """
    global _logfire_attached
    if not settings.logfire_enabled or _logfire_attached:
        return
    try:
        import logfire

        # loguru_handler() 返回 add() 的 kwargs dict {sink, format}：
        # 补 level 键控噪（高频 DEBUG 探测日志不进 logfire），解包传参
        config = logfire.loguru_handler()
        config["level"] = settings.log_level.upper()
        logger.add(**config)
        _logfire_attached = True
    except Exception:
        logger.opt(exception=True).warning("logfire loguru handler attach failed")


def logfire_event(level: str, event: str, **fields: Any) -> None:
    """logfire 结构化事件公共发射点（``GW_LOGFIRE_ENABLED`` 时才真正发出）。

    与 stderr 文本日志互补：字段化（可查询、可告警）、与 span 同 trace。
    任何失败静默——观测链路绝不影响业务主流程。
    """
    if not settings.logfire_enabled:
        return
    try:
        import logfire

        getattr(logfire, level)(event, **fields)
    except Exception:
        pass
