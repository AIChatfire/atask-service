"""适配器包（SPEC §3.2）。

``base`` 提供抽象契约与注册表；具体适配器（W5：kling/seedance）在各自
模块底部 ``register(XxxAdapter())`` 自注册。本 ``__init__`` 尽力导入已知
适配器模块触发自注册——未实现的模块静默跳过（骨架期 kling/seedance 尚不存在，
``import app.adapters`` 必须可用）。
"""

from __future__ import annotations

import importlib
import logging

from app.adapters.base import (
    CanonicalTaskRequest,
    SubmitContext,
    SubmitResult,
    TaskSnapshot,
    TaskStatus,
    UpstreamAdapter,
    UpstreamBizError,
    UpstreamError,
    UpstreamRateLimitError,
    UsageEstimate,
    get_adapter,
    register,
    registered_adapters,
)

logger = logging.getLogger(__name__)

for _mod in ("kling", "seedance"):
    try:
        importlib.import_module(f"app.adapters.{_mod}")
    except ImportError as exc:  # 适配器模块尚未交付（W5）时静默跳过
        logger.debug("adapter module %s not importable yet: %s", _mod, exc)

__all__ = [
    "CanonicalTaskRequest",
    "SubmitContext",
    "SubmitResult",
    "TaskSnapshot",
    "TaskStatus",
    "UpstreamAdapter",
    "UpstreamBizError",
    "UpstreamError",
    "UpstreamRateLimitError",
    "UsageEstimate",
    "get_adapter",
    "register",
    "registered_adapters",
]
