"""共享状态常量。

内部状态枚举与 new-api tasks 表状态口径一致（SUBMITTED/IN_PROGRESS/SUCCESS/
FAILURE/CANCELED），``ACTIVE``/``TERMINAL`` 元组为唯一判断点。

ADR-010 后网关零资金动作、无 HELD 挂起，故状态集合随之收敛。
"""

from __future__ import annotations

SUBMITTED = "SUBMITTED"
QUEUED = "QUEUED"            # 网关自写：已提交上游、等待推进
IN_PROGRESS = "IN_PROGRESS"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
CANCELED = "CANCELED"

#: 活跃（非终态）状态集合——CAS 迁移的合法起点
ACTIVE: tuple[str, ...] = (SUBMITTED, QUEUED, IN_PROGRESS)
#: 终态集合——不可逆，迟到快照丢弃
TERMINAL: tuple[str, ...] = (SUCCESS, FAILURE, CANCELED)

__all__ = [
    "ACTIVE",
    "CANCELED",
    "FAILURE",
    "IN_PROGRESS",
    "QUEUED",
    "SUBMITTED",
    "SUCCESS",
    "TERMINAL",
]
