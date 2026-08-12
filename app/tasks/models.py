"""SQLAlchemy 模型层 —— 全项目最关键共享契约（SPEC §3.3）。

**零自有表（决策 A）**：网关不拥有任何 ``gateway_`` 表——原 9 张自有表的
职责全部迁移到环境变量（biz 注册表）、Redis 数据结构（反查索引 / 延迟队列 /
欠费单）与 logfire 结构化日志（审计 / 对账报告）。本模块只保留 new-api
共享表 ``tasks`` 的读写映射：列集与简报 C §一 逐列一致，由 new-api GORM
AutoMigrate 维护；网关**不建、不迁、不 ALTER**，只读共享、只写自有行
（SPEC §4 三件套纪律），绝不进入 ``create_all``。

纪律提醒：
- ``group`` 是 MySQL 8 保留字：ORM 已用属性名 ``group_`` + 列名 ``"group"``
  处理；任何手写原生 SQL 必须写反引号 `` `group` ``（简报 C §四.8）。
- tasks 表全部 bigint 时间列无 DB 默认值（GORM 语义）：INSERT 显式自填，
  ``start_time``/``finish_time`` 显式填 0，**绝不能留 NULL**（SPEC §4.1）。
- 一切 UPDATE tasks 的 SQL 必含 ``platform LIKE 'gw\\_%'``（或对已知行先校验
  platform 前缀）+ status 前置条件（CAS，对齐 new-api UpdateWithStatus）。
"""

from __future__ import annotations

import secrets
from enum import StrEnum

from sqlalchemy import BigInteger
from sqlalchemy.dialects.mysql import JSON, LONGTEXT, VARCHAR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ---------------------------------------------------------------------------
# 状态枚举（SPEC §3.4）
# ---------------------------------------------------------------------------


class TaskStatus(StrEnum):
    """网关内部统一状态（架构 §4.1；纯内部模型，无对外 /v1/tasks 契约）。"""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELED = "canceled"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELED}
)


class DbTaskStatus(StrEnum):
    """new-api ``tasks.status`` 落库枚举（简报 C §二，原样大写）。

    网关只写前六值中的四个（SUBMITTED/IN_PROGRESS/SUCCESS/FAILURE）：
    不细分 SUBMITTED/QUEUED（内部 queued 落库恒 SUBMITTED/10%），
    永不使用 UNKNOWN；timeout/canceled 折叠进 FAILURE，靠 fail_reason
    前缀（``timeout:``/``canceled:``/``failed:``）区分（架构 §4.1 注）。
    """

    NOT_START = "NOT_START"
    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    UNKNOWN = "UNKNOWN"  # new-api 兜底值；网关不使用


# ---- 内部状态 ↔ tasks.status/progress（架构 §4.1 映射表，唯一权威映射） ----

_DB_STATUS: dict[TaskStatus, str] = {
    TaskStatus.QUEUED: DbTaskStatus.SUBMITTED.value,
    TaskStatus.RUNNING: DbTaskStatus.IN_PROGRESS.value,
    TaskStatus.SUCCEEDED: DbTaskStatus.SUCCESS.value,
    TaskStatus.FAILED: DbTaskStatus.FAILURE.value,
    TaskStatus.TIMEOUT: DbTaskStatus.FAILURE.value,
    TaskStatus.CANCELED: DbTaskStatus.FAILURE.value,
}

_DB_PROGRESS: dict[TaskStatus, str] = {
    TaskStatus.QUEUED: "10%",      # 网关不细分 QUEUED，无 20% 写入点
    TaskStatus.RUNNING: "30%",     # 之后可被上游进度覆盖（如 55%）
    TaskStatus.SUCCEEDED: "100%",
    TaskStatus.FAILED: "100%",
    TaskStatus.TIMEOUT: "100%",
    TaskStatus.CANCELED: "100%",
}

_DB_TO_INTERNAL: dict[str, TaskStatus] = {
    "NOT_START": TaskStatus.QUEUED,
    "SUBMITTED": TaskStatus.QUEUED,
    "QUEUED": TaskStatus.QUEUED,
    "IN_PROGRESS": TaskStatus.RUNNING,
    "SUCCESS": TaskStatus.SUCCEEDED,
    "FAILURE": TaskStatus.FAILED,  # 细分由 fail_reason 前缀还原（见 fail_reason_prefix）
}

# new-api ToVideoStatus() 同款对外映射（简报 C §二）：SUBMITTED/QUEUED→queued
_DB_TO_VIDEO: dict[str, str] = {
    "NOT_START": "queued",
    "SUBMITTED": "queued",
    "QUEUED": "queued",
    "IN_PROGRESS": "in_progress",
    "SUCCESS": "completed",
    "FAILURE": "failed",
}


def db_status(s: TaskStatus) -> str:
    """内部状态 → tasks.status 落库值。"""
    return _DB_STATUS[s]


def db_progress(s: TaskStatus) -> str:
    """内部状态 → tasks.progress 落库值。"""
    return _DB_PROGRESS[s]


def db_to_internal(db_value: str) -> TaskStatus:
    """tasks.status → 内部状态；未知值按 RUNNING 兜底（与 §13.4 骨架一致）。"""
    return _DB_TO_INTERNAL.get(db_value, TaskStatus.RUNNING)


def to_video_status(db_value: str) -> str:
    """tasks.status → 对外 videos 状态（queued/in_progress/completed/failed）。

    与 new-api ``ToVideoStatus()`` 完全一致；对外 schema 枚举见
    ``app.schemas.VideoStatus``。
    """
    return _DB_TO_VIDEO.get(db_value, "failed")


def fail_reason_prefix(db_fail_reason: str | None) -> TaskStatus | None:
    """从 fail_reason 前缀还原 FAILURE 的内部细分（timeout/canceled 单一口径）。

    返回 None 表示普通 failed 或 fail_reason 为空。
    """
    if not db_fail_reason:
        return None
    if db_fail_reason.startswith("timeout:"):
        return TaskStatus.TIMEOUT
    if db_fail_reason.startswith("canceled:"):
        return TaskStatus.CANCELED
    return None


# ---- platform 命名空间与 task_id 形制（SPEC §4.1 三件套之第一件） ----

GW_PLATFORM_LIKE = r"gw\_%"  # SQL LIKE 模式（反斜杠转义下划线）
GW_PLATFORM_PREFIX = "gw_"


def platform_for(adapter_name: str) -> str:
    """网关行 platform 值：``gw_{adapter}``（如 gw_kling）。

    自定义命名空间，避开 new-api 轮询适配器可解析的全部取值
    （suno/mj/渠道类型纯数字串，简报 C §三）——防 new-api 轮询器
    接管网关行的第一道闸（架构 §4.5）。
    """
    return f"{GW_PLATFORM_PREFIX}{adapter_name}"


def new_task_id() -> str:
    """new-api 同款 ID 形制：``task_`` + 32 位随机字符（对齐 GenerateTaskID()）。

    tasks.task_id 无 DB 唯一约束（简报 C §四.9），唯一性由随机性 +
    Redis 幂等键保证。
    """
    return "task_" + secrets.token_hex(16)


# ---------------------------------------------------------------------------
# Declarative Base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# 共享表：new-api tasks（只读共享、只写自有行；网关不建表）
# ---------------------------------------------------------------------------


class Task(Base):
    """new-api ``tasks`` 表映射（列集与简报 C §一 逐列一致）。

    **纪律**：
    - 本模型仅供读写映射；由 new-api AutoMigrate 维护，网关绝不 create/alter。
    - 网关 INSERT 时 ``quota`` 恒 0、``platform`` 恒 ``gw_{adapter}``、
      全部 bigint 时间列显式自填（``start_time``/``finish_time`` 填 0 非 NULL）、
      ``fail_reason`` 显式空串（SPEC §4.1/§4.2；V18 实测若该列默认 NULL 可改）。
    - ``properties``/``private_data``/``data`` 的 JSON 内部结构约定见
      SPEC §4.2（网关自有键收敛于 ``private_data.gateway`` 子对象）。
    """

    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at: Mapped[int] = mapped_column(BigInteger, index=True)   # unix 秒，自填
    updated_at: Mapped[int] = mapped_column(BigInteger)               # 每次 UPDATE 刷新
    task_id: Mapped[str] = mapped_column(VARCHAR(191), index=True)    # task_+32 随机
    platform: Mapped[str] = mapped_column(VARCHAR(30), index=True)    # 网关行: gw_{adapter}
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)      # new-api users.id
    group_: Mapped[str | None] = mapped_column(
        "group", VARCHAR(50), nullable=True
    )  # MySQL 保留字：列名加引号由 ORM 处理，手写 SQL 必须 `` `group` ``
    channel_id: Mapped[int] = mapped_column(BigInteger, index=True)   # new-api channels.id
    quota: Mapped[int] = mapped_column(BigInteger, default=0)         # 网关恒 0（三件套）
    action: Mapped[str] = mapped_column(VARCHAR(40), index=True)      # generate/textGenerate/...
    status: Mapped[str] = mapped_column(VARCHAR(20), index=True)      # DbTaskStatus 六值
    fail_reason: Mapped[str | None] = mapped_column(LONGTEXT, nullable=True)
    submit_time: Mapped[int] = mapped_column(BigInteger, index=True)  # 落库即填 now
    start_time: Mapped[int] = mapped_column(BigInteger, index=True)   # 首进 IN_PROGRESS 填
    finish_time: Mapped[int] = mapped_column(BigInteger, index=True)  # 进终态填
    progress: Mapped[str] = mapped_column(VARCHAR(20), index=True, default="0%")
    properties: Mapped[dict | None] = mapped_column(JSON, nullable=True)     # 见 SPEC §4.2
    private_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)   # gateway 子对象
    data: Mapped[dict | None] = mapped_column(JSON, nullable=True)           # 上游原始响应


# ---------------------------------------------------------------------------
# 零自有表（决策 A）：原 9 张 gateway_ 表全部删除，迁移去向：
#   biz 注册表 → BIZ_CONFIGS/BIZ_CONFIGS_FILE（app/registry.py）；
#   回调反查索引 → Redis tidx:{biz}:{upstream_task_id} + tasks 行兜底 SQL；
#   回调投递 / 计费 outbox → Redis 延迟队列（app/redis_queue.py，dlv/obx）；
#   计费审计 / 对账报告 → logfire 结构化日志 + 计费服务 /billing/logs；
#   计费逻辑缓存 → 删除（L1/L2 Redis 之外本就冗余）；freeze 分片台账 →
#   tasks.private_data.gateway + Redis 双写（重建路径在 renewer）；
#   欠费单 → Redis debt:order:{request_id} + debt:orders。
# ---------------------------------------------------------------------------


class _RemovedGatewayModels:  # pragma: no cover - 文档锚点，无运行期作用
    """占位说明见上；任何 gateway_ 表模型不得重新加入本模块。"""


__all__ = [
    "GW_PLATFORM_LIKE",
    "GW_PLATFORM_PREFIX",
    "TERMINAL_STATUSES",
    "Base",
    "DbTaskStatus",
    "Task",
    "TaskStatus",
    "db_progress",
    "db_status",
    "db_to_internal",
    "fail_reason_prefix",
    "new_task_id",
    "platform_for",
    "to_video_status",
]
