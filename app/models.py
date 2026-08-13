"""NewAPI 现有 tasks 表的映射说明（仅供参考，网关不建任何表）。

实际读写全部走 taskstore.py 的原生 SQL（JSON_MERGE_PATCH 等）。
部署前请对照你们 new-api 版本的 tasks 表核对列名与类型：
  - 时间字段是 int64 unix 秒（created_at/updated_at/submit_time/finish_time）
  - progress 是字符串（如 "0%"）
  - data 是 JSON 列 —— 网关全部扩展字段都在这里：
      biz, token_hash, idempotency_key, callback_url,
      freeze_amount, settled, key_id, upstream_task_id,
      source(tasks/videos/proxy), result, upstream_status
网关对 MySQL 只有这一张表的读写依赖；tokens/users 完全不碰
（身份由 billing 服务的 /api/v1/auth/inspect 提供）。
"""

from sqlalchemy import JSON, BigInteger, Column, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """声明式基类（SQLAlchemy 2.0 形态；仅映射说明用，网关不建表）。"""


class Task(Base):
    __tablename__ = "tasks"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(String(64), unique=True, index=True)   # 网关 UUID = billing request_id，不可枚举
    platform = Column(String(32), default="gateway", index=True)
    action = Column(String(32), default="")
    status = Column(String(32), default="SUBMITTED", index=True)
    fail_reason = Column(Text, default="")
    progress = Column(String(16), default="0%")
    submit_time = Column(BigInteger, default=0)
    start_time = Column(BigInteger, default=0)
    finish_time = Column(BigInteger, default=0)
    created_at = Column(BigInteger, default=0)
    updated_at = Column(BigInteger, default=0)
    data = Column(JSON)
    user_id = Column(BigInteger, index=True)                # 来自 billing inspect/freeze 响应
    channel_id = Column(BigInteger, default=0)
    quota = Column(BigInteger, default=0)
