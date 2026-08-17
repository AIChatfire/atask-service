"""进程级配置：全部环境变量驱动，``GW_`` 前缀（``.env.example`` 为全量样例）。

- ``get_settings()`` lru_cache 进程内单例；模块内一律
  ``from app.config import settings``，禁止散读 ``os.environ``；
- 三个外部微服务的地址/凭证全部在这里（keypool / pricing / newapi-billing），
  换实现只改 ``*_PROVIDER`` 选择子，见 ``app.services.providers``；
- 逗号分隔序列字段（探测退避阶梯）兼容 ``5,15,30,120`` 与 JSON 数组两种写法。
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _parse_seconds_ladder(value: Any) -> Any:
    """逗号分隔/JSON 数组 → tuple[int, ...]（退避阶梯字段共用解析器）。"""
    if isinstance(value, str):
        text = value.strip().strip("[]")
        return tuple(int(x.strip()) for x in text.split(",") if x.strip())
    if isinstance(value, list | tuple):
        return tuple(int(x) for x in value)
    return value


class Settings(BaseSettings):
    """全部环境变量驱动（``GW_`` 前缀，与 .env.example 一一对应）。"""

    model_config = SettingsConfigDict(env_prefix="GW_", env_file=".env", extra="ignore")

    # ---- 观测 ----
    app_env: str = "dev"
    app_version: str = "dev"
    log_level: str = "INFO"               # loguru 出口级别（排障时调 DEBUG）
    logfire_enabled: bool = False
    logfire_token: str | None = None

    # ---- 数据层（与 new-api 共享 MySQL 实例；网关零建表职责）----
    database_url: str = (
        "mysql+asyncmy://root:root@127.0.0.1:3306/newapi?charset=utf8mb4"
    )
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_recycle: int = 1800          # 必须小于 MySQL wait_timeout
    db_pool_pre_ping: bool = True
    redis_url: str = "redis://127.0.0.1:6379/0"

    # ---- 微服务：newapi-billing-service（身份内省 + 冻结/结算/取消）----
    billing_provider: str = "newapi-billing"
    billing_svc_url: str = "http://127.0.0.1:8080"

    # ---- 微服务：keypool-service（上游 key/base_url/渠道覆盖/路由提取配置/计费规则）----
    key_provider: str = "keypool"
    key_svc_url: str = "http://127.0.0.1:8081"
    key_svc_token: str = "change-me"
    key_group: str = "keypool"            # 统一分组：全部渠道挂在此 group 下集中维护

    # ---- 网关自身 ----
    gateway_public_base_url: str = "http://127.0.0.1:8000"  # 注入上游 callback_url 基址
    gateway_platform: str = "gateway"     # tasks.platform 标记（区分 new-api 自身任务）
    bind: str = "0.0.0.0:8000"
    admin_token: str | None = None        # /ops/* X-Admin-Token；空则只靠内网隔离
    http_timeout: float = 10.0            # 控制面（三微服务）HTTP 超时

    # ---- 计费闭环 ----
    freeze_ttl_seconds: int = 1800        # 预冻结 TTL（billing sweeper 过期自动解冻兜底）
    sk_session_ttl_seconds: int = 172800  # 用户令牌 Redis 暂存 TTL（终态 settle/cancel 用，48h）

    # ---- 鉴权/幂等/限流 ----
    auth_cache_ttl: int = 30              # billing /auth/inspect 结果缓存
    idem_ttl: int = 86400                 # Idempotency-Key → task_id（24h）
    rate_limit_per_minute: int = 60
    max_concurrent_tasks: int = 5         # 每用户并发任务上限

    # ---- 用户回调投递 ----
    callback_sign_secret: str = "change-me"  # 推送用户 callback_url 的 HMAC 签名密钥
    cb_dedup_ttl: int = 259200            # 上游回调去重（72h）

    # ---- 探测（不支持回调的上游）----
    poll_ladder_seconds: Annotated[tuple[int, ...], NoDecode] = (5, 15, 30, 120)
    poll_max_age_seconds: int = 86400     # 任务最大在途时长（超时转 FAILURE）
    task_stale_seconds: int = 300         # 非终态任务超过该时长未更新则 sweeper 重投探测

    # ---- 队列（taskiq）----
    event_max_attempts: int = 8           # 事件任务重试上限，超限落死信
    queue_warn_depth: int = 500           # 积压告警阈值

    @field_validator("poll_ladder_seconds", mode="before")
    @classmethod
    def _ladder(cls, value: Any) -> Any:
        return _parse_seconds_ladder(value)


@lru_cache
def get_settings() -> Settings:
    """进程内单例（测试经 ``get_settings.cache_clear()`` 重载）。"""
    return Settings()


settings = get_settings()
