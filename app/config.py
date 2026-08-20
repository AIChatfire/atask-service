"""进程级配置：全部环境变量驱动，``GW_`` 前缀（``.env.example`` 为全量样例）。

- ``get_settings()`` lru_cache 进程内单例；模块内一律
  ``from app.config import settings``，禁止散读 ``os.environ``；
- 两个外部微服务的地址/凭证全部在这里（keypool / newapi-billing），
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
    logfire_excluded_urls: str = (        # logfire 噪音路由排除（逗号分隔路径/前缀）
        "/healthz/live,/healthz/ready,/ops/queue,/ops/requeue,/ops/dlq/replay,/ops/tasks"
    )

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
    # biz -> 最近使用 channel_id 的 Redis 记忆 TTL（免费 GET 钉渠道直达租约用：
    # 免费请求不带 model，keypool select 对空 model 必拒 40010，所以网关不问它）
    route_channel_ttl_seconds: int = 86400

    # ---- 网关自身 ----
    gateway_public_base_url: str = "http://127.0.0.1:8000"  # 注入上游 callback_url 基址
    gateway_platform: str = "gateway"     # tasks.platform 标记（区分 new-api 自身任务）
    bind: str = "0.0.0.0:8000"
    admin_token: str | None = None        # /ops/* X-Admin-Token；空则只靠内网隔离
    http_timeout: float = 10.0            # 控制面（三微服务）HTTP 超时

    # ---- 计费闭环 ----
    freeze_ttl_seconds: int = 1800        # 预冻结 TTL（billing sweeper 过期自动解冻兜底）
    sk_session_ttl_seconds: int = 172800  # 用户令牌 Redis 暂存 TTL（终态 settle/cancel 用，48h）
    freeze_renew_margin_seconds: int = 600    # 冻结临期续期阈值（剩余 < 10min 才续）
    freeze_renew_batch: int = 100             # 每轮 sweep 续期上限
    hold_max_age_seconds: int = 14400         # HELD 挂起上限（4h，账户级；≤ 冻结续期可维持窗口）
    hold_max_age_rate_limited_seconds: int = 3600   # 限流（429）挂起上限（1h，独立更短窗口）
    held_rate_limited_backoff_seconds: int = 300    # 限流挂起重提交退避（固定 5m，不走阶梯）

    # ---- 鉴权/幂等/限流 ----
    auth_cache_ttl: int = 30              # billing /auth/inspect 结果缓存
    idem_ttl: int = 86400                 # Idempotency-Key → task_id（24h）
    idem_pending_ttl_seconds: int = 30    # 幂等占位（pending）TTL：覆盖 preflight→落库回填窗口
    idem_replay_wait_seconds: float = 25.0  # 同键并发等占位回填上限（超时按 409 冲突处理）
    rate_limit_per_minute: int = 60
    max_concurrent_tasks: int = 5         # 每用户并发任务上限
    # 并发槽键 TTL 兜底（防「占槽后崩溃」的永久泄漏；每次 acquire 刷新，
    # 精确校准另由 sweep conc_recalibrate 做）——必须 > 最长任务在途时长
    conc_ttl_seconds: int = 172800
    conc_recalibrate_batch: int = 200     # 每轮 sweep 校准的并发槽键数上限

    # ---- 用户回调投递 ----
    callback_sign_secret: str = "change-me"  # 推送用户 callback_url 的 HMAC 签名密钥
    cb_dedup_ttl: int = 259200            # 上游回调去重（72h）

    # ---- 探测（不支持回调的上游）----
    poll_ladder_seconds: Annotated[tuple[int, ...], NoDecode] = (5, 15, 30, 120, 300)
    poll_max_age_seconds: int = 86400     # 任务最大在途时长（超时转 FAILURE）
    task_stale_seconds: int = 300         # 非终态任务超过该时长未更新则 sweeper 重投探测

    # ---- 上游数据面（提交/探测出站）----
    upstream_breaker_threshold: int = 10  # 熔断：窗口内失败 N 次打开
    upstream_breaker_window_seconds: int = 30
    upstream_max_connections: int = 50    # 每 (biz, base_url, proxy) 连接池上限
    upstream_max_keepalive: int = 20
    native_buffer_limit_bytes: int = 1_048_576   # 原生查询响应缓冲上限（超限放弃 id 改写）
    upstream_index_ttl_seconds: int = 604800     # 上游 task_id → 本地 task_id 反查索引 TTL（7d）

    # ---- 提交重试（仅换 key 可能改变结果的确定性拒绝；模糊失败绝不重试防双重创建）----
    submit_max_attempts: int = 3          # 含首次提交
    submit_retryable_status_codes: Annotated[tuple[int, ...], NoDecode] = (401, 403, 429)
    # 提交互斥锁 TTL = submit_max_attempts × 渠道 timeout_sec + 本余量（动态派生，
    # 见 app/services/submit.submit_lock_ttl）；余量覆盖租约重建/落库/探测排程
    submit_lock_buffer_seconds: int = 60
    # 默认只含 key 级（401/403）与限流（429）：换 key/渠道才可能改变结果；
    # 任务级 4xx（如 400 内容审核）重打同一报文无意义，不默认重试（可按厂商条款追加）

    # ---- 不亏本兜底（孤儿收口 / 反向对账）----
    orphan_grace_seconds: int = 1800      # 非终态且无 upstream_task_id 超此时长 → 孤儿收口
    # 异步提交后必须覆盖「队列积压 + 提交耗时」窗口（stale 每 300s 补投一次 +
    # 提交最坏 渠道timeout×重打 + 锁余量，锁 TTL 动态派生见 submit_lock_ttl），
    # 过小会在队列积压时误杀在途任务
    reverse_reconcile_batch: int = 20     # 反向对账每轮抽查上限
    reverse_reconcile_window_seconds: int = 86400   # 只抽查近 24h 的 FAILURE
    reverse_reconcile_recheck_seconds: int = 3600   # 单任务核对间隔（在途上游每小时复查）

    # ---- 队列（taskiq）----
    event_max_attempts: int = 8           # 事件任务重试上限，超限落死信
    queue_warn_depth: int = 500           # 积压告警阈值
    taskiq_admin_url: str = ""            # taskiq-admin 看板地址（空 = 不上报）
    taskiq_admin_api_token: str = ""      # 看板 API access-token

    # ---- Sweep（每分钟补数巡检）----
    sweep_lock_ttl_seconds: int = 300     # 重入锁 TTL：慢轮（反向对账打上游）不叠加并发轮
    sweep_stale_batch: int = 200          # 每轮 stale 重投上限（积压追赶速度）
    sweep_orphan_batch: int = 50          # 每轮孤儿收口上限
    sweep_held_expire_batch: int = 100    # 每轮 HELD 超限判死上限
    sweep_unsettled_batch: int = 200      # 每轮结算补发上限
    queue_stats_cache_seconds: int = 55   # 队列观测快照缓存（全库 scan + 全表 GROUP BY 降频）

    @field_validator("poll_ladder_seconds", mode="before")
    @classmethod
    def _ladder(cls, value: Any) -> Any:
        return _parse_seconds_ladder(value)

    @field_validator("submit_retryable_status_codes", mode="before")
    @classmethod
    def _codes(cls, value: Any) -> Any:
        return _parse_seconds_ladder(value)


@lru_cache
def get_settings() -> Settings:
    """进程内单例（测试经 ``get_settings.cache_clear()`` 重载）。"""
    return Settings()


settings = get_settings()


