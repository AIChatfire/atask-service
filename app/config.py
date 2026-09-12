"""进程级配置：全部环境变量驱动，**键名 = 字段名大写、无前缀**（``.env.example`` 为全量样例）。

- ``get_settings()`` lru_cache 进程内单例；模块内一律
  ``from app.config import settings``，**禁止散读 ``os.environ``**
  （全项目唯一例外是 ``gunicorn.conf.py``——它由 master 进程在本单例之前加载）；
- 逗号分隔序列字段以字符串承载、消费侧自行解析（如 ``upstream_allowlist`` /
  ``batch_deny_prefixes``）。
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全部环境变量驱动（键名 = 字段名大写、**无前缀**，与 .env.example 一一对应）。"""

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")

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

    # ---- 网关自身 ----
    gateway_platform: str = "atask"      # tasks.platform 标记（区分 new-api 原生任务与 stask 行；与 stask-service 的 'stask' 对称命名）
    bind: str = "0.0.0.0:8000"
    admin_token: str | None = None        # 管理面（/ops/* 与 /admin/*）X-Admin-Token；空则整个管理面 404（fail-closed）

    # ---- 鉴权/幂等/限流 ----
    idem_ttl: int = 86400                 # Idempotency-Key → task_id（24h）
    idem_pending_ttl_seconds: int = 30    # 幂等占位（pending）TTL：覆盖受理→落库回填窗口
    idem_replay_wait_seconds: float = 25.0  # 同键并发等占位回填上限（超时按 409 冲突处理）
    rate_limit_per_minute: int = 60
    max_concurrent_tasks: int = 5         # 每用户并发任务上限
    # 并发槽键 TTL 兜底（防「占槽后崩溃」的永久泄漏；每次 acquire 刷新）——
    # 必须 > 最长任务在途时长
    conc_ttl_seconds: int = 172800

    # ---- 用户令牌会话 ----
    sk_session_ttl_seconds: int = 172800  # 用户令牌 Redis 暂存 TTL（探测/取消用，48h）

    # ---- 用户回调投递 ----
    callback_sign_secret: str = "change-me"  # 推送用户 callback_url 的 HMAC 签名密钥

    # ---- /batch 后台收敛 ----
    task_stale_seconds: int = 300         # 非终态任务超过该时长未更新则 sweeper 探测

    # ---- 上游数据面（提交/探测出站）----
    # 上游寻址：X-Upstream-Base-Url 头优先，回退 upstream_base_url；host 必须命中 allowlist。
    upstream_base_url: str = ""           # 上游默认基址（仅作 X-Upstream-Base-Url 缺省时的回退）
    upstream_allowlist: str = ""          # 允许的上游 host 列表（逗号分隔；空 = 全部拒绝，fail-closed）
    upstream_breaker_threshold: int = 10  # 熔断：窗口内失败 N 次打开
    upstream_breaker_window_seconds: int = 30

    # ---- /batch 中继（ADR-010：鉴权计费下沉上游，网关零资金动作）----
    # 约定式链路没有渠道级 timeout_sec（该渠道元数据已整体放弃），
    # 出站超时降级为这一个全局默认值。
    relay_timeout_seconds: float = 60.0
    # 受理提交体大小上限（字节）：防超大提交体把网关进程内存打爆。超限返回 413，
    # 且必须在任何副作用（幂等占位/并发槽/落 tasks 行）之前拒绝。Content-Length
    # 可缺失（chunked）也可被伪造，故读取时逐块累加封顶（不看头）。
    body_max_bytes: int = 1_048_576         # 1 MiB（与旧链路默认一致）
    # 免费 GET 透传（流式转发）时上游**声明**的响应体长度上限（字节）：超限即
    # 502 拒绝且不开始流式（body 不消费）。未声明长度（chunked）不设中途截断，
    # 理由见 app.services.relay.stream_upstream。默认 100 MiB。
    upstream_response_max_bytes: int = 104_857_600      # 100 MiB
    # 路径准入硬拒前缀（逗号分隔）：/batch/{path} 命中即拒（照 stask 的 /api、/console），
    # 防把上游管理面/控制台路径经本网关暴露出去。
    batch_deny_prefixes: str = "/api/,/console/"

    # ---- 队列（taskiq）----
    event_max_attempts: int = 8           # 事件任务重试上限，超限落死信
    # worker 并发上限（taskiq worker --max-async-tasks）。worker 的活儿是
    # await 上游 HTTP（IO 密集），设小会让队列白积压；但每个在飞任务持有 DB
    # 会话与上游连接，设太大在 DB 连接预算不足时会撞 max_connections——
    # 与 gunicorn worker 数**共享同一份 DB 预算**，调大前先看 DB_MAX_CONNECTIONS
    # 与 gunicorn.conf.py 的预算推导（standalone 单进程模式同样读这个值）。
    queue_max_async_tasks: int = 10240
    taskiq_admin_url: str = ""            # taskiq-admin 看板地址（空 = 不上报）
    taskiq_admin_api_token: str = ""      # 看板 API access-token

    # ---- Sweep（每分钟补数巡检）----
    batch_sweep_batch: int = 50           # 每轮 /batch 后台收敛（探测推进终态）上限
    # /batch 收敛独立重入锁 TTL：一轮可能串行探测 batch_sweep_batch 条 × 单条
    # 出站超时，最坏会超过 1 分钟 cron——锁保证慢轮不叠加并发轮（多副本同理）。
    batch_sweep_lock_ttl_seconds: int = 300
    queue_stats_cache_seconds: int = 55   # 队列观测快照缓存（全库 scan + 全表 GROUP BY 降频）


@lru_cache
def get_settings() -> Settings:
    """进程内单例（测试经 ``get_settings.cache_clear()`` 重载）。"""
    return Settings()


settings = get_settings()
