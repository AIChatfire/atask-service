from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全部配置环境变量驱动，前缀 GW_"""

    model_config = SettingsConfigDict(env_prefix="GW_", env_file=".env", extra="ignore")

    # 数据层（直连 NewAPI 的 MySQL 实例，网关只读写 tasks 表，零建表）
    database_url: str = "mysql+asyncmy://root:root@127.0.0.1:3306/newapi?charset=utf8mb4"
    redis_url: str = "redis://127.0.0.1:6379/0"
    db_pool_size: int = 20
    db_max_overflow: int = 10

    # 已有微服务
    pricing_svc_url: str = "http://127.0.0.1:9001"   # GET /pricing/rules?biz_type=&metric=
    billing_svc_url: str = "http://127.0.0.1:9002"   # /api/v1/auth/inspect、/api/v1/billing/{freeze,settle,cancel}
    key_svc_url: str = "http://127.0.0.1:9003"       # keypool：POST /v1/key:get、/v1/key:report
    key_svc_token: str = ""                          # keypool 服务级 Bearer token
    billing_admin_token: str = ""                    # worker 侧结算用的服务账号凭证

    # 微服务实现装配（providers 层，换实现只改这里）
    billing_provider: str = "newapi-billing"
    pricing_provider: str = "model-meta"
    key_provider: str = "keypool"

    # 配置中心微服务（动态路由主源；留空则只用 YAML + Redis 覆盖）
    config_svc_url: str = ""                         # GET {url}/gateway/routes
    config_pull_interval: int = 10                   # 拉取间隔（秒）

    # 网关行为
    routes_file: str = "gateway-routes.yaml"
    gateway_platform: str = "gateway"      # 写入 tasks.platform 的标识，与 NewAPI 自有任务区分
    callback_sign_secret: str = "change-me"
    freeze_ttl_seconds: int = 1800
    rate_limit_per_minute: int = 60
    max_concurrent_tasks: int = 5          # 每令牌并发任务上限
    sweep_interval_seconds: int = 60
    task_stale_seconds: int = 300
    http_timeout: float = 30.0

    # 各类 TTL（秒）
    auth_cache_ttl: int = 30               # 身份内省缓存
    pricing_cache_ttl: int = 60            # 计费规则缓存
    idem_ttl: int = 86400                  # 幂等键
    cb_dedup_ttl: int = 259200             # 回调去重 72h

    # 事件投递
    event_max_attempts: int = 8            # 超过进死信
    queue_warn_depth: int = 500            # 队列积压告警阈值（sweep 巡检每分钟检查）
    admin_token: str = ""                  # /ops/* 端点的 X-Admin-Token；留空则靠 Ingress 限内网

    # 轮询退避阶梯（逗号分隔秒数）与最大轮询时长
    poll_ladder: str = "5,15,60,300"
    poll_max_age_seconds: int = 24 * 3600

    # 观测
    logfire_enabled: bool = False
    logfire_token: str | None = None

    @property
    def poll_ladder_seconds(self) -> list[int]:
        return [int(x) for x in self.poll_ladder.split(",") if x.strip()]


settings = Settings()
