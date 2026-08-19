# SPEC.md — async-gateway 多代理并行实现契约（单一事实源）

> **⚠️ 历史文档（2026-08-17 标注，2026-08-19 补充）**：本 SPEC 对应旧 adapter 代架构
> （`app/adapters/{kling,seedance}` + `callbacks/{dispatcher,receiver}` +
> `billing/{outbox,renewer}` + pricing-service 缓存），该实现已下线
> （仅存于本地 stash，不随仓库分发）。
> **现行同构架构以 README.md / AGENTS.md 为准**：pricing-service 已废弃
> （计费规则 = keypool 渠道元数据 `billing.rule`，随租约下发本地求值）；
> 提交链路亦已异步化（创建落库即返回本地 task_id，上游提交在 taskiq worker
> 执行，不再有请求内同步提交/同步 502 契约），全文仅供参考，勿据以实现。
>
> 版本 v1.0（2026-08-12）。上游依据：`async_gateway_architecture.md` v2.1（下称「架构文档 §x」）、
> `research/brief_c_newapi_tasks.md`（下称「简报 C」）、`research/brief_a_billing_and_apis.md`（简报 A）、
> `research/brief_b_gateway_ha.md`（简报 B）。
>
> **本文件是多代理并行实现的唯一契约。** W1~W6 各代理不看对方代码、只依赖：
> ① 本 SPEC 的接口签名与语义；② 骨架仓库已交付的共享模块（可直接 import 阅读）。
> 骨架已交付文件如需改动，必须先改 SPEC 并通知全部代理；占位文件归各自代理全权实现。

## 目录

1. [项目树](#1-项目树)
2. [技术约束](#2-技术约束)
3. [接口契约](#3-接口契约)
4. [横切规则（不可违反）](#4-横切规则不可违反)
5. [数据模型速查](#5-数据模型速查)
6. [分工表 W1~W6](#6-分工表-w1w6)
7. [测试与验收](#7-测试与验收)
8. [附录：建议验证项对实现的影响](#8-附录建议验证项对实现的影响)

---

## 1. 项目树

仓库根：`/mnt/agents/output/project`（git，主分支 `main`，共享提交规范见 §6.4）。
「骨架」列 = 已交付的完整实现（本 SPEC 附首批提交）；「占位」列 = 归属代理全权实现。

```
project/
├── pyproject.toml                  [骨架] 钉版本依赖 + ruff/pytest/mypy 配置
├── README.md                       [骨架→W6 扩写部署/运维章节]
├── .env.example                    [骨架] 环境变量样例（§3.8）
├── .gitignore                      [骨架]
├── gunicorn.conf.py                [W6]  架构 §2.2 完整配置（preload/max_requests/hooks）
├── Dockerfile                      [W6]
├── docker-compose.yml              [W6]  架构 §12.2（gateway + worker + mysql8 + redis7；唯一部署形态）
├── app/
│   ├── __init__.py                 [骨架]
│   ├── config.py                   [骨架] Settings 全量定义（§3.1）
│   ├── errors.py                   [骨架] GatewayError + OpenAI 风格错误工厂 + 异常处理器（§3.5.2）
│   ├── schemas.py                  [骨架] 对外 API 模型 videos 形态（§3.5.3）
│   ├── db.py                       [骨架] 引擎/会话工厂（零自有表：只连共享 tasks，无建表职责）（§3.5.4）
│   ├── redis_client.py             [骨架] Redis 单例（§3.5.5）
│   ├── redis_queue.py              [骨架] Redis 延迟队列通用原语（dlv/obx：ZSET+HASH+Lua）（§3.5.8）
│   ├── http_clients.py             [骨架] 四个出站客户端单例（§3.5.6）
│   ├── observability.py            [骨架] logfire 配置（§3.5.7）
│   ├── registry.py                 [骨架] BizConfig + BizRegistry（BIZ_CONFIGS(_FILE) 环境变量源 + mtime 热更，§3.5.1）
│   ├── main.py                     [W1] create_app()：lifespan/异常处理/路由注册顺序（§4.2）
│   ├── auth.py                     [W1] Bearer 四级管线 + TokenInfo + 防 IDOR（§3.9.1）
│   ├── healthz.py                  [W1] /healthz/live + /healthz/ready（§3.9.3）
│   ├── middleware.py               [W1] 限流四层 + Redis 熔断器 + 幂等键 guard（§3.9.2）
│   ├── worker.py                   [W1] `python -m app.worker` 后台 worker 装配入口（§3.9.4）
│   ├── adapters/
│   │   ├── __init__.py             [骨架] re-export + 尽力导入适配器自注册
│   │   ├── base.py                 [骨架] UpstreamAdapter 协议 + 数据类 + 异常体系（§3.2）
│   │   ├── kling.py                [W5] KlingAdapter（两代并存，§3.13.1）
│   │   └── seedance.py             [W5] SeedanceAdapter（§3.13.2）
│   ├── tasks/
│   │   ├── __init__.py             [骨架]
│   │   ├── models.py               [骨架] ★最关键共享契约：全部模型 + 状态枚举/映射（§3.3/§3.4）
│   │   ├── manager.py              [W2] TaskManager：submit_task + transition CAS（§3.10.1）
│   │   └── poller.py               [W2] 轮询 worker：SKIP LOCKED 领取/退避/deadline（§3.10.2）
│   ├── billing/
│   │   ├── __init__.py             [骨架]
│   │   ├── client.py               [W3] BillingServiceClient freeze/settle/cancel/charge（§3.11.1）
│   │   ├── pricing.py              [W3] PricingLogic 三级缓存 + PricingEvaluator（§3.11.2）
│   │   ├── sandbox.py              [W3] asteval 四层防御子进程沙箱（§3.11.3）
│   │   ├── outbox.py               [W3] 事务性 outbox 补偿 worker（§3.11.4）
│   │   ├── renewer.py              [W3] FreezeRenewer 分片续期（§3.11.5）
│   │   └── reconcile.py            [W3] 每日对账入口（§3.11.6）
│   ├── callbacks/
│   │   ├── __init__.py             [骨架]
│   │   ├── receiver.py             [W4] 上游回调接收 + 队列消费（§3.12.1）
│   │   └── dispatcher.py           [W4] 用户回调可靠投递（§3.12.2）
│   └── routing/
│       ├── __init__.py             [骨架]
│       ├── videos.py               [W1] /{biz}/v1/videos 四端点（§3.9.5）
│       └── dynamic_router.py       [W1] catch-all 透传（§3.9.6）
└── tests/
    ├── test_skeleton_imports.py    [骨架] 冒烟：共享契约可导入 + 状态映射 + tasks 列集
    ├── conftest.py                 [W6] mock DB/Redis/上游 fixtures（§7.2）
    ├── test_auth.py / test_routing_order.py / test_videos.py        [W1]
    ├── test_task_manager.py / test_poller.py                        [W2]
    ├── test_billing_client.py / test_pricing_sandbox.py /
    │   test_outbox.py / test_renewer.py                             [W3]
    ├── test_receiver.py / test_dispatcher.py                        [W4]
    ├── test_kling.py / test_seedance.py                             [W5]
    └── test_smoke.py               [W6] 集成冒烟（§7.3）
```

---

## 2. 技术约束

| 约束 | 取值 | 说明 |
|---|---|---|
| Python | **3.12**（>=3.12） | `StrEnum`、`type X = ...` 可用；不使用 3.13 特性 |
| Web 框架 | FastAPI 0.115.14 + Starlette（随附） | 版本 pin 在 pyproject；简报 B §3 提示 ≥0.137 路由行为变更，升级需回归 §4.2 路由顺序测试 |
| ASGI 服务 | Gunicorn 23.0.0 + `uvicorn.workers.UvicornWorker`（uvicorn 0.34.0） | worker 公式 `(2×CPU)+1` 封顶 16（架构 §2.2，W6 落地） |
| ORM/驱动 | SQLAlchemy 2.0.41 async + **asyncmy 0.2.10** | 连 new-api 共享 MySQL 8；所有 SQL 为 MySQL 方言（§4.5） |
| Redis | redis-py 5.2.1 asyncio | `decode_responses=True` |
| HTTP 客户端 | httpx 0.28.1（http2） | 单例纪律见 `app/http_clients.py`，**禁止每请求新建** |
| 观测 | logfire 3.25.0 | `app/observability.py` 已封装；业务日志一律 `logfire.info/...` 参数化传值 |
| 表达式沙箱 | **asteval 1.0.9**（>=1.0.6，GHSA-vp47-9734-prjw 修复版） | 安全配置见 §3.11.3；订阅 GHSA |
| 重试 | tenacity 9.1.2 | 只重试 429/5xx 与 BillingLockBusy；尊重 Retry-After；**绝不重试其他 4xx** |
| 其他 | PyJWT 2.10.1（kling JWT）、ulid-py 1.1.0（evt_ 事件 ID）、pydantic 2.11.7 / pydantic-settings 2.9.1 | — |
| 开发依赖 | pytest 8.3.5 / pytest-asyncio 0.26.0（asyncio_mode=auto）/ ruff 0.11.13 / mypy 1.15.0 / respx 0.22.0 | — |

**版本纪律**：所有依赖在 `pyproject.toml` 钉死（`==`）；新增依赖须先改 SPEC 本节再改 pyproject。

---

## 3. 接口契约

> 已交付骨架的契约以代码为准，本节为语义索引；未交付模块（W1~W6）的签名在本节**逐字钉死**，
> 实现不得偏离（参数名、默认值、返回语义、异常类型）。docstring 语义为强制行为约定。

### 3.1 `app/config.py`（骨架已交付）

`Settings(BaseSettings)`：全部环境变量（§3.8 清单与字段一一对应）。
`settings = get_settings()` 进程内单例（lru_cache）。模块内一律 `from app.config import settings`，
禁止散读 `os.environ`（唯一例外：上游凭证按 `auth_secret_ref` 解析，见 §3.13）。

### 3.2 `app/adapters/base.py`（骨架已交付）

#### 3.2.1 `UpstreamAdapter`（`@runtime_checkable Protocol`）

| 成员 | 签名 | 语义（强制） |
|---|---|---|
| `name` | `ClassVar[str]` | 注册名，与 `BizConfig.adapter` 一致（`'kling'`/`'seedance'`） |
| `callback_capability` | `ClassVar[bool]` | 上游是否支持 webhook；False 时轮询是唯一推进通道 |
| `echoes_external_task_id` | `ClassVar[bool]` | 回调是否回显 `external_task_id`（决定 W4 反查路径，§4.6） |
| `submit` | `async (req: CanonicalTaskRequest, ctx: SubmitContext) -> SubmitResult` | 翻译并提交；必须注入 `ctx.gateway_callback_url`（支持时）与 `external_task_id=ctx.task_id`（支持回显时）；httpx 4xx（除 429）→ `UpstreamBizError`，429 → `UpstreamRateLimitError` |
| `poll` | `async (upstream_task_id: str, ctx: SubmitContext) -> TaskSnapshot` | 查询快照；kling 旧版查询路径需要的 action 从 `ctx.action` 取（W2 从 tasks 行取回后填入） |
| `parse_callback` | `(raw_body: bytes, headers: Mapping[str, str]) -> TaskSnapshot` | 同步纯函数；坏报文抛异常（网关回 400）；验签不在此做（W4 职责） |
| `map_status` | `(upstream_status: str) -> TaskStatus` | 吃掉代际拼写差异（succeed/succeeded）；**未知状态映射为 RUNNING**（不推进终态） |
| `estimate_usage` | `(req: CanonicalTaskRequest) -> UsageEstimate` | 顶格预估上下文；`amount_usd` 恒 `Decimal("0")` 占位，金额由 W3 求值 |
| `auth_headers` | `(cfg: Any) -> Mapping[str, str]` | 凭证从 `os.environ[cfg.auth_secret_ref...]` 解析；JWT 进程内缓存 key 必含 AK、过期前 60s 刷新 |
| `rewrite_callback_url` | `(raw_body: bytes, cfg: Any) -> bytes` | 摘除/改写用户 callback_url；非 JSON 原样返回 |

注册表：`register(adapter)` / `get_adapter(name) -> UpstreamAdapter`（未注册抛 `RuntimeError`）/
`registered_adapters() -> dict`。适配器模块底部 `register(XxxAdapter())` 自注册，
`app.adapters.__init__` 尽力导入触发（骨架已实现）。

#### 3.2.2 数据类（字段即契约，W2/W4/W5 三方依赖）

- `CanonicalTaskRequest{model:str, prompt:str, action:str, duration:float|None, resolution:str|None, mode:str|None, image:str|None, n:int=1, generate_audio:bool=False, callback_url:str|None, extra:dict|None}`
- `SubmitContext{biz:str, task_id:str, gateway_callback_url:str, upstream_base_url:str, secrets:Mapping[str,str], action:str=""}`
- `SubmitResult{upstream_task_id:str, raw:dict}`
- `TaskSnapshot{upstream_status:str, status:TaskStatus, result:dict|None, usage:dict|None, error:dict|None, event_id:str, raw:dict|None=None}`
- `UsageEstimate{amount_usd:Decimal, context:dict[str, float|str]}`

`TaskSnapshot.usage` 实收信号键约定（§3.11.2 求值上下文的来源）：`completion_tokens` /
`actual_duration` / `upstream_amount` / `resolution`。`event_id` 格式
`{provider}:{upstream_task_id}:{status}:{updated_at}`（上游有唯一事件 ID 时优先用之，V11 建议验证）。

#### 3.2.3 异常体系

`UpstreamError(RuntimeError)` 基类 → `UpstreamBizError(message, *, code=None)`（不重试，默认计熔断）、
`UpstreamRateLimitError(message, *, retry_after=None)`（**不计熔断**，尊重 Retry-After）。
submit/poll 只允许抛出该体系或 httpx 超时/传输异常。

### 3.3 `app/tasks/models.py`（骨架已交付，★最关键共享契约）

**共享表 `Task`（`__tablename__="tasks"`）**：列集与简报 C §一 逐列一致
（`id/created_at/updated_at/task_id/platform/user_id/`group`/channel_id/quota/action/status/
fail_reason/submit_time/start_time/finish_time/progress/properties/private_data/data`，
共 19 列，冒烟测试逐列断言）。`group` 保留字 → 属性名 `group_`，列名 `"group"`。
**网关绝不 create/alter 该表**；网关零自有表（决策 A），本模块不再含任何
`gateway_*` 自有表模型，`app.db` 亦无建表职责。

**原自有表 9 张已全部删除**（决策 A：语义迁移至 Redis 结构 / 环境变量 /
logfire 日志，映射明细见 §5.2）。

**模块级助手**（跨模块唯一权威映射，禁止各模块私自再定义）：

```python
def platform_for(adapter_name: str) -> str        # "gw_{adapter}"
def new_task_id() -> str                          # "task_" + secrets.token_hex(16)，len==37
def db_status(s: TaskStatus) -> str               # 内部 → tasks.status
def db_progress(s: TaskStatus) -> str             # 内部 → tasks.progress
def db_to_internal(db_value: str) -> TaskStatus   # tasks.status → 内部；未知→RUNNING
def to_video_status(db_value: str) -> str         # tasks.status → 对外 videos 状态
def fail_reason_prefix(db_fail_reason: str|None) -> TaskStatus | None  # timeout:/canceled: 还原
GW_PLATFORM_LIKE: str = r"gw\_%"                  # SQL LIKE 模式（转义下划线）
TERMINAL_STATUSES: frozenset[TaskStatus]
```

### 3.4 状态枚举（三层口径，§3.3 助手为唯一转换点）

#### 3.4.1 内部状态 `TaskStatus`（StrEnum，定义于 models.py，base.py re-export）

`queued / running / succeeded / failed / timeout / canceled`；`is_terminal` 属性。
`canceled` 为内部语义、无对外端点（V9 待验证上游取消 API）。

#### 3.4.2 tasks.status 落库值 `DbTaskStatus`（new-api 六值 + UNKNOWN）

`NOT_START / SUBMITTED / QUEUED / IN_PROGRESS / SUCCESS / FAILURE`（+`UNKNOWN` 网关不用）。
**网关只写 4 个值**：`SUBMITTED`（落库即此，progress `10%`）→ `IN_PROGRESS`（`30%`+）→
`SUCCESS`/`FAILURE`（`100%`）。timeout/canceled 折叠进 `FAILURE`，靠 `fail_reason`
前缀 `timeout:`/`canceled:`/`failed:` 区分（§13.4 `_fail_reason()` 语义，W2 唯一生成点）。

#### 3.4.3 对外 videos 状态 `app.schemas.VideoStatus`

`queued / in_progress / completed / failed`，与 new-api `ToVideoStatus()` 完全一致
（SUBMITTED/QUEUED→queued、IN_PROGRESS→in_progress、SUCCESS→completed、FAILURE→failed）。
查询/响应组装一律经 `to_video_status()`，禁止手写映射。

### 3.5 骨架已交付的其余共享模块

#### 3.5.1 `app/registry.py` — biz 注册表

```python
@dataclass BizConfig:  # 字段与 BIZ_CONFIGS/BIZ_CONFIGS_FILE 中的 JSON 对象一一对应（决策 B）
    biz: str; adapter: str; upstream_base_url: str; auth_type: str  # aksk_jwt|bearer_key
    auth_secret_ref: str; native_prefixes: list[str]; enabled: bool
    billing_keys: dict[str, Any]; default_freeze_amount_usd: str | None
    rate_limit: dict[str, Any]; newapi_channel_id: int | None; version: int
    display_name: str = ""; loaded_at: float = ...
    @property billing_mode -> str   # "prepaid"(默认)|"postpaid"

class BizRegistry:  # 配置快照 + 进程内 L1 + 文件 mtime watch；环境变量为唯一事实源
    async def get(self, biz: str, session: Any = None) -> BizConfig
        # L1(30s)→配置快照；未注册/disabled 抛 errors.not_found（404 OpenAI 风格）
        # session 形参仅为历史调用方签名兼容（DB 回源已删除），不使用
    def known_biz(self) -> list[str]          # 当前快照 biz 清单（观测/冒烟用）
    def reload(self) -> int                    # FILE 优先于内联 BIZ_CONFIGS；清空 L1
    def invalidate_local(self, biz: str) -> None
    async def invalidate_loop(self) -> None
        # BIZ_CONFIGS_FILE 设置时按 mtime 轮询（BIZ_WATCH_INTERVAL_SECONDS）热更；
        # 未设置时空转——改配置 = 改 .env + 重启 compose 服务

def parse_biz_configs(text: str) -> dict[str, BizConfig]  # JSON 数组解析+校验
registry: BizRegistry  # 模块级单例
```

**决策 B（注册表环境变量化）**：原 `gateway_biz_registry` 表 + L2 Redis +
pub/sub 失效广播全部删除；`BIZ_CONFIGS`（内联单行 JSON，经 .env / env_file
注入）或 `BIZ_CONFIGS_FILE`（挂卷文件，mtime watch 热更新）为唯一事实源。
多副本一致性由同一 env 注入保证；密钥一律 `auth_secret_ref` 引用 ENV 注入项。

`billing_keys` 键约定：`biz_type`(str，计费服务维度)、`metric`(str，默认 `"call"`)、
`billing_mode`(`"prepaid"|"postpaid"`)、`charge_on_get`(bool，默认 false)、
`allow_user_direct_callback`(bool，默认 false)。`rate_limit` 键：`user_rpm`/`biz_rpm`/`upstream_concurrency`。

#### 3.5.2 `app/errors.py` — 错误契约

`GatewayError(message, *, status_code, error_type, code, param, headers)`；
工厂：`unauthorized/forbidden/not_found/payment_required/rate_limited(retry_after)/
backpressure(retry_after)/upstream_error/idempotency_conflict`；
`error_body(message, error_type, *, code, param) -> dict`（OpenAI 风格唯一构造点）；
`gateway_exception_handler(request, exc)`（W1 在 main.py 对 `GatewayError` 与
`HTTPException` 注册）。**所有模块禁止裸 `raise HTTPException` 返回非 error 形制的 detail**
（历史形制 `{"error": {...}}` detail 会被处理器兼容透传）。

#### 3.5.3 `app/schemas.py` — 对外模型

`VideoStatus` / `VideoSubmitRequest` / `VideoRemixRequest` / `VideoSubmitResponse` /
`VideoStatusResponse` / `VideoError` / `ErrorResponse` / `CallbackEventEnvelope`
（字段见代码与架构 §11.2；供应商扩展一律走 `metadata`）。

#### 3.5.4 `app/db.py`

```python
def get_engine() -> AsyncEngine                       # 惰性单例（post-fork 安全）
def get_session_factory() -> async_sessionmaker[AsyncSession]
async def get_session() -> AsyncIterator[AsyncSession]  # FastAPI 依赖；异常自动 rollback
async def close_db() -> None
```
**零自有表（决策 A）**：`create_gateway_tables()` 已删除——网关无任何 MySQL
自有表/建表职责，本模块只连与 new-api 共享的实例（读写 `tasks` 自有行）。
事务纪律：调用方显式 `commit`；W2 终态迁移的 DB 写与 Redis 副作用（outbox/
delivery 入队）按「先 DB commit 后 Redis 入队」次序，失败由补偿通道收敛（§4.7）。

#### 3.5.5 `app/redis_client.py`

`async get_redis() -> redis.asyncio.Redis`（decode_responses=True 单例）/ `close_redis()`。

#### 3.5.6 `app/http_clients.py`

`upstream_client() / billing_client() / pricing_client() / delivery_client()`
（各自超时四元组见代码，架构 §8.1）/ `async close_http_clients()`。
billing_client/pricing_client 带 `base_url`（来自 settings）。

#### 3.5.7 `app/observability.py`

`setup_telemetry(app: FastAPI) -> None`：configure + instrument fastapi/httpx/sqlalchemy/redis。
W1 在 `create_app()` 内、路由注册前调用一次。

### 3.6 Redis key 命名空间（全量清单；新增 key 必须先登记本节）

| Key / 频道 | 类型 | TTL | 写方 | 读方 | 语义 |
|---|---|---|---|---|---|
| `tidx:{biz}:{upstream_task_id}` | STRING | `UPSTREAM_INDEX_TTL_SECONDS`（7d） | W2 submit | W4 receiver | 回调反查索引（决策 A-2）：上游 task_id → 网关 task_id；miss 走 SQL 兜底扫描 |
| `dlv:due` / `dlv:lease` / `dlv:dead` | ZSET | — | W4 dispatcher / 调度器 | W4 | 用户回调投递延迟队列（§3.5.8 原语；score=可领取/租约到期/死信时间） |
| `dlv:{id}` | HASH | 至收口/死信 | W4 dispatcher | W4 | 投递条目事实源（payload/state/attempts/lease_until + task_id/user_id/url/event_type） |
| `obx:due` / `obx:lease` / `obx:dead` | ZSET | — | W3 outbox / 调度器 | W3 | 计费 outbox 补偿队列（§3.5.8 原语；资金链路，>0 死信即告警） |
| `obx:{id}` | HASH | 至收口/死信 | W3 outbox | W3 | outbox 条目事实源（payload/state/attempts + task_id/op/last_error） |
| `debt:order:{request_id}` | HASH | 至清偿 | W1 透传 402 / W3 outbox | W3 | 欠费单（决策 A-9）：`{user_id, task_id, biz, amount, status(open|cleared), ...}`，request_id 幂等 |
| `debt:orders` | SET | — | 同上 | W3/运维 | open 欠费单 request_id 清单（清偿 SREM） |
| `apikey:{sha256(sk)}` | STRING JSON | 300s | auth 委托回源后 | auth | 委托结论缓存 L2（不落原始令牌） |
| `sksess:{task_id}` | STRING | deadline+1h | W2 submit | W2/W3 后台 | 可透传用户 sk（续期/outbox/对账取回凭据）；终态 transition DEL，敏感最小驻留 |
| `pricing:{biz}:{model}:{action}` | STRING JSON | 无（靠版本失效） | pricing 回源 | pricing | `{expr, expr_type, version, fallback_amount, updated_at}` |
| `pricing:version` | STRING 计数 | 永不过期 | 运营/逻辑服务变更 INCR | pricing | 失效广播版本号 |
| `cb:cap:{task_id}` | STRING | 任务 TTL | W2 submit | W4 receiver | capability token；**GET 不 GETDEL**（多次回调） |
| `wh:seen:{event_id}` | STRING | 86400s NX | W4 receiver | W4 | 回调幂等去重 |
| `queue:upstream_callbacks` | LIST | — | W4 receiver RPUSH | W4 consumer BLPOP/BRPOPLPUSH | 回调处理队列 |
| `idem:{user_id}:{key}` | STRING | 24h NX | W1 幂等 guard | W1 | Idempotency-Key 原子占位，值为首个响应+payload 哈希 |
| `idem:{user_id}:{req_hash}` | STRING | 24h | W1 透传 | W1 | 未带 Idempotency-Key 的透传 charge request_id 复用 |
| `freeze:shard:{task_id}` | HASH | 任务 TTL | W2 submit / W3 renewer | W2/W3 | `{seq, amount_usd, expires_at}` 热台账 |
| `freeze:renew:{task_id}` | STRING | 120s NX | W3 renewer | W3 | 多副本续期互斥锁 |
| `debt:{user_id}` | STRING | 至欠费清偿 | W1 透传 charge 402 | W1 | 欠费熔断名单：提交类 402 拒绝、查询类放行 |
| `circuit:{target}` | HASH | 永不过期 | W1 熔断器 | W1/W3/W4 | `{state, fail_count, opened_at}`；target ∈ `upstream:{biz}`/`billing-logic`/`billing-service`/`user-callback:{domain}` |
| `circuit:{target}:probe` | STRING | 30s NX | W1 熔断器 | — | half-open 单探针权 |
| `rl:user:{sk_hash}` | ZSET 滑动窗口 | 窗口长 | W1 | W1 | 用户级限流 |
| `rl:biz:{biz}` | STRING/Lua 令牌桶 | — | W1 | W1 | biz 级限流 |
| `rl:upstream:{biz}` | STRING 计数 Lua | 并发窗口 | W1/W2 | W1/W2 | 上游并发信号量（Lua 原子 INCR+EXPIRE/释放 DECR） |
| `rl:callback:{provider}` | STRING 固定窗口 | 60s | W4 | W4 | 回调端点防刷 |

### 3.7 环境变量清单（= `.env.example` 全量；config.py 字段一一对应）

| 变量 | config 字段 | 默认 | 说明 |
|---|---|---|---|
| `APP_ENV` / `APP_VERSION` | app_env / app_version | dev / dev | 观测 |
| `DATABASE_URL` | database_url | 本地 mysql+asyncmy | **与 new-api 共享实例** |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` / `DB_POOL_RECYCLE` / `DB_POOL_PRE_PING` | 同名 | 10/20/1800/true | 连接预算见架构 §14.2.6 |
| `REDIS_URL` | redis_url | 本地 | — |
| `BILLING_SERVICE_URL` / `PRICING_SERVICE_URL` | 同名 | 本地 | 计费两服务 |
| `PRICING_HTTP_TIMEOUT` | pricing_http_timeout | 3.0 | §5.2 |
| `QUOTA_PER_USD` | quota_per_usd | 500000 | quota=USD×500000，勿改 |
| `DEFAULT_TASK_TTL_SECONDS` | default_task_ttl_seconds | 172800 | 任务 deadline（48h 对齐方舟） |
| `NEWAPI_TASK_TIMEOUT_MINUTES` | newapi_task_timeout_minutes | 720 | **V20 实测后配置**；网关 deadline 必须更早 |
| `NEWAPI_SWEEP_MARGIN_SECONDS` | newapi_sweep_margin_seconds | 1800 | 安全余量 ≥2×轮询周期 |
| `FREEZE_SHARD_TTL_SECONDS` / `FREEZE_RENEW_WINDOW_SECONDS` | 同名 | 82800/3600 | 分片冻结 23h / 续期窗口 1h |
| `POLL_BATCH_SIZE` / `POLL_INTERVAL_SECONDS` / `POLL_BACKOFF_SECONDS` | 同名 | 50/2.0/(5,15,30,120) | 轮询 worker |
| `GATEWAY_PUBLIC_BASE_URL` | gateway_public_base_url | 本地 | 注入上游 callback_url 基址 |
| `DELIVERY_BACKOFF_SECONDS` / `DELIVERY_LEASE_SECONDS` | 同名 | (60,300,1800,7200,21600)/60 | 投递退避/租约 |
| `CALLBACK_SIGNING_SECRET_CURRENT` / `_OLD` | 同名 | dev-*/None | 用户回调签名密钥（轮换双密钥） |
| `TOKEN_L1_TTL_SECONDS` / `TOKEN_L1_MAXSIZE` / `TOKEN_REDIS_TTL_SECONDS` | 同名 | 60/10000/300 | 委托结论缓存（L1 进程内 / L2 Redis apikey:{sha256}） |
| `SYSTEM_API_TOKEN` | system_api_token | None | 跳过开关 `X-System-Token` 校验值；**未配置则 skip 头一律忽略**（启用即免签+免计费，仅限受控运维通道） |
| `KEYS_SERVICE_URL` / `KEYS_CACHE_TTL_SECONDS` | 同名 | None/300 | keys 轮询微服务 base url（V21 建议验证）；未配置或服务 5xx → 降级 env 静态密钥兜底；进程内租约缓存上限 |
| `BIZ_CONFIGS` / `BIZ_CONFIGS_FILE` | 同名 | None/None | biz 注册表（决策 B，唯一事实源）：内联单行 JSON / 挂卷文件（FILE 优先；示例见 `.env.example`） |
| `BIZ_WATCH_INTERVAL_SECONDS` | biz_watch_interval_seconds | 5.0 | BIZ_CONFIGS_FILE mtime 轮询间隔（热更） |
| `BIZ_L1_TTL_SECONDS` / `PRICING_L1_TTL_SECONDS` | 同名 | 30/300 | 进程内缓存 TTL（registry 无 L2 Redis，BIZ_REDIS_TTL_SECONDS 已删） |
| `UPSTREAM_INDEX_TTL_SECONDS` | upstream_index_ttl_seconds | 604800 | tidx 反查索引 TTL（7d）；SQL 兜底扫描窗口同口径 |
| `ASTEVAL_POOL_SIZE` / `ASTEVAL_CPU_LIMIT_SECONDS` / `ASTEVAL_MEM_LIMIT_MB` / `PRICING_EVAL_TIMEOUT_SECONDS` | 同名 | 4/2/256/5.0 | 沙箱 |
| `LOGFIRE_TOKEN` / `LOGFIRE_SAMPLE_HEAD` | 同名 | None/0.1 | 观测 |
| `BIND` | bind | 0.0.0.0:8000 | — |
| `GUNICORN_WORKERS` / `GUNICORN_TIMEOUT` / `GUNICORN_GRACEFUL_TIMEOUT` / `GUNICORN_LOG_LEVEL` | —（gunicorn.conf.py 直读） | 公式/120/30/info | W6 |
| `UPSTREAM_SECRET_KLING_AK` / `UPSTREAM_SECRET_KLING_SK` / `UPSTREAM_KEY_ARK` | —（适配器按 auth_secret_ref 直读 os.environ） | — | 上游凭证，Secret 注入 |

### 3.9 W1 模块契约（路由与应用装配）

#### 3.9.1 `app/auth.py`

鉴权**委托计费服务**（balance 语义，唯一事实源）：网关不自建 token 表查询、
不读 new-api token 缓存；缓存的只是「委托结论」而非鉴权本身。

```python
KEY_RE: re.Pattern  # re.compile(r"^sk-[A-Za-z0-9]{20,64}$")，兼容 new-api 形制
SKSESS_GRACE_SECONDS = 3600   # sksess TTL 在任务 deadline 之上再宽限 1h
SYSTEM_USER_ID = 0            # 系统跳过开关身份

class BillingAuthUnavailable(Exception):
    """计费服务鉴权委托不可达（5xx/超时/传输错误/坏报文）→ fail-closed 503。"""

@dataclass
class TokenInfo:               # 委托鉴权结论；raw 仅驻留内存供本次请求计费透传（不落缓存）
    user_id: int
    sk_hash: str               # sha256(raw)，缓存/限流索引
    group: str = "default"
    raw: str = ""
    is_system: bool = False    # 系统跳过开关身份（计费 no-op，§4.10）
    def dump(self) -> str: ...                # JSON；缓存不落 raw
    @classmethod
    def parse(cls, blob: str, raw: str) -> "TokenInfo": ...

class LocalTTLCache:                           # 进程内 LRU+TTL，容量有界
    def __init__(self, maxsize: int = 10_000, ttl: float = 60.0) -> None: ...
    def get(self, key: str) -> TokenInfo | None: ...
    def set(self, key: str, value: TokenInfo) -> None: ...

local_cache: LocalTTLCache                     # TTL 取 settings.token_l1_ttl_seconds

def token_hash(raw: str) -> str: ...           # SHA-256（缓存索引均用哈希不落明文）

async def verify_bearer(raw: str) -> TokenInfo | None:
    """四级管线：①KEY_RE 格式门禁（无 I/O）②SHA-256 哈希 ③L1 进程内(60s)→
    L2 Redis apikey:{h}(300s)（缓存委托结论）④委托
    GET {BILLING_SERVICE_URL}/api/v1/billing/balance（Bearer 用户 sk 透传）：
    200 取 user_id/group（兼容 {"data": {...}} 信封与平铺）；
    401/403 → None（无效令牌，不回填缓存）；
    5xx/超时/传输错误/坏报文 → BillingAuthUnavailable（fail-closed，绝不放行）。
    委托 200 才回填 L2+L1 并返回。"""

async def current_token(
    request: Request, authorization: Annotated[str | None, Header()] = None,
) -> TokenInfo:
    """FastAPI 依赖（两形态共用）。skip 开关生效 → system 身份（§4.10）；
    缺失/非法 → errors.unauthorized（401 OpenAI 风格）；
    BillingAuthUnavailable → errors.backpressure(503, Retry-After=5)。"""

# ---- 后台 user_sk 会话（sksess，敏感最小驻留） ----
async def store_user_sk(task_id: str, raw_sk: str, deadline_unix: int) -> None:
    """submit 成功时写 sksess:{task_id}（EX=deadline+1h）。"""
async def get_user_sk_for_task(task_id: str) -> str | None:
    """renewer/outbox/对账取回可透传的用户 sk；取不到 None（调用方告警）。"""
async def clear_user_sk(task_id: str) -> None:
    """终态 transition 时 DEL sksess:{task_id}。"""

async def get_owned_task(task_id: str, token: TokenInfo) -> dict[str, Any]:
    """防 IDOR：SELECT ... FROM tasks WHERE task_id=? AND platform LIKE 'gw\\_%'；
    不存在或 user_id 不符 → errors.not_found（404 而非 403，防存在性探测）；
    system 身份豁免归属校验。返回 dict 含 task_id/platform/user_id/status/progress/
    properties/private_data/data/fail_reason/created_at/updated_at/finish_time。"""
```

**系统跳过开关三态**（`_skip_auth_billing_applies`，审计 event=skip_auth_billing +
client_ip）：

| `X-Skip-Auth-Billing: true` | `SYSTEM_API_TOKEN` 配置 | `X-System-Token` | 行为 |
|---|---|---|---|
| 否 | — | — | 正常 sk 委托流程 |
| 是 | 未配置 | — | 头一律忽略，正常 sk 流程 |
| 是 | 已配置 | 错误/缺失 | 忽略 skip，按正常 sk 流程（防探测） |
| 是 | 已配置 | hmac.compare_digest 相等 | system 身份（user_id=0/group=system），跳过鉴权且计费 no-op |

#### 3.9.2 `app/middleware.py`

- `class CircuitBreaker`（Redis 共享状态）：`async def allow(target: str) -> bool`、
  `async def on_success(target: str)` / `async def on_failure(target: str)`；
  语义：`fail_count>=5 → open(30s) → half-open 单探针(NX 抢权) → closed`；429 不计失败。
  模块级单例 `circuit_breaker`。
- `async def check_user_rate_limit(token: TokenInfo, cfg: BizConfig | None) -> None`
  超限抛 `errors.rate_limited(retry_after)`；`async def check_biz_rate_limit(biz)`、
  `async def acquire_upstream_slot(biz) / release_upstream_slot(biz)`（Lua 原子化）。
- `async def idempotency_guard(request: Request, token: TokenInfo) -> str | None`
  提交端点依赖：`Idempotency-Key` 头存在时 `SET idem:{user}:{key} NX PX 24h`；
  占位失败 → 同 payload 哈希回放首个响应（抛专用 `IdempotentReplay(response)` 由
  W1 处理）/ 不同 payload → `errors.idempotency_conflict`；返回 key（供落 private_data）。
- `async def check_debt_block(token: TokenInfo) -> None`：`debt:{user_id}` 存在 →
  `errors.payment_required`（提交类端点调用；查询类不调用）。

#### 3.9.3 `app/healthz.py`

`router = APIRouter()`：`GET /healthz/live` 恒 200 `{"status":"ok"}`（零依赖）；
`GET /healthz/ready` 检查 Redis `PING` + DB `SELECT 1`，全过 200 否则 503
（不抛异常，直接构造 Response）。

#### 3.9.4 `app/worker.py`

`python -m app.worker` 入口：`asyncio.gather` 启动 W2 poller、W3 outbox worker、
W3 FreezeRenewer、W4 回调队列消费者、W4 DeliveryDispatcher 的 `run_forever()`；
每个任务包 `try/except` 重启循环 + `logfire.exception`；SIGTERM 时 cancel 并等待
（优雅停机 §8.5）。装配所需的 BillingServiceClient/PricingEvaluator/TaskManager/
session_factory 在此构造一次并注入。

#### 3.9.5 `app/routing/videos.py`

```python
router = APIRouter()

@router.post("/{biz}/v1/videos", status_code=201)
async def videos_submit(biz: str, body: VideoSubmitRequest, request: Request,
                        token: TokenInfo = Depends(current_token),
                        session: AsyncSession = Depends(get_session)) -> VideoSubmitResponse:
    """registry.get → 幂等 guard → check_debt_block →
    CanonicalTaskRequest 组装（metadata 提取 callback_url/resolution/mode/generate_audio
    等扩展；action 由 image/metadata.action 推导，对齐 new-api 枚举）→
    task_manager.submit_task(..., form="videos", idem_key=...) → 201 VideoSubmitResponse。
    402/429/503 经 errors 工厂；上游提交失败已由 manager 内 cancel 解冻后向上抛 502。"""

@router.get("/{biz}/v1/videos/{task_id}")
async def videos_get(biz: str, task_id: str,
                     token: TokenInfo = Depends(current_token)) -> VideoStatusResponse:
    """get_owned_task（404 防 IDOR）→ 组装：status=to_video_status、model 取自
    properties.origin_model_name、url 取自 private_data.result_url（SUCCESS 时）、
    error 取自 fail_reason（FAILED 时，timeout:/canceled: 前缀归一为 code）。"""

@router.get("/{biz}/v1/videos/{task_id}/content")
async def videos_content(biz: str, task_id: str,
                         token: TokenInfo = Depends(current_token)) -> Response:
    """get_owned_task → private_data.result_url 存在则 302 重定向（默认）或按
    biz 配置代理流式回传；未完成 → 409 OpenAI 风格错误。"""

@router.post("/{biz}/v1/videos/{video_id}/remix", status_code=201)
async def videos_remix(biz: str, video_id: str, body: VideoRemixRequest, request: Request,
                       token: TokenInfo = Depends(current_token),
                       session: AsyncSession = Depends(get_session)) -> VideoSubmitResponse:
    """get_owned_task 原任务（404）；非 SUCCESS → 422；request_snapshot 为基底 +
    body 白名单覆盖（prompt/metadata）→ action="remixGenerate" 新 task_id 独立
    freeze（架构 §13.4 remix 桩语义），form="videos_remix"。"""
```

模块级 `task_manager: TaskManager` 依赖装配：videos.py 声明
`task_manager: TaskManager` 模块变量，由 main.py/worker 装配注入
（`set_task_manager(tm)` 函数设置），与 §13.5 receiver 同款模式。

#### 3.9.6 `app/routing/dynamic_router.py`

`router = APIRouter()`，`@router.api_route("/{biz}/{native_path:path}", methods=["GET","POST","PUT","DELETE","PATCH"])`：

```python
async def passthrough(biz: str, native_path: str, request: Request,
                      token: TokenInfo = Depends(current_token),
                      session: AsyncSession = Depends(get_session)) -> Response:
    """架构 §13.1 语义为准：registry.get → native_prefixes 白名单（不符 404）→
    鉴权改写（摘 Authorization/Host/Content-Length，注入 adapter.auth_headers）→
    POST 且非 allow_user_direct_callback 时 adapter.rewrite_callback_url →
    upstream_client 转发（超时 → 504）→ 响应原样回传 + X-Gateway-Biz 头。
    计费闭环（§5.6）：GET 默认不计费（billing_keys.charge_on_get 可覆盖）；
    POST 2xx → 解析用量 → PricingEvaluator 求值 → charge 异步入 outbox
    （request_id='pt:...'，§4.8）；charge 402 → 欠费三连（欠费单+debt 名单+
    告警，响应仍原样回传 + X-Gateway-Billing: debt 头）；响应含上游 task_id →
    落 passthrough_tracked 自有行（W2 提供 track_passthrough_task()，§3.10.1）。"""
```

### 3.10 W2 模块契约（任务状态机）

#### 3.10.1 `app/tasks/manager.py`

```python
FREEZE_SHARD_TTL: int          # = settings.freeze_shard_ttl_seconds
RENEW_WINDOW: int              # = settings.freeze_renew_window_seconds

class PaymentRequired(Exception):
    """→ W1 转 HTTP 402；任务不落库，幂等键释放。"""

class TaskManager:
    def __init__(self, billing: BillingServiceClient, pricing: PricingEvaluator) -> None: ...

    async def submit_task(self, session: AsyncSession, *, biz_cfg: BizConfig,
                          req: CanonicalTaskRequest, token: TokenInfo,
                          form: str, idem_key: str | None) -> dict[str, Any]:
        """§13.4 语义为准，顺序不可换：①求值顶格预估（phase="freeze"）→
        ②freeze(request_id={task_id}:0, ttl=min(任务TTL, FREEZE_SHARD_TTL))，
        InsufficientBalance → PaymentRequired；③capability + adapter.submit，
        失败 cancel 当前分片后重抛；④INSERT tasks 自有行（§4.1 字段纪律）commit，失败同样 cancel；
        ⑤写 cb:cap / freeze:shard / tidx 反查索引 Redis（决策 A-2，§4.6）。返回 {"task_id", "status":"queued",
        "created_at": now_unix}。任务 TTL/deadline 计算：
        ttl=settings.default_task_ttl_seconds；
        deadline_unix = now + min(ttl, newapi_task_timeout_minutes*60 - sweep_margin)（§4.1）。"""

    async def transition(self, session: AsyncSession, *, task_id: str,
                         snapshot: TaskSnapshot, channel: str) -> bool:
        """唯一状态仲裁点（§4.3/§13.4）。SELECT ... FOR UPDATE（WHERE 必含
        platform LIKE 'gw\\_%'）→ 终态不可逆 + 乱序防护（rank 不回退）→
        SUCCESS 时先事务外求值（phase="settle"，PricingEvalError → settle 金额
        None + outbox reevaluate 标记）→ CAS UPDATE（WHERE status=旧值 AND
        status NOT IN ('SUCCESS','FAILURE')；rowcount!=1 → rollback + False）→
        终态副作用计划（§4.7 零自有表口径）：状态 UPDATE 先 commit，随后
        outbox（settle/cancel + cancel_prev_shards）/delivery（有 callback_url 时）
        入 Redis obx/dlv 队列；skip 路径（billing_state=none）计费 no-op 仅
        delivery（§4.10）。终态 DEL sksess:{task_id}。
        True=赢得迁移；False=竞态落败/乱序/不存在（正常路径非异常）。
        channel ∈ "poll"|"callback"|"sweep"（审计用）。"""

    async def track_passthrough_task(self, session: AsyncSession, *, biz_cfg: BizConfig,
                                     token: TokenInfo, upstream_task_id: str,
                                     action: str, request_snapshot: dict[str, Any],
                                     raw_response: dict[str, Any]) -> str:
        """透传 tracked 行（§5.6.4）：form='passthrough_tracked'、billing_state='none'、
        不 freeze；其余字段纪律同 submit_task 第④步。返回网关 task_id。"""

async def current_freeze_shard(session: AsyncSession, task_id: str) -> int:
    """两级读取（§13.4 _current_freeze_shard 语义）：Redis 热台账 → tasks 行
    private_data.gateway.freeze_shard_seq（顺带重建 Redis）→ 双失回退 0 + 告警。
    W3 outbox worker 与 renewer 共用。"""
```

状态映射一律使用 `app.tasks.models` 的 `db_status/db_progress/db_to_internal`；
fail_reason 生成单点：`_fail_reason(target, snapshot) -> str`（`timeout: `/`canceled: `/`failed: ` 前缀）。

`private_data` 写入结构（§4.1，键清单封闭，新增键须改 SPEC）：

```json
{
  "upstream_task_id": "...",           // new-api 既有键
  "result_url": "...",                 // new-api 既有键（SUCCESS 时写）
  "gateway": {
    "biz": "...", "form": "videos|videos_remix|passthrough_tracked",
    "sk_hash": "...", "idempotency_key": null, "callback_url": null,
    "billing_state": "none|frozen|settled|cancelled|charged",
    "skip_billing": false,                 // 系统身份提交（§4.10）→ true；终态计费 no-op
    "deadline_unix": 0, "freeze_shard_seq": 0,
    "freeze_shard_amount_usd": "...", "freeze_shard_expires_at": 0,
    "next_poll_at": 0,                 // 轮询退避游标（W2 维护）
    "usage_actual": null,              // 终态实收信号（W2 写）
    "request_snapshot": {}             // CanonicalTaskRequest asdict（remix 继承源）
  }
}
```

`properties` 结构（与 new-api 完全同形）：`{"input": prompt[:200],
"upstream_model_name": 映射后模型名, "origin_model_name": 用户请求原始模型}`。

#### 3.10.2 `app/tasks/poller.py`

```python
class PollWorker:
    def __init__(self, task_manager: TaskManager,
                 session_factory: async_sessionmaker[AsyncSession]) -> None: ...
    async def run_forever(self) -> None:
        """每轮：SELECT ... FROM tasks WHERE platform LIKE 'gw\\_%'
        AND status IN ('SUBMITTED','QUEUED','IN_PROGRESS')
        AND JSON_EXTRACT(private_data,'$.gateway.next_poll_at') <= now
        ORDER BY submit_time LIMIT :batch FOR UPDATE SKIP LOCKED →
        deadline_unix 到期 → transition(timeout 快照, channel="sweep")；
        否则按 biz 分组 adapter.poll()（填 SubmitContext.action 从行内 action 取）→
        transition(channel="poll")；未推进的行回写 next_poll_at=now+backoff(attempt)
        （序列 settings.poll_backoff_seconds 封顶+jitter）。
        空轮 sleep(settings.poll_interval_seconds)；异常 logfire.exception 后继续。
        绝不扫描/触碰 platform 非 'gw_' 前缀的行（§4.1 纪律）。"""
```

### 3.11 W3 模块契约（计费）

#### 3.11.1 `app/billing/client.py`

```python
QUOTA_PER_UNIT = 500_000

class InsufficientBalance(Exception): ...        # 计费服务 402 → W1 转 HTTP 402
class BillingLockBusy(Exception):
    def __init__(self, retry_after_ms: int): ...  # 409；同 request_id 退避重试安全

class BillingServiceClient:
    def __init__(self) -> None: ...              # 持 billing_client() 单例
    async def freeze(self, *, request_id: str, biz_type: str, metric: str,
                     amount_usd: Decimal, ttl_seconds: int, user_sk: str,
                     attrs: dict[str, Any] | None = None) -> dict[str, Any]: ...
    async def settle(self, *, request_id: str, actual_usd: Decimal, user_sk: str,
                     attrs: dict[str, Any] | None = None) -> dict[str, Any]: ...
    async def cancel(self, *, request_id: str, user_sk: str) -> dict[str, Any]: ...
    async def charge(self, *, request_id: str, biz_type: str, metric: str,
                     amount_usd: Decimal, user_sk: str,
                     verify_only: bool = False) -> dict[str, Any]: ...
```

语义：POST `{billing_service_url}/api/v1/billing{path}`，`Authorization: Bearer {user_sk}`
（透传用户令牌，§5.1）；金额字符串序列化；freeze 的 `ttl_seconds=min(ttl, 86400)`；
402→InsufficientBalance、409→BillingLockBusy（tenacity 重试 ≤5 次，等待策略优先消费
retry_after_ms）；5xx/超时向上抛（调用方入 outbox）。响应取 `resp.json()["data"]`。

#### 3.11.2 `app/billing/pricing.py`

```python
@dataclass
class PricingLogic:
    expr: str
    expr_type: str                  # 'asteval' | 'python_func' | 'json_logic'
    version: int
    fallback_amount_usd: Decimal

class PricingEvalError(RuntimeError): ...

class PricingEvaluator:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None: ...
    async def get_logic(self, biz: str, model: str, action: str) -> PricingLogic:
        """L1(5min,settings.pricing_l1_ttl_seconds)→L2 Redis pricing:{biz}:{model}:{action}
        →L3 GET /api/v1/pricing/logic?biz&model&action（pricing_client，超时3s重试1次熔断）。
        全 miss → fail-closed：用 biz 注册表 default_freeze_amount_usd 构造
        固定金额逻辑（expr 形如常量）并告警；绝不放行免费请求。expr 长度>1024 拒收。"""
    async def get_logic_for_task(self, session: AsyncSession, task_id: str) -> PricingLogic:
        """从 tasks 行 private_data.gateway.request_snapshot 取回 (biz, model, action)
        后委托 get_logic（settle 阶段用，保证与 freeze 同维度）。"""
    async def evaluate(self, logic: PricingLogic, context: dict[str, float | str],
                       *, phase: str = "freeze") -> Decimal:
        """asteval → sandbox 子进程求值（超时 settings.pricing_eval_timeout_seconds）；
        json_logic → 无代码执行面求值。结果 quantize 6 位小数 ROUND_HALF_UP。
        失败：phase='freeze' → 返回 fallback_amount_usd + warning（fail-closed）；
        phase='settle' → 抛 PricingEvalError（绝不静默顶格，§13.3）。"""
```

求值上下文变量名契约（§5.3，V8 建议验证）：`duration:float, resolution:str, mode:str,
quantity:float, usage_tokens:float, generate_audio:float(0/1), has_image_input:float(0/1),
service_tier:str`。W5 `estimate_usage()` 与 W2 `_actual_context()` 必须产出同名键。

#### 3.11.3 `app/billing/sandbox.py`

```python
MAX_EXPR_LEN = 1024

def ast_precheck(expr: str) -> None:
    """§13.3 _ast_precheck 语义：长度>1024 / 节点>200 / ** 右操作数非 ≤4 小字面量 /
    字符串字面量 >256 → ValueError。"""

def eval_expr_subprocess(expr: str, context: dict[str, float | str]) -> float:
    """子进程入口（ProcessPoolExecutor target，必须模块级可 pickle）：
    setrlimit CPU=settings.asteval_cpu_limit_seconds / AS=asteval_mem_limit_mb →
    ast_precheck → asteval Interpreter(minimal=True, no_while=True, no_for=True,
    no_functiondef=True, no_print=True, use_numpy=False, max_time=10,
    builtins_readonly=True, readonly_symbols=[abs,min,max,round,float,int]) →
    symtable 仅注入 context → 结果必须非负数值否则 ValueError。"""
```

进程池：模块级 `get_eval_pool() -> ProcessPoolExecutor`（惰性，大小
settings.asteval_pool_size）。`python_func` 类型 V5 未定 → **不实现 exec 路径**，
`evaluate` 遇 `python_func` 按求值失败处理（freeze 降级/settle 抛错）。

#### 3.11.4 `app/billing/outbox.py`

```python
class OutboxWorker:
    def __init__(self, billing: BillingServiceClient, pricing: PricingEvaluator,
                 session_factory: async_sessionmaker[AsyncSession]) -> None: ...
    async def run_forever(self) -> None:
        """零自有表（决策 A-4）：Redis obx 队列（§3.5.8）——调度器每轮先 Lua
        回收过期 lease 回 due，再 Lua 原子领取到期项（多副本互斥与原 SKIP LOCKED
        等价）→ 按 op 调 BillingServiceClient（payload.request_id 原样，幂等安全；
        settle 且 payload.reevaluate=true → 先 PricingEvaluator.evaluate(phase='settle')
        重估，仍失败保持挂起不计 attempts）→ 成功后：条目出队 + tasks 行
        billing_state 回写（settle→'settled'、cancel→'cancelled'、charge→'charged'；
        UPDATE 带 platform LIKE 'gw\\_%' 条件，不校验 status）+
        payload.cancel_prev_shards 逐个 cancel（幂等无副作用）→
        失败 attempts+1、指数退避+jitter 重排、attempts>20 死信（obx:dead）告警。
        计费审计改 logfire 结构化日志（决策 A-5，对账读计费服务 /billing/logs）。"""
```

#### 3.11.5 `app/billing/renewer.py`

```python
class FreezeRenewer:
    def __init__(self, billing: BillingServiceClient,
                 session_factory: async_sessionmaker[AsyncSession]) -> None: ...
    async def run_forever(self) -> None:
        """§13.4 FreezeRenewer 语义为准：5min 一轮扫 billing_state='frozen' 在途行；
        分片 expires_at-now < RENEW_WINDOW → freeze:renew:{task_id} NX 锁 →
        freeze({task_id}:{seq+1}, ttl=min(剩余, FREEZE_SHARD_TTL)) → cancel 旧分片 →
        回写 Redis 热台账 + JSON_SET private_data '$.gateway.freeze_shard_seq'
        （WHERE 含 platform LIKE 'gw\\_%'）→ 失败告警下轮重试。
        user_sk 取回：W1 auth.py `get_user_sk_for_task(task_id)` 读
        sksess:{task_id}（§3.9.1）；取不到 None → 调用方告警人工介入。"""
```

#### 3.11.6 `app/billing/reconcile.py`

```python
async def run_daily_reconciliation(session_factory, billing: BillingServiceClient,
                                   pricing: PricingEvaluator) -> None:
    """§5.5 五项：frozen>24h 任务对账轮询 / 台账流水比对 / kling 3.0 三方对账 /
    对账结果走 logfire 结构化日志（决策 A-5，不落自有表）/ tasks 滞留巡检告警。"""
```

### 3.12 `app/keys.py`（keys 轮询微服务客户端）

> **【建议验证 V21】** 端点/字段为设计约定，需与 keys 微服务实际契约对齐
> （字段名集中在一处解析，便于契约确认后改）。

```python
@dataclass
class KeyLease:
    """acquire 得到的密钥租约（credentials 键集与 SubmitContext.secrets 对齐）。"""
    key_id: str
    credentials: dict[str, str]     # ak/sk（aksk_jwt）或 api_key（bearer_key）
    expires_at: float               # monotonic 到期点；fresh() 判定

@dataclass
class KeyProviderClient:
    """GET {KEYS_SERVICE_URL}/keys/{provider}/acquire
      → 200 {"key_id", "credentials": {"ak"?, "sk"?, "api_key"?}, "ttl"?}；
      进程内缓存 min(服务端 ttl, KEYS_CACHE_TTL_SECONDS)（默认 300s）。
    POST {KEYS_SERVICE_URL}/keys/{key_id}/report 体 {"ok", "status_code"?}
      ——上游 401/403 由适配器同步上下文触发 report_auth_failure（租约立即剔除
      + 异步上报，供轮询剔除坏 key）；report 尽力而为，失败仅告警。
    降级：KEYS_SERVICE_URL 未配置（enabled=False，acquire 恒 None、report 恒
    no-op）或 acquire 5xx/超时/坏报文 → None，由
    manager.resolve_submit_secrets 回退 env 静态密钥（§3.13 例外条款直读
    os.environ 逻辑保留为 fallback）。4xx（非 5xx）视为契约异常同样回退。"""

key_provider: KeyProviderClient    # 模块级单例（进程内租约缓存随进程生命周期）
```

### 3.12.1 `app/callbacks/receiver.py`（W4）

```python
router = APIRouter()
NOT_FOUND_RETRY_DELAYS = [2, 5, 15, 30, 60]

@router.post("/callbacks/{biz}/{provider}/{capability}")
async def recv_upstream_callback(biz: str, provider: str, capability: str,
                                 request: Request,
                                 session: AsyncSession = Depends(get_session)) -> Response:
    """顺序不可颠倒（§7.1）：raw=await request.body()（原始字节）→
    resolve_gateway_task_id → cb:cap GET + hmac.compare_digest 校验（失败 401）→
    HMAC 框架（有 X-Signature 才强制，±300s 重放窗）→ adapter.parse_callback
    （异常 400）→ wh:seen:{event_id} SET NX EX 86400（重复 200 幂等 ACK）→
    RPUSH queue:upstream_callbacks → 202。本端点不过 Bearer 中间件，但过限流/观测。"""

async def resolve_gateway_task_id(session: AsyncSession, biz: str, provider: str,
                                  raw: bytes) -> str | None:
    """§13.5 _resolve_gateway_task_id 语义：①external_task_id/external_id 回显直取；
    ②否则 upstream_id（id/task_id 字段）→ Redis 优先 GET tidx:{biz}:{upstream_task_id}
    （决策 A-2），miss 回 tasks 表 SQL 兜底扫描 + 校验 platform LIKE 'gw\\_%' 双保险。
    provider→platform 用 platform_for(provider)。"""

async def process_upstream_callback(msg: dict[str, Any]) -> None:
    """队列消费者（§13.5）：parse → 反查（not-found 按 NOT_FOUND_RETRY_DELAYS 延迟重试，
    仍无 → 丢弃+告警，轮询兜底）→ task_manager.transition(channel='callback')。"""

class CallbackQueueConsumer:
    def __init__(self, task_manager: TaskManager, session_factory) -> None: ...
    async def run_forever(self) -> None:
        """BRPOPLPUSH queue:upstream_callbacks → queue:upstream_callbacks:processing
        （可靠队列模式，处理完 LREM；副本死亡消息不丢，§8.5）→ process_upstream_callback。"""
```

模块级 `task_manager: TaskManager` + `set_task_manager(tm)` 装配（同 videos.py 模式）。

### 3.12.2 `app/callbacks/dispatcher.py`（W4）

```python
BACKOFF_SECONDS = [60, 300, 1800, 7200, 21600]     # 来自 settings.delivery_backoff_seconds
LEASE_SECONDS = 60

def sign_headers(user_id: int, raw_body: bytes, delivery_id: str) -> dict[str, str]:
    """§13.6 _sign_headers 语义：X-Signature 't={ts},v1={hmac_hex}' +
    轮换期 v1_old；X-Delivery-Id=delivery_id（evt id，重试不变）。"""

class DeliveryDispatcher:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None: ...
    async def run_forever(self) -> None:
        """§13.6 语义为准：同事务 SELECT FOR UPDATE SKIP LOCKED 领取（含 lease 过期回收）
        → UPDATE delivering+lease → commit 后投递（delivery_client，超时10s）→
        2xx delivered / 410 或 4xx(除408,429) dead / 5xx·超时 退避重排（attempts 超上限
        dead + 告警）。域级熔断 user-callback:{domain}（open 时直接重排不投递）。"""
```

### 3.13 W5 模块契约（适配器）

#### 3.13.1 `app/adapters/kling.py`

`class KlingAdapter`：`name="kling"`、`callback_capability=True`、
`echoes_external_task_id=True`。两代并存（代际由 model 名判断：`kling-v3*` 或含 `3.0`）：
旧版路径 `v1/videos/{text2video,image2video}`、duration 为**字符串**、成功态 `succeed`；
3.0 路径 `{text,image}-to-video/kling-3.0`、settings/options 信封、成功态 `succeeded`、
`billing[].amount` 实收信号。鉴权 AK/SK HS256 JWT（`{iss,exp:+1800,nbf:-5}`，进程内缓存
key 含 AK、过期前 60s 刷新）。信封 `code!=0` → UpstreamBizError(code)。模块底部
`register(KlingAdapter())`。事实依据：简报 A §3；callback schema V1/V2 未确认——
`parse_callback` 复用查询响应解析（行业惯例），字段缺失容错。

#### 3.13.2 `app/adapters/seedance.py`

`class SeedanceAdapter`：`name="seedance"`、`callback_capability=True`、
`echoes_external_task_id=False`（回调只有 `cgt-` 前缀上游 id → W4 走索引表反查）。
端点 `POST {base}/api/v3/contents/generations/tasks`、查询 `GET .../tasks/{id}`；
`Authorization: Bearer $ARK_API_KEY`（os.environ[cfg.auth_secret_ref]）。
状态映射：`queued→QUEUED, running→RUNNING, succeeded→SUCCEEDED, failed→FAILED,
expired→TIMEOUT`。实收信号 `usage.completion_tokens` + 实际 resolution + generate_audio。
`parse_callback` 直接复用查询响应解析（回调体=查询响应体，简报 A §4 已确认）。
模块底部 `register(SeedanceAdapter())`。

---

## 4. 横切规则（不可违反）

### 4.1 tasks 表三件套纪律（R7.1，简报 C §四）

1. **陌生 platform**：网关行 `platform` 恒为 `platform_for(adapter)` = `gw_{adapter}`；
   一切 UPDATE/DELETE tasks 的 WHERE 必含 `platform LIKE 'gw\_%'`（`GW_PLATFORM_LIKE`）
   或对已知行先校验前缀；new-api 自有行对网关**只读**。
2. **quota 恒 0**：INSERT 显式写 0，永不更新该列（new-api 超时清扫器无视 platform，
   quota=0 保证被扫到也只置失败不动账——最大资金风险点）。
3. **网关 deadline 早于 new-api 清扫器**：`deadline_unix = submit_time +
   min(default_task_ttl_seconds, newapi_task_timeout_minutes*60 - newapi_sweep_margin_seconds)`，
   保证网关总是先于清扫器把自有行收敛到终态。

**写入配套纪律**：全部 bigint 时间列 INSERT 显式自填——`created_at/updated_at/submit_time=now`、
`start_time/finish_time=0`（**绝不 NULL**，否则 `start_time=0` 判断不成立）、
`fail_reason=''`（V18 实测若默认 NULL 可改 NULL 并回填本节）；每次 UPDATE 刷新 `updated_at`；
一切状态迁移走 `TaskManager.transition()` status-CAS，**禁止散落的裸 UPDATE status**；
禁止无条件批量 UPDATE（new-api `TaskBulkUpdateByID` 是反例）；不补写 new-api `logs` 表
（计费审计落 logfire 结构化日志，决策 A-5）。

### 4.2 路由注册顺序（不可妥协的不变量）

`app/main.py` 中 include_router 顺序 = Starlette 首匹配优先级：

```
app.include_router(health_router)       # /healthz/live /healthz/ready
app.include_router(callbacks_router)    # /callbacks/{biz}/{provider}/{capability}
# docs/openapi 由 FastAPI 创建时注册，天然先于用户路由
app.include_router(videos_router)       # /{biz}/v1/videos（四端点精确路由）
app.include_router(dynamic_router)      # ANY /{biz}/{native_path:path} —— 永远最后
```

catch-all 若先于固定路由会吞掉回调与探针（计费无法收敛、Pod 被反复摘除）。
W6 `test_smoke.py` 必须含注册顺序断言（遍历固定路径断言不被 catch-all 截获）。

**层级冲突约定**：`POST /{biz}/v1/videos`（三层）走 videos 契约；
`POST /{biz}/v1/videos/text2video`（四层）落入 catch-all 走透传。网关 task_id
恒 `task_` 前缀，与上游原生 action 段（text2video/image2video）不歧义；
W5/W1 在 biz 注册校验钩子中强制 native_prefixes 与 `v1/videos` 的前缀冲突扫描。

### 4.3 计费 request_id 分片规则

- 任务形态：`request_id = {task_id}:{seq}`（首片 `:0`；续期分片 `:{seq+1}`；
  终态 settle/cancel 打当前活跃分片 + `cancel_prev_shards` 逐个 cancel 历史分片）。
- 透传 charge：`pt:` 前缀——带 Idempotency-Key 时 `pt:{user_id}:{sha256(key)}`；
  未带时 `pt_{ulid}` 并写 `idem:{user}:{req_hash}`（24h）供重试命中复用；
  GET 查询类不生成计费 request_id。
- 金额纪律：内部全程 `Decimal` 美元 ≤6 位小数，字符串序列化；`ttl_seconds` 必传
  （服务端 >86400 截断，分片 ttl 恒 ≤82800）。

### 4.4 错误响应格式

所有非 2xx 响应体恒为 `{"error": {"message", "type", "param", "code"}}`
（OpenAI 风格，对齐简报 A §1 new-api 约定）。只允许两条产出路径：
① `app/errors.py` 工厂函数抛 `GatewayError`；② HTTPException detail 已是
`{"error": {...}}` 形制（处理器兼容透传）。`type` 取值：
`authentication_error`(401) / `permission_error`(403) / `invalid_request_error`(400/404/422) /
`billing_error`(402) / `rate_limit_error`(429) / `upstream_error`(502/504) /
`idempotency_error`(409) / `server_error`(5xx)。429/503 必带 `Retry-After` 头。

### 4.5 SQL 方言与 MySQL 纪律

- 所有 SQL 为 **MySQL 8 方言**：无 `RETURNING`、无 `::jsonb`、无部分索引；
  JSON 列直接收 JSON 文本（`json.dumps` 后绑定）；批量 IN 用
  `text(...).bindparams(bindexpanding=True)`；时间比较口径统一 `UTC_TIMESTAMP(6)`
  （NOW() 返回会话时区，禁止混用）。
- `` `group` `` 保留字：ORM 已处理（属性 `group_`）；**任何手写 SQL 必须反引号**。
- 多副本并发领取一律 `SELECT ... FOR UPDATE SKIP LOCKED`（MySQL 8 语义=PG）。
- utf8mb4；索引列 varchar ≤191（对齐 new-api 惯例）。

### 4.6 回调反查纪律

kling（echoes_external_task_id=True）→ 回调直接取 `external_task_id`；
seedance 等不回显上游 → **Redis 优先（决策 A-2）**：`GET tidx:{biz}:{upstream_task_id}`
（W2 submit 时写入，TTL `UPSTREAM_INDEX_TTL_SECONDS` 默认 7d）；miss 时回 tasks 表
SQL 兜底扫描 + 校验 `platform LIKE 'gw\_%'` 双保险（防 task_id 撞车串号，简报 C §四.9）。
原 `gateway_task_upstream_index` 表已删除（零自有表）。

### 4.7 事务性 outbox（零自有表口径）

终态副作用不再与状态 CAS 同 DB 事务：状态 CAS UPDATE 先 commit，outbox/delivery
条目随后入 Redis 队列（`obx`/`dlv`，§3.5.8；入队失败仅告警，由对账/轮询兜底
收敛——决策 A-4 接受的最终一致窗口）。`billing_state` 的 settled/cancelled/charged
仍由 W3 outbox worker 在计费调用成功后回写（定向 UPDATE 带 platform 前缀、不校验
status；tasks 是共享表，继续写）。计费审计不再落 `gateway_billing_audit` 表，改
logfire 结构化日志（决策 A-5），对账读计费服务 `/billing/logs`。

### 4.8 透传计费闭环

默认 postpaid+charge（§3.9.6 语义）；上游调用前先 `charge(verify_only=true)` 预检
余额（决策 A-9，402 直接拒绝零上游成本，biz 可关 `billing_keys.verify_only_precheck=false`）；
charge 402 → 欠费三连（Redis 欠费单 `debt:order:{request_id}` HASH + `debt:orders`
SET + `debt:{user_id}` 熔断名单 + 告警），响应原样回传 + `X-Gateway-Billing: debt`。

### 4.9 日志与观测纪律

结构化日志参数化传值（不拼 f-string）；prompt 等用户内容不进日志；
金额用字符串（防 scrubbing 误伤）；异步段用 `task_id` span 属性关联提交 trace；
资金链路事件（扣费失败/续期失败/欠费单/死信）`logfire.error/warning` 必发（>0 即告警口径）。

### 4.10 系统跳过开关（skip 路径计费 no-op 仅 delivery）

`X-Skip-Auth-Billing: true` + 有效 `X-System-Token`（三态判定见 §3.9.1）→
system 身份（user_id=0，`is_system=True`）。skip 路径**计费全链 no-op**：
submit 不 freeze（任务行 `billing_state='none'` + `skip_billing=true`，无
冻结单故 renewer 不续期、不写 sksess）；终态 transition **不入队计费 outbox，
仅保留用户回调 delivery 入队**；get_owned_task 豁免归属校验。每次生效记
审计日志 event=skip_auth_billing + client_ip。SYSTEM_API_TOKEN 未配置时
skip 头一律忽略——启用该开关等同免签+免计费，仅限受控运维通道。

---

## 5. 数据模型速查

### 5.1 共享表 tasks（new-api 所有；网关只读共享、只写自有行）

列集与简报 C §一 逐列一致，ORM 见 `app/tasks/models.py::Task`（19 列，冒烟测试逐列断言）。
网关概念 → 列映射的权威表在架构文档 §4.2，要点：

| tasks 列 | 网关取值 |
|---|---|
| `task_id` | `new_task_id()` = `task_`+32 hex |
| `platform` | `gw_{adapter}` |
| `user_id` / `` `group` `` / `channel_id` | 令牌解析 / 令牌分组 / `biz_cfg.newapi_channel_id` |
| `quota` | 恒 0 |
| `action` | `generate`/`textGenerate`/`firstTailGenerate`/`referenceGenerate`/`remixGenerate`（W1 推导，对齐 new-api 枚举） |
| `status` / `progress` | §3.4.2 四值映射（models.py 助手） |
| `properties` / `private_data` / `data` | 结构见 §3.10.1（JSON 文本落库） |
| `fail_reason` | `timeout:`/`canceled:`/`failed:` 前缀单一口径 |
| 五个 bigint 时间列 | §4.1 自填纪律 |

### 5.2 零自有表（决策 A：原 9 张 `gateway_` 表全部删除，一张不留）

网关不再拥有任何 MySQL 表、不再携迁移（`migrations/` 与 alembic 已删除；
compose 无 `docker-entrypoint-initdb.d`）。原表语义映射：

| 原自有表 | 新载体 |
|---|---|
| `gateway_biz_registry` | **环境变量** `BIZ_CONFIGS` / `BIZ_CONFIGS_FILE`（决策 B；.env/env_file 承载，§3.5.1） |
| `gateway_task_upstream_index` | Redis `tidx:{biz}:{upstream_task_id}`（决策 A-2；miss SQL 兜底，§4.6） |
| `gateway_callback_deliveries` | Redis 延迟队列 `dlv:*`（决策 A-4；§3.5.8 原语） |
| `gateway_billing_outbox` | Redis 延迟队列 `obx:*`（决策 A-4；§3.5.8 原语） |
| `gateway_billing_audit` | logfire 结构化日志（决策 A-5）；对账读计费服务 `/billing/logs` |
| `gateway_pricing_cache` | 进程内 L1 + 计费逻辑服务回源（`pricing:{...}` Redis 缓存保留，非自有表） |
| `gateway_freeze_shards` | Redis 热台账 `freeze:shard:{task_id}` + tasks 行 `private_data.gateway` 冷备（双丢告警人工） |
| `gateway_reconciliation_reports` | 对账结果走 logfire + 计费服务流水（不落自有表） |
| `gateway_debt_orders` | Redis `debt:order:{request_id}` HASH + `debt:orders` SET（决策 A-9，§4.8） |

Redis 全量键清单见 §3.6；持久化要求 AOF everysec（丢失窗口 ≤1s，§3.5.8）。
`app/tasks/models.py` 现仅含共享 `tasks` 行模型 + 状态枚举/映射。

---

## 6. 分工表 W1~W6

### 6.1 模块 → 文件 → 依赖

| 模块 | 文件清单（全权负责） | 依赖（只许依赖这些） | 可完全并行？ |
|---|---|---|---|
| **W1** 路由与装配 | `app/main.py`、`app/auth.py`、`app/healthz.py`、`app/middleware.py`、`app/worker.py`、`app/routing/videos.py`、`app/routing/dynamic_router.py` + `tests/test_auth.py`、`test_routing_order.py`、`test_videos.py` | 骨架全部；W2 `TaskManager`/`PaymentRequired`（按 §3.10.1 签名，可 mock）；W3 `PricingEvaluator`（透传求值，按 §3.11.2 签名 mock） | ⚠️ 代码可并行写（签名已钉死），集成须等 W2/W4 |
| **W2** 任务状态机 | `app/tasks/manager.py`、`app/tasks/poller.py` + `tests/test_task_manager.py`、`test_poller.py` | 骨架全部；W3 `BillingServiceClient`/`PricingEvaluator`/`InsufficientBalance`/`PricingEvalError`（按 §3.11 签名 mock）；W5 适配器（按 §3.2 协议 mock） | ✅ 完全并行 |
| **W3** 计费 | `app/billing/{client,pricing,sandbox,outbox,renewer,reconcile}.py` + `tests/test_billing_client.py`、`test_pricing_sandbox.py`、`test_outbox.py`、`test_renewer.py` | 骨架全部；W1 `get_user_sk_for_task`（按 §3.9.1 签名 mock）；W2 `current_freeze_shard`（按 §3.10.1 签名 mock） | ✅ 完全并行 |
| **W4** 回调 | `app/callbacks/{receiver,dispatcher}.py` + `tests/test_receiver.py`、`test_dispatcher.py` | 骨架全部；W2 `TaskManager`（按 §3.10.1 签名 mock）；W5 适配器（按 §3.2 协议 mock） | ✅ 完全并行 |
| **W5** 适配器 | `app/adapters/{kling,seedance}.py` + `tests/test_kling.py`、`test_seedance.py` | 仅骨架（base 协议 + http_clients + config） | ✅ 完全并行 |
| **W6** 运维与测试基建 | `gunicorn.conf.py`、`Dockerfile`、`docker-compose.yml`、`tests/conftest.py`、`tests/test_smoke.py`、README 扩写 | 骨架全部；各模块路由/worker 装配按 SPEC 签名 | ✅ 并行（冒烟测试最后随集成调通） |

### 6.2 骨架已交付（不属于任何 W，改动须 SPEC 变更）

`pyproject.toml`、`app/{__init__,config,errors,schemas,db,redis_client,http_clients,
observability,registry}.py`、`app/tasks/models.py`、`app/adapters/{__init__,base}.py`、
各子包 `__init__.py`、`.env.example`、`.gitignore`、`README.md`、`tests/test_skeleton_imports.py`。

### 6.3 合并顺序建议

```
第 0 批（已在 main）：骨架首提交
第 1 批（互不阻塞，任意序）：W5 → W3
第 2 批：W2（依赖 W3 真实代码做联调）→ W4（依赖 W2）
第 3 批：W1（装配全部路由/worker）
第 4 批：W6 冒烟 + 运维件收口
```

git 协作：各代理用 worktree/分支 `feat/w{n}-*`，rebase 到最新 main 后 MR；
骨架文件出现冲突立即停止并报备（说明 SPEC 需变更），不得私自覆盖他人改动。

---

## 7. 测试与验收

### 7.1 单元测试要求（每模块；不依赖真实 MySQL/Redis/上游）

- **DB mock**：用 `unittest.mock.AsyncMock` 包装 session（`execute` 返回预置
  mappings/rowcount），或以 SQLite+aiosqlite 仅测纯 SQL 拼接函数（禁止依赖
  MySQL 专有语法在 SQLite 上跑）；模型层测试用 `Task.__table__` 元数据断言（骨架已示范）。
- **Redis mock**：`fakeredis` 或 AsyncMock（`get/set/hset/hgetall/pubsub`）。
- **上游 mock**：`respx` 拦 httpx 单例；W5 每个适配器至少覆盖：submit 成功/
  信封错误（BizError）/429（RateLimitError）/poll 各状态映射（含 succeed vs
  succeeded 拼写）/parse_callback 坏报文/estimate_usage 上下文键全集/JWT 缓存含 AK。
- **W2**：transition 竞态（rowcount=0 → False、终态不可逆、乱序不回退）、
  submit_task 402 不落库、submit 失败 cancel、三件套字段断言（INSERT 参数含
  quota=0、platform='gw_x'、时间列非 NULL）；poller 只扫 `gw\_%` 前缀行。
- **W3**：沙箱拦截 `9**9**9`/`__class__` 逃逸/死循环/超长表达式；evaluate
  freeze 降级 vs settle 抛错；client 402/409/retry_after_ms 语义；outbox 成功
  回写 billing_state、分片列表逐个 cancel；renewer 续期 + 旧分片 cancel +
  freeze_shard_seq 回写。
- **W4**：capability 错误 401、重复 event 200 幂等 ACK、坏报文 400、反查两路径
  （回显/索引表+双保险）、not-found 延迟重试后丢弃；dispatcher 2xx/410/4xx/5xx
  各分支、lease 过期回收、签名头形制（t/v1/轮换 v1_old）。
- **W1**：四级管线（格式门禁无 I/O 快拒、缓存命中、回源）、防 IDOR 404、
  路由注册顺序断言、videos 响应状态枚举经 `to_video_status`。

### 7.2 测试基建（W6 conftest.py）

统一 fixtures：`mock_session`（AsyncMock session）、`fake_redis`、`respx_router`、
`biz_cfg_factory`（BizConfig 工厂）、`token_factory`（TokenInfo 工厂）、
`task_row_factory`（tasks 行 dict 工厂，字段与 §5.1 映射一致）。

### 7.3 集成冒烟标准（W6 `tests/test_smoke.py`，CI 必跑）

1. `python -c "from app.main import app"` 可导入（无 DB/Redis 环境变量也不炸——
   引擎/客户端全部惰性创建）。
2. `uvicorn app.main:app` **无 DB 可启动**，`GET /healthz/live` 返回 200
   （ready 依赖外置状态可 503，不阻断启动）。
3. 路由表断言：`app.routes` 中 `/healthz/live`、`/callbacks/...`、`/openapi.json`、
   `/{biz}/v1/videos` 均先于 `/{biz}/{native_path:path}` 注册；
   模拟请求固定路径断言不被 catch-all 截获（§4.2 不变量）。
4. 零自有表断言（决策 A）：仓库无 `migrations/`/alembic/ORM `gateway_*` 模型残留，
   compose 无 `docker-entrypoint-initdb.d` 挂载（`tests/test_w6_smoke.py` 静态断言）。

### 7.4 代码质量线（全部代理合并前自查）

- 类型注解覆盖全部公开函数签名（`def` 参数与返回值）；`mypy` 对骨架模块无错误。
- `ruff check app/ tests/` 全绿（配置在 pyproject；中文全角标点 RUF001-3 已豁免）。
- **禁止遗留 TODO/FIXME/XXX**；禁止 stub 函数（`...`/`pass` 空实现）混入提交。
- 禁止新增未登记依赖；禁止散读 `os.environ`（§3.1 例外条款除外）；
  禁止每请求新建 `httpx.AsyncClient`；禁止在共享表上 create/alter。

---

## 8. 附录：建议验证项对实现的影响

| 项 | 对代码的落点 | 实现期处理 |
|---|---|---|
| V1/V2 kling 两代回调 schema | W5 `parse_callback` | 复用查询解析 + 字段缺失容错；轮询兜底保正确性 |
| V3 方舟回调无签名 | W4 receiver | capability token 为唯一强制校验；HMAC 框架预留不强制 |
| V4 逻辑服务契约字段 | W3 `get_logic` | 按 `{expr, expr_type, version, fallback_amount}` 设计约定实现，字段名集中常量化便于改 |
| V5 Python 函数形态 | W3 sandbox | **不实现 exec 路径**；`python_func` 按求值失败处理 |
| V21 keys 微服务契约 | `app/keys.py` / W2 `resolve_submit_secrets` | 端点/字段按设计约定实现（§3.12），字段名集中解析便于对齐；服务不可用全链降级 env 静态密钥兜底 |
| V17 多冻结单并存约束 | W3 renewer | 续期失败告警 + 下轮重试；窗口 1h 内 12 次机会 |
| V18 tasks 实际 DDL | W2 INSERT | `fail_reason=''`；实测默认 NULL 则改并回填 §4.1 |
| V19 轮询器共存 | —（运维实测） | 代码无需分支；噪音日志与 new-api 运维同步 |
| V20 TaskTimeoutMinutes | config | `NEWAPI_TASK_TIMEOUT_MINUTES` 实测后配置 |
| V8 求值变量名 | W2/W3/W5 上下文键 | §3.11.2 契约表为唯一口径 |
| V9 取消 API | — | `canceled` 仅内部状态，无对外端点 |

---

*SPEC 完。骨架首提交含本文件引用；任何契约变更 = 改 SPEC + 通知全部代理 + 变更记录追加于文末。*
