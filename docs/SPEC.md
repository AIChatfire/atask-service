# Spec — atask-service v2.0（规格即契约）

> 生成日期：2026-09-12
> 依据：当前实现（`app/`）+ **本仓库 ADR-010**（对外形态统一为 `/queue/{上游路径}`，鉴权与计费全部下沉上游）+ `AGENTS.md` + `README.md`
> 状态：已确认（本版按 ADR-010 整篇重写；ADR-010 取代**本仓库 ADR-002 / ADR-005 / ADR-006 / ADR-007**，并重写**本仓库 ADR-008** 的分工表述）
> 文档版本 v2.0；包版本以 `pyproject.toml` 为准（当前 0.2.0）

本文件是**团队内部契约**：范围、API、数据、验收标准全部锁定。
不在本 Spec 列表内的功能一律不做；任何改动走 §13 变更流程。
文中每一条 `app/...` 路径、模块名、函数名、配置项名都对应现实现，不得凭记忆改写。

> 跨仓库引用纪律：本仓库与 `stask-service` **各有一套独立的 ADR 编号**，同一编号在两仓库含义不同。
> 本文引用任何 ADR 一律写明仓库名（如「本仓库 ADR-010」）。

---

## 1. 产品定义

- **一句话描述**：**任务队列服务**——对上承接用户请求（限流 / 幂等 / 并发上限），对下把请求**原样中继**到上游并**持有任务事实源**（提交 → 推进 → 取结果）。**当前准入范围只接受异步任务**（上游本身就是任务式接口）。
- **目标用户**：接入异步任务 API 的应用开发者；以及新增/替换上游时只改一个 `base_url` 与白名单、零发版即可上线的运营。
- **核心问题**：异步上游的提交、探测、取消形态大同小异，但客户端直连要各自适配、各自管理轮询与回调。本网关在**不改上游一行代码**的前提下，把「受理即返回本地 task_id、由后台推进到终态、可选回调」这套任务语义统一提供出来；**资金动作与凭证权威全部留在上游**。
- **定位边界**：网关是**中继层 + 任务事实源**——用户令牌、钱包、渠道、配额扣减的权威都不在网关本地。
  网关**不做鉴权内省**（用户 token 原样透传，有效性由上游判定）、**零资金动作**（不 freeze / settle / cancel，配额由上游 relay 扣减）。
  网关无状态（自有状态全在 Redis），**零自有 MySQL 表**，只读写与 new-api 同实例的 `tasks` 表。
- **与 stask-service 的分工**（**本仓库 ADR-008**，2026-09-13 修订口径）：两者**都是任务队列服务**，不是「谁转谁」——区别在**当前准入哪种任务**：atask 只接受**异步任务**，做的是**排队异步**（把**上游异步**接管为**本地异步**：本地 task_id、入队排队、后台推进到终态，并持有事实源）；stask 只接受**同步任务**（上游是同步生成接口，由 stask 完成任务化）。
- **已知耦合**：任务行落在与上游同实例的 `tasks` 表上（**本仓库 ADR-001**），这是当前唯一一处非 HTTP 依赖；网关自有状态全部放 Redis（`app/redis.py`）。

---

## 2. MVP 范围（锁定）

| 优先级 | 能力 | 实现入口 | 验收标准摘要 |
|---|---|---|---|
| P0 | 受理 `POST /queue/{path}` | `app/routers/queue_task.py::queue_create` → `app/services/relayflow.py::create_queue_task` | 落库即返回本地 `task_id`，请求内零上游往返（AC-01） |
| P0 | 查询 `GET /queue/{path}/{task_id}` | `queue_task.py::queue_get` → `relayflow.py::view_queue_task` | 非终态按需探测，终态回放快照零上游往返（AC-21 / AC-22） |
| P0 | 取消 `DELETE /queue/{path}/{task_id}` | `queue_task.py::queue_delete` → `relayflow.py::cancel_queue_task` | 本地置 CANCELED + 尽力源头止损（AC-25） |
| P0 | 免费 GET 透传 | `relayflow.py::free_queue_get` | 末段非本地 id 时按 IP 限流原样转发，不落 tasks 行（AC-24） |
| P0 | 幂等键原子占位（`Idempotency-Key`） | `app/services/idem.py` | 同键回放同 `task_id`；真并发 409（AC-08 / AC-09） |
| P0 | 上游寻址与安全三防线 | `app/services/upstream_addr.py` | 头优先 + 白名单 fail-closed + scheme / userinfo 校验（AC-03 / AC-04） |
| P0 | 本地限流与并发上限 | `app/deps/ratelimit.py` | 按 token hash 限流与占槽，超限 429（AC-06 / AC-07） |
| P0 | 约定式上游交互 | `app/services/relay.py::call_upstream` | 原样转发 method/query/body，固定 Bearer（AC-14） |
| P0 | worker 提交与失败三档 | `relayflow.py::submit_queue_task`、`app/queue.py::queue_submit_task` | 4xx 判死 / 5xx 留活重试 / 2xx 缺 id 判死；回填走 CAS 守卫，在飞取消不复活（AC-15 ~ AC-20） |
| P0 | 单一终态收口点 | `relayflow.py::_finalize_queue` | 快照 → 还槽 → 回调 → 清会话，恰好一次（AC-26 / AC-27） |
| P0 | 后台收敛 sweep | `app/queue.py::queue_sweep_task` → `relayflow.py::sweep_queue_once` | cron 每分钟、最旧优先、独立重入锁（AC-29 / AC-30） |
| P0 | 用户回调签名投递 | `app/services/notify.py::push`、`app/queue.py::publish_notify` | HMAC-SHA256，至少一次，用户按 task_id+status 去重（AC-32） |
| P0 | 状态自动映射 | `app/services/statusmap.py::map_status` | 上游措辞 → 内部状态，未识别告警不判死（AC-21） |
| P0 | 共享表时间列归一 | `app/services/taskstore.py::as_unix_seconds` / `_secs` | 读侧归一 + SQL 比较归一（AC-33） |
| P0 | 管理面 fail-closed（`/ops/*` 与 `/admin/*`） | `app/deps/admin.py::require_admin` | 未配 `ADMIN_TOKEN` 时整个管理面 404（AC-36） |
| P0 | 状态迁移日志恰好一条 | `app/services/statelog.py::record_transition` | 只在 CAS 抢到推进权时记一条（AC-28） |
| P1 | 管理看板与运行时热配置 | `app/routers/admin.py`、`app/services/dynconf.py` | 白名单两项可热改，安全项永不可写（AC-38） |
| P1 | 可观测（loguru / logfire / ops 视图） | `app/logging.py`、`app/observability.py`、`app/routers/ops.py` | 令牌不进日志，状态变化唯一记录点 |

---

## 3. 明确不做（Out-of-Scope — 锁定）

| 不做 | 原因 | 依据 |
|---|---|---|
| 自建计费 / 冻结 / 结算 / 解冻 | 资金动作全在上游 relay 内闭环，网关零资金动作 | 本仓库 ADR-010 §3 |
| 网关侧身份内省 | 用户 token 原样透传，有效性由上游判定；网关只做本地限流/幂等/并发 | 本仓库 ADR-010 §2 |
| keypool / billing 两微服务协同 | 上游原生 relay 已完成渠道选择与配额扣减，再 lease key + freeze 属重复资产与重复风险 | 本仓库 ADR-010 |
| 建任何 MySQL 表 / migration | 复用 new-api `tasks` 表，靠 `platform` 划分自有行 | 本仓库 ADR-001 |
| 跨服务直连数据库 | 红线：一律 HTTP，不碰上游数据库 | `AGENTS.md` 红线 |
| 渠道级能力（`model_mapping` / `param_override` / `default_params` / `result_url_template` / `supports_callback` / `body_allowlist` / 渠道级 `timeout_sec` / `auth_type`） | 零配置的对价：一律按 new-api 约定硬编码，不引入本地路由文件 | 本仓库 ADR-010 §5 |
| `/{biz}` 全部路由形态与任何旧形态兼容 | 已彻底移除，不做旧版本兼容 | 本仓库 ADR-010 §1 |
| 结果 TTL 清理 / 产物字节转存 / 直链改写 | 网关只转发与持有任务事实，不搬字节、不重写产物地址 | 本仓库 ADR-010 |
| 执行中上游任务的真中止保证 | 无解冻；`DELETE` 只做尽力源头止损 + 本地置 CANCELED | 本仓库 ADR-010 已知限制 4 |
| HELD 挂起 / 冻结续期 / 孤儿收口 / 反向对账 | 无冻结即无这些「不亏本兜底」的需求 | 本仓库 ADR-010 |
| max-age 判死 | 刻意不设：会话 TTL 已天然给探测设上界，判死会永久丢失可能已成功的任务 | 本仓库 ADR-010 已知限制 2 |
| 流式（SSE / chunked）响应任务化 | 任务模型与流式语义冲突 | 本仓库 ADR-010 |
| 幂等占位心跳续期 | 本期不做，占位 TTL 覆盖受理→落库回填窗口即可 | `app/services/idem.py` |

---

## 4. 技术架构（锁定，版本锚定）

| 层 | 技术 | 版本 | 锁定原因 |
|---|---|---|---|
| Web 框架 | FastAPI | 0.115.14 | 项目基线 |
| ASGI | uvicorn / gunicorn | 0.34.0 / 23.0.0 | `UvicornWorker` + `preload_app`，post-fork 惰性单例 |
| 配置 / 校验 | pydantic / pydantic-settings | 2.11.7 / 2.9.1 | 无前缀配置单例（`env_prefix=""`，`extra="ignore"`） |
| ORM / 驱动 | SQLAlchemy[asyncio] / asyncmy | 2.0.41 / 0.2.10 | 只用 `text()` 原生 SQL，ORM 仅作表映射说明 |
| Redis | redis (asyncio) | 5.2.1 | `decode_responses=True`，Lua 原子操作 |
| HTTP 出站 | httpx[http2] | 0.28.1 | 进程级共享 AsyncClient 连接池（`app/services/httpc.py`） |
| 队列 | taskiq / taskiq-redis | 0.11.18 / 1.0.2 | `ListQueueBroker` + `RedisScheduleSource` |
| 日志 | loguru | 0.7.3 | stdlib 桥接，`backtrace=False, diagnose=False` |
| 观测 | logfire | 3.25.0 | 配 `opentelemetry-instrumentation-fastapi` / `-httpx` `0.56b0`，装配单点在 `app/observability.py` |
| 测试 | pytest / pytest-asyncio / respx | 8.3.5 / 0.26.0 / 0.22.0 | `asyncio_mode=auto`，手写 FakeRedis + 内存 taskstore |
| Lint / Type | ruff / mypy | 0.11.13 / 1.15.0 | mypy 做成一个 pytest 用例 |
| 运行时 | Python | ≥ 3.12 | `pyproject.toml` `requires-python` |
| 部署 | Docker Compose（gateway + worker[内嵌 scheduler] + redis + taskiq-admin） | - | 见 `docker-compose.yml`；scheduler 必须单副本 |
| 认证 | 透传用户 `Authorization: Bearer`，**网关不做内省**，有效性由上游判定 | - | 本仓库 ADR-010 §2 |
| 管理认证 | 独立 `X-Admin-Token`（`ADMIN_TOKEN`） | - | 与用户令牌完全分离 |

**依赖钉版唯一处 = `pyproject.toml`**，不写 `requirements.txt`。

架构纪律（均为机械可核验项）：

- **唯一数据访问点** `app/services/taskstore.py`：`tasks` 表原生 SQL 只允许出现在这里（`tests/test_static_gates.py::test_raw_sql_only_in_taskstore`）。
- **配置唯一入口** `app/config.py`：业务模块一律 `from app.config import settings`；`os.environ` 只允许出现在 `gunicorn.conf.py`（单一例外，master 进程在本单例之前加载）。
- **唯一出站** `app/services/relay.py::call_upstream`：所有上游请求（提交 / 探测 / 取消 / 免费透传）都经它，复用共享连接池与 `app/services/upstream.py` 的熔断件。
- **唯一寻址入口** `app/services/upstream_addr.py`：`resolve_upstream_base` + `assert_upstream_allowed`。
- **单一终态收口点** `app/services/relayflow.py::_finalize_queue`：视图 / worker / sweep 三路径共用，不许各写一套。
- **状态迁移日志唯一记录点** `app/services/statelog.py::record_transition`。
- **惰性单例**：DB 引擎（`app/db.py::get_engine`）、Redis（`app/redis.py::r`）、HTTP 客户端（`app/services/httpc.py::shared_client`）全部首次使用时创建，`preload_app` fork 后安全。
- **进程形态**：web（`gunicorn -c gunicorn.conf.py app.main:app`）、worker（`taskiq worker app.queue:broker`）、scheduler（`taskiq scheduler app.queue:scheduler`，**必须单副本**）、单进程（`python -m app.standalone`，见 `app/standalone.py`）。

---

## 5. API 端点清单（锁定）

路由注册顺序即 Starlette 首匹配优先级，**不可更换**（`app/main.py::create_app`）：
`healthz → ops → admin → /queue/{path:path}`。
通配 `queue_task_router` 永远最后；`/ops/*` 与 `/admin/*` 必须先于通配，否则会被当成 `path` 吞掉（AC-40，`tests/test_static_gates.py::test_router_mount_order`）。

### 5.1 对外唯一形态：`/queue/{path:path}`

`{path}` 是**上游原生路径**（如 new-api 视频生成 `v1/tasks`）。**`{biz}` 段已从 URL 移除**。
同一路径按**方法 + 末段是否为本地 task_id** 分派：

| Method | Path | 功能 | 认证 | 响应 |
|---|---|---|---|---|
| POST | `/queue/{path:path}` | 受理：落库即返回本地 `task_id`，上游提交交 worker | Bearer（必须） | `202 {"task_id","status":"SUBMITTED"}` + `Location: /queue/{path}/{task_id}` |
| GET | `/queue/{path:path}` | 末段是本地 `task_id` → 任务视图；否则免费透传 | 视图免鉴权；透传必须 Bearer | 视图恒 `200`（本地排队态 / 上游原话 / 快照回放）；透传回上游状态码 |
| DELETE | `/queue/{path:path}` | 末段是本地 `task_id` → 取消；否则 `404` | 无 | `200 {"task_id","status":"canceled"}` / `404` |

**受理（POST）**（`relayflow.create_queue_task`）：

1. `extract_token`（缺 Bearer → `401`）；2. 限流 + 幂等占位（`_rate_and_place`）；3. 路径准入（命中 `QUEUE_DENY_PREFIXES` → `403`）；4. 寻址 + 白名单校验（缺失/非法 → `400`）；5. 回调地址取值（`X-Callback-Url` 头优先、body 顶层 `callback_url` 兜底）+ 准入校验（非法 → `400`，见 AC-44 / AC-45）；6. 并发占槽（超限 → `429`）；7. 落库 + 写令牌会话 + 入队。
   请求头形态：`Authorization`、`Idempotency-Key`、`X-Upstream-Base-Url`、`X-Callback-Url`；
   请求体形态：顶层 `callback_url` 是回调地址的等价通道（头优先）。**请求内零上游往返**。
   全部请求侧校验（body 上限 `413`、分批头、路径准入、寻址、回调地址）都在**任何副作用之前**，被拒的请求不留痕。

**查询（GET，末段为本地 id）**（`relayflow.view_queue_task`）：

- 终态 → 回放 `data.upstream_snapshot`（无快照则按落库字段构建等价报文），**零上游往返**；
- 非终态且有 `upstream_task_id` / `upstream_base_url` / 令牌会话 → 探测 `GET {base}{path}/{upstream_id}` 并推进本地状态；探测不可达 / 熔断 / 护栏拒绝 → 回本地排队态，**绝不 `404`**；
- 非终态但尚无上游 id（提交在飞）→ 本地排队态直出；
- 任务不存在 → `404`。

**免费透传（GET，末段非本地 id）**（`relayflow.free_queue_get`）：按 IP 限流 → 原样转发上游，保留状态码与 `Content-Type`（可能是图片/二进制产物，绝不硬写 JSON），**不落 tasks 行**；缺 token → `401`；上游不可达 → `502`。

### 5.2 探针（无认证）

| Method | Path | 功能 | 响应 |
|---|---|---|---|
| GET | `/healthz/live` | 存活探针，零依赖 | `200 {"status":"ok"}` |
| GET | `/healthz/ready` | 就绪探针：Redis `PING` + DB `SELECT 1`（`app/healthz.py`） | 全过 `200`，任一失败 `503` + `checks` |

### 5.3 运维端点（`X-Admin-Token`；`ADMIN_TOKEN` 未配置时整个 `/ops/*` 与 `/admin/*` 返回 `404`）

| Method | Path | 功能 |
|---|---|---|
| GET | `/ops/queue` | 队列快照：`pending` / `delayed` / `dlq` / `tasks_by_status`（短缓存 `QUEUE_STATS_CACHE_SECONDS`） |
| GET | `/ops/batches` | 攒批概览：每个归组键攒了多少条（`waiting`）、还有多久到期（`due_in` 为负 = 已过期仍在等，是排障信号）。**刻意不放用户面**：归组键在 `token_model` 维度或客户端自定义 `X-Batch-Key` 时含 token 指纹 |
| GET | `/ops/tasks/{task_id}` | 任务诊断视图（脱敏 + 令牌会话存在性与 TTL） |
| POST | `/ops/requeue/{task_id}` | 立即重投提交队列（`queue.publish_queue_submit`） |
| POST | `/ops/dlq/replay` | 死信重放补号（`queue.replay_dlq`，`?limit=` 默认 100） |

### 5.4 管理面（prefix `/admin`；同一 `X-Admin-Token`，`app/routers/admin.py`）

| Method | Path | 功能 |
|---|---|---|
| GET | `/admin`、`/admin/` | 单文件看板页面（零构建，`app/static/admin.html`） |
| GET | `/admin/api/overview?window=` | 队列健康 + 状态分布 + 窗口内失败数 + 运行时信息 |
| GET | `/admin/api/tasks?status&model&task_id&since&limit&offset` | 任务列表（分页 + 精确筛选，白名单投影） |
| GET | `/admin/api/tasks/{task_id}` | 任务详情（脱敏） |
| POST | `/admin/api/tasks/{task_id}/requeue` | 重投提交；**终态任务 `409` 拒绝** |
| GET | `/admin/api/config` | 读取热配置全量视图 + 只读项及原因 |
| PUT | `/admin/api/config` | 批量写覆盖值（白名单外拒绝，整批校验，`400` / `503`） |
| POST | `/admin/api/config/reset` | 重置回落 env（POST 而非 DELETE：DELETE 带 body 在客户端/代理上行为不一致） |

错误响应统一 `{"error": {"message","type","param","code"}}`（`app/errors.py::register_exception_handlers`）；请求校验失败返回 `422` + `code=validation_error`。

### 5.5 攒批（batching，见本仓库 ADR-011）

`POST /queue/{path}` 接受三个可选分批头。**攒批只改变「上游提交」的时机，不做合并请求**
（上游是 new-api 约定式异步接口，没有批量端点）。

| Header | 取值 | 语义 |
|---|---|---|
| `X-Batch-Size` | 正整数，≤ 1000 | 本批攒够 N 条即放行。传 1 表示「这条不攒批」 |
| `X-Batch-Wait` | 正整数，≤ `MAX_BATCH_WAIT_SECONDS` | 本批窗口 T（秒），自**首个**成员起算，后续成员不刷新 |
| `X-Batch-Key` | ≤ 64 字符 | 归组键（谁和谁算同一批）。**显式优先于 `BATCH_GROUP_BY`**；非法字符替换为 `_`，超长截断（都不报错） |

- 三个头**逐字段显式优先**：客户端给 N 不给 T → T 取 `BATCH_WAIT_SECONDS`，服务端也没配则
  取 `MAX_BATCH_WAIT_SECONDS`（**不给兜底会让整批立刻到期 = 攒批静默失效**）。
- 客户端可以把 N 从 0 抬到 ≥2 **开启**服务端未配的攒批（此时才生效）；总闸门
  `BATCH_ENABLED` 是最终闸门。
- 非法值一律 `400`，`error.code` ∈ `invalid_batch_size` / `invalid_batch_wait` /
  `batch_wait_too_long`，`error.param` 为对应头名。**校验在任何副作用之前**（不落库、不占槽、
  不入批），且 `BATCH_ENABLED=false` 时同样报错——否则「关着不报错、打开才报错」会变成
  切换开关后才暴露的客户端 bug。
- 攒批时 202 响应体在原 `{task_id, status}` 之上追加
  `batch_key` / `batch_state`（只可能 `waiting`）/ `batch_size` / `batch_wait`；
  非攒批路径响应体**逐字节不变**。
- **行为差异**：攒批路径等待期**不占并发槽**（占槽点搬到放行），因此**不再因并发满而
  `429`**，改为排队；放行时占不到槽则指数退避 + 抖动重排（`data.requeue_attempts` 落库）。
- **取消**会退批（`batching.leave`）：不退的话一批声明 N=100 而其中几条被取消，计数
  永远差几条到不了 N，只能干等 T。

---

## 6. 数据模型（锁定 — 复用 new-api `tasks` 表，零建表）

**平台隔离**：网关行 `platform = settings.gateway_platform`（默认 `"atask"`，配置项 `GATEWAY_PLATFORM`）。
所有读写的 `WHERE` 必带 `platform`（`app/services/taskstore.py::cas` / `patch_data` / `stale_queue_active` / `search_tasks` / `counts_by_status`），
`stask-service` 的 `platform='stask'` 行与 new-api 原生任务行天然互不可见（**本仓库 ADR-001**）。

**`task_id` 形态**：`{biz_slug}_{uuid4hex}`（`app/services/ids.py::new_task_id`）。
受理链路固定传 `"queue"`，故本链路 task_id 形如 `queue_<32 位十六进制>`；
形态判定 `app/services/nativeapi.py::LOCAL_ID_RE` = `^[a-z0-9-]{1,20}_[0-9a-f]{32}$`（GET/DELETE 据此区分视图与透传）。

**列契约**（网关零建表，`tasks` 表由 new-api AutoMigrate 维护；实际读写走 `taskstore.py` 原生 SQL）：
`task_id` / `platform` / `action` / `status` / `fail_reason` / `progress` / `submit_time` / `start_time` /
`finish_time` / `created_at` / `updated_at` / `data`(JSON) / `user_id` / `channel_id` / `quota`。
- 受理时写：`action='task'`、`status='SUBMITTED'`、`progress='0%'`、`user_id=0`、`channel_id=0`、`quota=0`（`taskstore.create`）。
- `quota` **恒写 `0`**——资金由上游 relay 扣减，不在 `tasks.quota` 上体现。
- 终态一律把 `progress` 置 `100%`、用**秒**刷 `finish_time`（`taskstore.cas`）。

**`data` JSON 字段契约**（构造点 `app/services/relayflow.py::create_queue_task`，`JSON_MERGE_PATCH` 增量合并）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `source` | str | 恒 `"queue"`（sweep 候选过滤依据） |
| `model` | str | 浅解析 body 的 `model` / `model_name`；缺失为 `""` |
| `token_hash` | str | sha256(raw token)，限流 / 并槽 / 幂等键口径；**不是凭证** |
| `request_method` / `request_path` / `request_query` | str | 提交时的原文（转发与探测基底） |
| `request_body` | str | 原文按 UTF-8 文本保留（**默认转发体**，也是排障与原文回放依据）；二进制按 `errors="replace"` 降级 |
| `submit_body` | str | **仅在受理时为摘除回调字段而重构过 body 时才落键**（见下方 `callback_url`）：转发体优先取它、缺键回退 `request_body`。绝大多数请求不写该键，故转发仍是原文 |
| `request_content_type` | str | 提交时的 `Content-Type`，转发时原样回设 |
| `upstream_base_url` | str | 受理时校验通过的上游基址，worker / 探测只认它 |
| `callback_url` | str | 可空。取值：`X-Callback-Url` 头**优先**，body 顶层 `callback_url` **兜底**（上游 API 文档口径）。仅在默认「网关接管」模式下落键；`CALLBACK_PASSTHROUGH_UPSTREAM=true` 时不落键（回调交上游）。落键前必过 `callback_addr.assert_callback_allowed`（fail-closed 白名单） |
| `upstream_task_id` | str | 上游任务 id，**绝不对外暴露**（对外报文里被逐字节改写回本地 id） |
| `upstream_status` | str | 上游状态原话（状态映射与快照回显用） |
| `upstream_snapshot` | obj | 终态上游原始报文（≤ `nativeapi.SNAPSHOT_MAX_BYTES` = 8192 字节；空报文不落键），原生查询逐字段同构回放 |
| `slot_flags` | int | **并发槽掩码**（本链路只有第一层，故 0/1）。它是「还槽的唯一依据」：终态 / 取消只在 `claim_slot_release` 把 >0 原子置 0 成功时才 DECR。**缺键视为已占槽**（本特性上线前创建的在途任务受理时必然占过槽） |
| `batch_state` | str | 攒批状态（见下方子表）。**缺键 = 收到即提交的立即路径**（不写该键） |
| `batch_key` | str | 归组键（谁和谁算同一批的唯一判据），已过白名单归一；非攒批不落键 |
| `batch_size` / `batch_wait` | int | 本次提交**生效的** N / T（客户端头叠加后的结果，不是配置原值） |
| `batch_due_at` | int | 「本任务的**下一次**可放行时刻」：批次到期由首个成员写定（Redis 回读的**真实 ZSCORE**），退避重排时改写为重试时刻。sweep 的超期兜底只靠这一个谓词 |
| `requeue_attempts` / `requeue_due_at` | int | 放行时占不到并发槽的退避次数与下次重试时刻（次数落库：Redis 丢数据后不会让退避重新从 1 秒起步） |

> **刻意不写** `freeze_amount` / `settled`（**本仓库 ADR-010 §3**：网关零资金动作）。
> 管理面历史投影白名单里仍保留这两个键名，但本链路永不写入（读出来是空值）。

**状态机**（常量以 `app/schemas.py` 为准）：
`SUBMITTED → QUEUED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`；无 `HELD`。
`ACTIVE = (SUBMITTED, QUEUED, IN_PROGRESS)`（CAS 合法起点），
`TERMINAL = (SUCCESS, FAILURE, CANCELED)`（不可逆，迟到快照丢弃）。
状态迁移一律 CAS：`taskstore.cas` 的 `rowcount == 1` 才视为抢到推进权（终态恰好一次）。

**攒批的 `data.batch_state` 子状态**（与状态列**正交**，见本仓库 ADR-011）：
攒批等待期复用既有的 `SUBMITTED`（它本来就是「已受理、未提交上游」），**不新增状态**。

| `batch_state` | 含义 | 是否占着并发槽 | 放行权起点 |
|---|---|---|---|
| 缺键 | 收到即提交（立即路径，含本特性上线前的历史行） | 是 | 不适用 |
| `waiting` | 在批次里等 N/T | 否 | 是 |
| `releasing` | 已抢到放行权，正在占槽 / 投递 | 否（此刻掩码仍为 0） | **否**（否则同一条会被两条路径同时抢到） |
| `released` | 已放行，提交已投递 | 是 | 否 |
| `requeued` | 放行时占不到并发槽，退避重排中 | 否 | 是 |

对外（`GET /queue/{path}/{task_id}` 与 202 响应）**只暴露 `waiting` / `released`**：
`requeued` 必须映射成 `released`，否则「靠 `batch_state` 判断是否在排队」的客户端会把
「占不到槽、正在退避」误判成在等 N/T，去等一个永远不会到达的批次事件。

**时间列纪律（最硬的连带纪律，本仓库 ADR-004）**：
`tasks` 是共享表，时间列**可能被其他写入方写成毫秒**（new-api 原生模块用 UnixMilli 写法）。三道纪律缺一不可：

1. 写侧恒写秒（`taskstore._now()`，`int(time.time())`）；
2. 读侧统一归一 `taskstore.as_unix_seconds`（`> 1e11` 视为毫秒折算，缺失 / 非法 → 0），`_row_to_dict` 对全部时间列归一；
3. SQL 时间比较必须套 `taskstore._secs(col)` = `IF(col > 1e11, col DIV 1000, col)`（`stale_queue_active` / `stale_batch_waiting` / `search_tasks` 全部包裹）。

---

## 7. 管理看板（`/admin`，锁定）

- **形态**：单文件 HTML（`app/static/admin.html`），零构建；`GET /admin` 与 `GET /admin/` 返回该页。
- **页面本身不鉴权**：它只是空壳，所有数据都要带密钥调 `/admin/api/*`；密钥存浏览器 `sessionStorage`，不落 URL。
- **启用闸门**：`app/deps/admin.py::admin_enabled()` 为假（`ADMIN_TOKEN` 未配置）时页面与全部 API 一致 404。
- **脱敏纪律**：列表 / 详情只做白名单投影；`token_hash`、`request_body`、上游原始报文 `upstream_snapshot` 一律不出现；令牌会话只给存在性与 TTL（`tokensession.session_info`）。
- **破坏性操作边界**：只提供「重投提交」与「配置回落」两类写操作；**没有删除任务入口**——终态推进的唯一入口是 `relayflow._finalize_queue`，管理面绝不绕过它。
- **热配置白名单**（`app/services/dynconf.py::MUTABLE`，当前**五项**）：
  `max_concurrent_tasks`、`upstream_breaker_threshold`、
  `batch_enabled`（攒批总闸门，线上止血开关）、`batch_size`（每批条数 N）、
  `batch_wait_seconds`（批次窗口 T）。
  读取优先级「Redis 覆盖 > env > 代码默认」，带 5 秒进程内缓存；Redis 不可用时回落 env，绝不成为可用性单点。
  `IMMUTABLE_REASONS` 显式登记永不可热改的安全项（`database_url` / `redis_url` / `admin_token` /
  `callback_sign_secret` / `callback_allowlist` / `callback_passthrough_upstream` /
  `gateway_platform` / `rate_limit_per_minute` / `upstream_allowlist` /
  `batch_group_by` / `max_batch_wait_seconds`）及其原因。

---

## 8. 设计 Token

不适用（网关无自建前端产物）。日志遵循 `app/logging.py` 的 loguru 终端格式，
`docs/SPEC.md`、源码、决策文档与 `docs/**` 全部 markdown 均**不含任何 emoji 字符**
（由 `tests/test_static_gates.py::test_no_emoji_in_docs` 与 `::test_no_emoji_in_source` 机械断言）。

---

## 9. 验收标准（EARS 格式，锁定 — QA 唯一依据）

| 编号 | 功能 | EARS 验收标准 | 优先级 |
|---|---|---|---|
| AC-01 | 受理 | 当客户端 `POST /queue/{path}` 且带合法 `Authorization: Bearer`、上游基址可解析且通过白名单，系统应在落库后返回 `202` + `{task_id, status:"SUBMITTED"}` + `Location: /queue/{path}/{task_id}`，且**请求内零上游往返**（提交交 worker） | P0 |
| AC-02 | 路径准入 | 若规整后的 `{path}` 命中 `QUEUE_DENY_PREFIXES`（默认 `/api/,/console/`），系统应返回 `403` 且不落 tasks 行（判定先于寻址与占槽） | P0 |
| AC-03 | 上游寻址 | 当请求带 `X-Upstream-Base-Url` 头，系统应以该头为上游基址；头缺失/为空时应回退 `UPSTREAM_BASE_URL`；两者都为空应返回 `400` | P0 |
| AC-04 | 寻址安全 | 若上游基址不是 `http`/`https`、含 URL userinfo 或无 host，系统应返回 `400`；若 host 未命中 `UPSTREAM_ALLOWLIST`，或 `UPSTREAM_ALLOWLIST` 为空，系统应返回 `400`（fail-closed：白名单为空即全部拒绝） | P0 |
| AC-05 | 鉴权 | 若受理请求缺少合法 `Authorization: Bearer`，系统应返回 `401`；系统**不做内省**，令牌有效性由上游判定 | P0 |
| AC-06 | 限流 | 当某 `token_hash` 在 60 秒滑动窗口内请求数超过 `RATE_LIMIT_PER_MINUTE`，系统应返回 `429` 并携带 `Retry-After: 10` | P0 |
| AC-07 | 并发上限 | 当某 `token_hash` 在途任务数达到 `MAX_CONCURRENT_TASKS`（热改项 `max_concurrent_tasks`），系统应返回 `429` + `Retry-After: 30`；并发键按 `token_hash` 计，带 `CONC_TTL_SECONDS` 兜底 | P0 |
| AC-08 | 幂等回放 | 当同一 `Idempotency-Key` 已回填 `task_id`，系统应在占位与落库之前短路并回放首个任务视图，不产生第二个任务、不产生第二次占槽 | P0 |
| AC-09 | 幂等并发 | 当同一 `Idempotency-Key` 真并发且占位未回填，非占位者应在 `IDEM_REPLAY_WAIT_SECONDS`（默认 25s）内短轮询；窗口内回填则回放，超时或占位消失应返回 `409`，绝不放行重建 | P0 |
| AC-10 | 幂等归还 | 若受理链路任一步失败（路径拒绝 / 寻址失败 / 落库异常 / 入队异常），系统应 CAS 归还幂等占位（仅当值仍为 `pending`，`LUA_CAS_DELETE`）并归还并发槽 | P0 |
| AC-11 | 令牌会话 | 当受理成功，系统应把用户明文令牌**仅**写入 Redis 会话（键 `atask:sk:{task_id}`，TTL = `SK_SESSION_TTL_SECONDS`）；明文令牌绝不落 tasks 表、绝不进日志、绝不出现在任何响应里 | P0 |
| AC-12 | 脱敏回显 | 系统应把 `request_body` 原样保留于 `tasks.data`（转发体基底），但管理面与 `/ops/*` 视图绝不回显 `request_body` 与令牌本体；令牌会话只暴露存在性与 TTL | P0 |
| AC-13 | 零资金动作 | 当任务创建与流转，系统应不写 `freeze_amount` / `settled`，不调用任何冻结 / 结算 / 取消接口 | P0 |
| AC-14 | worker 提交 | 当 worker 消费 `queue_submit_task`，系统应按 `data.upstream_base_url` + `data.request_path` 原样转发 method / query / body 至 `POST {base}{path}`，鉴权固定 `Authorization: Bearer <用户 token>` | P0 |
| AC-15 | id 提取 | 当提交响应为 2xx，系统应取响应里的 `id`，缺失时回退 `task_id`；两者都缺失应经单一收口点落 `FAILURE`（`fail_reason` 含 `missing task id`），不静默挂起 | P0 |
| AC-16 | 提交成功 | 当提取到上游 id 且映射状态非终态，系统应回填 `upstream_task_id` / `upstream_status` 并置 `QUEUED`；**若任务在此期间已被取消或判死，系统必须保持原终态不变**（回填走带起点的 CAS，抢不到只把上游 id 记进 `data` 供追溯，绝不复活状态列） | P0 |
| AC-17 | 提交即终态 | 当提交响应直接给出终态状态词，系统应经单一终态收口点落终态 | P0 |
| AC-18 | 失败档一 | 当上游返回 4xx，系统应经单一收口点落 `FAILURE`、释放并发槽、清令牌会话，**不重试** | P0 |
| AC-19 | 失败档二 | 当上游返回 5xx 或传输层错误（`RelayError` 599），系统应抛 `RelayError` 交 queue 层退避重试，任务**留活**非终态、**不释放并发槽**、**保留令牌会话** | P0 |
| AC-20 | 失败档三 | 当上游返回 2xx 但缺 `id`/`task_id`，系统应经单一收口点落 `FAILURE`、释放并发槽、清令牌会话 | P0 |
| AC-21 | 视图探测 | 当 `GET /queue/{path}/{task_id}` 且任务非终态且有 `upstream_task_id`、`upstream_base_url` 与令牌会话，系统应探测 `GET {base}{path}/{upstream_id}` 并按 `statusmap.map_status` 推进本地状态；探测不可达 / 熔断 / 护栏拒绝应回本地排队态，**绝不 `404`** | P0 |
| AC-22 | 终态回放 | 当任务为终态，系统应回放 `data.upstream_snapshot`（无快照时按落库字段构建等价报文），**零上游往返** | P0 |
| AC-23 | 报文同构 | 系统应把探测 / 回放报文里的上游 id 逐字节改写为本地 `task_id`（`nativeapi.rewrite_ids`，不重新序列化），且 `status` 保留上游原话 | P0 |
| AC-24 | 免费透传 | 当 `GET /queue/{path}` 且末段不是本地 `task_id`，系统应按 IP 限流后原样转发上游（保留状态码与 `Content-Type`），**不落 tasks 行**；缺 token 返回 `401`，上游不可达返回 `502` | P0 |
| AC-25 | 取消 | 当 `DELETE /queue/{path}/{task_id}` 且任务非终态，系统应 CAS 置 `CANCELED`、释放并发槽、尽力 `DELETE {base}{path}/{upstream_id}` 源头止损（失败只告警）、**在止损之后清令牌会话**（明文 sk 不得留到会话 TTL——顺序不可颠倒，止损要用会话里的 sk）；任务已终态应回放该终态视图；末段非本地 id 应返回 `404` | P0 |
| AC-26 | 终态恰好一次 | 当终态推进 CAS 未抢到（`rowcount != 1`），系统应整段不执行快照落库 / 释槽 / 回调 / 清会话，返回 False | P0 |
| AC-27 | 收口顺序 | 当任务进入终态，系统应按「CAS 抢推进权（同一条 UPDATE 落快照，≤ `SNAPSHOT_MAX_BYTES`）→ 记一条状态迁移日志 → 还并发槽 → 投递回调（有 `callback_url` 时）→ 清令牌会话」执行，且视图 / worker / sweep 三路径共用 `_finalize_queue` 同一实现 | P0 |
| AC-28 | 迁移日志 | 当 CAS 抢到推进权，系统应经 `statelog.record_transition` 恰好记一条状态迁移日志；未抢到推进权时不记 | P0 |
| AC-29 | 后台收敛 | 当任务 `source='queue'`、非终态、有 `upstream_task_id` 且 `_secs(updated_at)` 早于 `now - TASK_STALE_SECONDS`，`queue_sweep_task`（cron `*/1 * * * *`）应按**最旧优先**（`ASC`）取至多 `QUEUE_SWEEP_LIMIT` 条探测，映射到终态的走单一收口点推进 | P0 |
| AC-30 | 收敛重入锁 | 当另一轮 sweep 持有 `atask:queue_sweep_lock`，系统应跳过本轮并返回 0（不叠加并发轮）；释放锁用 CAS 删除（只删自己持有的 `guard`） | P0 |
| AC-31 | 会话过期 | 若某任务的令牌会话已不存在，sweep 应跳过该任务（DEBUG 级），**绝不判死、绝不释放并发槽** | P0 |
| AC-32 | 用户回调 | 当任务转终态且 `data.callback_url` 非空，系统应经 `queue.publish_notify` → `notify.push` 投递含 `X-Gateway-Signature: t=<ts>,v1=<hmac-sha256(CALLBACK_SIGN_SECRET, ts + "." + body)>` 的 JSON 体，至少一次（用户按 `task_id + status` 去重）；无 `callback_url` 不投递 | P0 |
| AC-33 | 时间归一 | 系统应把 `tasks` 全部时间列经 `taskstore.as_unix_seconds` 归一，SQL 时间比较套 `_secs(col)`，写侧恒写 unix 秒 | P0 |
| AC-34 | 平台隔离 | 系统对所有 `tasks` 表读写应恒带 `platform = GATEWAY_PLATFORM`，绝不读写 `stask` 或 new-api 原生任务行；`quota` 列恒写 `0` | P0 |
| AC-35 | 访问单点 | 系统对 `tasks` 表的原生 SQL 应只出现在 `app/services/taskstore.py` | P0 |
| AC-36 | 管理鉴权 | 若 `ADMIN_TOKEN` 未配置，系统应对 `/ops/*` 与 `/admin/*` 一律返回 `404`；已配置时缺失 / 错误 `X-Admin-Token` 应返回 `401`，且终端用户令牌不得通过（`secrets.compare_digest`） | P0 |
| AC-37 | 管理脱敏 | 管理端点应采用白名单投影，绝不返回 `token_hash` / `request_body` / `upstream_snapshot` / 令牌本体 | P0 |
| AC-38 | 热配置 | 当写入不在 `dynconf.MUTABLE` 内的键，系统应返回 `400` 且整批不落盘；`IMMUTABLE_REASONS` 中的安全项永不可写；Redis 写失败应返回 `503`，绝不谎报成功 | P1 |
| AC-39 | 熔断 | 当某上游 `host:port` 在 `UPSTREAM_BREAKER_WINDOW_SECONDS` 内失败数达到上游熔断阈值（热改项 `upstream_breaker_threshold`），系统应拒绝出站（`upstream.BreakerOpenError`）；探测路径据此回本地排队态，不判死任务 | P1 |
| AC-40 | 路由顺序 | 系统应按 `healthz → ops → admin → /queue/{path:path}` 顺序注册，通配永远最后，保证 `/ops/*` 与 `/admin/*` 不被吞掉 | P0 |
| AC-41 | 错误形制 | 系统应把所有非 2xx 响应归一为 `{"error": {"message","type","param","code"}}`；校验失败返回 `422` + `code=validation_error` | P0 |
| AC-42 | 零 emoji | `docs/SPEC.md`、源码、仓库根级与 `docs/**` 全部 markdown 应不含任何 emoji 字符 | P1 |
| AC-43 | 配置键名 | 配置键名应等于 `Settings` 字段名大写、不带前缀；未知变量应被静默忽略（`extra="ignore"`）——系统**不提供旧键名或别名兼容**，写错键名不会有任何提示 | P0 |
| AC-44 | 回调地址准入 | 当请求给出回调地址且处于默认「网关接管」模式，系统应在**任何副作用之前**校验：仅 `http`/`https`、无 URL userinfo、有 host、字面 IP 须为全局可路由地址（**即使白名单显式列出私网 IP 也拒**）、host 必须命中 `CALLBACK_ALLOWLIST`（**空 = 全拒**，fail-closed）；非法地址返回 `400` 且不留痕（不落库、不占并发槽、不占幂等键）。`CALLBACK_PASSTHROUGH_UPSTREAM=true` 时取值与校验一并跳过、body 原样转发 | P0 |
| AC-45 | 回调地址取值与摘除 | 系统应按「`X-Callback-Url` 头**优先**、body 顶层 `callback_url` **兜底**」确定生效地址（上游 API 文档口径）；网关接管模式下把该字段从转发体**摘除**（转发改走 `data.submit_body`，`data.request_body` 仍存原文），使转发体不含该键——消除上游与网关的双投递；摘除只在 body 真带该键时发生，否则转发体仍是原文 | P0 |

---

## 10. 边界与约束

- Python ≥ 3.12；依赖钉版唯一处 = `pyproject.toml`。
- **MySQL 与 new-api 共享实例**，连接预算 `进程数 × (DB_POOL_SIZE + DB_MAX_OVERFLOW) ≤ max_connections × 0.8`。
  gunicorn worker 数由该预算反推（`gunicorn.conf.py`）；`DB_MAX_CONNECTIONS` / `DB_WEB_SHARE` / `GUNICORN_WORKERS`
  是 `gunicorn.conf.py` 直读的 env（**不是** `Settings` 字段），`DB_POOL_SIZE` / `DB_MAX_OVERFLOW` 同时是 `Settings` 字段，改一处须同步另一处。
- **Redis 独立实例**（compose 内 `redis` 服务），`--appendonly yes --appendfsync everysec`。
  键统一前缀 `atask:`（`app/redis.py`）；队列 `atask:taskiq`，延迟任务 `atask:sched:*`，死信 `atask:events:dlq`，
  幂等键 `atask:idem:*`，令牌会话 `atask:sk:*`，并发槽 `atask:conc:*`，熔断 `atask:breaker:*`，收敛锁 `atask:queue_sweep_lock`，
  攒批成员索引 `atask:batch:{归组键}` 与到期索引 `atask:batch:due`（键里拼的是**归组键**而非模型名，
  已过白名单归一——客户端可控字符串不经约束地进键名会带来键空间污染）。
  **前缀只有一处定义**（`app/redis.py::KEY_PREFIX`，当前为 `atask`，命名对齐 `GATEWAY_PLATFORM`；
  与同族 stask-service 的 `REDIS_KEY_PREFIX`（默认 `st`）对称），业务模块只 import 常量、
  不自己拼前缀。**换前缀 = 换一整套键空间**：改名后旧键不再被读写，而队列本身
  （`atask:taskiq` / `atask:sched:*` / `atask:events:dlq`）也在其中——所以换前缀**必须先把
  队列排空**，否则积压的待执行消息、延迟任务与死信会一并变成无人认领的孤儿键（幂等键与
  令牌会话的丢失只影响在飞窗口，队列丢失是真丢任务）。
  Redis 只放「丢了能重建」的状态，事实源永远是 `tasks` 表。
  **攒批的例外同样成立**：批次索引丢了，成员在 DB 里仍是 `batch_state='waiting'` +
  `batch_due_at`，由 sweep 的超期兜底捞回（见 §5.5 与本仓库 ADR-011 §10）。
- **上游出站统一走 `relay.call_upstream`**：共享连接池 + 全局超时 `RELAY_TIMEOUT_SECONDS`（默认 60s）+ 熔断（键取上游 `host:port`）。
  空基址按 599 模糊失败拦下（配置/基础设施问题，不判死）。
- **终态快照上限** 8192 字节（`nativeapi.SNAPSHOT_MAX_BYTES`），超限不落键；探测报文正常 <2KB。
- **提交体上限 `BODY_MAX_BYTES`（默认 1 MiB，`413` 拒绝）**：`relayflow._read_body_limited` 先按
  `Content-Length` 快速拒绝（该头可缺失或伪造，**只信头等于没防**），再在 `request.stream()` 读取
  过程中逐块累加、超过即中止——绝不先 `await request.body()` 把整段读进内存再判长度。
  校验在任何副作用（限流/幂等占位/并发槽/落库）**之前**：被拒的请求不留痕，否则会污染幂等键
  并泄漏并发槽。分批头（§5.5）的校验同属这一段，且同样在任何副作用之前。
- **gunicorn timeout 必须留足余量**：`gunicorn.conf.py` 默认 `timeout = max(180, GUNICORN_REQ_MAX_SECONDS + 120)`；
  web 侧探测 / 取消 / 免费透传最长等待 `RELAY_TIMEOUT_SECONDS`，调大它要同步确认 timeout 与 `graceful_timeout`（须 `< timeout - 5` 且 `< compose stop_grace_period`）。
- **scheduler 必须单副本**（多份会重复触发每分钟 sweep）；worker 扩副本时拆回独立 scheduler。
- **管理面 fail-closed**：`ADMIN_TOKEN` 未配置时 `/ops/*` 与 `/admin/*` 全部 `404`（不暴露端点存在）；配置 `.env` 时该项为必填。
- 可观测开关 `LOGFIRE_ENABLED` 默认关闭；开启后任何观测失败只告警，绝不影响业务主流程。
- **上游接入契约**：接入不符合 new-api 约定（`POST {base}{path}`、`id`/`task_id`、`status`、`DELETE {base}{path}/{id}`）的上游**需要改代码**——本网关刻意不提供渠道级适配。

### 10.1 傻瓜式接入新上游（运维速查）

**接入新上游 = 配一个 `base_url` + 白名单，网关零代码改动、零路由文件、零渠道元数据。**

1. 部署侧配置 `UPSTREAM_BASE_URL`（默认上游）与 `UPSTREAM_ALLOWLIST`（允许的 host 列表，逗号分隔，**空 = 全拒**）。
2. nginx 侧对 `/queue/` 无条件 `proxy_set_header X-Upstream-Base-Url "<真实上游>"`（覆盖客户端同名头，见 **本仓库 ADR-010 §4** 与已知限制 3）。
3. 客户端以 `POST /queue/{上游原生路径}` 提交，`{path}` 直接照抄上游路径（如 `v1/tasks`）；查询 / 取消用同一前缀 + 本地 `task_id`。
4. 无需改任何 `app/` 代码；若上游不满足约定（提交返回 `id`、状态字段 `status`、探测 `GET {base}{path}/{id}`），才需要改 `app/services/relay.py`。

---

### 10.2 攒批的开启与止血（运维速查）

- **开启**：`BATCH_SIZE>=2`（同时给出 `BATCH_WAIT_SECONDS` 窗口），或让客户端带
  `X-Batch-Size`（服务端配 0 时客户端可把它抬到 >=2，总闸门仍是 `BATCH_ENABLED`）。
- **止血**：`BATCH_ENABLED=false`（热改项，管理面可在线操作，无需重启）。**它只辖「入批」
  这一件事**：已经在批里等着的任务不会被它丢下，仍由 T 触发与 sweep 的超期兜底放行——
  止血开关的语义必须精确，顺手关掉无关功能会让延迟任务与已入批的任务一起饿死。
- **部署要求**：scheduler 必须带 `--update-interval 1`（三个形态都要改：`Makefile`、
  `docker-compose.yml`、`app/standalone.py`）。不设它时 T 触发的实际放行最坏晚约 60s
  （taskiq 0.11 默认按分钟对点唤醒），**正确性不受影响**（投递丢失/迟到由 sweep 兜底）。
- **默认值**：`BATCH_SIZE=0` = 不攒批（收到即提交），开箱行为与本特性之前逐字节一致。
- **观察**：`GET /ops/batches` 看每个归组键攒了多少条；`due_in` 为负表示已过期仍在等
  （T 触发的延迟任务漏了，等 sweep 捞回）——排障时最先看这个字段。

### 10.3 攒批的已知限制（本仓库 ADR-011）

1. **T 触发精度取决于 scheduler 轮询间隔**（见 §10.2），不设 `--update-interval` 时最坏晚约 60s。
2. **Redis 整体丢数据时最坏多等一个 sweep 周期 + 宽限 120s**（`_BATCH_RESCUE_GRACE_SECONDS`）
   ——这是「Redis 只放可重建索引」的必然代价，stask 同样存在。
3. **不做 per-model 分批策略**：分批参数只有「全局配置 + 客户端头」两种来源；stask 的
   `model_policies` 策略表刻意不复刻（本仓库没有那套基建，且与「零渠道配置」冲突）。
4. `data.batch_due_at` 一个字段承载两种粒度（整批的 deadline / 退避重排后单条的重试时刻），
   排障时别把单条的重试时刻当成整批的窗口。

---

## 11. 内嵌已知坑

### 11.1 ADR-010 已知限制（如实转述，**本仓库 ADR-010「已知限制」5 条**）

| 编号 | 已知限制 | 后果与处置 |
|---|---|---|
| L-1 | **令牌会话过期后任务无法自愈**。探测上游需要用户 token，而网关只把 token 存在 Redis 会话里（TTL = `SK_SESSION_TTL_SECONDS`，48h）——这是「鉴权下沉上游」的必然代价：网关不持有长期凭证 | 会话过期后 sweep 会跳过该任务（DEBUG 级，不报错、**绝不判死、绝不释放并发槽**），该任务会**永久停在非终态**。处置：客户端可 `DELETE` 取消，或由管理面介入 |
| L-2 | **没有 max-age 判死，这是刻意的**。旧 poller 有超时转 FAILURE；新链路**不设** | 理由：会话 TTL 已天然给探测设了上界（48h 后自动跳过），而判死会**永久丢失一个可能已在上游成功的任务结果**——判死不可逆，无明确收益则不做 |
| L-3 | **`X-Upstream-Base-Url` 头的可信性完全依赖 nginx 配置正确** | 若 nginx 未无条件覆盖，客户端可伪造该头把请求（连同用户 sk）指向任意 host；`UPSTREAM_ALLOWLIST` 是第二道防线，**两道都必须配** |
| L-4 | **取消语义退化**：不再有「解冻」，`DELETE` 只做尽力源头止损 + 本地置 CANCELED | 上游取消形态（`DELETE {base}{path}/{id}`）属**约定推断**，未经上游文档验证；若某上游取消端点不是此形态，需改代码 |
| L-5 | **转发体的「逐字节原样」有一个例外**：当 body 顶层带 `callback_url` 且处于默认「网关接管」模式时，该字段被**摘除**后再转发（转发体改走 `data.submit_body` 这个语义等价重构体） | 存在的理由：消除「上游也回调 + 网关也回调」的双投递（客户端会收到两份通知，其中一份没有本网关的签名），并避免把回调地址额外暴露给上游。代价：**仅该字段存在时**转发体不再逐字节同构（JSON 语义等价，键序/空白可能变化）；`CALLBACK_PASSTHROUGH_UPSTREAM=true` 时无此例外，body 完全原样转发 |

### 11.2 稳定坑

| 坑 | 技术栈指纹 | 根因 | 修法 |
|---|---|---|---|
| 共享表时间列混入毫秒 | mysql/new-api-tasks | new-api 原生任务模块用 UnixMilli 写法 | 读侧 `as_unix_seconds` 兜底归一；SQL 谓词套 `_secs(col)`；只命中本服务写的秒值行（本仓库 ADR-004） |
| 三方共写一张表 | mysql/new-api-tasks | 表被 new-api + atask + stask 三方写 | 所有读写 `WHERE` 恒带 `platform`；`taskstore.py` 为唯一数据访问点（本仓库 ADR-001） |
| taskiq `with_labels(delay=)` 不生效 | taskiq-redis/ListQueueBroker | `ListQueueBroker` 不支持 delay 标签 | 延迟任务一律走 `schedule_by_time`（`app/queue.py::_retry_or_dlq`） |
| gunicorn preload + 全局连接池 | gunicorn/preload_app | fork 前建连接会在子进程间共享 socket | 引擎 / Redis / HTTP 客户端全部惰性单例 |
| loguru `diagnose=True` 泄露 token | loguru | 异常回溯打印帧局部变量，含 raw token | 固定 `backtrace=False, diagnose=False`（`app/logging.py`） |
| 上游 `base_url` 缺失被当成任务失败 | httpx | 相对路径发请求报 "Target host is not specified" | 出站前硬校验，缺失归模糊类（599）走重试，绝不判死（`app/services/relay.py`） |
| 通配路由吞掉字面路由 | fastapi/starlette | 路由匹配按注册顺序首匹配，通配若在前会整片吞掉 `/ops/*`、`/admin/*` | `/queue/{path:path}` 必须最后注册（`app/main.py`，`test_router_mount_order`） |

---

## 12. 端到端验证步骤

```bash
# 1. 安装与静态门禁（不依赖 MySQL / Redis / 上游）
make setup                                            # 建 venv + 装依赖 + 生成 .env（需 uv）
.venv/bin/python -m ruff check app tests scripts gunicorn.conf.py
.venv/bin/python -m mypy app/
.venv/bin/python -m pytest tests/ -q                  # 含 test_typecheck（mypy）与 test_static_gates

# 2. 起服务（二选一）
make standalone                                       # 单进程 web + worker + scheduler，免 .env 可跑
# docker compose up -d --build                        # 或 compose 形态
curl -sf http://127.0.0.1:8000/healthz/ready          # 断言：200 {"status":"ok",...}

# 3. 零上游、零计费检查（随时可跑，不产生任何上游请求）
curl -s http://127.0.0.1:8000/healthz/live            # 断言：{"status":"ok"}
# 管理面 fail-closed：未配置 ADMIN_TOKEN 时应当是 404
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/ops/queue
# 缺 Authorization 在寻址/占槽之前就被拒，不落库、不产生上游请求
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8000/queue/v1/tasks \
  -H 'Content-Type: application/json' -d '{}'                              # 断言：401
# 路径准入：命中默认 deny 前缀，带 token 也 403（此时尚未寻址）
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8000/queue/api/models \
  -H 'Authorization: Bearer sk-xxx' -H 'Content-Type: application/json' -d '{}'   # 断言：403
# 白名单 fail-closed：默认 UPSTREAM_ALLOWLIST 为空，给 base 也 400
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8000/queue/v1/tasks \
  -H 'Authorization: Bearer sk-xxx' -H 'X-Upstream-Base-Url: http://upstream.test' \
  -H 'Content-Type: application/json' -d '{}'                              # 断言：400
```

**真实提交的计费红线（先读这段再决定是否开跑）**

1. `scripts/bench_submit.py` **默认 dry-run**；真发请求必须显式加 **`--execute`**（**注意：参数名是 `--execute`，不是 `--explicit`**），
   且除非再加 `--yes` 会在终端二次确认。自动化场景不要加 `--yes` 绕过人工确认。
2. **真实发请求会触发真实计费**；**执行前必须向 provider 侧确认当前哪个组合免费，不要假定任何组合免费**。
   `bench_submit.py` 的默认 body 是 `{"model": "your-model", "prompt": "bench", "duration": 5}`，**不含任何免费档位字段**，
   因此对它加 `--execute` **不保证免费**。
3. **脚本现状（存疑点，务必先修再用）**：`scripts/bench_submit.py` **尚未迁移到 `/queue` 形态**——
   它的目标 URL 仍按 `{base_url}/{biz}/v1/tasks` 拼接（旧形态），而旧形态已随 **本仓库 ADR-010** 删除。
   在脚本更新前，`--execute` 打到的路径已不存在；真发请改用下面第 4 步的 curl（路径为 `/queue/...`）。
4. 若确需一次真实提交（示例；`model` / `duration` 等字段以 provider 当前口径为准，**不代表免费**），
   且 `X-Upstream-Base-Url` 的 host 已在 `UPSTREAM_ALLOWLIST` 内：

```bash
curl -s -X POST http://127.0.0.1:8000/queue/v1/tasks \
  -H 'Authorization: Bearer sk-xxx' \
  -H 'X-Upstream-Base-Url: http://newapi:3000' \
  -H 'X-Callback-Url: https://your-app.example.com/hook' \
  -H 'Content-Type: application/json' \
  -d '{"model":"your-model","duration":8,"prompt":"e2e"}' | tee /tmp/atask.json
TASK=$(.venv/bin/python -c "import json;print(json.load(open('/tmp/atask.json'))['task_id'])")
curl -s "http://127.0.0.1:8000/queue/v1/tasks/$TASK"          # 断言：200（非终态本地态 / 终态快照）
curl -s -X DELETE "http://127.0.0.1:8000/queue/v1/tasks/$TASK"   # 断言：200，status=canceled（尽力源头止损）
```

5. **幂等重放**（同一 `Idempotency-Key` 应回放同一 `task_id`）：

```bash
curl -s -X POST http://127.0.0.1:8000/queue/v1/tasks \
  -H 'Authorization: Bearer sk-xxx' -H 'X-Upstream-Base-Url: http://newapi:3000' \
  -H 'Idempotency-Key: e2e-001' -H 'Content-Type: application/json' \
  -d '{"model":"your-model","prompt":"x"}' | tee /tmp/atask2.json
# 去掉 Idempotency-Key 前，重复上述请求应返回同一 task_id
```

6. MySQL 是与 new-api 共用的同一实例：禁止在生产数据上做破坏性动作，禁止打爆连接。
   压测前清理自己的幂等键与并发槽，压测后按脚本/响应打印的 `task_id` 逐个 `DELETE` 并核账。

---

## 13. 变更记录

| 日期 | 变更 | 原因 | 影响范围 |
|---|---|---|---|
| 2026-09-12 | Spec v2.0 按 **本仓库 ADR-010** 整篇重写 | 架构换向：对外形态统一为 `/batch/{上游路径}`（该前缀于 2026-09-13 改名为 `/queue`，见下行），鉴权与计费全部下沉上游；旧版 Spec 描述的 keypool + 计费 + `/{biz}` 路由架构已整体删除 | 全量 |
| 2026-09-12 | 验收标准重编号（AC-01 ~ AC-43） | 五级失败分流降为三档；移除 HELD / 冻结 / 租约 / 对账类 AC，新增 `/batch` 受理-探测-取消-收敛类 AC | §9 |
| 2026-09-12 | §11 改为 ADR-010「已知限制」5 条 + 稳定坑 | 旧版未决项登记已随被取代的 ADR 失效；已知限制须写进运维文档 | §11 |
| 2026-09-13 | 定位口径修订：**两个服务都是任务队列服务**；atask 侧明确为**排队异步**（**上游异步 → 本地异步**：本地 task_id、入队排队、后台推进到终态），stask 侧为**同步接口任务化**（同产本地异步）；「异步 / 同步」是**当前准入的任务类型**而非转换方向 | 原文「异步转异步 / 同步转异步」的提法会被读成「谁转谁」，与本服务实际身份不符（用户指正） | §1（产品定义）、§14（与 stask 的分工） |
| 2026-09-13 | 新增提交回填的 **CAS 守卫**（在飞取消不复活终态） | 竞态缺陷：上游提交在飞期间取消 → 裸回填把 `CANCELED` 复活成 `QUEUED`（`QUEUED + progress=100% + finish_time 已写`），随后 sweep 会给已取消的任务投「成功」回调；旧链路 KI-D 修过同一问题，ADR-010 重写时丢失守卫 | §2（提交链路）、§9（AC-16）、`relayflow.submit_queue_task` |
| 2026-09-13 | **路由前缀 `/batch` → `/async`**，概念词根一次改净（env `QUEUE_*`、Redis `atask:queue_sweep_lock`、taskiq `queue_*`、task_id 前缀 `queue_`、`data.source='queue'`） | 命名对齐定位：本服务是任务队列、做的是排队异步，`batch` 是换向前的遗留词；用户定调「命名优先于兼容性」 | §2（对外形态）、全量文档；**需仓库外同步**：nginx、stask deny-list、客户端 |
| 2026-09-13 | **Redis 键前缀 `gw:` → `atask:`**，并收敛到 `app/redis.py::KEY_PREFIX` 单一构造点（`queue.py` / `dynconf.py` / `tokensession.py` 不再各自拼前缀） | `gw:`（gateway）是换向前的遗留缩写：`GW_` 环境变量前缀早已移除、渠道分组概念也随 ADR-010 退场，「gateway」在本仓库已无对应实体；改后与 `GATEWAY_PLATFORM='atask'` 及同族 stask 的 `REDIS_KEY_PREFIX`（默认 `st`）一致 | 全仓键名、全量文档；**部署影响**：见 §10（队列键一并改名，切换前必须排空队列） |
| 2026-09-13 | 新增**攒批放行**：`BATCH_*` 配置项、`X-Batch-Size` / `X-Batch-Wait` / `X-Batch-Key` 三个头、`data.batch_*` 与 `data.slot_flags` 字段、`GET /ops/batches`、`app/services/batching.py` | 削掉提交突发、成组推进，对齐同族 stask-service 的既有能力；**等待期不占并发槽**故攒批路径受理不再 429（改为排队），这是唯一的行为变更。决策见 `docs/decisions/ADR-011-batch-release-gating.md` | §5.3、§5.5、§6、§7、§10（边界与运维速查）、受理与放行链路 |
| 2026-09-13 | §10 更正**提交体上限**的描述：实际有 `BODY_MAX_BYTES`（默认 1 MiB，超限 `413`），且校验在任何副作用之前；并补上 `atask:batch:*` 键 | 原文写「提交体当前无显式上限…旧链路 1 MiB 上限随 preflight 删除」，与代码（`relayflow._read_body_limited`）不符——是一条文档与实现的漂移 | §10 |
| 2026-09-13 | 新增**回调地址准入**与 **body 回调字段支持**：配置项 `CALLBACK_ALLOWLIST` / `CALLBACK_PASSTHROUGH_UPSTREAM`、新模块 `app/services/callback_addr.py`、新字段 `data.submit_body`、AC-44 / AC-45；L-5 由「body 回调字段不做拦截」改为「摘除后转发」；新增对接文档 `docs/CALLBACK-CONTRACT.md` | 客户按上游 API 文档（火山方舟 Seedance 口径）把回调地址放在 body 顶层 `callback_url`，而原实现只认 `X-Callback-Url` 头 → **回调静默不发生**（客户端会一直等一个永不到达的通知）；同时原实现对该地址零校验，等于给互联网开一个 SSRF 出站跳板 | §5.1（受理步骤）、§6（`data` 契约）、§7（热配置白名单与不可热改清单）、§9（AC-44/45）、§11（L-5）、新增 `docs/CALLBACK-CONTRACT.md` |
| - | 关键决策 | 见 **本仓库** `docs/decisions/ADR-001`（复用 tasks 表）、`ADR-004`（时间列归一）、`ADR-009`（异常分层）、`ADR-010`（本版依据）、`ADR-011`（攒批）；`ADR-002 / ADR-005 / ADR-006 / ADR-007` 已被 ADR-010 取代 | 全量 |
