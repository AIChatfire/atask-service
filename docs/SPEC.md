# Spec — atask-service v2.0（规格即契约）

> 生成日期：2026-09-12
> 依据：当前实现（`app/`）+ **本仓库 ADR-010**（对外形态统一为 `/batch/{上游路径}`，鉴权与计费全部下沉上游）+ `AGENTS.md` + `README.md`
> 状态：已确认（本版按 ADR-010 整篇重写；ADR-010 取代**本仓库 ADR-002 / ADR-005 / ADR-006 / ADR-007**，并重写**本仓库 ADR-008** 的分工表述）
> 文档版本 v2.0；包版本以 `pyproject.toml` 为准（当前 0.2.0）

本文件是**团队内部契约**：范围、API、数据、验收标准全部锁定。
不在本 Spec 列表内的功能一律不做；任何改动走 §13 变更流程。
文中每一条 `app/...` 路径、模块名、函数名、配置项名都对应现实现，不得凭记忆改写。

> 跨仓库引用纪律：本仓库与 `stask-service` **各有一套独立的 ADR 编号**，同一编号在两仓库含义不同。
> 本文引用任何 ADR 一律写明仓库名（如「本仓库 ADR-010」）。

---

## 1. 产品定义

- **一句话描述**：异步任务型 AI 模型的统一接入网关——对上承接用户请求（限流 / 幂等 / 并发上限），对下把请求**原样中继**到上游异步任务接口，并持有任务事实源。
- **目标用户**：接入异步任务 API 的应用开发者；以及新增/替换上游时只改一个 `base_url` 与白名单、零发版即可上线的运营。
- **核心问题**：异步上游的提交、探测、取消形态大同小异，但客户端直连要各自适配、各自管理轮询与回调。本网关在**不改上游一行代码**的前提下，把「受理即返回本地 task_id、由后台推进到终态、可选回调」这套任务语义统一提供出来；**资金动作与凭证权威全部留在上游**。
- **定位边界**：网关是**中继层 + 任务事实源**——用户令牌、钱包、渠道、配额扣减的权威都不在网关本地。
  网关**不做鉴权内省**（用户 token 原样透传，有效性由上游判定）、**零资金动作**（不 freeze / settle / cancel，配额由上游 relay 扣减）。
  网关无状态（自有状态全在 Redis），**零自有 MySQL 表**，只读写与 new-api 同实例的 `tasks` 表。
- **与 stask-service 的分工**（**本仓库 ADR-008**，按 ADR-010 重写）：atask 包的是**本来就是异步任务型**的上游（异步转异步，持有任务事实源）；stask 包的是**同步生成接口**（同步转异步）。
- **已知耦合**：任务行落在与上游同实例的 `tasks` 表上（**本仓库 ADR-001**），这是当前唯一一处非 HTTP 依赖；网关自有状态全部放 Redis（`app/redis.py`）。

---

## 2. MVP 范围（锁定）

| 优先级 | 能力 | 实现入口 | 验收标准摘要 |
|---|---|---|---|
| P0 | 受理 `POST /batch/{path}` | `app/routers/batch_task.py::batch_create` → `app/services/relayflow.py::create_batch_task` | 落库即返回本地 `task_id`，请求内零上游往返（AC-01） |
| P0 | 查询 `GET /batch/{path}/{task_id}` | `batch_task.py::batch_get` → `relayflow.py::view_batch_task` | 非终态按需探测，终态回放快照零上游往返（AC-21 / AC-22） |
| P0 | 取消 `DELETE /batch/{path}/{task_id}` | `batch_task.py::batch_delete` → `relayflow.py::cancel_batch_task` | 本地置 CANCELED + 尽力源头止损（AC-25） |
| P0 | 免费 GET 透传 | `relayflow.py::free_batch_get` | 末段非本地 id 时按 IP 限流原样转发，不落 tasks 行（AC-24） |
| P0 | 幂等键原子占位（`Idempotency-Key`） | `app/services/idem.py` | 同键回放同 `task_id`；真并发 409（AC-08 / AC-09） |
| P0 | 上游寻址与安全三防线 | `app/services/upstream_addr.py` | 头优先 + 白名单 fail-closed + scheme / userinfo 校验（AC-03 / AC-04） |
| P0 | 本地限流与并发上限 | `app/deps/ratelimit.py` | 按 token hash 限流与占槽，超限 429（AC-06 / AC-07） |
| P0 | 约定式上游交互 | `app/services/relay.py::call_upstream` | 原样转发 method/query/body，固定 Bearer（AC-14） |
| P0 | worker 提交与失败三档 | `relayflow.py::submit_batch_task`、`app/queue.py::batch_submit_task` | 4xx 判死 / 5xx 留活重试 / 2xx 缺 id 判死（AC-15 ~ AC-20） |
| P0 | 单一终态收口点 | `relayflow.py::_finalize_batch` | 快照 → 还槽 → 回调 → 清会话，恰好一次（AC-26 / AC-27） |
| P0 | 后台收敛 sweep | `app/queue.py::batch_sweep_task` → `relayflow.py::sweep_batch_once` | cron 每分钟、最旧优先、独立重入锁（AC-29 / AC-30） |
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
- **单一终态收口点** `app/services/relayflow.py::_finalize_batch`：视图 / worker / sweep 三路径共用，不许各写一套。
- **状态迁移日志唯一记录点** `app/services/statelog.py::record_transition`。
- **惰性单例**：DB 引擎（`app/db.py::get_engine`）、Redis（`app/redis.py::r`）、HTTP 客户端（`app/services/httpc.py::shared_client`）全部首次使用时创建，`preload_app` fork 后安全。
- **进程形态**：web（`gunicorn -c gunicorn.conf.py app.main:app`）、worker（`taskiq worker app.queue:broker`）、scheduler（`taskiq scheduler app.queue:scheduler`，**必须单副本**）、单进程（`python -m app.standalone`，见 `app/standalone.py`）。

---

## 5. API 端点清单（锁定）

路由注册顺序即 Starlette 首匹配优先级，**不可更换**（`app/main.py::create_app`）：
`healthz → ops → admin → /batch/{path:path}`。
通配 `batch_task_router` 永远最后；`/ops/*` 与 `/admin/*` 必须先于通配，否则会被当成 `path` 吞掉（AC-40，`tests/test_static_gates.py::test_router_mount_order`）。

### 5.1 对外唯一形态：`/batch/{path:path}`

`{path}` 是**上游原生路径**（如 new-api 视频生成 `v1/tasks`）。**`{biz}` 段已从 URL 移除**。
同一路径按**方法 + 末段是否为本地 task_id** 分派：

| Method | Path | 功能 | 认证 | 响应 |
|---|---|---|---|---|
| POST | `/batch/{path:path}` | 受理：落库即返回本地 `task_id`，上游提交交 worker | Bearer（必须） | `202 {"task_id","status":"SUBMITTED"}` + `Location: /batch/{path}/{task_id}` |
| GET | `/batch/{path:path}` | 末段是本地 `task_id` → 任务视图；否则免费透传 | 视图免鉴权；透传必须 Bearer | 视图恒 `200`（本地排队态 / 上游原话 / 快照回放）；透传回上游状态码 |
| DELETE | `/batch/{path:path}` | 末段是本地 `task_id` → 取消；否则 `404` | 无 | `200 {"task_id","status":"canceled"}` / `404` |

**受理（POST）**（`relayflow.create_batch_task`）：

1. `extract_token`（缺 Bearer → `401`）；2. 限流 + 幂等占位（`_rate_and_place`）；3. 路径准入（命中 `BATCH_DENY_PREFIXES` → `403`）；4. 寻址 + 白名单校验（缺失/非法 → `400`）；5. 并发占槽（超限 → `429`）；6. 落库 + 写令牌会话 + 入队。
   请求头形态：`Authorization`、`Idempotency-Key`、`X-Upstream-Base-Url`、`X-Callback-Url`。**请求内零上游往返**。

**查询（GET，末段为本地 id）**（`relayflow.view_batch_task`）：

- 终态 → 回放 `data.upstream_snapshot`（无快照则按落库字段构建等价报文），**零上游往返**；
- 非终态且有 `upstream_task_id` / `upstream_base_url` / 令牌会话 → 探测 `GET {base}{path}/{upstream_id}` 并推进本地状态；探测不可达 / 熔断 / 护栏拒绝 → 回本地排队态，**绝不 `404`**；
- 非终态但尚无上游 id（提交在飞）→ 本地排队态直出；
- 任务不存在 → `404`。

**免费透传（GET，末段非本地 id）**（`relayflow.free_batch_get`）：按 IP 限流 → 原样转发上游，保留状态码与 `Content-Type`（可能是图片/二进制产物，绝不硬写 JSON），**不落 tasks 行**；缺 token → `401`；上游不可达 → `502`。

### 5.2 探针（无认证）

| Method | Path | 功能 | 响应 |
|---|---|---|---|
| GET | `/healthz/live` | 存活探针，零依赖 | `200 {"status":"ok"}` |
| GET | `/healthz/ready` | 就绪探针：Redis `PING` + DB `SELECT 1`（`app/healthz.py`） | 全过 `200`，任一失败 `503` + `checks` |

### 5.3 运维端点（`X-Admin-Token`；`ADMIN_TOKEN` 未配置时整个 `/ops/*` 与 `/admin/*` 返回 `404`）

| Method | Path | 功能 |
|---|---|---|
| GET | `/ops/queue` | 队列快照：`pending` / `delayed` / `dlq` / `tasks_by_status`（短缓存 `QUEUE_STATS_CACHE_SECONDS`） |
| GET | `/ops/tasks/{task_id}` | 任务诊断视图（脱敏 + 令牌会话存在性与 TTL） |
| POST | `/ops/requeue/{task_id}` | 立即重投提交队列（`queue.publish_batch_submit`） |
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

---

## 6. 数据模型（锁定 — 复用 new-api `tasks` 表，零建表）

**平台隔离**：网关行 `platform = settings.gateway_platform`（默认 `"atask"`，配置项 `GATEWAY_PLATFORM`）。
所有读写的 `WHERE` 必带 `platform`（`app/services/taskstore.py::cas` / `patch_data` / `stale_batch_active` / `search_tasks` / `counts_by_status`），
`stask-service` 的 `platform='stask'` 行与 new-api 原生任务行天然互不可见（**本仓库 ADR-001**）。

**`task_id` 形态**：`{biz_slug}_{uuid4hex}`（`app/services/ids.py::new_task_id`）。
受理链路固定传 `"batch"`，故本链路 task_id 形如 `batch_<32 位十六进制>`；
形态判定 `app/services/nativeapi.py::LOCAL_ID_RE` = `^[a-z0-9-]{1,20}_[0-9a-f]{32}$`（GET/DELETE 据此区分视图与透传）。

**列契约**（网关零建表，`tasks` 表由 new-api AutoMigrate 维护；实际读写走 `taskstore.py` 原生 SQL）：
`task_id` / `platform` / `action` / `status` / `fail_reason` / `progress` / `submit_time` / `start_time` /
`finish_time` / `created_at` / `updated_at` / `data`(JSON) / `user_id` / `channel_id` / `quota`。
- 受理时写：`action='task'`、`status='SUBMITTED'`、`progress='0%'`、`user_id=0`、`channel_id=0`、`quota=0`（`taskstore.create`）。
- `quota` **恒写 `0`**——资金由上游 relay 扣减，不在 `tasks.quota` 上体现。
- 终态一律把 `progress` 置 `100%`、用**秒**刷 `finish_time`（`taskstore.cas`）。

**`data` JSON 字段契约**（构造点 `app/services/relayflow.py::create_batch_task`，`JSON_MERGE_PATCH` 增量合并）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `source` | str | 恒 `"batch"`（sweep 候选过滤依据） |
| `model` | str | 浅解析 body 的 `model` / `model_name`；缺失为 `""` |
| `token_hash` | str | sha256(raw token)，限流 / 并槽 / 幂等键口径；**不是凭证** |
| `request_method` / `request_path` / `request_query` | str | 提交时的原文（转发与探测基底） |
| `request_body` | str | 原文按 UTF-8 文本保留（**原样转发体基底**）；二进制按 `errors="replace"` 降级 |
| `request_content_type` | str | 提交时的 `Content-Type`，转发时原样回设 |
| `upstream_base_url` | str | 受理时校验通过的上游基址，worker / 探测只认它 |
| `callback_url` | str | 可空；只来自 `X-Callback-Url` 头 |
| `upstream_task_id` | str | 上游任务 id，**绝不对外暴露**（对外报文里被逐字节改写回本地 id） |
| `upstream_status` | str | 上游状态原话（状态映射与快照回显用） |
| `upstream_snapshot` | obj | 终态上游原始报文（≤ `nativeapi.SNAPSHOT_MAX_BYTES` = 8192 字节；空报文不落键），原生查询逐字段同构回放 |

> **刻意不写** `freeze_amount` / `settled`（**本仓库 ADR-010 §3**：网关零资金动作）。
> 管理面历史投影白名单里仍保留这两个键名，但本链路永不写入（读出来是空值）。

**状态机**（常量以 `app/schemas.py` 为准）：
`SUBMITTED → QUEUED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`；无 `HELD`。
`ACTIVE = (SUBMITTED, QUEUED, IN_PROGRESS)`（CAS 合法起点），
`TERMINAL = (SUCCESS, FAILURE, CANCELED)`（不可逆，迟到快照丢弃）。
状态迁移一律 CAS：`taskstore.cas` 的 `rowcount == 1` 才视为抢到推进权（终态恰好一次）。

**时间列纪律（最硬的连带纪律，本仓库 ADR-004）**：
`tasks` 是共享表，时间列**可能被其他写入方写成毫秒**（new-api 原生模块用 UnixMilli 写法）。三道纪律缺一不可：

1. 写侧恒写秒（`taskstore._now()`，`int(time.time())`）；
2. 读侧统一归一 `taskstore.as_unix_seconds`（`> 1e11` 视为毫秒折算，缺失 / 非法 → 0），`_row_to_dict` 对全部时间列归一；
3. SQL 时间比较必须套 `taskstore._secs(col)` = `IF(col > 1e11, col DIV 1000, col)`（`stale_batch_active` / `search_tasks` 全部包裹）。

---

## 7. 管理看板（`/admin`，锁定）

- **形态**：单文件 HTML（`app/static/admin.html`），零构建；`GET /admin` 与 `GET /admin/` 返回该页。
- **页面本身不鉴权**：它只是空壳，所有数据都要带密钥调 `/admin/api/*`；密钥存浏览器 `sessionStorage`，不落 URL。
- **启用闸门**：`app/deps/admin.py::admin_enabled()` 为假（`ADMIN_TOKEN` 未配置）时页面与全部 API 一致 404。
- **脱敏纪律**：列表 / 详情只做白名单投影；`token_hash`、`request_body`、上游原始报文 `upstream_snapshot` 一律不出现；令牌会话只给存在性与 TTL（`tokensession.session_info`）。
- **破坏性操作边界**：只提供「重投提交」与「配置回落」两类写操作；**没有删除任务入口**——终态推进的唯一入口是 `relayflow._finalize_batch`，管理面绝不绕过它。
- **热配置白名单**（`app/services/dynconf.py::MUTABLE`，当前**两项**）：
  `max_concurrent_tasks`、`upstream_breaker_threshold`。
  读取优先级「Redis 覆盖 > env > 代码默认」，带 5 秒进程内缓存；Redis 不可用时回落 env，绝不成为可用性单点。
  `IMMUTABLE_REASONS` 显式登记永不可热改的安全项（`database_url` / `redis_url` / `admin_token` /
  `callback_sign_secret` / `gateway_platform` / `rate_limit_per_minute` / `upstream_allowlist`）及其原因。

---

## 8. 设计 Token

不适用（网关无自建前端产物）。日志遵循 `app/logging.py` 的 loguru 终端格式，
`docs/SPEC.md`、源码、决策文档与 `docs/**` 全部 markdown 均**不含任何 emoji 字符**
（由 `tests/test_static_gates.py::test_no_emoji_in_docs` 与 `::test_no_emoji_in_source` 机械断言）。

---

## 9. 验收标准（EARS 格式，锁定 — QA 唯一依据）

| 编号 | 功能 | EARS 验收标准 | 优先级 |
|---|---|---|---|
| AC-01 | 受理 | 当客户端 `POST /batch/{path}` 且带合法 `Authorization: Bearer`、上游基址可解析且通过白名单，系统应在落库后返回 `202` + `{task_id, status:"SUBMITTED"}` + `Location: /batch/{path}/{task_id}`，且**请求内零上游往返**（提交交 worker） | P0 |
| AC-02 | 路径准入 | 若规整后的 `{path}` 命中 `BATCH_DENY_PREFIXES`（默认 `/api/,/console/`），系统应返回 `403` 且不落 tasks 行（判定先于寻址与占槽） | P0 |
| AC-03 | 上游寻址 | 当请求带 `X-Upstream-Base-Url` 头，系统应以该头为上游基址；头缺失/为空时应回退 `UPSTREAM_BASE_URL`；两者都为空应返回 `400` | P0 |
| AC-04 | 寻址安全 | 若上游基址不是 `http`/`https`、含 URL userinfo 或无 host，系统应返回 `400`；若 host 未命中 `UPSTREAM_ALLOWLIST`，或 `UPSTREAM_ALLOWLIST` 为空，系统应返回 `400`（fail-closed：白名单为空即全部拒绝） | P0 |
| AC-05 | 鉴权 | 若受理请求缺少合法 `Authorization: Bearer`，系统应返回 `401`；系统**不做内省**，令牌有效性由上游判定 | P0 |
| AC-06 | 限流 | 当某 `token_hash` 在 60 秒滑动窗口内请求数超过 `RATE_LIMIT_PER_MINUTE`，系统应返回 `429` 并携带 `Retry-After: 10` | P0 |
| AC-07 | 并发上限 | 当某 `token_hash` 在途任务数达到 `MAX_CONCURRENT_TASKS`（热改项 `max_concurrent_tasks`），系统应返回 `429` + `Retry-After: 30`；并发键按 `token_hash` 计，带 `CONC_TTL_SECONDS` 兜底 | P0 |
| AC-08 | 幂等回放 | 当同一 `Idempotency-Key` 已回填 `task_id`，系统应在占位与落库之前短路并回放首个任务视图，不产生第二个任务、不产生第二次占槽 | P0 |
| AC-09 | 幂等并发 | 当同一 `Idempotency-Key` 真并发且占位未回填，非占位者应在 `IDEM_REPLAY_WAIT_SECONDS`（默认 25s）内短轮询；窗口内回填则回放，超时或占位消失应返回 `409`，绝不放行重建 | P0 |
| AC-10 | 幂等归还 | 若受理链路任一步失败（路径拒绝 / 寻址失败 / 落库异常 / 入队异常），系统应 CAS 归还幂等占位（仅当值仍为 `pending`，`LUA_CAS_DELETE`）并归还并发槽 | P0 |
| AC-11 | 令牌会话 | 当受理成功，系统应把用户明文令牌**仅**写入 Redis 会话（键 `gw:sk:{task_id}`，TTL = `SK_SESSION_TTL_SECONDS`）；明文令牌绝不落 tasks 表、绝不进日志、绝不出现在任何响应里 | P0 |
| AC-12 | 脱敏回显 | 系统应把 `request_body` 原样保留于 `tasks.data`（转发体基底），但管理面与 `/ops/*` 视图绝不回显 `request_body` 与令牌本体；令牌会话只暴露存在性与 TTL | P0 |
| AC-13 | 零资金动作 | 当任务创建与流转，系统应不写 `freeze_amount` / `settled`，不调用任何冻结 / 结算 / 取消接口 | P0 |
| AC-14 | worker 提交 | 当 worker 消费 `batch_submit_task`，系统应按 `data.upstream_base_url` + `data.request_path` 原样转发 method / query / body 至 `POST {base}{path}`，鉴权固定 `Authorization: Bearer <用户 token>` | P0 |
| AC-15 | id 提取 | 当提交响应为 2xx，系统应取响应里的 `id`，缺失时回退 `task_id`；两者都缺失应经单一收口点落 `FAILURE`（`fail_reason` 含 `missing task id`），不静默挂起 | P0 |
| AC-16 | 提交成功 | 当提取到上游 id 且映射状态非终态，系统应回填 `upstream_task_id` / `upstream_status` 并置 `QUEUED` | P0 |
| AC-17 | 提交即终态 | 当提交响应直接给出终态状态词，系统应经单一终态收口点落终态 | P0 |
| AC-18 | 失败档一 | 当上游返回 4xx，系统应经单一收口点落 `FAILURE`、释放并发槽、清令牌会话，**不重试** | P0 |
| AC-19 | 失败档二 | 当上游返回 5xx 或传输层错误（`RelayError` 599），系统应抛 `RelayError` 交 queue 层退避重试，任务**留活**非终态、**不释放并发槽**、**保留令牌会话** | P0 |
| AC-20 | 失败档三 | 当上游返回 2xx 但缺 `id`/`task_id`，系统应经单一收口点落 `FAILURE`、释放并发槽、清令牌会话 | P0 |
| AC-21 | 视图探测 | 当 `GET /batch/{path}/{task_id}` 且任务非终态且有 `upstream_task_id`、`upstream_base_url` 与令牌会话，系统应探测 `GET {base}{path}/{upstream_id}` 并按 `statusmap.map_status` 推进本地状态；探测不可达 / 熔断 / 护栏拒绝应回本地排队态，**绝不 `404`** | P0 |
| AC-22 | 终态回放 | 当任务为终态，系统应回放 `data.upstream_snapshot`（无快照时按落库字段构建等价报文），**零上游往返** | P0 |
| AC-23 | 报文同构 | 系统应把探测 / 回放报文里的上游 id 逐字节改写为本地 `task_id`（`nativeapi.rewrite_ids`，不重新序列化），且 `status` 保留上游原话 | P0 |
| AC-24 | 免费透传 | 当 `GET /batch/{path}` 且末段不是本地 `task_id`，系统应按 IP 限流后原样转发上游（保留状态码与 `Content-Type`），**不落 tasks 行**；缺 token 返回 `401`，上游不可达返回 `502` | P0 |
| AC-25 | 取消 | 当 `DELETE /batch/{path}/{task_id}` 且任务非终态，系统应 CAS 置 `CANCELED`、释放并发槽、尽力 `DELETE {base}{path}/{upstream_id}` 源头止损（失败只告警）；任务已终态应回放该终态视图；末段非本地 id 应返回 `404` | P0 |
| AC-26 | 终态恰好一次 | 当终态推进 CAS 未抢到（`rowcount != 1`），系统应整段不执行快照落库 / 释槽 / 回调 / 清会话，返回 False | P0 |
| AC-27 | 收口顺序 | 当任务进入终态，系统应按「CAS 抢推进权（同一条 UPDATE 落快照，≤ `SNAPSHOT_MAX_BYTES`）→ 记一条状态迁移日志 → 还并发槽 → 投递回调（有 `callback_url` 时）→ 清令牌会话」执行，且视图 / worker / sweep 三路径共用 `_finalize_batch` 同一实现 | P0 |
| AC-28 | 迁移日志 | 当 CAS 抢到推进权，系统应经 `statelog.record_transition` 恰好记一条状态迁移日志；未抢到推进权时不记 | P0 |
| AC-29 | 后台收敛 | 当任务 `source='batch'`、非终态、有 `upstream_task_id` 且 `_secs(updated_at)` 早于 `now - TASK_STALE_SECONDS`，`batch_sweep_task`（cron `*/1 * * * *`）应按**最旧优先**（`ASC`）取至多 `BATCH_SWEEP_BATCH` 条探测，映射到终态的走单一收口点推进 | P0 |
| AC-30 | 收敛重入锁 | 当另一轮 sweep 持有 `gw:batch_sweep_lock`，系统应跳过本轮并返回 0（不叠加并发轮）；释放锁用 CAS 删除（只删自己持有的 `guard`） | P0 |
| AC-31 | 会话过期 | 若某任务的令牌会话已不存在，sweep 应跳过该任务（DEBUG 级），**绝不判死、绝不释放并发槽** | P0 |
| AC-32 | 用户回调 | 当任务转终态且 `data.callback_url` 非空，系统应经 `queue.publish_notify` → `notify.push` 投递含 `X-Gateway-Signature: t=<ts>,v1=<hmac-sha256(CALLBACK_SIGN_SECRET, ts + "." + body)>` 的 JSON 体，至少一次（用户按 `task_id + status` 去重）；无 `callback_url` 不投递 | P0 |
| AC-33 | 时间归一 | 系统应把 `tasks` 全部时间列经 `taskstore.as_unix_seconds` 归一，SQL 时间比较套 `_secs(col)`，写侧恒写 unix 秒 | P0 |
| AC-34 | 平台隔离 | 系统对所有 `tasks` 表读写应恒带 `platform = GATEWAY_PLATFORM`，绝不读写 `stask` 或 new-api 原生任务行；`quota` 列恒写 `0` | P0 |
| AC-35 | 访问单点 | 系统对 `tasks` 表的原生 SQL 应只出现在 `app/services/taskstore.py` | P0 |
| AC-36 | 管理鉴权 | 若 `ADMIN_TOKEN` 未配置，系统应对 `/ops/*` 与 `/admin/*` 一律返回 `404`；已配置时缺失 / 错误 `X-Admin-Token` 应返回 `401`，且终端用户令牌不得通过（`secrets.compare_digest`） | P0 |
| AC-37 | 管理脱敏 | 管理端点应采用白名单投影，绝不返回 `token_hash` / `request_body` / `upstream_snapshot` / 令牌本体 | P0 |
| AC-38 | 热配置 | 当写入不在 `dynconf.MUTABLE`（当前两项）内的键，系统应返回 `400` 且整批不落盘；`IMMUTABLE_REASONS` 中的安全项永不可写；Redis 写失败应返回 `503`，绝不谎报成功 | P1 |
| AC-39 | 熔断 | 当某上游 `host:port` 在 `UPSTREAM_BREAKER_WINDOW_SECONDS` 内失败数达到上游熔断阈值（热改项 `upstream_breaker_threshold`），系统应拒绝出站（`upstream.BreakerOpenError`）；探测路径据此回本地排队态，不判死任务 | P1 |
| AC-40 | 路由顺序 | 系统应按 `healthz → ops → admin → /batch/{path:path}` 顺序注册，通配永远最后，保证 `/ops/*` 与 `/admin/*` 不被吞掉 | P0 |
| AC-41 | 错误形制 | 系统应把所有非 2xx 响应归一为 `{"error": {"message","type","param","code"}}`；校验失败返回 `422` + `code=validation_error` | P0 |
| AC-42 | 零 emoji | `docs/SPEC.md`、源码、仓库根级与 `docs/**` 全部 markdown 应不含任何 emoji 字符 | P1 |
| AC-43 | 配置键名 | 配置键名应等于 `Settings` 字段名大写、不带前缀；未知变量应被静默忽略（`extra="ignore"`）——系统**不提供旧键名或别名兼容**，写错键名不会有任何提示 | P0 |

---

## 10. 边界与约束

- Python ≥ 3.12；依赖钉版唯一处 = `pyproject.toml`。
- **MySQL 与 new-api 共享实例**，连接预算 `进程数 × (DB_POOL_SIZE + DB_MAX_OVERFLOW) ≤ max_connections × 0.8`。
  gunicorn worker 数由该预算反推（`gunicorn.conf.py`）；`DB_MAX_CONNECTIONS` / `DB_WEB_SHARE` / `GUNICORN_WORKERS`
  是 `gunicorn.conf.py` 直读的 env（**不是** `Settings` 字段），`DB_POOL_SIZE` / `DB_MAX_OVERFLOW` 同时是 `Settings` 字段，改一处须同步另一处。
- **Redis 独立实例**（compose 内 `redis` 服务），`--appendonly yes --appendfsync everysec`。
  键统一前缀 `gw:`（`app/redis.py`）；队列 `gw:taskiq`，延迟任务 `gw:sched:*`，死信 `gw:events:dlq`，
  幂等键 `gw:idem:*`，令牌会话 `gw:sk:*`，并发槽 `gw:conc:*`，熔断 `gw:breaker:*`，收敛锁 `gw:batch_sweep_lock`。
  Redis 只放「丢了能重建」的状态，事实源永远是 `tasks` 表。
- **上游出站统一走 `relay.call_upstream`**：共享连接池 + 全局超时 `RELAY_TIMEOUT_SECONDS`（默认 60s）+ 熔断（键取上游 `host:port`）。
  空基址按 599 模糊失败拦下（配置/基础设施问题，不判死）。
- **终态快照上限** 8192 字节（`nativeapi.SNAPSHOT_MAX_BYTES`），超限不落键；探测报文正常 <2KB。
- **提交体当前无显式上限**：`relayflow.create_batch_task` 用 `await request.body()` 一次性读入内存并按文本存进 `data.request_body`；
  旧链路的 1 MiB 上限随 `preflight` 一并删除。部署侧须在反代 / 网关层自设请求体上限。
- **gunicorn timeout 必须留足余量**：`gunicorn.conf.py` 默认 `timeout = max(180, GUNICORN_REQ_MAX_SECONDS + 120)`；
  web 侧探测 / 取消 / 免费透传最长等待 `RELAY_TIMEOUT_SECONDS`，调大它要同步确认 timeout 与 `graceful_timeout`（须 `< timeout - 5` 且 `< compose stop_grace_period`）。
- **scheduler 必须单副本**（多份会重复触发每分钟 sweep）；worker 扩副本时拆回独立 scheduler。
- **管理面 fail-closed**：`ADMIN_TOKEN` 未配置时 `/ops/*` 与 `/admin/*` 全部 `404`（不暴露端点存在）；配置 `.env` 时该项为必填。
- 可观测开关 `LOGFIRE_ENABLED` 默认关闭；开启后任何观测失败只告警，绝不影响业务主流程。
- **上游接入契约**：接入不符合 new-api 约定（`POST {base}{path}`、`id`/`task_id`、`status`、`DELETE {base}{path}/{id}`）的上游**需要改代码**——本网关刻意不提供渠道级适配。

### 10.1 傻瓜式接入新上游（运维速查）

**接入新上游 = 配一个 `base_url` + 白名单，网关零代码改动、零路由文件、零渠道元数据。**

1. 部署侧配置 `UPSTREAM_BASE_URL`（默认上游）与 `UPSTREAM_ALLOWLIST`（允许的 host 列表，逗号分隔，**空 = 全拒**）。
2. nginx 侧对 `/batch/` 无条件 `proxy_set_header X-Upstream-Base-Url "<真实上游>"`（覆盖客户端同名头，见 **本仓库 ADR-010 §4** 与已知限制 3）。
3. 客户端以 `POST /batch/{上游原生路径}` 提交，`{path}` 直接照抄上游路径（如 `v1/tasks`）；查询 / 取消用同一前缀 + 本地 `task_id`。
4. 无需改任何 `app/` 代码；若上游不满足约定（提交返回 `id`、状态字段 `status`、探测 `GET {base}{path}/{id}`），才需要改 `app/services/relay.py`。

---

## 11. 内嵌已知坑

### 11.1 ADR-010 已知限制（如实转述，**本仓库 ADR-010「已知限制」5 条**）

| 编号 | 已知限制 | 后果与处置 |
|---|---|---|
| L-1 | **令牌会话过期后任务无法自愈**。探测上游需要用户 token，而网关只把 token 存在 Redis 会话里（TTL = `SK_SESSION_TTL_SECONDS`，48h）——这是「鉴权下沉上游」的必然代价：网关不持有长期凭证 | 会话过期后 sweep 会跳过该任务（DEBUG 级，不报错、**绝不判死、绝不释放并发槽**），该任务会**永久停在非终态**。处置：客户端可 `DELETE` 取消，或由管理面介入 |
| L-2 | **没有 max-age 判死，这是刻意的**。旧 poller 有超时转 FAILURE；新链路**不设** | 理由：会话 TTL 已天然给探测设了上界（48h 后自动跳过），而判死会**永久丢失一个可能已在上游成功的任务结果**——判死不可逆，无明确收益则不做 |
| L-3 | **`X-Upstream-Base-Url` 头的可信性完全依赖 nginx 配置正确** | 若 nginx 未无条件覆盖，客户端可伪造该头把请求（连同用户 sk）指向任意 host；`UPSTREAM_ALLOWLIST` 是第二道防线，**两道都必须配** |
| L-4 | **取消语义退化**：不再有「解冻」，`DELETE` 只做尽力源头止损 + 本地置 CANCELED | 上游取消形态（`DELETE {base}{path}/{id}`）属**约定推断**，未经上游文档验证；若某上游取消端点不是此形态，需改代码 |
| L-5 | **body 里的回调字段不做拦截**：请求体逐字节原样转发 | 若用户自行在 body 里放回调字段，上游可能直接回调、与网关回调形成双投递。网关不解析 body 语义（这是「零配置 / 原样转发」的对价） |

### 11.2 稳定坑

| 坑 | 技术栈指纹 | 根因 | 修法 |
|---|---|---|---|
| 共享表时间列混入毫秒 | mysql/new-api-tasks | new-api 原生任务模块用 UnixMilli 写法 | 读侧 `as_unix_seconds` 兜底归一；SQL 谓词套 `_secs(col)`；只命中本服务写的秒值行（本仓库 ADR-004） |
| 三方共写一张表 | mysql/new-api-tasks | 表被 new-api + atask + stask 三方写 | 所有读写 `WHERE` 恒带 `platform`；`taskstore.py` 为唯一数据访问点（本仓库 ADR-001） |
| taskiq `with_labels(delay=)` 不生效 | taskiq-redis/ListQueueBroker | `ListQueueBroker` 不支持 delay 标签 | 延迟任务一律走 `schedule_by_time`（`app/queue.py::_retry_or_dlq`） |
| gunicorn preload + 全局连接池 | gunicorn/preload_app | fork 前建连接会在子进程间共享 socket | 引擎 / Redis / HTTP 客户端全部惰性单例 |
| loguru `diagnose=True` 泄露 token | loguru | 异常回溯打印帧局部变量，含 raw token | 固定 `backtrace=False, diagnose=False`（`app/logging.py`） |
| 上游 `base_url` 缺失被当成任务失败 | httpx | 相对路径发请求报 "Target host is not specified" | 出站前硬校验，缺失归模糊类（599）走重试，绝不判死（`app/services/relay.py`） |
| 通配路由吞掉字面路由 | fastapi/starlette | 路由匹配按注册顺序首匹配，通配若在前会整片吞掉 `/ops/*`、`/admin/*` | `/batch/{path:path}` 必须最后注册（`app/main.py`，`test_router_mount_order`） |

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
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8000/batch/v1/tasks \
  -H 'Content-Type: application/json' -d '{}'                              # 断言：401
# 路径准入：命中默认 deny 前缀，带 token 也 403（此时尚未寻址）
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8000/batch/api/models \
  -H 'Authorization: Bearer sk-xxx' -H 'Content-Type: application/json' -d '{}'   # 断言：403
# 白名单 fail-closed：默认 UPSTREAM_ALLOWLIST 为空，给 base 也 400
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8000/batch/v1/tasks \
  -H 'Authorization: Bearer sk-xxx' -H 'X-Upstream-Base-Url: http://upstream.test' \
  -H 'Content-Type: application/json' -d '{}'                              # 断言：400
```

**真实提交的计费红线（先读这段再决定是否开跑）**

1. `scripts/bench_submit.py` **默认 dry-run**；真发请求必须显式加 **`--execute`**（**注意：参数名是 `--execute`，不是 `--explicit`**），
   且除非再加 `--yes` 会在终端二次确认。自动化场景不要加 `--yes` 绕过人工确认。
2. **真实发请求会触发真实计费**；**执行前必须向 provider 侧确认当前哪个组合免费，不要假定任何组合免费**。
   `bench_submit.py` 的默认 body 是 `{"model": "your-model", "prompt": "bench", "duration": 5}`，**不含任何免费档位字段**，
   因此对它加 `--execute` **不保证免费**。
3. **脚本现状（存疑点，务必先修再用）**：`scripts/bench_submit.py` **尚未迁移到 `/batch` 形态**——
   它的目标 URL 仍按 `{base_url}/{biz}/v1/tasks` 拼接（旧形态），而旧形态已随 **本仓库 ADR-010** 删除。
   在脚本更新前，`--execute` 打到的路径已不存在；真发请改用下面第 4 步的 curl（路径为 `/batch/...`）。
4. 若确需一次真实提交（示例；`model` / `duration` 等字段以 provider 当前口径为准，**不代表免费**），
   且 `X-Upstream-Base-Url` 的 host 已在 `UPSTREAM_ALLOWLIST` 内：

```bash
curl -s -X POST http://127.0.0.1:8000/batch/v1/tasks \
  -H 'Authorization: Bearer sk-xxx' \
  -H 'X-Upstream-Base-Url: http://newapi:3000' \
  -H 'X-Callback-Url: https://your-app.example.com/hook' \
  -H 'Content-Type: application/json' \
  -d '{"model":"your-model","duration":8,"prompt":"e2e"}' | tee /tmp/atask.json
TASK=$(.venv/bin/python -c "import json;print(json.load(open('/tmp/atask.json'))['task_id'])")
curl -s "http://127.0.0.1:8000/batch/v1/tasks/$TASK"          # 断言：200（非终态本地态 / 终态快照）
curl -s -X DELETE "http://127.0.0.1:8000/batch/v1/tasks/$TASK"   # 断言：200，status=canceled（尽力源头止损）
```

5. **幂等重放**（同一 `Idempotency-Key` 应回放同一 `task_id`）：

```bash
curl -s -X POST http://127.0.0.1:8000/batch/v1/tasks \
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
| 2026-09-12 | Spec v2.0 按 **本仓库 ADR-010** 整篇重写 | 架构换向：对外形态统一为 `/batch/{上游路径}`，鉴权与计费全部下沉上游；旧版 Spec 描述的 keypool + 计费 + `/{biz}` 路由架构已整体删除 | 全量 |
| 2026-09-12 | 验收标准重编号（AC-01 ~ AC-43） | 五级失败分流降为三档；移除 HELD / 冻结 / 租约 / 对账类 AC，新增 `/batch` 受理-探测-取消-收敛类 AC | §9 |
| 2026-09-12 | §11 改为 ADR-010「已知限制」5 条 + 稳定坑 | 旧版未决项登记已随被取代的 ADR 失效；已知限制须写进运维文档 | §11 |
| - | 关键决策 | 见 **本仓库** `docs/decisions/ADR-001`（复用 tasks 表）、`ADR-004`（时间列归一）、`ADR-009`（异常分层）、`ADR-010`（本版依据）；`ADR-002 / ADR-005 / ADR-006 / ADR-007` 已被 ADR-010 取代 | 全量 |
