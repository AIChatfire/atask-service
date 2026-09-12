# 项目说明（AI 入口）

## 这是什么

异步 AI 网关（atask-service）：异步任务型模型（视频生成等）的统一接入网关，
与 new-api 生态共用用户体系、钱包与渠道配置。**对外唯一形态是 `POST /batch/{path}`**、
`GET /batch/{path}/{task_id}`、`DELETE /batch/{path}/{task_id}`——`{path}` 是上游原生
路径（如 `v1/tasks`）。

**外部协同：没有外部微服务**。这是本仓库 ADR-010 换向的结果——旧架构里的
keypool-service（上游 key + 渠道元数据 + 计费规则）与 newapi-billing-service
（内省 + freeze/settle/cancel）**已整体移除**，相关模块（`providers/`、`pricing.py`、
`leasing.py`、`held.py`、`registry.py`、`preflight.py` 等）不在仓库里了：

- **鉴权**：网关**不做内省**，用户 token 以 `Authorization: Bearer` 原样透传上游，
  由上游 relay 判定有效性；网关只做本地可做的事（限流、幂等、并发上限）。
- **计费**：网关**零资金动作**——不 freeze / settle / cancel，配额由上游 new-api
  原生 relay 扣减（预扣 + 实结）。

换向动机：当上游本身就是 new-api 时，它的原生 relay 已完成渠道选择与配额扣减，
网关再 lease 一把上游 key、再 freeze 一笔额度属于**重复资产 + 重复风险**。

## 目录结构

- app/routers/   HTTP 入口（`batch_task` 通配中继 + `ops` 运维 + `admin` 看板 + `healthz` 探针）
- app/deps/      请求侧横向件（`identity` 令牌提取、`ratelimit` 限流/并发、`admin` 管理面鉴权）
- app/services/  编排层：
  - `relayflow.py`   **唯一链路**的生命周期（受理 / 视图 / 取消 / worker 提交 / sweep / 单一终态收口点）
  - `relay.py`       约定式上游交互（`extract_upstream_task_id` / `upstream_status` / `call_upstream`）
  - `nativeapi.py`   原生报文工具（`normalize` / `is_local_id` / `rewrite_ids` / `capture_snapshot`）
  - `statusmap.py`   上游状态自动映射（内置字典 + 前缀猜测）
  - `upstream_addr.py` 上游寻址与安全三防线（`X-Upstream-Base-Url` + allowlist）
  - `upstream.py`    上游出站熔断护栏
  - `taskstore.py`   共享 `tasks` 表数据访问单点（时间列归一）
  - `idem.py` / `ids.py` / `tokensession.py` / `notify.py` / `httpc.py` / `dynconf.py` / `statelog.py`
- app/queue.py   taskiq 任务定义与发布门面（`batch_submit_task` / `batch_sweep_task` / `notify_task`）
- app/schemas.py 共享状态常量（`SUBMITTED`/`QUEUED`/`IN_PROGRESS`/`SUCCESS`/`FAILURE`/`CANCELED`，`ACTIVE`/`TERMINAL`）
- app/static/admin.html  管理看板单文件页面
- tests/         pytest（respx 拦 HTTP，FakeRedis + 内存 taskstore，无外部依赖）

## 常用命令

- 测试：`.venv/bin/python -m pytest tests/ -q`（含 mypy 类型检查，见
  tests/test_typecheck.py；单跑 `.venv/bin/python -m mypy app/`）
- Lint：`.venv/bin/python -m ruff check app tests`
- 安装：`.venv/bin/pip install -e ".[dev]"`
- 本地依赖：`docker compose up -d redis`（MySQL 用与 new-api 共享的实例）
- 运行：网关 `gunicorn -c gunicorn.conf.py app.main:app`；
  后台 `sh -c "taskiq scheduler app.queue:scheduler & exec taskiq worker app.queue:broker"`
  （scheduler 合并进 worker，必须单副本；worker 扩副本时拆回独立 scheduler）
- 单进程联调：`.venv/bin/python -m app.standalone`

## 关键约定

- **对外形态唯一**：`/batch/{上游原生路径}`。`{biz}` 段已从 URL 移除；不做任何旧形态
  兼容（本仓库 ADR-010）。提交 `POST {base}{path}` 原样转发 method / query / body；
  提取上游任务 id 取 `id`、缺失回退 `task_id`；探测 / 取消
  `{base}{path}/{upstream_task_id}`；状态字段 `status`；鉴权固定 Bearer。
- **零渠道配置**：去掉 keypool 后，渠道路由元数据（`task_id_path` / `probe_path` /
  `auth_type` / 渠道级 `timeout_sec` / `model_mapping` / `result_url_template` 等）全部
  失去来源，一律按 new-api 约定硬编码，**不引入任何本地配置文件**。接入新上游 =
  配一个 base_url + 白名单；不符合 new-api 约定的上游需要改代码。
- **上游寻址与安全三防线**（`app/services/upstream_addr.py`）：`X-Upstream-Base-Url`
  头（nginx **无条件覆盖**客户端同名气头）优先 → 回退配置 `UPSTREAM_BASE_URL`；
  host 必须命中 `UPSTREAM_ALLOWLIST`；仅 `http`/`https`、拒 URL userinfo、
  **白名单为空即全部拒绝**（fail-closed，防用户 sk 被打到野地址）。
- **鉴权不做内省**：`app/deps/identity.py::extract_token` 只把 `Authorization`
  解成 `TokenCtx`（`raw` 原始令牌 + `hash` 本地身份替身）。`hash` 用于限流 / 并发 /
  幂等键；`raw` **只进 Redis 会话**，终态即清。
- **零资金动作**：`tasks.data` 不写 `freeze_amount` / `settled`；没有 HELD 挂起、
  没有冻结续期、没有孤儿资金收口、没有解冻。取消只做尽力源头止损。
- **提交失败三档**（`app/services/relayflow.py::submit_batch_task`）：
  上游 4xx → FAILURE + 还并发槽 + 清会话；5xx / 传输错误 → **留活重试**
  （不判死——上游可能已接单，判死会放过真实在跑的单）；2xx 缺 id → FAILURE。
- **单一终态收口点**：`relayflow._finalize_batch` 是视图探测 / 后台 sweep / worker
  提交**共用**的唯一终态收口实现，顺序即语义：CAS 抢推进权 → 记一条状态迁移日志
  → 落终态快照（≤8KB）→ 释放并发槽 → 投递用户回调 → 清令牌会话。CAS 抢不到即
  整段不执行——这是终态事件「恰好一次」的唯一保证，不许各路径各写一套。
- **终态收敛双通道**：① 客户端轮询驱动视图探测（GET 时按需探测，无后台 poller）；
  ② 后台 `batch_sweep_task`（cron 每分钟、独立重入锁 `K_BATCH_SWEEP_LOCK`）按
  `TASK_STALE_SECONDS` 探测非终态任务并推进——候选**最旧优先**（`updated_at ASC`），
  用 DESC 会让最旧那批永远轮不到探测，而它们最可能已在上游成功。
  令牌会话过期则跳过（DEBUG 级，绝不判死、绝不释放并发槽，见 ADR-010 已知限制）。
- **用户回调**：受理时接受 `X-Callback-Url` 头，终态经 `app/services/notify.py`
  HMAC-SHA256 签名后投递，走既有 `queue.publish_notify`（重试 + 死信）。
  **不读 body 里的回调字段**（body 原样转发，网关不解析语义）。
- **原生报文同构 + 终态零上游往返**：探测响应把上游 id 逐字节改写回本地 id
  （`nativeapi.rewrite_ids`），报文其余部分 100% 同构；终态把上游原始报文落
  `data.upstream_snapshot`（`capture_snapshot`，≤8KB，空报文不落键），查询直接回放。
- **幂等原子占位**（`app/services/idem.py`）：SET NX 写 `pending` 占位把「先查后写」
  变原子——同 Idempotency-Key 真并发只有占位者继续创建链路；其余短轮询等占位回填为
  task_id（`pending` → `task_id`）后回放，超时/过期按 409 冲突（不放行重建）。
- **并发上限按 token hash**（`app/deps/ratelimit.py`）：不依赖内省与余额，上限走运行时
  热配置 `max_concurrent_tasks`；槽键带 TTL 兜底，防「占槽后崩溃」永久泄漏。
- **免费 GET 透传**（末段不是本地 id）：原样转发上游、上游状态码与 `Content-Type`
  原样回吐、**不落 tasks 行**，按 IP 限流；必须带 `Authorization`。
- **共享表时间列不可信**：`tasks` 表与 new-api 共享，时间列可能被其他写入方写成
  毫秒（UnixMilli）。一切时间计算必须走 `taskstore.as_unix_seconds` 归一，SQL 比较
  必须套 `_secs(col)` 表达式（本仓库 ADR-004）。
- **错误响应统一** `{"error": {...}}`（`app/errors.py` 注册点）；内部状态常量以
  `app/schemas.py` 为准。
- **日志统一 loguru**：业务模块 `from app.logging import log`，装配点 `app/logging.py`
  （web 在 main、worker 在队列中间件 startup），级别 `LOG_LEVEL`；**状态变化唯一
  记录点**是 `app/services/statelog.py`（Redis 去重，只在状态变化时发一条）；
  可观测装配单点在 `app/observability.py`（web / worker / standalone 只传 `component`）。
- **配置键名不带前缀**：键名 = `Settings` 字段名大写（`app/config.py`；
  `.env.example` 是键名的完整清单）。**不提供任何旧键名或别名兼容**：未知变量一律被
  静默忽略（`extra="ignore"`，刻意的），因此**写错键名不会有任何提示**，网关会带
  默认值启动（默认 `DATABASE_URL` 指向 `root:root@127.0.0.1`，表现为连库失败而非
  配置报错）。
- **管理面 fail-closed**：`/ops/*` 与 `/admin/*` 共用 `X-Admin-Token`（`ADMIN_TOKEN`），
  **未配置密钥时整个管理面返回 404**（不是 401，也不依赖内网隔离）——忘配密钥不等于
  裸奔，见 `app/deps/admin.py`。
- **通配路由必须最后注册**：`/batch/{path:path}` 是唯一可变路径路由，Starlette 按注册
  顺序首匹配——排在字面前缀路由（`/healthz/*`、`/ops/*`、`/admin/*`）之前会把它们整片
  吞掉且不报错。由 `tests/test_static_gates.py` 机械保证（见 `app/main.py` 装配顺序）。

## 红线

- 禁止跨服务读库（网关只读写 new-api `tasks` 表，零建表职责）
- 本仓库不含 `deploy/`：反向代理由宝塔面板代管，不要在网关仓里新增反代配置
- 第三方密钥结构只允许从 `.env.example` 推断（本仓库无 `config/` 目录；密钥只走环境变量）
- 用户令牌不落 tasks 表、不进日志（令牌会话只放 Redis，终态即清）；网关**不持有上游 key**
- 网关零资金动作：不许新增 freeze / settle / cancel 之类的计费调用
- 真实发请求会触发真实计费，**不假定任何组合免费**：`scripts/bench_submit.py`
  默认 dry-run，真发必须显式 `--execute`，且除非再加 `--yes` 会在终端二次确认
