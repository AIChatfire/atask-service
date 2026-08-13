# atask-service 优化报告

> 日期：2026-08-12 · 基线 commit：`c9f46e3` → 交付 HEAD：`09bc6da`
> 仓库：`/mnt/agents/output/atask-service`（分支 main）

## 一、交付总览

| 指标 | 优化前 | 优化后 |
|---|---|---|
| 测试收集 | 20 个测试文件中 **16 个收集失败** | 26 个测试文件全部收集成功 |
| 测试结果 | 42 passed / 1 failed / 9 errors | **324 passed / 0 failed** |
| ruff | 未验 | 全绿（E/F/W/I/UP/B/ASYNC/RUF） |
| mypy | 未验 | 37 个源文件零告警 |
| 应用装配 | `from app.main import app` 崩溃 | 可装配，12 条路由；worker 入口可导入 |
| 代码规模 | 90 个 py 文件（含 legacy） | 37 个 app 模块，8073 行（净删约 2300 行 legacy） |

## 二、根因诊断

仓库处于「两代代码并存」的中间态：

- **新代（SPEC 契约目标）**：`app/adapters/`、`app/tasks/`、`app/billing/`、`app/callbacks/`、`app/routing/` 等，`docs/SPEC.md` 与全部测试针对此代；
- **旧代（legacy）**：`app/services/`、`app/routers/`、`app/deps/`、`app/queue.py`、`app/redis.py` 及旧的 `main.py/config.py/db.py`——SPEC 项目树中不存在，却仍占据着骨架文件。

后果：新代模块引用的 36 个 Settings 字段（`biz_l1_ttl_seconds`、`delivery_backoff_seconds` 等）在 config.py 中不存在；`db.py` 缺少 SPEC 约定的 `get_session_factory()` 等接口——测试大面积在收集阶段即崩溃，应用无法启动。

## 三、优化执行（三个阶段）

### 阶段 1 · 测试项目（骨架对齐，commit `0dbf7d5` → merge `4528d69`）

以 `docs/SPEC.md` 为单一事实源重建骨架，先让测试体系可运行：

1. **重写 `app/config.py`**：43 个 Settings 字段与 SPEC §3.1/§3.7 一一对应；删除仅 legacy 使用的旧字段；退避阶梯字段支持逗号分隔解析。
2. **重写 `app/db.py`**：惰性引擎单例（post-fork 安全）+ `get_session_factory()`/`get_session()`（异常自动 rollback）+ `close_db()`；零建表职责。
3. **重写 `app/schemas.py`**：恢复为 SPEC 约定的对外 videos 形态模型（原文件被 legacy 的 RouteConfig/Quote 等内容占据）。
4. **重写 `app/main.py`**：`create_app()` 工厂 + 三个异常处理器 + SPEC §4.2 路由注册顺序（healthz → callbacks → videos → catch-all 永远最后）。
5. **重写 `.env.example`**：与 Settings 字段一一对应（原文件是失效的 `GW_` 前缀旧变量）。
6. 修复 `gunicorn.conf.py`、`Dockerfile`（依赖唯一钉版源为 pyproject.toml）。

结果：**301 collected / 301 passed**，测试基线从零可用变为全绿。

### 阶段 2 · 根据测试优化

骨架对齐后全量测试即通过，无遗留失败项。本阶段的「测试驱动优化」体现于阶段 3 的**测试先行**：每个架构修复均先写失败测试锚定，再实施修复（新增 23 个测试用例，含一个静态 SQL 方言守卫测试文件）。

### 阶段 3 · 按架构业务逻辑解耦优化

#### 3.1 业务逻辑解耦：legacy 整体清除（同 merge `4528d69`）

删除 SPEC 项目树之外的整个旧代：`app/services/`（14 文件）、`app/routers/`（6）、`app/deps/`（4）、`app/queue.py`、`app/redis.py`、`app/models.py`、`gateway-routes.yaml`、`docker-compose.yaml`、`requirements.txt`（共 −2300 行）。删除前逐一 grep 确认新代无引用。从此每个职责只有单一实现：

- Redis 连接：`redis_client.py` 唯一单例（原 `redis.py`/`redis_client.py` 双份）
- 队列原语：`redis_queue.py`（dlv/obx ZSET+HASH+Lua）唯一（原另有 `queue.py`）
- 路由层：`routing/` 唯一（原另有 `routers/`）
- 外部服务收口：`http_clients.py` 五单例 + `billing/client.py` + `pricing.py` + `keys.py`

#### 3.2 架构审计驱动的修复（merge `a8055ba` + `1969fc4`，测试先行）

独立架构审计产出 P0×1 / P1×6 / P2×12，全部修复并回归：

**P0 — HTTP 层异常逃出错误形制（§4.4）**
- 上游异常（`UpstreamError`/429/httpx 超时）原会冒泡为 Starlette 默认 500 纯文本；`RequestValidationError` 输出 `{"detail": [...]}`。
- 修复：`app/errors.py::register_exception_handlers()` 单一注册点——429+Retry-After / 502 / 504 / 422 / 兜底 500 全部归一为 OpenAI 风格 `{"error": {...}}`。

**P1 — 正确性与资金链路**
1. **SQL 护栏失效（9 处）**：`GW_PLATFORM_LIKE` 以 f-string 内嵌进 SQL，MySQL 字面量转义吃掉 `\` 使 `_` 退化为通配符，platform 护栏名存实亡 → 全部改绑定参数 `:gw_like`；新增 `tests/test_sql_dialect.py` AST 静态守卫，永久防回归。
2. **行锁内做沙箱求值**：`transition()` 在 `SELECT ... FOR UPDATE` 持锁期间执行 pricing 沙箱（最长 5s）→ 重构为「无锁预读 → 锁外求值 → 加锁复核 → CAS」，消除与 poller/callback/renewer 三通道的锁竞争。
3. **对账兜底必死信**：对账重入队的 outbox 条目取不到 user_sk（sksess 在终态即删）→ 清除时机移至「计费收敛后」（outbox worker 成功路径），SPEC 已同步契约修订。
4. **kling poll 不分代际**：v3 任务永远打到旧版查询路径 → `SubmitContext` 增 `model` 字段（向后兼容），按代际选路。
5. **remix 漏处理 BillingLockBusy** → 与 submit 同口径转 503。
6. **透传 429 的 Retry-After 被响应头白名单剥掉** → 白名单补 `retry-after`。

**P2 — 加固与卫生**：回调反查补 `platform_for(provider)` 收窄、回调 429 补 Retry-After、透传 request_id 复用键改 `SET NX`（消除并发重复计费窗口）、pricing 求值异常改保守金额 fallback 入 outbox（绝不免费放行）、biz 注册补 native_prefixes 冲突校验钩子、回调 processing 队列补启动恢复（worker 装配）、os.environ 散读收口进 Settings、删除并行开发期遗留的 ImportError 兜底死代码桩、全库注释 drift 清理、SPEC §9 变更记录同步。

#### 3.3 文档与部署对齐（merge `09d6187`）

- **README 全面重写**：架构图（API + `python -m app.worker` 五组件 + dlv/obx 队列原语）、资金闭环（分片 request_id `{task_id}:{seq}` + 三重兜底）、环境变量、运维手册全部对齐真实代码；旧术语集中保留在「迁移说明」节。
- `docker-compose.yml` 修复真实 bug：healthcheck 用的 `curl` 在 `python:3.12-slim` 镜像中不存在（必失败）→ 改标准库 urllib 探活。
- `.dockerignore` 删除陈旧条目。

## 四、质量线与验证

```
pytest：324 passed / 0 failed（26 个测试文件）
ruff check app/ tests/：All checks passed
mypy app/：Success: no issues found in 37 source files
装配：from app.main import app（12 路由）/ import app.worker 均正常
```

## 五、残余风险与后续建议

1. **sksess TTL 窗口**：超过 deadline+1h（约 12.5h）的漏结算任务无法自动取回 user_sk，对账会告警转人工（可观测降级，非静默失败）。如需更长窗口，调大 `SKSESS` TTL 或引入凭据重取通道。
2. **kling v3 轮询**按代码内 docstring 契约实现并有单测锚定，但建议上线前用真实上游报文做一次契约测试。
3. **docker-compose** 健康检查修复未实际构建镜像验证（环境无 docker），但 urllib 方案在 slim 镜像中必然可用。
4. `UPSTREAM_CALLBACK_SECRET_{PROVIDER}` 已登记 SPEC §3.7，仍由 receiver 直读 os.environ，建议后续收口进 Settings。
5. 建议补真实 MySQL/Redis 的集成测试环境（当前单测全 fake，符合 SPEC §7.2，但上线前需要一轮端到端演练，见 README checklist）。

## 六、提交时间线

| commit | 内容 |
|---|---|
| `0dbf7d5` | 骨架对齐 SPEC + 清除 legacy（+520/−2300） |
| `e49c01a` | README 重写 + compose/ignore 漂移修复 |
| `5c92dc1` | 资金链路/SQL 护栏/锁外求值/对账收敛/kling 代际（+9 测试） |
| `430ee44` | HTTP 错误形制归一 + 路由/回调/计费加固（+14 测试） |
| `09bc6da` | SPEC 契约修订同步（sksess/SubmitContext.model/worker_id） |
