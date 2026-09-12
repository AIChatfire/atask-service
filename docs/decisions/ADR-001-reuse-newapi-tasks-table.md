# ADR-001: 复用 new-api `tasks` 表，网关零建表

> **仓库独立声明**：本 ADR 属于 **atask-service** 的决策系列。stask-service
> 有一份**同名同号但内容不同**的 `ADR-001`（那边 `platform='stask'`，本仓库
> `platform='gateway'`）。两套编号各自独立、互不代表，交叉引用时必须带仓库名
> （如「stask-service ADR-001」）。

## Status: Accepted (2026-09-12)

## Background

atask-service 需要持久化「任务事实源」：状态、渠道口径、上游任务 id、
冻结金额、终态结果、对账台账。可选方案有三：

- (a) 自建 `atask_tasks` 表（独立 schema + migration）；
- (b) 复用 new-api 已有的 `tasks` 表（共享实例，按 `platform` 划分行）；
- (c) 只用 Redis（无 MySQL 依赖）。

约束来自产品定位：网关「与 new-api 生态共用用户体系、钱包（`users.quota`）
与渠道配置」（`README.md`）。也就是说身份、资金、渠道都在 new-api 侧，网关
只是这些能力的编排层。再建一张任务表等于把「任务事实源」从 new-api 生态里
拆出去，看板/SQL 巡检/运维习惯全要重做；只用 Redis 则丢持久性，任务在
Redis 故障时不可恢复。

还有一个跨仓库约束：stask-service 的 `ADR-001`（stask 仓库）明确写了
「atask-service 已经在复用 `tasks` 表（`platform='gateway'`），它有一个
sweeper 会扫描终态但未结算的任务做兜底重发」。本决策是这个既成事实的
正式化。

## Decision

**复用 new-api `tasks` 表，`platform = 'gateway'` 划分自有行。零建表、
零 migration。网关自有状态（幂等键、令牌会话、并发槽、熔断、路由缓存、
上游 id 反查索引、sweep 重入锁）**全部放 Redis**（`app/redis.py`），
MySQL 只写 `tasks` 一张表。**

实现单点：`app/services/taskstore.py`（唯一数据访问点，纪律集中在一个文件
里可审）。写入形态见 `taskstore.create`（app/services/taskstore.py:56）。

### 行契约（写侧）

| 列 | 取值 | 为什么 |
|---|---|---|
| `platform` | 恒 `'atask'`（`GATEWAY_PLATFORM`，app/config.py） | 与 new-api 原生任务、stask 行三方隔离；所有读写的 `WHERE` 必带它 |
| `task_id` | `{biz_slug}_{uuid4hex}`（`deps/preflight.new_task_id`） | 本地即权威 id，带 biz 前缀便于识别与分流 |
| `channel_id` | keypool 渠道 id，**必须是 new-api 里真实存在的渠道 id** | new-api 原生任务轮询按 channel_id 分组；渠道不存在会导致批量误判 FAILURE（见下） |
| `user_id` | new-api `users.id`（`/auth/inspect` 内省得到） | 共享表里可读，网关不做 key 管理 |
| `quota` | 恒 `0` | 资金走 billing 冻结/结算，不在 `tasks.quota` 上体现；即便被 new-api 超时清理误动，退款金额也是 0 |
| `status` / `progress` | new-api 原生枚举 + 网关自写态（见 `app/schemas.py:26`） | 共享表对看板/SQL 巡检保持可读 |
| 扩展字段 | 全部塞 `data` JSON 列（`JSON_MERGE_PATCH` 合并），**绝不 ALTER** | 表 schema 由 new-api 掌控，加列会被其 AutoMigrate 波及 |

### 与 new-api 原生任务（suno/mj）的共存

new-api 有任务轮询（`UpdateVideoTasks` 按 platform 找 adaptor）与超时清理
（`sweepTimedOutTasks`）。`platform` 是自定义值（非 `suno`/`mj`）时
`GetTaskAdaptorFunc` 返回 nil，轮询只记日志、任务不动。但**渠道不存在**
会走到更危险的路径（同 stask ADR-006 的核实结论：`CacheGetChannel` 在
adaptor nil 检查之前，渠道不存在会强制 FAILURE）。因此 `channel_id`
必须是真实渠道 id。

### 与 stask 行（`platform='stask'`）的共存

两服务共写一张表，但 `platform` 不同（`'gateway'` vs `'stask'`），
**靠 platform 过滤天然互不可见**：

- 网关所有写入点 `taskstore.cas` / `patch_data` 的 `WHERE` 恒带
  `platform = :p`（taskstore.py:182、:218）；
- 网关 sweeper 的候选取数（`terminal_unsettled`、`stale_active`、
  `orphan_active`、`held_expired`、`reconcile_candidates`）同样恒带
  `platform = 'gateway'`，**看不到 stask 行**。

> **与 stask 文档描述不符之处（已核实）**：stask 仓库的
> `ADR-001` / `SPEC.md` 声称其 `data` 恒写 `freeze_amount: 0` 与
> `settled: true`，让「atask 的 sweeper（按 `settled != 'true'` 找候选）
> 天然跳过 stask 行」。但**当前 stask 代码并不写这两个键**
> （`app/services/submit.py::build_task_data` / `batch_fields` 的 data
> 里没有 `freeze_amount`，也没有 `settled`）。好在实际隔离并不依赖它们：
> 网关 sweeper 已经用 `platform = 'gateway'` 过滤，无论如何都扫不到
> `platform='stask'` 的行。stask 的这两处文档描述是**过期/未落地的
> 契约**，见 `OPEN-DECISIONS.md`。

### 时间列

`tasks` 是共享表，时间列单位可能被其他写入方污染（毫秒）。这是独立决策，
见 **atask-service ADR-004**。本 ADR 只确立「复用这张表」这一条。

## Consequences

- 正面：查询/看板天然与 new-api 打通；无 migration 负担；新建任务不需要
  走 DDL 变更流程；stask-service 的运维经验可直接复用。
- 正面：任务事实源只有一处（MySQL `tasks`），Redis 全丢也能靠 sweeper
  从表里重建队列意图（`reconcile.sweep_once`）。
- 负面：表 schema 由 new-api 掌控，它 AutoMigrate 改列会波及本服务。
  缓解：只依赖 `task_id/platform/status/data/user_id/channel_id` 等稳定列。
- 负面：`data ->> '$.xxx'` 无索引（零建表红线下不能加虚拟列），
  `get_by_upstream_id` 的 SQL 兜底是全表扫描。缓解：Redis 反查索引
  `gw:tidx:*`（taskstore.py:115）、所有扫描带 platform + 状态 + 时间窗
  三重收敛并带 LIMIT。
- 负面：三方共写一张表，任何一方漏加 `platform` 过滤都是生产事故。
  缓解：`taskstore.py` 是唯一数据访问点。
- 负面：`quota=0` 与网关真实冻结金额（在 `data.freeze_amount`）分离，
  读表的人可能误以为网关任务不计费。缓解：`/ops/tasks/{task_id}` 视图
  显式呈现 `freeze_amount`/`settled`。

## Related ADRs

- **atask-service ADR-004**（共享表时间列归一：本表最硬的连带纪律）
- **atask-service ADR-008**（与 stask-service 的分工：同一张表、不同 platform）
- stask-service ADR-001 / ADR-006（另一仓库的共表契约，编号独立）
