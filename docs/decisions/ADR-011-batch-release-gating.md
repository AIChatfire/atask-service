# ADR-011: 攒批放行——把「上游提交」从受理时刻解耦

**Status**: Accepted (2026-09-13)
**Related**: 本仓库 ADR-010（受理与提交链路、失败三档、单一终态收口点）、
本仓库 ADR-001（复用 `tasks` 表）、本仓库 ADR-004（共享表时间列）
**跨仓库参照**: stask-service 的 `app/services/batching.py` / `app/services/dispatch.py`
（同构移植，但「放行的内容」不同，见 §1）

## Background

stask-service 已有「攒批」：受理不立刻执行，而是攒够 N 条或等够 T 秒再整批放行
（`batching.py` 的两触发器一放行点）。本仓库（atask-service）此前完全没有这项能力——
受理即投递上游提交，突发提交会原样打到上游。

需要它的三个真实场景：

1. **削掉提交突发**。客户端一次提交几百条时，网关会瞬间向上游打出几百个 `POST`；
   上游 new-api 的渠道并发/速率限制会被瞬时打满，表现为大量 429 与重试放大。
2. **成组推进**。同一批任务的放行时刻接近，客户端看到的是「一起开始、一起结束」，
   而不是被上游窗口随机切成前后两波。
3. **给上游留出调度余地**。等待窗口内网关可以按 N 切成波次，波次节奏由提交速率
   自然形成，不需要引入令牌桶之类的主动限速器。

## Decision

### 1. 攒批改变的是「上游提交」的时机，不是执行，更不是合并请求

stask 的「放行」= 交出**执行**（它执行的是同步上游调用）；本仓库的「放行」= 投递
**上游提交**（`queue.publish_queue_submit` → `relayflow.submit_queue_task`）。两者
语义同构，落点不同。

**明确不做「把 N 条合并成一次上游请求」**：上游是 new-api 约定式异步接口
（`POST {base}{path}` 返回任务 id，再逐个探测），没有批量端点。stask 同样不做
（其 `ARCH-scheduling-and-concurrency.md` §7 已确认）。攒批只改变**什么时候**发起
提交，不改变请求的数量与形态。

### 2. 两个触发器，一个放行点

    N 触发：成员数达到 batch_size  → queue.batch_release_task(key, "size")
    T 触发：本批 deadline 到期     → queue.batch_release_task(key, "due")
    兜底  ：sweep 扫超期未放行     → relayflow.release_batched_task(task_id, "sweep")
    退避  ：占槽失败重排到点       → relayflow.release_batched_task(task_id, "requeue")

前三条走 `batching.release(key)`，它用 `LUA_BATCH_CLAIM` **原子摘取**整批成员
（DEL 成员键 + ZREM 到期键在同一次 EVAL 内），所以 N 与 T 并发时只有一方拿得到成员
列表——**不需要额外的放行锁**。

- **N 触发只投递、不同步放行**：一批几百条若在提交响应里同步放行，响应时间会被拉成
  秒级，而投递失败本该由 T 与兜底接住。投递失败只告警，不上抛（批次已在 Redis 里）。
- **T 触发用 taskiq 的 `schedule_by_time`，不用 cron 自旋**。stask 的实现是「每分钟
  cron + 函数内自旋 4×15s」，那是因为它当时只有 cron。本仓库已有
  `RedisScheduleSource`（`app/queue.py` 的失败重试就在用），直接排一个延迟任务更精确、
  更省事。
- **deadline 只由首个成员写定**（`ZADD NX`），后续成员不刷新：T 是「自本批开始攒起」
  的窗口，若后续成员都刷新，涓涓细流会让批次永远等不到放行。

### 3. 等待期不占并发槽——这是唯一改变对外语义的一条

并发槽的占用点从**受理**搬到**放行**（`relayflow.release_batched_task` 里
`ratelimit.conc_try_acquire`）。

为什么必须搬：不搬的话等待期也被算进并发额度，一批还没放行就把自己的槽耗光，
**批次永远不可能大于并发上限**——而「一批 N 条 > 在途上限」正是攒批存在的理由。

代价（必须让调用方知道）：

- 攒批路径**不再因并发满而 429**，改为排队。客户端如果依赖 429 做退避，要改。
- 上限只由**限流**（`RATE_LIMIT_PER_MINUTE`）与**批次窗口**约束。这正是 stask 的
  取舍：客户端要的是「帮我排队」，把一个已经返回 202 之后的失败推回给客户端没有接收方。
- 占不到槽 → **指数退避 + 抖动重排**（`batching.requeue`），不是失败。抖动是必需的：
  一批 200 条同时占不到槽时，固定延迟会让它们下一轮又同时涌向同一个满的闸门。

### 4. 复用 `SUBMITTED` 状态，不新增状态

atask 的 `SUBMITTED` 本来就是「已受理、未提交上游」，攒批等待期恰好就是它。批次状态
全部落在 `data.batch_state`（与状态列正交，照 stask 的做法）：

| `batch_state` | 含义 | 是否占着并发槽 |
|---|---|---|
| 缺键（老行）/ 未写 | 收到即提交（立即路径） | 是 |
| `waiting` | 在批次里等 N/T | 否 |
| `releasing` | 已抢到放行权，正在占槽/投递 | 否（此刻掩码仍为 0） |
| `released` | 已放行，提交已投递 | 是（`slot_flags=1`） |
| `requeued` | 放行时占不到槽，退避重排中 | 否 |

对外只暴露 `waiting` / `released`（`batching.public_state`）。`requeued` **必须**
映射成 `released` 才能漏出去：漏出去会让「靠 `batch_state` 判断是否在排队」的客户端
把「占不到槽、正在退避」误判成在批次里等 N/T，于是去等一个永远不会到达的批次事件。

### 5. 分批维度：全局配置 + 客户端头，不做 per-model 策略表

- 归组键（谁和谁算同一批）= `X-Batch-Key`（显式优先）> `BATCH_GROUP_BY`
  （`model` 默认跨 token 合并 / `token_model` 与并发维度对齐）。归组键**直接拼进
  Redis 键名**（`atask:batch:{key}`），所以过 `sanitize_key` 白名单（非法字符换 `_`、
  截 64），显式键也要过——这个不变量由 `group_key` 自己保证，不靠调用方记得归一。
- 参数来源：`BATCH_SIZE` / `BATCH_WAIT_SECONDS`（热改）+ 客户端
  `X-Batch-Size` / `X-Batch-Wait` **逐字段显式优先**。
- 客户端只给 N 不给 T → 必须用 `MAX_BATCH_WAIT_SECONDS` 兜底：不给兜底会让
  `due_at = now`（整批立刻到期）= 攒批静默失效，而任务还占着等待态。
- **不做 stask 的 `model_policies` 按模型策略表**：本仓库没有那套基建（渠道元数据已随
  ADR-010 退场），且与「零渠道配置」的既定立场一致。真要按模型差异化，先加
  `BATCH_*` 之外的独立决策，而不是复刻一张策略表。

### 6. 两层幂等缺一不可（丢了就是真金损失）

- **批次级**：`LUA_BATCH_CLAIM` 的摘取即互斥——整批只会被摘走一次；
- **成员级**：`taskstore.claim_for_release` 条件更新
  （`status='SUBMITTED' AND data.batch_state IN ('waiting','requeued')` → `'releasing'`）。

第一层救不了「同一成员被两条路径各捞到一次」：两条放行路径会**先各自读到 `waiting`，
再去抢权**，那是一个 TOCTOU 窗口，只有条件更新的影响行数能挡住它。少了第二层，上游
会被调两次，而上游 relay 照扣两次配额，网关零资金动作、无从补救。

`claim_for_release` **只动 `data`，绝不动状态列**：状态列承载终态不可逆（取消/判死
都在抢它），放行权是另一件正交的事——旧链路 KI-D 修过「裸改状态列把 CANCELED 复活成
QUEUED」的事故，不要在放行路径上重犯。

### 7. 还并发槽必须「谁把掩码置零，谁去 DECR」

`taskstore.claim_slot_release`：把 `data.slot_flags` 从 >0 **原子置 0**，
`rowcount==1` 才算抢到还槽权，然后才 `conc_release`。

三条路径都可能来还：终态收口、取消、放行后复核发现「放行途中已被取消」。它们会真并发
——例如放行刚落下 `slot_flags=1`，取消侧读到它并去还槽，放行侧复核也读到「已取消」并
去还槽：**两次 DECR 会把别人的槽还掉**，而 `LUA_CONC_RELEASE` 只钳 0、发现不了（症状
是该 token 的并发额度永久变多，且没有任何日志）。把判定与置零放进同一条 UPDATE，这件事
才变成恰好一次。

**缺键一律视为「已占槽」**（`COALESCE(..., 1)`）：本特性上线前创建的在途任务没有
`slot_flags` 键，而受理时占槽是当时的唯一路径——把缺键当「没占过」会让这批在途任务终态
时不还槽，槽位一直漏到 TTL（48h）。

### 8. 提交入口唯一：等待态任务不得被直接提交

`submit_queue_task` 开头对 `batch_state IN ('waiting','requeued')` 直接短路。
`/ops/requeue`、DLQ 重放、任何补投都会经过这个函数；等待期任务的并发槽还没占，
它的唯一入口是 `release_batched_task`。少了这道闸门，**任何一次人工补单都能绕过并发闸门**。

放行路径也有一道对称的复核：落完掩码后重读一次状态，若已非活跃（被取消了），
把刚占的槽还回去并**绝不投递**——否则会为一条用户已经不要的任务调上游，钱花在
用户不要的结果上。

### 9. 取消必须退批

取消时 `batching.leave`：一批声明 N=100 而其中 5 条被取消，计数就永远差 5 条到不了 N，
只能干等 T 兜底——等待时长凭空变长，而客户端看不出原因。批次空了顺手清到期索引
（`LUA_BATCH_LEAVE` 内完成），否则每轮都会捞到一个空批次并触发一次无成员的放行。

### 10. 兜底只依赖 DB 事实源（Redis 只放可重建索引）

放行只由 taskiq 延迟任务驱动，而延迟任务由 Redis 承载：它丢了、或者放行抢到权之后进程
崩了，这些任务在 DB 里仍是「等待放行」，**没有任何别的东西会推动它们**——sweep 的探测
通道要求 `upstream_task_id` 非空，天然不碰它们。

于是 `taskstore.stale_batch_waiting` + `relayflow._rescue_overdue_batches` 补上第二条
通道（挂在既有的每分钟 `queue_sweep_task` 上）。两类候选、**两个不同的判据**：

- `waiting` / `requeued`：判 `data.batch_due_at <= now - 宽限`（它已经该被放行了）；
- `releasing`：判 `updated_at <= now - 宽限`（**不能用 `batch_due_at`**——放行正是由
  「到期」触发的，它的 due 必然已是过去时刻，拿它判会把**正在飞的放行**也捞出来并退回
  等待态，破坏成员级互斥）。

`batch_due_at` 是「本任务的**下一次**可放行时刻」：批次窗口到期由首个成员写定，退避重排
时改写为新的重试时刻。一个字段同时覆盖「批次到期」与「退避到点」，所以它必须落库
（`_join_batch` 里落的是 Redis 回读的**真实 ZSCORE**，不是本地算出的值——`ZADD NX` 命中
时后者偏晚，重建会让整批放行时刻集体后移）。

`batch_enabled=false` **只辖「入批」一件事**：已经在批里等着的任务不会被它丢下，
仍由 T 触发与兜底放行。止血开关的语义必须是精确的。

## Consequences

- 攒批默认**关闭**（`BATCH_SIZE=0` = 收到即提交），因此开箱行为与本特性之前逐字节一致；
  启用方式是配 `BATCH_SIZE>=2`，或由客户端用 `X-Batch-Size` 声明（仍受 `BATCH_ENABLED`
  总闸门管辖）。
- 受理响应在攒批时多回报 `batch_key` / `batch_state` / `batch_size` / `batch_wait`
  （照 stask 的 R-20 口径）；非攒批路径的响应体不变。
- 新增观测：`GET /ops/batches`（管理面）。**刻意不放用户面**——归组键在
  `BATCH_GROUP_BY=token_model` 或客户端自定义 `X-Batch-Key` 时含 token 指纹。
- 幂等重放**不重放批次信息**：重放不改动任何调度/批次状态，批次参数由重放请求自己查库
  可见，不按新请求的头重算。
- 放行延迟实测值**本仓库暂无数据**（`docs/perf-regression.md` 的纪律：不编数字）。
  stask 的实测参考是「N 触发 1~7s、T 触发 ≈ `batch_wait` + 一个调度相位」，本仓库的
  T 精度取决于 `--update-interval`（见已知限制）。

## 已知限制

1. **T 触发的精度取决于 scheduler 的轮询间隔**。taskiq 0.11 的 `run_scheduler_loop`
   默认按分钟对点唤醒（`next_run = now + 1min`），不设 `--update-interval` 时
   `batch_wait` 的实际放行最坏晚约 60s。三个部署形态都要带上它：
   `Makefile` 的 `make scheduler`、`docker-compose.yml` 的 worker 命令、
   `app/standalone.py` 的 `run_scheduler_task(interval=...)`。
   **正确性不依赖它**（投递丢失或迟到都由 §10 的兜底接住），它只影响准点程度。
2. **Redis 整体丢数据时最坏多等一个 sweep 周期 + 宽限期**（默认 120s + 最多 60s）。
   这是「Redis 只放可重建索引」的必然代价，stask 同样存在。
3. **归组键含 token 指纹时不能放用户面**（见 Consequences 的 `/ops/batches`）。
4. **不做 per-model 策略**（见 §5）：需要按模型差异化分批参数时，得新增独立的决策。
5. **退避重排复用 `batch_due_at` 字段**表达「下一次可放行时刻」。语义上它是「本批的
   deadline」，对已退避的单条任务则是「我这条的重试时刻」——一个字段两种粒度，排障时
   别把单条的重试时刻当成整批的窗口。

## Related

- [ADR-010](ADR-010-queue-path-zero-billing.md) —— 受理/提交链路、失败三档、单一终态收口点；
  本 ADR 是它的**增量**，不取代它。
- [ADR-001](ADR-001-reuse-newapi-tasks-table.md) —— 批次状态落在 `data` JSON 列（零建表）。
- [ADR-004](ADR-004-time-columns-normalized.md) —— 兜底查询的时间比较套 `_secs()`。
- `docs/ARCH-queue-relay-lifecycle.md` §12.1 —— 攒批在并发模型中的位置。
- `docs/SPEC.md` §5.5 / §6 —— 分批头与 `data.batch_*` 字段的契约。
- 跨仓库：stask-service 的 `app/services/batching.py` 是同一套语义的另一个实现。
