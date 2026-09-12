# 性能回归与验收口径（atask-service）

> 建立日期：2026-09-12（随本仓库 ADR-010 换向重写）
> 结论状态：**尚无基线数据；本文是口径与复现方法清单，不是成绩单**

## 0. 先读这条：本文不含任何实测性能数字

atask-service **当前没有任何经真实环境采集的性能数字**。本文因而**故意不写** QPS / P50 /
P99 / 吞吐 / CPU 占用的具体数值——写出没有测过的数字，比留空危害大得多：它会让后来者误以为
「已经测过、结果如此」，从而跳过测量直接引用。

**铁律**：

- 任何性能结论必须现场采集，连同原始输出、代码版本（`git rev-parse HEAD`）、运行环境一并
  归档；本文不提供可被引用的成绩。
- 本文只回答四件事：**要测哪些路径**（§1）、**有哪些容量约束**（§2）、**怎么复现**（§3）、
  **没有基线时怎么判断退化**（§5）。
- 文中出现的配置默认值（如 `DB_POOL_SIZE=20`）是**代码里写死的初始值**，不是实测容量——
  引用它们时必须连同「这是默认配置、不是压测结果」一并说明。

参照体例：stask-service 的 `docs/perf-regression.md`（其结论同样是「当前没有可采信的收益
数字」）。

## 1. 待测的关键路径与性能敏感点

现行对外形态只有 `/batch/{上游原生路径}`（本仓库 ADR-010）。要测的路径与敏感点如下：

| # | 路径（代码入口） | 敏感点 | 期望口径（定性，不是数字） |
|---|---|---|---|
| 1 | 受理：`POST /batch/{path}`（`app/routers/batch_task.py` → `relayflow.create_batch_task`） | 限流（Redis Lua）+ 幂等占位 + 并发占槽 + 幂等回填，随后落库即返回 | **请求内零上游往返**；延迟只由本地 Redis + MySQL 写决定；这是限流/幂等/并发真正的竞争面 |
| 2 | worker 提交：`app/queue.py::batch_submit_task` → `relayflow.submit_batch_task` | 上游 RTT 主导；受 `RELAY_TIMEOUT_SECONDS`、队列积压、`QUEUE_MAX_ASYNC_TASKS` 影响 | 不在用户同步路径内；关注单 worker 吞吐与上游连接占用 |
| 3 | 视图查询：`GET /batch/{path}/{task_id}` → `relayflow.view_batch_task` | **非终态**缓冲转发 `await` 上游至 `RELAY_TIMEOUT_SECONDS`；**终态零上游往返**（快照回放） | 终态查询应与本地读 DB 同级；非终态受上游 RTT 与 gunicorn `timeout` 约束 |
| 4 | 免费透传：`GET /batch/{path}`（末段非本地 id）→ `relayflow.free_batch_get` | 按 **IP** 限流；原样转发上游（可能是图片/二进制大包）；长时间占用 worker | 关注 worker 被长转发占用的时长与响应体大小，而非 CPU |
| 5 | 后台收敛：`app/queue.py::batch_sweep_task` → `relayflow.sweep_batch_once` | 每分钟一轮、独立重入锁、最旧优先；每轮串行探测至多 `BATCH_SWEEP_BATCH` 条 | 关注积压能否在合理轮数内收敛，而非吞吐峰值 |
| 6 | 用户回调：`app/queue.py::notify_task` → `notify.push` | 出站 POST（timeout 15s），至少一次投递、失败退避重投 | 关注投递成功率与重投次数，不并入受理延迟 |

## 2. 已知容量约束与推导关系

下面这些数字都由「与 new-api 共享 MySQL 实例」这一约束推导，**改之前先读
`gunicorn.conf.py` 文件头**，不要把它们当可随意调的旋钮。

### 2.1 DB 连接预算：gunicorn worker 数与队列并发共享同一份预算

- 单 worker 连接池上限 = `DB_POOL_SIZE + DB_MAX_OVERFLOW`（默认 20 + 10；`app/config.py` 与
  `gunicorn.conf.py` 的 `_DB_PER_WORKER` 同字段，改一处要同步另一处）。
- 实例预算 = `DB_MAX_CONNECTIONS`（默认 200，为 new-api 自身与管理连接留量，
  `gunicorn.conf.py` 的 `_DB_BUDGET`）；web 可占比例 = `DB_WEB_SHARE`（默认 0.6）。
- 反推 worker 数：`workers = max(2, min(_BY_CPU, _BY_BUDGET))`，其中
  `_BY_BUDGET = DB_MAX_CONNECTIONS × DB_WEB_SHARE ÷ (DB_POOL_SIZE + DB_MAX_OVERFLOW)`；
  **CPU 核数只是上限，不是依据**（`_BY_CPU = min(16, cpu_count × 2 + 1)`）。
- 队列侧并发上限是 `QUEUE_MAX_ASYNC_TASKS`（默认 10240）。worker 进程同样各持连接池——它与
  web worker **共享同一个 `DB_MAX_CONNECTIONS` 预算**。因此「把 worker 并发或队列并发调大」
  与「把 gunicorn worker 数调大」在 DB 连接上是同一笔账，必须一起算。
- 注意：`DB_MAX_CONNECTIONS` / `DB_WEB_SHARE` 是 `gunicorn.conf.py` 直读的 env（**不是**
  Settings 字段）；`DB_POOL_SIZE` / `DB_MAX_OVERFLOW` 同时是 Settings 字段，两处必须一致。

### 2.2 上游出站共享连接池

- 中继出站统一走 `app/services/httpc.py::shared_client`（进程级缓存，key = 构造参数组合）。
  `app/services/relay.py::call_upstream` 只传 `timeout`，因此在 `RELAY_TIMEOUT_SECONDS` 恒定
  时**全进程共用一个上游连接池**——这比旧链路「按渠道组合各建池」显著收敛。
- **本仓库已没有 `UPSTREAM_MAX_*` 之类的连接池配置**（随旧链路删除）。连接上限取 httpx
  库默认（httpx 0.28.1 实测 `DEFAULT_LIMITS = Limits(max_connections=100,
  max_keepalive_connections=20, keepalive_expiry=5.0)`）——这是**库默认、不是本项目的容量
  结论**；需要更高并发时要么显式传 `limits=`，要么承认库默认就是天花板。
- 连接池随进程退出由 `httpc.close_all()`（lifespan）统一释放；评估容量时按
  「web worker 数 + worker 副本数 + standalone 进程数」放大。

### 2.3 队列观测快照缓存

- `QUEUE_STATS_CACHE_SECONDS`（默认 55）：`GET /ops/queue` 与 `GET /admin/api/overview` 读的
  是缓存快照，用于把「全库 scan + 全表 GROUP BY」降频。压测期间读队列观测值时，注意这是最多
  滞后一个缓存周期的快照，不要当作实时值。

### 2.4 `/batch` 后台收敛：每轮上限与重入锁

- `BATCH_SWEEP_BATCH`（默认 50）：每轮收敛的处理上限；cron 周期固定每分钟。
  积压追赶速度 ≈ `BATCH_SWEEP_BATCH ÷ 1 分钟`；把队列打满后要观察 sweep 能否在合理轮数内
  收敛，而不是只看吞吐峰值。
- `TASK_STALE_SECONDS`（默认 300）：任务多久未被推进才进入收敛候选——它决定「客户端停止
  轮询后多久开始兜底」。
- `BATCH_SWEEP_LOCK_TTL_SECONDS`（默认 300）：收敛重入锁 TTL。一轮可能串行探测
  `BATCH_SWEEP_BATCH` 条 × 单条最长 `RELAY_TIMEOUT_SECONDS`，最坏会超过 1 分钟 cron；
  锁保证慢轮不叠加并发轮（多副本同理）。**该 TTL 必须 ≥ 一轮最坏耗时**，否则锁会在慢轮
  中途过期、第二轮叠加进来。

### 2.5 出站超时与 gunicorn `timeout` 的关系

- `RELAY_TIMEOUT_SECONDS`（默认 60.0）：提交/探测/取消共用的全局出站超时（旧链路的渠道级
  `timeout_sec` 已随本仓库 ADR-010 放弃）。
- 非终态视图与免费透传都是**缓冲转发**，会 `await` 上游到 `RELAY_TIMEOUT_SECONDS`；因此
  gunicorn `timeout`（由 `GUNICORN_REQ_MAX_SECONDS` 默认 60 推导，下限 180s）必须大于最长
  合法转发的实际耗时——`timeout` 小于它就等于把正常请求当成卡死 worker 杀掉。
  **不变式：`RELAY_TIMEOUT_SECONDS` 必须落在 gunicorn `timeout` 之内**，调大前者必须同步
  检查后者。

### 2.6 并发槽、限流与令牌会话 TTL（与吞吐无关，与正确性有关）

这些值不决定吞吐，但压测时会被误判成「容量不够」，先说清：

- `MAX_CONCURRENT_TASKS`（默认 5）：**每用户**并发任务上限（按 `token_hash`），运营可经
  `dynconf` 在线调；压测若用同一个 token 打，会把 429 读成容量瓶颈——那是并发阈值，不是
  服务容量。
- `CONC_TTL_SECONDS`（默认 172800）：并发槽键 TTL 兜底，必须 > 最长在途任务时长。
- `RATE_LIMIT_PER_MINUTE`（默认 60）：滑动窗口限流；计费接口按 `token_hash`，免费透传按 IP。
- `SK_SESSION_TTL_SECONDS`（默认 172800）：用户令牌会话 TTL；会话过期后任务无法探测
  （本仓库 ADR-010 已知限制 ①）。

## 3. 复现方法：`scripts/bench_submit.py`

```bash
# 默认 dry-run：只探活 + 打印计划，不发任何请求
.venv/bin/python scripts/bench_submit.py --token sk-xxx

# 经 Makefile（等价）：
make bench TOKEN=sk-xxx BIZ=example MODEL=your-model
```

- **默认 dry-run**：不加 `--execute` **只打印计划**（将提交多少次、body 长什么样、目标 URL），
  零请求、零计费；探活也在 `--execute` 之后才发生。
- 真打流量必须显式 `--execute`；除非再加 `--yes`，否则会先在终端二次确认。
- 分位数用线性插值（与 numpy.percentile 默认口径一致），不引第三方依赖。
- 已知局限：该脚本只报告单批次的 p50/p90/p99，没有预热、多轮离散度、双侧 CPU 采样——
  **不能单独作为回归验收依据**（口径见 §5，方法可借鉴 stask 的 `scripts/bench_architecture.py`）。

**必须先修的事实（否则测的不是本架构）**：该脚本当前仍把目标 URL 拼成
`{base_url}/{biz}/v1/tasks`、取消示例也指向旧形态——**旧形态在本架构里已不存在**（本仓库
ADR-010）。要压新链路，须先把脚本目标改为 `{base_url}/batch/{上游原生路径}`（并把
`--biz` 语义改为「上游原生路径」），否则请求只会 404，测到的是错误路径的延迟。
`scripts/bench_submit.py` 不属本文件的改动范围，故在此登记为**待修项**。

## 4. 安全红线（先读这段再压测）

1. **真实提交会触发上游真实计费**。每一次成功的创建请求都会在上游 relay 产生一次真实的
   任务与配额扣减；压测不是「空跑」。
2. **不要假定任何组合免费**：真实发请求会触发真实计费；**执行前必须向 provider 侧确认
   当前哪个组合免费**，不要凭本文一句话直接开跑。
3. **必须二次确认**：脚本要求 `--execute`，且除非 `--yes` 会在终端等待输入 `yes`。自动化
   场景**不要**图省事加 `--yes` 跳过人工确认。
4. **不要在共享资源上做破坏性动作**：MySQL 是与 new-api 共用的同一实例，打爆连接会级联
   拖垮 new-api（跨服务故障）。禁止在生产数据上 `FLUSHDB`；压测前清理自己的幂等键与并发槽，
   避免历史任务把槽位耗尽。
5. 压测机上不要同时跑重型任务；记录 Python / 依赖版本、启动方式、worker 数、CPU/内存限制，
   敏感值只记「是否配置」，不记 token / 密码 / 连接串。

## 5. 回归判定方法（在没有基线的前提下）

没有基线时，**先建基线，再谈回归**。步骤如下：

1. **采基线**：固定硬件、固定并发、固定配置，跑 N 轮（建议 ≥ 5），每轮记录中位数与离散度。
   多轮相对离散度超过约 20% 时信号不足，先降噪再测。
2. **A/B 单变量**：一次只改一项配置/一处代码，前后用完全相同的请求体、并发、配额、超时、
   连接池、上游基址与白名单。
3. **判定**：同一指标前后中位数之差，**只有超过轮间离散度**才算退化或提升；达不到就判
   「需重测」，不得宣称收益。
4. **容量结论的边界**：任何结论都必须限定「在记录的环境/配置/负载下」；开发机 / 共享 CI
   的数字**不得**外推为生产容量。

### 观测点与查看位置

| 观测点 | 看什么 | 在哪里看 |
|---|---|---|
| 受理同步段耗时 | p50 / p90 / p99（脚本自带，线性插值） | `scripts/bench_submit.py` 输出；或 logfire trace 面板按 span 名过滤 |
| worker 提交 / 探测耗时 | 单次 span 耗时、上游 RTT | logfire trace 面板（service_name：`async-gateway` / `atask-worker` / `atask-standalone`）；容器日志（`LOG_LEVEL=DEBUG` 可看全链路） |
| 队列深度 / 积压 | pending / delayed / dlq / 任务状态分布 | `GET /ops/queue`（`X-Admin-Token`；注意 §2.3 缓存滞后）与 `GET /admin/api/overview` |
| worker 饱和 | 提交/探测是否排队、单 worker 吞吐 | taskiq-admin 看板（compose 内 `127.0.0.1:3000`）；容器日志 |
| DB 连接占用 | 实际连接数 vs §2.1 推导预算 | 实例 `SHOW STATUS LIKE 'Threads_connected'`；`gunicorn.conf.py::on_starting` 启动日志打印的预算与生效 worker 数 |
| sweep 收敛 | 积压是否在合理轮数内回落；锁是否被长期持有 | 容器日志（sweep DEBUG/INFO 行）、`GET /ops/queue`、Redis `gw:batch_sweep_lock` |
| 回调投递 | 投递成功/失败、重投次数、死信 | 容器日志、`GET /ops/queue` 的 `dlq`、logfire `taskiq_task_failed` 事件 |
| 双侧 CPU | 客户端 CPU vs 服务端 CPU | `--server-pid` 类采样 + `docker stats --no-stream` 快照 |

判据补充：**没有双侧 CPU 数据时，不得宣称服务端饱和或容量提升**——高客户端 CPU、低服务端
CPU 时的吞吐数字是客户端瓶颈，不能当服务容量结论（完整归因表见 stask-service
`docs/perf-regression.md` §3）。

## 6. 当前结论

```text
数字可信度: 无数字，尚无任何真实环境采集
基线有效性: 无优化前基线，需先按 §5 采集
口径确认: 已定义待测路径（§1）、容量约束（§2）、复现脚本（§3）
复现前提: scripts/bench_submit.py 仍指向旧形态路径，须先改为 /batch/{原生路径}（§3）
环境资格: 开发机 / 共享 CI 只能做同环境 A/B，无生产容量资格
安全前提: 不假定任何组合免费，须先向 provider 确认当前免费组合
瓶颈归属: 未知；尚无双侧 CPU 采样
```

正式验收报告必须附每轮原始输出、代码版本（`git rev-parse HEAD`）、环境声明与资源快照，
而不是只留一个汇总百分比。
