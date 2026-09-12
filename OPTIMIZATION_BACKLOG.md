# atask-service 优化 Backlog（已收敛）

> 状态：已归档（2026-09-12）。有效内容已沉淀进 `docs/decisions/`
> （ADR-001 / ADR-003 / ADR-004 / ADR-005 / OPEN-DECISIONS），
> 本文件仅作历史记录保留，不再更新。

> **收敛日期：2026-08-19**。本清单为「提交异步化」改造的执行清单，全部条目
> 已落地验收。过程性内容（链路文字版/收益估算/行号级修改指引）已删除，
> 需要考古见 git 历史。背景一句话：创建请求内同步提交上游（最坏 4 分钟+）
> → 改造为 preflight+落库即返回本地 task_id，上游提交移交 taskiq worker。
> 边界：proxy 原生透传保持同步转发（契约决定），本次只覆盖 tasks/videos 入口。

## 清单核验（全部完成）

- [x] **[1] `app/queue.py`**：`submit_task` 队列任务（延迟 import 防循环、
  异常走 `_retry_or_dlq`）+ `publish_submit(task_id)` 门面 + `_DLQ_TASKS`
  注册（DLQ 类型 `SUBMIT`）
- [x] **[2] `app/services/submit.py`**：worker 侧 `submit_one(task_id)`，
  提交段整体迁入；幂等前置检查（终态/已有 upstream_task_id → return）+
  Redis 互斥锁 `gw:submit_lock:{task_id}`（TTL 300s）防补投/重放双建
- [x] **[3] `create_task` 同步段瘦身**：幂等重放短路 → `conc_acquire` →
  submit_path 校验 → `taskstore.create(SUBMITTED)` → `idem.set_task_id`
  同步写 → `publish_submit` → 返回 `{"task_id", "status": "SUBMITTED"}`
- [x] **[4] 提交段搬走**：重试循环 / HELD 分支 / FAILURE 分支 / task_id
  提取失败分支 / 成功回填 + `keys.report` + `schedule_poll` 全部迁入
  submit.py；flow.py 无残留
- [x] **[5] routers 契约**：202 响应不再带 upstream_task_id；上游拒绝从
  同步 502 改为异步 FAILURE（无调用方依赖同步 502，仅测试已同步改写）
- [x] **[6] `public_view`**：SUBMITTED 对外原样呈现（"已受理、排队待提交"，
  videos 形态小写 `submitted`）；HELD→QUEUED 映射不变
- [x] **[7] `orphan_active` 判死阈值**：`ORPHAN_GRACE_SECONDS`
  **600 → 1800**，覆盖「队列积压 + 提交耗时」窗口（stale 每 300s 补投 +
  提交最坏 60s×3 重打 + 互斥锁 TTL 300s），防队列积压误杀在途任务
- [x] **[8] `sweep_once` SUBMITTED 补投**：stale 且无 upstream_task_id 的
  SUBMITTED 任务重投 `submit_task`（submit_one 幂等 + 互斥锁，重复补投安全）
- [x] **[9] 双重提交窗口兜底**：渠道 `client_request_id_param` 注入 task_id
  供上游幂等反查 + 现有反向对账；窗口语义已写入 AGENTS.md 关键约定
- [x] **[10] tests**：新增 `tests/test_submit.py`（成功/补偿/重打/幂等/互斥/
  异常传播 12 用例）；create 契约断言改异步执行；e2e/held/retry/ops 全量改写
- [x] **[11] 回归**：`pytest tests/ -q` 169 passed + `ruff check app tests` 全绿

## 三个取舍（最终结论）

1. **客户端契约变化——接受**：202 只含本地 task_id（`{biz}_{uuid4hex}`）；
   上游拒绝异步 FAILURE，客户端靠 GET / 用户回调感知 fail_reason。
2. **双重提交窗口——接受**：worker 在"上游已接单、落库前"崩溃时，靠渠道
   `client_request_id_param` 注入 task_id 幂等反查 + 反向对账兜底；孤儿判死
   阈值已配合调至 1800s。
3. **freeze 占用时间变长——接受，无需改**：排队期持冻结由 sweep 续期机制
   （`expiring_freezes` + billing renew）覆盖。

## 遗留观察（Known Issues，不阻塞，择期处置）

- **KI2 提交互斥锁 TTL 与渠道 timeout 耦合——已根治**：锁 TTL 按路由动态
  派生（`submit_max_attempts` × 渠道 `timeout_sec` +
  `SUBMIT_LOCK_BUFFER_SECONDS`，换渠道重打时按新路由刷新，见
  `app/services/submit.py::submit_lock_ttl`），不再硬编码 300s；sweep 补投
  前查 `gw:submit_lock:{task_id}`，锁在则本轮让路（`reconcile.sweep_once`）。
  两道防线叠加，「锁先于在飞提交过期 → 补投并发 → 上游双建」窗口闭合。
- **KI3 幂等键并发窗——已根治**：preflight 改原子占位（SET NX 写
  `pending`，短 TTL `IDEM_PENDING_TTL_SECONDS`，见
  `app/services/idem.py`），「先查后写」变原子；同键并发请求短轮询等占位
  在同一键上回填为 task_id 后回放，超时/过期按 409 冲突（不放行重建，
  资金侧零风险）；创建链路失败 CAS 归还占位（preflight/flow/proxy 三处
  兜底）。并发测试见 `tests/test_idem_concurrency.py`。
- **KI-C timeout_sec 负数配置——已根治**：`RouteConfig.timeout_sec` 加
  `ge=0` 校验（app/schemas.py），负数在路由构建期响亮报错（ValidationError
  → 500），不再带到提交期让锁 TTL 派生/Redis SET ex 才炸。
- **KI-D 提交成功复活终态——守卫已落，残余为配置耦合（文档不变量）**：
  submit 成功回填改 CAS（仅 SUBMITTED/QUEUED → QUEUED），在飞期间被判死
  （孤儿收口/取消）的任务不复活、不覆盖退款事实，上游孤儿单靠
  `client_request_id` 反查对账/人工处理。残余：渠道 `timeout_sec` 极大
  （锁 TTL = 3×timeout+buffer 超 `orphan_grace_seconds` 1800s，即
  timeout >~580s）时孤儿收口可能在在飞提交期间判死——运维不变量：
  `orphan_grace_seconds > submit_max_attempts × max(渠道 timeout_sec)
  + submit_lock_buffer_seconds`，调参时保持。
- **KI-E 锁 TTL 刷新残余窗口（可接受，不阻塞）**：重打换渠道时锁 TTL 用
  SET xx 刷新，锁恰在重打间隙丢失（说明已超最坏窗口）仅告警、当次提交
  继续——残余并发窗由 sweep 补投前查锁 + `client_request_id` 对账兜底。
- **KI-F 幂等占位 TTL 极端窗口（可选续期，不阻塞）**：占位 TTL 30s，
  preflight 超 30s 未完成（billing/keypool 长时间故障）时占位过期，后来
  同键请求可能重建——概率极低且 billing `request_id` 唯一约束兜底资金侧；
  可选方案：占位心跳续期（创建链路按期 EXPIRE 续占位）。
- **观察项（设计不对称，仅记录）**：proxy 透传形态落 tasks 行但不占并发
  槽（`conc_acquire` 只覆盖 tasks/videos 创建链路；透传是同步流式转发，
  占用语义与异步任务不同）。终态 finalize 的 `conc_release` 对未占槽任务
  DECR 由 Lua 钳 0，无负槽风险；如需统一并发限流口径再议。
  *2026-08-20 更新*：命中渠道 `submit_path` 的原生提交已改走
  `flow.create_task`，与 tasks/videos 同口径占槽；不对称只剩「其余方法的
  纯透传」这一类。

## 原生路径拦截（2026-08-20 收敛）

背景：`POST /example/v2/video_generation` 落通配透传 → 同步转发 → 客户端拿到
**上游** task_id + 上游 RTT，与「原生接口同构 + 本地 id + 秒级返回」的目标冲突。

- [x] **[N1] 免费 GET 不再空 model 问 keypool**：`select(group, model)` 对空
  model 必拒 40010——删掉这次必然失败的出站，改「Redis `biz→channel_id`
  记忆（`app/services/routecache.py`，唯一写入点 = preflight 成功租约）→
  进程路由缓存」两级钉回 channel_id 直达租约，全落空才 404。
  **不缓存上游 key**（凭证轮换/禁用治理属 keypool，缓存明文违反红线）。
- [x] **[N2] 原生查询按 path 里的 id 反查任务钉渠道**：`nativeapi.
  path_task_id_candidates` 零成本形态预筛（路径无 id 形态 → 零查库）→ 本地
  id 主键直查 → 唯一候选做一次 `get_by_upstream_id` 兜底；命中即拿
  `channel_id`/`key_id` 直达租约（精确解，跨副本稳）。
- [x] **[N3] 原生提交/查询/取消三条路径拦截**：判定全部来自渠道路径模板
  （`submit_path`/`probe_path`/`cancel_path`，支持路径段与查询参数两种占位
  形态），报文塑形按 `task_id_path`/`probe_task_id_path`/`status_path`/
  `result_path`/`error_path` + `ok_check` 信封反向构建；上游 id → 本地 id
  为**字节级替换**（不重新序列化，字段顺序/未知字段/数值写法全部原样）。
- [x] **[N4] 客户端轮询驱动状态推进**：`polling.advance_from_probe` 从
  `poll_one` 抽出复用——原生查询拿到的上游快照顺带推进本地状态（终态走
  CAS 恰好一次，活跃态 patch_data 幂等），结果比下一轮 poller 更早可见。
- [x] **[N6] 终态零往返 + 逐字段同构**：`flow.finalize_task` 把上游终态原始
  报文落 `data.upstream_snapshot`（`nativeapi.capture_snapshot`，≤8KB 才落，
  防 data 列膨胀）；原生查询遇终态直接回放该快照并把上游 id 改写为本地 id
  （`replay_snapshot`）——usage/trace_id 等网关不认识的字段全都在，且不再打
  上游（终态本地即权威，上游终态记录还有保留期问题）。无快照（旧任务/本地
  判死）时回退按配置反向构建，状态词三档取值（`upstream_status` 上游原话 →
  渠道 `status_map` 逆映射 → 内置词表），且终态绝不回显活跃态原话。
- [x] **[N5] 测试**：`tests/test_native_passthrough.py`（提交/嵌套 id 路径+
  信封塑形/双向 id 改写/首探前快照/`probe_task_id_path`/终态结算+快照回放/
  上游原话回放/终态不回显活跃词/上游不可达回落/取消/非生命周期路径透传
  11 用例）+ `test_idem_concurrency.py` 原生重放保持原生形状 +
  `test_free_passthrough.py` 三条选渠道纪律。

行为变更（已接受）：

1. 原生提交响应 **200**（原生语义）而非 202，且只含 `task_id_path` 一个字段。
2. 原生提交路径的用户自带 `callback_url`/`webhook` 由网关摘除并改为签名投递
   （`build_submit_body` 纪律），与 tasks/videos 入口一致。
3. 原生查询在「上游未接单 / 上游不可达 / 无终态快照」时返回按配置反向构建的
   报文，字段只保证 status/id/result/error 这几处（有终态快照时逐字段同构）。

## keypool 精确直达 + 产物转存（2026-08-20 收敛，无需兼容旧版）

- [x] **[K1] `channel_id + key_index` 单 key 精确直达**：keypool select 支持
  `mode=direct`（跳过轮换批次/轮询游标/usage 打分，不访问 Redis），网关端口
  `KeyProvider.lease` 加 `key_index` 参数。任务级操作（探测/取消/原生查询/
  回调/反向对账）一律带 `data.key_index` 钉回创建时那把 key——根治同渠道挂
  多上游账号 key 时「B 账号 key 查 A 账号任务 404」的隐患（原 polling 也有
  此坑）。`key_index` 必须搭配 `channel_id`，单独出现被忽略。
- [x] **[K2] 降级纪律**：key 级失败（40010 越界——永久性，重试无意义；或
  key 被禁）→ 自动降级为渠道直达（渠道内换健康 key）；40002 渠道不存在
  原样上抛，无从降级。**提交链路不钉 key**（首打钉渠道 `key_id` 直达，
  key 级确定性拒绝重打换新鲜租约）；HELD 恢复排空不钉渠道（自动切健康账号）。
- [x] **[K3] `app/services/leasing.py`**：任务级钉回租约统一收口
  （`lease_for_task` / `route_for_task`），polling / reconcile / callback /
  flow.try_upstream_cancel / proxy 生命周期拦截全部迁入，钉 key 语义一处维护；
  旧任务无 `key_index` 快照 → 自动退渠道直达。
- [x] **[K4] 产物转存/镜像 `result_url_template`**（`app/services/
  resulturl.py`，纯函数零 I/O）：渠道配模板（如 `https://myhost.com/
  {upstream_result_url}`），终态时 `result_path` 提取的上游直链按模板改写；
  占位符 6 个（原样 / URL-encode / 去 scheme / host / path / task_id），
  未知占位符原样保留（响亮暴露配置错误）。生效范围全入口一致：
  `finalize_task` 改写 `data.result`（原始直链另存 `data.upstream_result`
  供对账/回源）→ 任务视图/用户回调自动跟随；原生查询与终态快照回放对报文
  做**字节级替换**（同时处理原文与 JSON 转义两种字节形态），其余字节
  100% 同构。网关不搬运字节，转存由模板指向的服务负责。模板为空 = 不改写。
- [x] **[K5] 测试**：`tests/test_leasing.py`（精确直达/降级/提交不钉 key）+
  `tests/test_resulturl.py`（模板渲染/多产物列表/字节级替换同构/finalize
  全视图改写/原生查询直链不外泄）+ `test_providers.py` 对齐新 select 契约。
  全量 243 passed + ruff 全绿。

## 长期运行稳定性加固（2026-08-20 收敛，无需兼容旧版）

性能与稳定性审查（长期运行 + 任务量增长）发现的 5 项问题，全部落地：

- [x] **[P1] 并发槽泄漏根治**（[高危] 最高险：`gw:conc:*` INCR 无 TTL，「占槽后
  崩溃」永久泄漏，累积到上限该用户永远 429，只能人工删键）：双保险——
  ① `LUA_CONC_ACQUIRE` 挂 TTL 兜底（`CONC_TTL_SECONDS`，默认 48h，每次
  acquire 刷新；须 > 最长任务在途时长）；② sweep 每轮 `conc_recalibrate()`
  （`app/deps/ratelimit.py`）按 tasks 表事实源（`taskstore.
  active_counts_by_token`，HELD 除外，口径同 acquire/release）回写：泄漏收回、
  少计补齐、归零删键。TTL 管兜底、校准管精确，两者独立成立。
- [x] **[P2] sweep 重入锁**（[高危] cron 每分钟触发，慢轮——反向对账打上游——
  超 1 分钟时叠加并发轮 → 重复补投/重复 renew/重复对账）：`sweep_once` 加
  `gw:sweep_lock` SET NX（TTL `SWEEP_LOCK_TTL_SECONDS` 300s，崩溃自动
  释放），拿不到直接跳过本轮；finally 释放。
- [x] **[P3] tidx 上游 id 反查索引**（[高危] `get_by_upstream_id` 的
  `data ->> '$.upstream_task_id'` 无索引=全表扫描，零建表红线不能加虚拟列；
  原生查询按上游 id 轮询是热路径，表大后必炸）：Redis `gw:tidx:{upstream_
  task_id}` → task_id（TTL `UPSTREAM_INDEX_TTL_SECONDS` 7d）。写入点收口
  在 `taskstore.patch_data`（补丁含 upstream_task_id 自动写，覆盖 submit/
  held 恢复/proxy 回填三处，零调用点改动）；读取先索引（命中后校验
  `data.upstream_task_id` 一致防脏指向）→ miss 落 SQL 兜底 → SQL 命中回写。
  索引丢失只是退化为慢查询，正确性不依赖 Redis。
- [x] **[P4] 硬编码参数配置化**（[中危] 预留调参空间）：熔断阈值/窗口
  （`UPSTREAM_BREAKER_*`）、上游连接池（`UPSTREAM_MAX_*`）、原生缓冲
  上限（`NATIVE_BUFFER_LIMIT_BYTES`）、sweep 各批次（`SWEEP_*_BATCH`）
  全部提为环境变量；poll ladder 加 300s 长尾档（长视频任务减少无效探测）；
  `upstream.client_for` 缓存键补 timeout 维度（渠道热更 timeout_sec 后新
  租约自动落新连接池，不再被旧池粘住）。
- [x] **[P5] queue_stats 降频缓存**（[中危] 每分钟 `_watch_queue` 全库
  `scan gw:submit_lock:*` + tasks 全表 GROUP BY）：快照缓存进 `gw:queue_
  stats`（TTL `QUEUE_STATS_CACHE_SECONDS` 55s），/ops/queue 与 sweep 共
  用；缓存不可用降级直算。
- [x] **[P6] 测试**：`tests/test_perf_hardening.py` 13 用例（校准泄漏收回/
  归零删键/少计补齐/一致跳过+HELD 口径/acquire 挂 TTL；sweep 锁被占跳过且
  不误删他轮锁/正常轮释放/异常轮也释放；tidx 命中零 SQL/脏指向回落 SQL 并
  纠正索引/SQL 命中回写/patch_data 写索引/无 id 不碰索引）。FakeRedis 同步
  TTL 语义与 scan_iter(count=)，InMemoryTaskStore 补 active_counts_by_token。
  全量 257 passed + ruff + mypy 全绿。

## 时间列单位混用根治（2026-08-20 收敛，无需兼容旧版）

背景：线上任务「秒失败」——tasks 表是共享表，时间列被其他写入方写成毫秒
（UnixMilli，如 `finish_time=1787199556679`），网关一切秒口径的时间比较
（探测超龄/stale/孤儿判死/HELD 判死/对账窗口）遇到毫秒值全部失真：毫秒值
被当秒比较 → 新任务瞬间超龄判死（FAILURE + 解冻），且判死不可逆。

- [x] **[T1] 归一单点**：`taskstore.as_unix_seconds`（>1e11 视为毫秒折算秒）
  + `_row_to_dict` 读侧对全部时间列统一归一——消费方（flow.duration/
  public_view/polling/ops）拿到的永远是秒；flow 内的重复实现删除改引用。
- [x] **[T2] SQL 侧同口径**：`_secs(col)` 表达式（`IF(col>1e11, DIV 1000)`）
  套住全部 SQL 时间比较（stale_active/orphan_active/held_expired/
  reconcile_candidates/oldest_held 排序）——读侧 Python 归一救不了在 SQL
  里做的 cutoff 比较。
- [x] **[T3] 判死二次核龄**：`_orphan_closeout` 在 finalize 前按归一后的秒
  重算年龄，不足 grace 跳过并告警（判死是不可逆资金动作，查询层被脏时间列
  骗过也有最后一道防线）；`polling.poll_one` 超龄计算同样归一 + 负值钳 0 +
  submit_time 缺失回退 created_at，超时文案带实际配置值。
- [x] **[T4] 终态写口径**：`taskstore.cas` 终态一律 `progress='100%'`
  （不只 SUCCESS——失败/取消停 0% 会被看板误读为在跑）、`finish_time` 恒写
  秒、WHERE 补 `platform`（共享表红线：绝不动别人的行）。
- [x] **[T5] 测试**：test_polling（毫秒 submit_time 不秒判超时/缺失回退
  created_at）+ test_reconcile（查询层误选年轻任务时二次核龄挡判死）+
  test_flow（taskstore 行级时间列归一）。全量 261 passed + ruff + mypy 全绿。



## 提交期模糊失败不再判死（2026-08-20 收敛，无需兼容旧版）

背景：线上任务「秒失败」第二个根因——`fail_reason=I/O error on POST request
for "": Target host is not specified`（渠道 base_url 缺失时 httpx 拿相对
路径发请求）。这类**基础设施/配置故障**被当成任务失败判死（FAILURE + 解冻
+ finish_time 落库），刚提交的任务立即终态。

- [x] **[S1] base_url 缺失哨兵**：`upstream.resolve_base_url` +
  `_require_base_url`——渠道与租约 base_url 双空时抛 599 UpstreamError
  （文案明示 "channel base_url missing … infrastructure, not task
  failure"），submit/probe 出站前硬校验，**零出站**、不再产生 Java 风格
  误导文案。
- [x] **[S2] 模糊失败留活重试**：`_submit_rejected` 三级分流改为——
  账户级/限流 → HELD；任务级 4xx/信封错 → FAILURE + 解冻；**AMBIGUOUS
  （599 网络/超时/base_url 缺失、5xx、熔断）→ 保 SUBMITTED/QUEUED +
  patch `data.last_submit_error` 观测，sweep stale 补投下轮重试**
  （submit_one 幂等短路 + 互斥锁 + 终态守卫已保证重复提交安全；持续失败
  由 orphan_grace 兜底判死——"确实从未接单"的正确口径）。`_submit` 持锁
  体冒泡的 UpstreamError 也收编同一分流（不再冒泡 DLQ 了事）。
- [x] **[S3] 测试改写**：test_submit/test_submit_retry 的 5xx 用例从
  「FAILURE + 解冻」翻转为「留活 SUBMITTED + 观测字段 + 不解冻」；
  新增 base_url 双空哨兵用例（零出站 + 响亮文案）。262 passed +
  ruff + mypy 全绿。

注：`finish_time=1787214476106`（毫秒）与 `result_url` 列写入仍来自部署版
与本地仓库的漂移（2026-08-17 已发现），本地已修（cas 终态恒写秒 + 终态一律
progress=100%），部署侧需同步本版本。
