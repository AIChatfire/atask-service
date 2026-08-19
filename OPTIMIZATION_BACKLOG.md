# atask-service 优化 Backlog（已收敛）

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
- [x] **[7] `orphan_active` 判死阈值**：`GW_ORPHAN_GRACE_SECONDS`
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
  `GW_SUBMIT_LOCK_BUFFER_SECONDS`，换渠道重打时按新路由刷新，见
  `app/services/submit.py::submit_lock_ttl`），不再硬编码 300s；sweep 补投
  前查 `gw:submit_lock:{task_id}`，锁在则本轮让路（`reconcile.sweep_once`）。
  两道防线叠加，「锁先于在飞提交过期 → 补投并发 → 上游双建」窗口闭合。
- **KI3 幂等键并发窗——已根治**：preflight 改原子占位（SET NX 写
  `pending`，短 TTL `GW_IDEM_PENDING_TTL_SECONDS`，见
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
