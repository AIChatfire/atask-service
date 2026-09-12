# ADR-003: 创建接口异步化——preflight + 落库即返回本地 task_id

## Status: Accepted (2026-09-12) —— **部分被取代**

> **部分被取代（2026-09-12）**：**本仓库 ADR-010** 移除了本篇的一半前提。逐半看：
>
> - **保留**：「创建接口异步化」——`POST /batch/{上游路径}` 仍是落库即返回本地
>   `task_id`、上游提交交 worker。这半截继续有效。
> - **失效**：「双重提交窗口与孤儿收口」——`ORPHAN_GRACE_SECONDS`、渠道
>   `client_request_id_param` 注入、提交互斥锁 TTL 动态派生，三者**全部随旧链路删除**。
>   **新架构下没有任何等价物**：幂等原子占位只覆盖「同一 `Idempotency-Key` 的重复请求」，
>   不覆盖「上游已接单、落库前崩溃」这个窗口——该双建窗口**不再有任何兜底**。
> - 另：本篇标题与文中提到的 `preflight` 模块已删除，其现存的职责（token 哈希、限流、
>   幂等占位、落库）已内联进中继受理链路。
>
> 保留本文是为了记录当时的权衡与被否决的替代方案；**不要据「孤儿收口」那半截实施**。

## Background

早期实现里，创建请求在请求内**同步**调用上游提交接口（最坏上游超时 4
分钟+）。这有三个问题：

1. 客户端 RTT = 上游 RTT，最坏 4 分钟级，客户端连接易被网关/nginx 断掉；
2. 上游拒绝时同步返回 502，把「传输层失败」和「任务失败」混在一起；
3. 网关 web 进程被上游慢响应占满，吞吐随上游抖动而崩。

`OPTIMIZATION_BACKLOG.md` 记录了这个改造的完整执行清单。边界很明确：
本次只覆盖 `tasks` / `videos` / 原生提交入口；**proxy 原生透传保持同步
转发**（契约决定，纯透传语义与异步任务不同）。

## Decision

创建链路拆成两段：

**web 进程（同步段，`flow.create_task`）**：

```
幂等重放短路
→ conc_acquire（并发槽）
→ submit_path 校验
→ taskstore.create（SUBMITTED）
→ idem.set_task_id（同步写幂等键）
→ queue.publish_submit(task_id)（taskiq 消息落 Redis list）
→ 立即返回 {"task_id": <本地 id>, "status": "SUBMITTED"}
```

本地 `task_id = {biz_slug}_{uuid4hex}`（`deps/preflight.new_task_id`）。
202 响应**不再带 upstream_task_id**——上游提交结果由 GET / 用户回调感知。

**worker 进程（异步段，`submit.submit_one`）**：取租约 → 建路由 → 持锁
提交 → 提取上游 id → CAS 回填 → 进探测/回调闭环（见 ADR-005 的分流）。

### 双重提交窗口与两道防线

同步段与异步段之间存在一个**真实窗口**：worker 已经把提交打进上游、上游
也接单了，但在回填 `upstream_task_id` 之前 worker 崩溃。此时上游有一条在跑
的任务，本地却「没有上游 id」，表现上和「从未提交」不可区分。若直接补投，
就会在上游创建第二条任务——双重提交。

两道防线：

1. **渠道配 `client_request_id_param` 时注入本地 task_id 供上游幂等反查**
   （`upstream.build_submit_body(..., client_request_id=task_id)`，
   submit.py:134）。上游若支持幂等键，重复提交会被上游自身去重/可反查。
   这是「根治」路径，依赖上游配合。
2. **孤儿收口兜底**（`reconcile._orphan_closeout`，reconcile.py:39）：
   「非终态、无 `upstream_task_id`、创建超 `ORPHAN_GRACE_SECONDS`
   （默认 1800s）」的任务判为孤儿 → FAILURE + 解冻。
   这是「确实从未接单」的正确口径——容忍窗口内反复重试，超过窗口即认定
   上游没接单并止损。

阈值为什么是 1800s（`OPTIMIZATION_BACKLOG.md` [7]）：必须覆盖「队列积压 +
提交耗时」窗口。最坏 = stale 每 300s 补投一次 + 提交最坏
`submit_max_attempts × 渠道 timeout_sec`（3×60=180s）+ 互斥锁 TTL
余量；600s 在队列积压时会误杀在途任务，故调到 1800s。

> 配置不变量（无代码强制，见 `OPEN-DECISIONS.md` KI-D）：
> `orphan_grace_seconds > submit_max_attempts × max(渠道 timeout_sec)
> + submit_lock_buffer_seconds`。渠道 `timeout_sec` 配得极大
> （> ~580s）时，孤儿收口可能在在飞提交期间判死。

### 提交互斥锁 TTL 按路由动态派生

`gw:submit_lock:{task_id}` 是 SET NX 原子互斥，防 sweep 补投 / DLQ 重放与
在飞提交并发双建。TTL **不硬编码**，按路由派生
（`submit.submit_lock_ttl`，submit.py:48）：

```
TTL = ceil(submit_max_attempts × route.timeout_sec) + SUBMIT_LOCK_BUFFER_SECONDS
```

为什么不能硬编码 300s：渠道 `timeout_sec` 可能比 300s 大，锁会**先于在飞
提交过期**——锁一释放，sweep 的补投就与仍在飞的提交并发，上游双建。
换渠道重打时锁 TTL 按**新路由**刷新（`SET ... xx`，submit.py:175）。
sweep 补投前查锁让路（reconcile.py:195）。两道防线叠加，
「锁先于在飞提交过期」的窗口闭合（KI2 根治）。

### 幂等键原子占位（KI3 根治）

原始实现是「先查重放、落库后回填」——真并发下两个请求都查不到，双双进入
创建链路 → **双建任务、双冻结**。根治方式是把「先查后写」变原子：

- preflight 以 `SET NX` 写占位（值 `pending`，短 TTL
  `IDEM_PENDING_TTL_SECONDS`，默认 30s，config.py:86）；
- 同 Idempotency-Key 真并发只有**占位者**继续创建链路，其余在**同一键**上
  短轮询等占位回填为真实 task_id（`pending → task_id`）后回放
  （`idem.wait_task_id`，preflight.py:118）；
- 超时/占位过期按 **409 冲突**返回——**不放行重建**（重建会双建双冻结；
  409 让客户端用同键重试，资金侧零风险，billing `request_id` 唯一约束
  是最后兜底）；
- 创建链路失败 **CAS 归还占位**（preflight / flow / proxy 三处兜底），
  让同键重试立即重建，而不是干等占位 TTL。

关键判据：占位 TTL 必须覆盖最坏 preflight 时长（billing 内省 + keypool
租约 + freeze 往返）。极端窗口（preflight 超 30s 时占位过期）见
`OPEN-DECISIONS.md` KI-F。

### 原生路径拦截（透传形态的生命周期入口）

通配 `/{biz}/{原生路径}` 默认同步透传，但命中渠道路径模板的三条路径被
改写为网关语义（**零硬编码、判定全来自渠道配置**，
`app/services/nativeapi.py` + `app/routers/proxy.py`）：

| 渠道模板 | 方法 | 改写后语义 |
|---|---|---|
| `submit_path` | POST | 走 `flow.create_task` 异步受理，**零上游往返秒级返回**；响应按 `task_id_path`（+ `ok_check` 信封）塑形为原生形状、值是本地 task_id |
| `probe_path` | GET | 按 URL 里的 id 反查 tasks 行（本地 id 主键直查、上游 id 兜底反查），用 `channel_id` 钉回直达租约转发；响应缓冲后把上游 id **逐字节改写**回本地 id（其余字节 100% 同构） |
| `cancel_path` | 非 GET | 走本地 cancel 链路（解冻 + 尽力源头止损），**绝不当新任务报价冻结** |

其余路径透传语义一字不改。三个必须记住的机制：

1. **终态零上游往返**：finalize 时把上游终态原始报文落
   `data.upstream_snapshot`（≤8KB 才落，防 data 列膨胀，
   `nativeapi.capture_snapshot`），原生查询遇终态直接 `replay_snapshot`
   回放——逐字段同构（`usage`/`trace_id` 等网关不认识的字段全在），
   且不再打上游（终态本地即权威，上游终态记录还有保留期问题）。
2. **首探前/上游不可达绝不 404**：按配置反向构建快照，
   `probe_task_id_path` 指定快照里 id 的字段路径，状态词三档取值
   （`data.upstream_status` 上游原话 → 渠道 `status_map` 逆映射 →
   内置词表，`nativeapi.status_word`），终态绝不回显活跃态原话。
3. **id 改写是字节级替换**（`nativeapi.rewrite_ids`），不重新序列化——
   字段顺序、未知字段、数值写法全部原样。

已接受的行为变更：原生提交响应 **200**（原生语义）而非 202，且只含
`task_id_path` 一个字段；原生提交路径的用户自带 `callback_url`/`webhook`
由网关摘除并改为签名投递（与 tasks/videos 入口一致）。

### 长期运行稳定性纪律（并发槽与 sweep）

两条「不可自愈漂移」的根治（`OPTIMIZATION_BACKLOG.md` P1/P2），是长跑
稳定性判据，不是一次性修复：

- **并发槽泄漏双保险**：`gw:conc:*` 的 INCR 若「占槽后崩溃」会永久泄漏，
  累积到上限该用户永远 429，只能人工删键。两道防线**独立成立**：
  ① `LUA_CONC_ACQUIRE` 挂 TTL（`CONC_TTL_SECONDS`，默认 48h，每次
  acquire 刷新；**须 > 最长任务在途时长**）；② sweep 每轮
  `conc_recalibrate()`（`app/deps/ratelimit.py`）按 tasks 表事实源
  （`taskstore.active_counts_by_token`，HELD 除外，口径同 acquire/release）
  回写——泄漏收回、少计补齐、归零删键。TTL 管兜底、校准管精确。
- **sweep 重入锁**：sweep 每分钟触发，慢轮（反向对账打上游）可能超 1 分钟，
  叠加并发轮会重复补投/重复 renew/重复对账。`gw:sweep_lock` 用 `SET NX`
  （TTL `SWEEP_LOCK_TTL_SECONDS`，默认 300s，崩溃自动释放），拿不到
  直接跳过本轮，`finally` 释放。

## Consequences

- 正面：创建接口零上游往返（`原生提交` 场景「秒级返回」）；客户端 RTT 与
  上游抖动解耦；web 进程不再被慢上游占用。
- 正面：上游拒绝从「同步 502」改为「异步 FAILURE」，语义更诚实——
  任务已经在册，失败通过任务状态与回调告知。
- 负面：**客户端契约变化**——202 只含本地 task_id（`{biz}_{uuid4hex}`），
  上游 id 不再出现在响应里；上游拒绝异步 FAILURE，客户端靠 GET / 回调
  感知 `fail_reason`。这是有意接受的取舍（`OPTIMIZATION_BACKLOG.md`
  「三个取舍」）。
- 负面：**双重提交窗口存在但被收窄**——靠 `client_request_id_param` 幂等
  反查 + 孤儿收口兜底，不能 100% 消除（上游不支持幂等键时只能靠窗口收口）。
- 负面：freeze 占用时间变长（排队期持冻结）。缓解：sweep 续期机制
  （`expiring_freezes` + billing renew，reconcile.py:110）覆盖。

## Related ADRs

- **atask-service ADR-005**（提交失败五级分流：模糊失败留活重试）
- **atask-service ADR-004**（时间列归一：孤儿收口判死前的二次核龄）
- **atask-service ADR-002**（零路由文件：原生路径拦截的判定来源）
- **atask-service ADR-006**（任务级租约钉回：原生查询按 channel_id 钉回）
- `app/services/flow.py`；`app/services/submit.py`；
  `app/services/reconcile.py`；`app/services/nativeapi.py`；
  `app/routers/proxy.py`；`OPTIMIZATION_BACKLOG.md`；`AI_TODO.md`

> 来源：本 ADR 的「原生路径拦截」「幂等键原子占位」「长期运行稳定性纪律」
> 三节由 `AI_TODO.md` 与 `OPTIMIZATION_BACKLOG.md` 中已收敛的有效内容
> 提炼而成（两份历史文档已于 2026-09-12 归档）。
