# ADR-010: 对外形态统一为 `/batch/{上游路径}`，鉴权与计费全部下沉上游

**Status**: Accepted (2026-09-12)
**Supersedes**: **本仓库 ADR-002 / ADR-005 / ADR-006 / ADR-007**
**Rewrites**: **本仓库 ADR-008**——atask 与 stask 的分工不再是「有无资金动作」，而是「包哪种上游」。

## Background

本仓库现状：

- 网关从 **keypool** 取上游 key（任务级**租约钉 key**，`channel_id + key_index` 精确直达）；
- 经 **newapi-billing-service** 做身份内省与 **freeze / settle / cancel**，计费规则随租约
  下发、网关本地沙箱求值；
- 对外形态是 `/{biz}/v1/tasks`、`/{biz}/v1/videos` 与通配 `/{biz}/{原生路径}`。

**问题：这套把上游已经做过的事又做了一遍。** 当上游本身就是 new-api 时，它的原生 relay
已经完成了渠道选择与配额扣减（预扣 + 实结）；网关再 lease 一把上游 key、再 freeze 一笔
额度，属于**重复资产 + 重复风险**：两份凭证都要治理、两处资金动作都要对账、两套失败分流
都要判死。

决策依据是 stask-service 的既有取舍（见 `docs/stask-service-design.md`）：它把
「同步生成 API 加 `/async` 前缀即任务化」做得极薄——**计费零代码，资金操作全部在上游
relay 内闭环**，网关只用用户自己的 sk 转发。

## Decision

### 1. 对外形态（唯一形态）

| 方法 | 路径 | 语义 |
|---|---|---|
| `POST` | `/batch/{path}` | 受理。`{path}` 是**上游原生路径**（如 new-api 视频生成 `v1/tasks`） |
| `GET` | `/batch/{path}/{task_id}` | 查询。`task_id` 取**最后一段** |
| `DELETE` | `/batch/{path}/{task_id}` | 取消 |

- 响应统一 `202 + {task_id, status}`，并带 `Location: /batch/{path}/{task_id}` 头。
- **`{biz}` 段从 URL 彻底消失**：biz 本来就由渠道元数据提供，URL 段只是入口标签，
  去掉不丢信息（提交链路的选渠道一直只依赖 body 里的 `model`）。
- 不再有 `/{biz}/v1/tasks`、`/{biz}/v1/videos`、`/{biz}/{原生路径}` 等旧形态。
- **不做任何旧形态兼容**（用户明确要求「无需兼容旧版本」）。

#### 为什么不是 `/async`（命名理由，不是随意挑的）

1. **本仓库是「异步转异步」**：上游本身就是异步任务型接口，网关只是再包一层统一受理。
   `async` 这个词描述的是「把同步接口异步化」，那正是 **stask 的语义**——
   而 stask 与 atask 是两个独立服务（本仓库 ADR-008）。
2. **更硬的理由是 nginx 前缀分流冲突**：`docs/stask-service-design.md` §7 的 nginx
   方案里 `location /async/ { proxy_pass http://stask:8000; }`——**`/async/` 已经归
   stask**。同域名下两个服务不可能共用同一前缀，atask 必须另占一个。

### 2. 鉴权：网关不做内省

用户 token 以 `Authorization` **原样透传**上游，由上游判定有效性。
网关只做本地可做的事：限流（按 token hash）、幂等、并发上限。

### 3. 计费：网关零资金动作

**不 freeze、不 settle、不 cancel。** 配额由上游 relay 扣减。
因此 `tasks.data` 不再写 `freeze_amount` / `settled`，网关不再有「资金侧兜底」这一层。

### 4. 上游寻址（仿 stask）

优先级：**`X-Upstream-Base-Url` 头（由 nginx 无条件注入并覆盖客户端同名气头）**
\> **配置项 `UPSTREAM_BASE_URL`**。

安全三防线（防用户 sk 被打到野地址）：

1. nginx `proxy_set_header` 无条件覆盖客户端同名头；
2. host 必须命中 **`UPSTREAM_ALLOWLIST`**，否则 `400`；
3. 仅接受 `http`/`https`，**拒绝 URL userinfo**，且**白名单为空即全部拒绝**（fail-closed）。

### 5. 渠道配置：按 new-api 约定，**零配置**

去掉 keypool 后，渠道路由元数据（`task_id_path` / `probe_path` / `status_path` /
`auth_type` / `timeout_sec` / `model_mapping` / `param_override` / `default_params` /
`body_allowlist` / `result_url_template` / `supports_callback` …）全部失去来源。
**一律按 new-api 约定硬编码，不引入任何本地配置文件。**

| 环节 | 约定 |
|---|---|
| 提交 | `POST {upstream_base}{path}`，原样转发 method / query / body |
| 鉴权 | `Authorization: Bearer <用户 token>` 原样透传（固定 Bearer，不再有 `auth_type`） |
| 提取上游任务 id | 提交响应里取 `id`，缺失时回退 `task_id` |
| 探测 | `GET {upstream_base}{path}/{upstream_task_id}` |
| 状态字段 | 响应里的 `status` |
| 上游基址 | `X-Upstream-Base-Url` 头 → 配置回退（§4） |
| 超时 | 全局配置项（不再是**渠道级** `timeout_sec`） |

### 6. 终态收敛与用户回调

- **后台 sweep**（`batch_sweep_task`，cron 每分钟，独立重入锁）按 `TASK_STALE_SECONDS`
  探测非终态任务、推进到终态；**最旧优先**（`_secs('updated_at') ASC`）——
  用 DESC 会让最旧那批永远轮不到探测，而它们最可能已在上游成功。
- **用户回调**：受理时接受 `X-Callback-Url` 头，终态时经 `notify.sign` 签名后投递，
  走既有 `queue.publish_notify`（重试 + 死信）。无回调 URL 则不投递。
- **单一终态收口点**：快照落库（≤8KB）→ 还并发槽 → 投递回调 → 清令牌会话，
  视图路径 / worker 路径 / sweep 路径**共用同一份实现**，不许各写一套。

### 7. 删除 keypool 协同

删除：`app/services/providers/`（keypool + billing 两个适配层）、`pricing.py`（计费规则
沙箱）、`leasing.py`（租约钉 key）、`held.py`（HELD 挂起）、`deps/preflight.py` 里的
内省/报价/冻结链路、`reconcile.py` 的资金对账与孤儿收口。
`AGENTS.md` 开头「外部协同只有两个微服务」随之失效。

## Consequences

**正向**

- 网关**不再持有上游 key**：凭证面从「上游 key + 服务级 token」缩到「无」，只剩链路转发；
- 资金动作集中在上游一处，**风险单点清晰**，不再有「网关冻结 vs 上游扣费」的双账对齐问题；
- 接入新上游 = 配一个 base_url + 白名单，**零渠道元数据依赖、零计费规则配置**；
- 失败分流从「五级（含资金分支）」退化为三档，判死不再是不可逆资金动作。

**负向（如实记录）**

- **失去网关侧冻结兜底**：若上游 relay 的预扣在失败时不回滚，网关无从补偿——
  与 stask 的开放问题①同质，只能靠上游保证；
- 失去任务级租约钉 key（不再有「上游 key」这个概念，也就没有换 key 查不到任务的问题）；
- HELD 挂起 / 冻结续期 / 孤儿收口的「不亏本兜底」**不再适用**，相关不变量（如
  `ORPHAN_GRACE_SECONDS` 与提交锁 TTL 的关系）一并作废；
- 并发上限不再能按「余额付得起几个在途任务」推导，只能按固定上限。

**明确放弃的渠道级能力**（这是「零配置」的对价，不许含糊）

- `model_mapping`（按渠道改写 model）
- `param_override` / `default_params`（渠道级参数覆盖与默认值）
- `result_url_template`（产物直链改写 / 转存镜像）
- `supports_callback`（渠道是否支持回调——改为固定策略：后台 sweep + 可选用户回调）
- `body_allowlist`（提交体字段白名单）
- 渠道级 `timeout_sec`（降级为全局默认）
- `auth_type`（固定 Bearer；x-api-key / none 形态不再支持）

**接入不符合 new-api 约定的上游需要改代码。**

**保留不变**（这些是 atask 区别于 stask 的本体能力）

- **异步受理**：`POST /batch/{path}` 落库即返回本地 task_id（客户端侧零上游往返），
  上游提交交 worker；
- **任务事实源**：复用 new-api `tasks` 表，`platform='gateway'` 隔离（**本仓库 ADR-001**）；
- **共享表时间列归一**（**本仓库 ADR-004**）：`as_unix_seconds` / `_secs()` / 判死前二次核龄；
- **原生报文同构**：探测响应把上游 id 逐字节改写回本地 id；**终态零上游往返**（快照回放）；
- **幂等原子占位**（SET NX 占位 → 回填，真并发 409）；
- **并发上限**：按 token hash 计，不依赖内省与余额。

## 已知限制（登记在案，必须写进运维文档）

1. **令牌会话过期后任务无法自愈**。探测上游需要用户 token，而网关只把 token 存在
   Redis 会话里（TTL = `SK_SESSION_TTL_SECONDS`，48h）——这是「鉴权下沉上游」的必然
   代价：网关不持有长期凭证。会话过期后 sweep 会跳过该任务（DEBUG 级，不报错、
   **绝不判死、绝不释放并发槽**），该任务会**永久停在非终态**。处置：客户端可
   `DELETE` 取消，或由管理面介入。
2. **没有 max-age 判死，这是刻意的**。旧 poller 有 `POLL_MAX_AGE_SECONDS` 超时转
   FAILURE；新链路**不设**。理由：会话 TTL 已天然给探测设了上界（48h 后自动跳过），
   而判死会**永久丢失一个可能已在上游成功的任务结果**——判死不可逆，无明确收益则不做。
3. **`X-Upstream-Base-Url` 头的可信性完全依赖 nginx 配置正确**。若 nginx 未无条件覆盖，
   客户端可伪造该头把请求（连同用户 sk）指向任意 host；`UPSTREAM_ALLOWLIST` 是第二道
   防线，**两道都必须配**。
4. **取消语义退化**：不再有「解冻」，`DELETE` 只做尽力源头止损 + 本地置 CANCELED。
   上游取消形态（`DELETE {base}{path}/{id}`）属**约定推断**，未经上游文档验证。
5. **body 里的回调字段不做拦截**：请求体逐字节原样转发，若用户自行在 body 里放回调
   字段，上游可能直接回调、与网关回调形成双投递。网关不解析 body 语义（这是「零配置 /
   原样转发」的对价）。
6. **收敛延迟是分钟级——这是明确接受的取舍**。后台收敛只按 `TASK_STALE_SECONDS`
   （默认 300s）这个 **stale 阈值**选候选任务，**没有探测阶梯**（旧链路曾有
   5/15/30/120/300 秒的阶梯 + 每分钟 sweep 重投）。因此任务完成后**最长约 300–360s**
   才会被后台观察到 → **用户回调延迟为分钟级**；客户端主动 `GET /batch/{path}/{task_id}`
   仍是即时探测。
   **明确决定：不恢复阶梯。** 阶梯会显著抬高探测频率、把负载压到上游，而收益只是
   「回调早几分钟到」；需要更快结果时应由客户端轮询，而不是把阶梯加回来。
7. **并发槽只靠 TTL 自愈，不做定期校准**。旧链路有按 `tasks` 表事实源定期校准槽位
   （每分钟 `conc_recalibrate`），换向后只剩 `CONC_TTL_SECONDS`（默认 48h）的 TTL 兜底：
   槽泄漏**最长 48h 才消失**，期间该用户的**有效并发上限偏低**（不会高于配置值，只是暂时更严）。
   **明确决定：接受。** 加回校准要按 token 扫表聚合，成本不低，而收益仅是「极端情况下少等几小时」。

## Related

- **本仓库 ADR-001**（复用 tasks 表）、**ADR-004**（时间列归一）、**ADR-009**（异常分层）——**继续有效**
- **本仓库 ADR-002 / ADR-005 / ADR-006 / ADR-007**——**已被本决策取代**
- **本仓库 ADR-003**——**部分被取代**：「创建异步化」一半保留；**「双重提交窗口与孤儿收口」
  一半已删且无任何等价物**（该双建窗口不再有兜底，见 ADR-003 的 superseded 块）
- **本仓库 ADR-008**——分工表述需按本决策重写
- `docs/stask-service-design.md`（stask 侧设计，`/async` 形态与寻址的出处）
