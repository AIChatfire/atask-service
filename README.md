# atask-service（任务队列服务）

**任务队列服务**：把「提交 → 推进 → 取结果」这套任务语义从上游剥离出来统一提供——
对上承接用户请求（令牌原样透传、本地限流/幂等/并发上限），对下把请求中继到上游并
**持有任务事实源**（`/queue` 中继形态），与 new-api 生态共用用户体系、钱包与渠道配置。

**当前准入范围：只接受异步任务**，做的事是**排队异步**——把**上游异步**任务接管成
**本地异步**任务（本地 task_id → 入队排队 → 后台推进到终态 → 可选回调），并持有任务
事实源。「异步 / 同步」描述的是**当前接受哪种任务**，不是「转」的方向：同族的
stask-service 只接受同步任务（由它完成任务化），两者产出的都是**本地异步**任务，
差别只在输入（见本仓库 ADR-008）。

**核心特点**（现行架构见本仓库 ADR-010）

- **零资金动作**：网关不冻结、不结算、不解冻，也不写任何资金字段；配额由上游
  new-api 原生 relay 扣减（预扣 + 实结）。网关也**不持有上游 key**——用户 token
  原样透传，凭证面缩到「无」。
- **零渠道配置**：渠道路由全部按 new-api 约定硬编码，接入新上游 =
  配一个 base_url + 白名单，**零渠道元数据依赖、零计费规则配置**。
- **异步受理**：`POST /queue/{path}` 落库即返回本地 task_id，客户端侧零上游往返；
  上游提交交 worker（`app/services/relayflow.py`）。
- **可选攒批放行**：`BATCH_SIZE>=2`（或客户端声明 `X-Batch-Size`）时把「上游提交」
  从受理时刻解耦，攒够 N 条或等够 T 秒才**整批提交**（**不合并请求**——上游是约定式
  异步接口，没有批量端点）；等待期**不占并发槽**，故攒批路径受理不再 429、改为排队。
  默认 `BATCH_SIZE=0` = 收到即提交，开箱行为与无此特性时逐字节一致（本仓库 ADR-011）。
- **无状态**：自有状态全在 Redis，零自有 MySQL 表（只读写与 new-api 共享的
  `tasks` 表，`platform='atask'` 隔离）。

## 架构

```
用户 ──► gateway（FastAPI）
          │  Authorization: Bearer sk-...（原样透传上游，网关不做内省）
          │
          ├─► 上游 new-api 原生异步任务接口（渠道选择与配额扣减都在上游 relay 内闭环）
          │
          ├─► Redis（taskiq 队列、幂等键、令牌会话、并发槽、熔断计数）
          └─► MySQL（与 new-api 共享 tasks 表，任务事实源，platform='atask'）

后台：taskiq worker（上游提交 + 用户回调投递 + scheduler 每分钟 queue_sweep 收敛；
scheduler 必须单副本，worker 扩副本时拆回独立服务）
```

## 对外前缀与网关内部前缀（两层）

| 层 | 前缀 | 谁负责 |
|---|---|---|
| 对外（客户端看到） | **`/async`**（与 stask 统一，客户端只记一个） | nginx |
| 网关自身端点 | **`/queue/{上游原生路径}`** | 本仓库（`app/routers/queue_task.py`） |

nginx 在同一域名下做两件事：

1. **按路径分流**：同步类上游的路径先路由给 stask（`location /async/<同步类路径>/` → stask）；
2. **其余 `/async/*` 反代到 atask 并重写为内部 `/queue/*`**：
   `location /async/ { proxy_pass http://atask:8000/queue/; }`
   （`proxy_pass` 带 URI 段时，用 `/queue/` 替换掉匹配到的 `/async/`）。

**为什么这样分层**：`/async` 对调用方的承诺是「异步交付」，两个服务都成立，所以对外可以
共用；而「这条路径属于哪类上游」在共域名下只能由 nginx 的路径表给出——**代价是接入新上游
要多写一条 `location`**（不共用前缀则新上游零 nginx 改动，两种取舍见本仓库 ADR-010 §1）。
**漏配必须响亮**：兜底建议 `return 404`（fail-closed），别默认甩给某一个服务——否则一个
漏配的 stask 路径会打到 atask，症状在两边日志里都只露一半。

**历史**：2026-09-13 之前对外前缀是 `/batch`（换向前「批量提交」时期的遗留词）；当日先
改 `/queue`，最终定为「对外 `/async` + 网关内部 `/queue`」。不做任何别名兼容（本仓库既定立场）。

**一行记法**：`/async` 对调用方承诺「异步交付」，`/queue` 是网关内部「排队接管」的机制名
（与 `data.source='queue'`、`queue_*` 词根同源）。取舍与历史见本仓库 ADR-010 §1。

## API 形态

| 端点 | 说明 |
|---|---|
| `POST /queue/{path:path}` | 受理：落库即返回 `202 + {task_id, status}`，带 `Location: /queue/{path}/{task_id}` 头；需 `Authorization` + 可选 `Idempotency-Key` |
| `GET /queue/{path:path}` | 末段是本地 `task_id` → 任务视图（非终态按需探测上游，终态零上游往返）；末段不是本地 id → **免费透传**（原样转发上游，不落 tasks 行） |
| `DELETE /queue/{path:path}` | 取消：本地 CAS 置 CANCELED + 尽力源头止损 + 清令牌会话（末段不是本地 id 则 404） |
| `GET /ops/queue` | 队列健康快照（积压/延迟/死信/状态分布） |
| `GET /ops/tasks/{task_id}` | 任务内部诊断视图（令牌会话只给存在性与 TTL） |
| `POST /ops/requeue/{task_id}` | 手动补投：立即把非终态任务重新放入提交队列 |
| `POST /ops/dlq/replay` | 死信重放（补号） |
| `GET /admin`、`GET /admin/api/*` | 管理看板：页面 + 其 JSON API（与 `/ops/*` **共用** `X-Admin-Token`） |
| `GET /healthz/live`、`GET /healthz/ready` | 探针（ready = Redis PING + SELECT 1） |

`{path}` 是**上游原生路径**（如 new-api 视频生成的 `v1/tasks`），`{biz}` 段已从 URL
彻底移除。**管理面 fail-closed**：`/ops/*` 与 `/admin/*` 共用 `X-Admin-Token`
（配置项 `ADMIN_TOKEN`），**未配置密钥时整个管理面返回 404**——不是 401，也不依赖
内网隔离（见 `app/deps/admin.py`）。

**两条 GET 语义**（同一个通配路径，按末段判定）：

- **任务视图**（末段是本地 id）：本地 `tasks` 行是权威。非终态按需用落库的
  `upstream_base_url` + `request_path` + `upstream_task_id` 探测上游并推进状态，
  对外报文**与上游同构**——上游 id 逐字节改写回本地 id，`status` 用上游原话；
  终态直接回放落库快照（`data.upstream_snapshot`，≤8KB），**零上游往返**。
- **免费透传**（末段不是本地 id）：原样转发上游，上游状态码与 `Content-Type`
  原样回吐（可能是二进制产物，绝不硬写 JSON），**不产生任何本地任务事实**。
  按 IP 限流；必须带 `Authorization`（无凭证既无身份做限流、转发也必被上游拒）。

## 接入上游：零渠道配置（按 new-api 约定）

**上游寻址**：`X-Upstream-Base-Url` 头（由 nginx **无条件注入并覆盖客户端同名气头**）
优先，回退配置项 `UPSTREAM_BASE_URL`。host 必须命中 `UPSTREAM_ALLOWLIST`，否则 400；
仅接受 `http` / `https`、拒绝 URL userinfo、**白名单为空即全部拒绝**（fail-closed）。
安全三防线与取舍见 `app/services/upstream_addr.py`。

**交互约定**（全部硬编码，无本地配置文件）：

| 环节 | 约定 |
|---|---|
| 提交 | `POST {upstream_base}{path}`，原样转发 method / query / body |
| 鉴权 | `Authorization: Bearer <用户 token>` 原样透传（固定 Bearer） |
| 提取上游任务 id | 提交响应里取 `id`，缺失时回退 `task_id` |
| 探测 | `GET {upstream_base}{path}/{upstream_task_id}` |
| 状态字段 | 响应里的 `status`（原话保留，映射只驱动本地状态机） |
| 超时 | 全局配置项 `RELAY_TIMEOUT_SECONDS`（不再有渠道级 `timeout_sec`） |

**注入实现**（唯一实现，无其他入口）：提取纯函数与出站在
`app/services/relay.py`（`extract_upstream_task_id` / `upstream_status` /
`call_upstream`），路径规整与 id 改写/快照在 `app/services/nativeapi.py`，
状态映射在 `app/services/statusmap.py`。

请求形态（`{path}` 用上游原生路径）：

```bash
# 受理：立刻拿到本地 task_id（不等上游）
curl -X POST https://gw.example.com/queue/v1/tasks \
  -H 'Authorization: Bearer sk-user-xxx' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-key-1' \
  -H 'X-Callback-Url: https://app.example.com/webhook' \
  -d '{"model":"your-model","prompt":"a cat"}'
# → 202 {"task_id":"queue_5f2c...e91","status":"SUBMITTED"}
#   Location: /queue/v1/tasks/queue_5f2c...e91

# 回调地址也可以放在 body 顶层（上游 API 文档口径，如火山方舟 Seedance）
# -d '{"model":"your-model","prompt":"a cat","callback_url":"https://app.example.com/webhook"}'

# 查询：末段是本地 task_id → 任务视图（非终态按需探测上游）
curl https://gw.example.com/queue/v1/tasks/queue_5f2c...e91 \
  -H 'Authorization: Bearer sk-user-xxx'

# 取消
curl -X DELETE https://gw.example.com/queue/v1/tasks/queue_5f2c...e91 \
  -H 'Authorization: Bearer sk-user-xxx'
```

**用户回调地址**有两个等价通道：`X-Callback-Url` 头（**优先**）与 body 顶层
`callback_url` 字段（**兜底**，上游 API 文档口径，如火山方舟 Seedance）。地址必须命中
`CALLBACK_ALLOWLIST`（**为空即全部拒绝**，fail-closed），且私网/回环/链路本地**字面 IP
一律拒绝**——否则等于给互联网开一个 SSRF 出站跳板。非法地址在**任何副作用之前**被拒
（不落库、不占并发槽、不占幂等键）。

默认「网关接管」模式下，body 里的 `callback_url` 会被**从转发体摘除**后再送上游，以消除
「上游也回调 + 网关也回调」的双投递（转发改走 `data.submit_body`，`data.request_body`
仍存原文）；置 `CALLBACK_PASSTHROUGH_UPSTREAM=true` 时改为原样转发、由上游自己回调。
**客户端对接契约（签名、验签、幂等去重、与火山的差异）见
[`docs/CALLBACK-CONTRACT.md`](docs/CALLBACK-CONTRACT.md)。**

## 计费、可靠性与收敛

- **零资金动作**：不冻结、不结算、不解冻，无挂起态；取消只做尽力源头止损。
  失败分流因此只有三档（见下）。
- **幂等原子占位**：`Idempotency-Key` 以 SET NX 写占位（`pending`，短 TTL）把
  「先查后写」变原子——同键真并发只有占位者继续创建链路，其余短轮询等占位回填为
  task_id 后回放，超时/过期按 409 冲突（不放行重建）。实现：
  `app/services/idem.py`，接线 `app/services/relayflow.py`。
- **提交失败三档**：上游 4xx（确定性拒绝）→ FAILURE + 还并发槽 + 清会话；
  5xx / 传输错误（模糊失败）→ **留活重试**（不判死——上游可能已接单）；
  2xx 却缺 id → FAILURE。实现：`app/services/relayflow.py::submit_queue_task`。
- **单一终态收口点**：`relayflow._finalize_queue` 是视图路径 / worker 路径 / sweep
  路径共用的**唯一**收口实现——CAS 抢推进权 → 记一条状态迁移日志 → 落终态快照 →
  释放并发槽 → 投递用户回调 → 清令牌会话。CAS 抢不到即整段不执行，保证终态事件
  「恰好一次」。**取消路径刻意不复用**它（多一步「尽力源头止损」且必须在清会话之前，
  因为它要用会话里的 sk 发 `DELETE`），故**终态收口动作没有单一来源**——新增终态动作时
  必须同时问「取消路径要不要」（本仓库 ADR-012 已知限制）。
- **攒批放行**（本仓库 ADR-011）：受理落库为 `SUBMITTED` + `data.batch_state='waiting'`
  入批等待，由「成员数达到 N」或「本批 deadline 到期」触发整批放行（另有 sweep 的超期
  兜底）；放行动作在 `relayflow.release_batched_task`，批次索引在
  `app/services/batching.py`。三条不要动的约定：① **等待期不占并发槽**（占槽点从受理
  搬到放行，否则批次永远不可能大于并发上限）；② **两层幂等缺一不可**（批次级 Redis
  原子摘取 + 成员级 DB 条件更新，少了成员级会让上游被调两次）；③ **还槽必须走
  `taskstore.claim_slot_release`**（谁把 `data.slot_flags` 置零谁去 DECR）。
- **用户回调投递**：**只在终态**推一次。地址按上节的通道与准入规则确定，终态经
  `app/services/notify.py` 以 HMAC-SHA256 签名（`X-Gateway-Signature: t=...,v1=...`）后
  投递，走既有 `queue.publish_notify`（重试 + 死信）。无回调地址则不投递。
  **中间态不推**——网关是中继而非执行者，只在被轮询或巡检时才观测上游，中间态是抽样
  而非事件流。**主动取消不推**（调用方自己发起，结果它已知）。
- **后台收敛**：`queue_sweep_task`（cron 每分钟，`app/queue.py`）探测非终态任务并
  推进到终态，用独立重入锁 `K_QUEUE_SWEEP_LOCK` 防慢轮叠加；候选**最旧优先**
  （`updated_at ASC`），因为它们最可能已在上游成功。同一条 sweep 也负责攒批的超期兜底。
- **并发上限 + 限流**：并发槽按 token hash 计（不依赖内省与余额），上限走运行时
  热配置 `max_concurrent_tasks`；免费 GET 与受理按限流（`RATE_LIMIT_PER_MINUTE`）。
  实现：`app/deps/ratelimit.py`。
- **上游出站熔断**：窗口内失败达阈值即打开（键取目标 host），出站复用进程级连接池。
  实现：`app/services/upstream.py` + `app/services/httpc.py`。
- **保留不变的既有契约**：复用 new-api `tasks` 表 + `platform='atask'` 隔离
  （本仓库 ADR-001）；共享表时间列归一（本仓库 ADR-004，`taskstore.as_unix_seconds`
  / `_secs()`）；**原生报文同构**与**终态快照回放**（终态零上游往返）；幂等原子
  占位；按 token hash 的并发上限。

**已知限制**（登记在案，详见本仓库 ADR-010 与 ADR-012）：

1. **令牌会话过期后任务无法自愈**：探测需用户 token，而网关只把 token 存 Redis
   会话（TTL = `SK_SESSION_TTL_SECONDS`，48h）。会话过期后 sweep 跳过该任务
   （DEBUG 级，不报错、**绝不判死、绝不释放并发槽**），任务停在非终态。
2. **刻意不设 max-age 判死**：判死不可逆，会永久丢失一个可能已在上游成功的任务。
   也因此不像上游那样提供「任务超时（expired）」类回调通知。
3. **`X-Upstream-Base-Url` 头的可信性完全依赖 nginx 配置正确**：`UPSTREAM_ALLOWLIST`
   是第二道防线，**两道都必须配**。
4. **取消语义退化**：`DELETE` 只做尽力源头止损 + 本地置 CANCELED；上游取消形态
   （`DELETE {base}{path}/{id}`）属约定推断，未经上游文档验证。取消**不投递回调**。
5. **转发体的「逐字节原样」有一个例外**：body 顶层带 `callback_url` 且处于默认
   「网关接管」模式时，该字段被**摘除**后再转发（转发体走 `data.submit_body` 这个
   语义等价重构体，键序/空白可能与原文不同；`data.request_body` 仍存原文）。
   这样反而**消除了双投递**——若原样转发给上游，上游可能也回调一次。
   `CALLBACK_PASSTHROUGH_UPSTREAM=true` 时无此例外。
6. **回调地址不防 DNS rebinding**：白名单配域名时，若该域名被解析到内网则发现不了。
   不做解析后校验是因为 TOCTOU——校验时的解析结果与 HTTP 客户端出站时的解析结果
   不保证一致。信任边界交给白名单（只放行可信域名）。

## 观测

日志统一走 loguru（`app/logging.py` 装配 stderr sink 并桥接 stdlib，`LOG_LEVEL`
控制级别，排障调 DEBUG 即可看全链路；**令牌/上游 key 绝不进日志**）。
**状态变化唯一记录点**是 statelog（`app/services/statelog.py`，Redis 去重，只在任务
状态变化时发一条，运行中连探多轮零事件）。`/ops/tasks/{task_id}` 提供任务诊断视图
（含 token_hash 截断与令牌会话存在性/TTL）。`LOGFIRE_ENABLED=true` 接入 logfire
（装配单点 `app/observability.py`，web / worker / standalone 三种形态只传 `component`）。
队列执行层另有 taskiq-admin 看板（`TASKIQ_ADMIN_URL` / `TASKIQ_ADMIN_API_TOKEN`，
只绑回环；注意这与网关自己的 `/admin` 看板是**两套东西**）。

## 本地开发

```bash
make setup      # 建 venv（uv，Python 3.12）+ 装依赖 + 生成 .env
make check      # 三项门禁：ruff + mypy + pytest（不需要 MySQL/Redis/上游）
make standalone # 单进程起全套（web + worker + scheduler 同事件循环）
make            # 无参数列出全部命令
```

`make standalone` 是本地联调的主要形态：一条命令起 web + taskiq worker + scheduler，
**免 .env 也能跑**（配置全有代码默认值），且与 compose 形态的配置、状态机、失败
分流完全一致——本地跑通的链路对线上有参考价值。它明确不适合生产扩容场景，
切换标准见 `app/standalone.py` 文件头。

单测**不依赖真实** MySQL / Redis / 上游（respx 拦截出站，FakeRedis（含 Lua 脚本的
逐条等价实现）+ 内存 taskstore）。**跑 `pytest` 等于同时跑类型检查与静态门禁**：

- `tests/test_typecheck.py` —— `mypy app/` 零报错
- `tests/test_static_gates.py` —— 结构断言（**不写死条数**，条目会随迭代增长；
  写死就会出现「文档说九条、实际十四条」这种自我漂移）：ruff 洁净、
  源码与文档无 emoji、**tasks 表 SQL 只能出现在 `app/services/taskstore.py`**、
  **`os.environ` 只允许出现在一处**（`gunicorn.conf.py`——它在 pydantic 单例之前
  由 master 进程加载；门禁按 AST 判定属性访问，docstring 里提到它不算）、
  **全仓不得含真实凭据**、`app/services` 不得有无调用方的公开函数、
  不得有无人在抛的异常类、配置项与 `.env.example` 必须对齐、
  配置键名不带前缀（`env_prefix` 保持为空）、通配路由必须最后注册等。

这些门禁的作用是让「靠自觉的纪律」变成机械断言——违反之后功能照常工作，
只有下一个人读代码时才发现，所以必须由 CI 拦。

## 决策记录

关键决策（含被否决的替代方案、真实踩过的坑、上游源码事实）在
[`docs/decisions/`](docs/decisions/)：ADR-001 ~ ADR-012 + `OPEN-DECISIONS.md`
（未决事项登记册）。**现行架构的权威是本仓库 ADR-010**
（`docs/decisions/ADR-010-queue-path-zero-billing.md`）；攒批见
`docs/decisions/ADR-011-batch-release-gating.md`，用户回调的地址准入与投递见
`docs/decisions/ADR-012-callback-address-admission.md`。

面向调用方的接口契约：任务链路见 [`docs/SPEC.md`](docs/SPEC.md)，
**回调对接见 [`docs/CALLBACK-CONTRACT.md`](docs/CALLBACK-CONTRACT.md)**。

注意：**本仓库与 stask-service 各有一套独立的 ADR 编号**，同一编号在两仓库含义
不同，交叉引用时必须写明仓库名（见 `docs/decisions/README.md`）。

## 部署

> **部署前请核对键名，并读 [`overview.md`](overview.md) 的「重要契约变化」**：
> 配置全部环境变量驱动，**键名 = `Settings` 字段名大写、不带前缀**（`.env.example`
> 是键名的完整清单）。**不提供任何旧键名或别名兼容**：未知变量一律被静默忽略
> （`extra="ignore"`，刻意的），因此**写错键名不会有任何提示**，网关会带默认值启动
> （默认 `DATABASE_URL` 指向 `root:root@127.0.0.1`，表现为连库失败而非配置报错）。
> 管理面 fail-closed（未配 `ADMIN_TOKEN` 即整片 404），`TASKIQ_ADMIN_API_TOKEN`
> 为必填。

```bash
cp .env.example .env       # 填库地址、上游白名单、回调白名单与回调签名密钥
docker compose up --build  # gateway + taskiq worker（内嵌 scheduler）+ redis + taskiq-admin
```

（MySQL 不在 compose 内：`DATABASE_URL` 指向与 new-api 共享的实例。）
依赖钉版唯一处是 `pyproject.toml`（无 `requirements.txt`）。关键环境变量
（**键名 = `Settings` 字段名大写、不带前缀**，`.env.example` 是配置项的完整清单，
由 `tests/test_static_gates.py` 保证不懈怠）：

- `DATABASE_URL`（指向与 new-api 共享的 MySQL 实例）
- `REDIS_URL`
- `UPSTREAM_BASE_URL` + `UPSTREAM_ALLOWLIST`（上游寻址与防 SSRF 白名单，
  **白名单为空即全部拒绝**）
- `CALLBACK_ALLOWLIST`（回调地址白名单，**不配则带回调地址的提交直接 400**——
  回调是网关的**出站**请求，默认开放等于默认暴露）
- `CALLBACK_SIGN_SECRET`（用户回调 HMAC 密钥，**必须强随机**）
- `ADMIN_TOKEN`（管理面 `X-Admin-Token`；未配则整个管理面 404）
- `TASKIQ_ADMIN_API_TOKEN`（taskiq-admin 看板）

`gunicorn.conf.py` 是全项目**唯一**允许直读 `os.environ` 的地方（它在 pydantic
单例之前由 master 进程加载）；其 worker 数与 timeout 由「与 new-api 共享 MySQL 的
连接预算」和「最长合法请求」反推，改之前先读文件头。

**发布前必做**：`CALLBACK_ALLOWLIST` 与 `UPSTREAM_ALLOWLIST` 都是 fail-closed 开关，
上线前先确认两者都已按实际对接方配置；`BATCH_SIZE` 保持默认 `0` 时无需其他改动。
