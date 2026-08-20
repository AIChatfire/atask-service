# atask-service（异步 AI 网关）

异步任务型 AI 模型的统一网关：对上承接用户请求（鉴权/计费/幂等），对下适配
任意异步上游（视频生成等），与 new-api 生态共用用户体系、钱包与渠道配置。

**核心特点**

- **两微服务协同**：上游凭证、渠道配置与**计费规则**（渠道 gateway 块
  `billing.rule`，随租约下发、网关本地沙箱求值）都走
  [keypool-service](https://github.com/AIChatfire/keypool-service)，资金走
  [newapi-billing-service](https://github.com/AIChatfire/newapi-billing-service)；
  网关本身无状态（自有状态全在 Redis），零自有 MySQL 表（只读写 new-api
  `tasks` 表）。~~pricing-service~~ 已废弃（定价从"按模型全局价"变
  "按渠道价"，贴渠道成本）。
- **傻瓜式模型接入**：接入新模型 = 在 new-api 建一个渠道 + 在渠道 gateway
  块配 `billing` 计费规则，**网关零代码改动、零路由文件**。
- **计费闭环**：提交顶格预估 freeze → 终态按实际用量 settle（多退少补）/
  cancel（全额解冻），全链路幂等。

## 架构

```
用户 ──► gateway（FastAPI）
          │  Authorization: Bearer sk-...（new-api 个人令牌）
          │
          ├─► newapi-billing-service   /api/v1/auth/inspect（身份内省）
          │                            /api/v1/billing/freeze|settle|cancel|renew
          ├─► keypool-service          POST /v1/keys/select（租约+渠道全量元数据
          │                            + billing 计费规则，本地沙箱求值零远程调用）
          │                            POST /v1/keys/report（用量/失败上报，驱动自动禁启）
          │
          ├─► 上游（minimax / seedance / kling / …）提交 + 探测/回调
          │
          ├─► Redis（taskiq 队列、幂等键、令牌会话、熔断、缓存）
          └─► MySQL（new-api tasks 表，任务事实源）

后台：taskiq worker（探测/结算/通知/HELD 排空执行 + 内嵌 scheduler 延迟派发与每分钟
sweep 补数；scheduler 必须单副本，worker 扩副本时拆回独立服务）
```

## 傻瓜式接入新模型（3 步，网关零改动）

以 MiniMax-H3 为例：

**① new-api 建渠道**（keypool 复用 new-api 渠道体系）：

- 挂到**统一分组**（默认 `keypool`，`GW_KEY_GROUP` 可改）下集中维护；
  选渠道 = `select(group, model)`，**model 决定渠道**，与 URL 路径解耦；
- **biz 从渠道取**：`setting.gateway.biz` 显式指定 → 渠道 `name` → URL 段兜底；
- 凭证、`base_url`、`model_mapping`、`param_override`、`header_override`、
  `status_code_mapping`、`setting.proxy` 全部在渠道上配；
- 网关提取配置放渠道的 `header_override.upstream` 嵌套块（与
  `setting.gateway` 两处等价任选，优先级从高到低；仅支持当前同构配置位，
  不再兼容旧版 `other.gateway`；装配请求头时会自动剥离，不会作为 HTTP 头
  透给上游）：

```json
{
  "header_override": {
    "upstream": {
      "biz": "minimax",
      "submit_path": "/v2/video_generation",
      "probe_path": "/v2/query/video_generation/{upstream_task_id}",
      "status_path": "task.status",
      "result_path": "task.content.url",
      "error_path": "task.error",
      "settle_usage_map": {"duration": "task.usage.output_seconds"},
      "pricing_biz_type": "video_generation"
    }
  }
}
```

**② 渠道配计费规则**：在上面同一块网关配置里加 `billing` 子块（规则唯一
事实源 = keypool 渠道元数据，随租约下发，网关本地 asteval 沙箱求值，
零额外远程调用；`rule` 为 Python 函数 `calulate(request)`，返回 USD 金额，
结算 = 规则值 × `discount_rate`）：

```json
"billing": {
  "rule": "def calulate(request):\n    return round(float(request.get('duration') or 5) * 0.026, 6)",
  "type": "second",
  "discount_rate": 1.0
}
```

**③ 完成**。提交报文以用户请求体为基底原样透传（MiniMax 的
`content[]` 多模态结构——t2va/i2va/首尾帧/r2va——无需网关理解），网关自动：
渠道模型映射改写、参数覆盖合并、回调注入（`supports_callback` 时）、
按路径提取 task_id/状态/结果、终态用实际产出秒数重跑规则结算。

**可选 · 产物转存/镜像**：渠道 gateway 块加
`"result_url_template": "https://myhost.com/{upstream_result_url}"`，终态时
`result_path` 提取出的上游直链按模板改写。占位符：`{upstream_result_url}`
（原样）、`{upstream_result_url_encoded}`（百分号编码，当查询参数值时用）、
`{upstream_result_url_no_scheme}`、`{upstream_result_host}`、
`{upstream_result_path}`、`{task_id}`。改写后的镜像链在任务视图、用户回调
与原生查询报文中**全面替换**上游直链（原始直链另存 `data.upstream_result`
供对账/回源）。网关不搬运字节，转存由模板指向的服务负责。

字段全集与缺省值见 `app/schemas.py:RouteConfig` 与
`app/services/registry.py:_GATEWAY_DEFAULTS`。

## API 形态

| 端点 | 说明 |
|---|---|
| `POST /{biz}/v1/tasks` | 通用任务提交（202；需 `Authorization` + 可选 `Idempotency-Key`） |
| `GET /{biz}/v1/tasks/{task_id}` | 任务查询（task_id 即凭证，免鉴权） |
| `POST /{biz}/v1/tasks/{task_id}/cancel` | 取消任务 |
| `POST /{biz}/v1/videos` | new-api 兼容视频形态（与 tasks 共用流程） |
| `GET /{biz}/v1/videos/{task_id}` | 视频任务查询 |
| `POST /callback/{biz}/{task_id}` | 上游 webhook 入站（HMAC 验签 + 去重） |
| `ANY /{biz}/{原生路径}` | 动态透传（GET 免费按 IP 限流；写方法计费透传） |
| `POST /{biz}/{渠道 submit_path}` | **原生提交**：报文与上游同构，返回本地 task_id，零上游往返秒级受理 |
| `GET /{biz}/{渠道 probe_path}` | **原生查询**：本地 id / 上游 id 都认，报文与上游同构（上游 id 改写为本地 id） |
| `POST /{biz}/{渠道 cancel_path}` | **原生取消**：走本地 cancel（解冻 + 尽力源头止损） |
| `GET /ops/queue`、`GET /ops/tasks/{task_id}`、`POST /ops/requeue/{task_id}`、`POST /ops/dlq/replay` | 运维（`X-Admin-Token`） |
| `GET /healthz/live`、`GET /healthz/ready` | 探针 |

### 原生形态接入（零客户端改造）

想让客户端**一行代码不改**地从直连上游切到网关？直接打上游原生路径即可——
网关按渠道配置的三个路径模板识别生命周期端点（判定零硬编码）：

```bash
# 提交：请求体与上游完全一致，立刻拿到本地 task_id（不等上游）
curl -X POST https://gw.example.com/minimax/v2/video_generation \
  -H 'Authorization: Bearer sk-user-xxx' -H 'Content-Type: application/json' \
  -d '{"model":"MiniMax-H3","content":[{"type":"text","text":"..."}],"duration":5}'
# → {"task_id":"minimax_5f2c...e91"}      ← 与上游报文同构，值是本地 id

# 查询：拿本地 id 直接查原生端点，报文与上游逐字节同构
curl https://gw.example.com/minimax/v2/query/video_generation/minimax_5f2c...e91
# → {"task":{"id":"minimax_5f2c...e91","status":"succeeded","content":{...}}}
```

要点：响应形状由渠道 `task_id_path`/`probe_task_id_path`/`status_path`/
`result_path`/`ok_check` 反向构建；上游任务 id 只存在于 `tasks.data`，对外
一律回显本地 id；上游还没接单时返回排队态快照（200，绝不 404）；其余原生
路径维持原样流式透传。

## 计费闭环与可靠性

- **幂等**：客户端 `Idempotency-Key` → 任务级去重（freeze 前短路，绝不重复
  冻结）；billing `request_id = task_id` 唯一约束兜底。
- **结算三档**：`actual_amount_path`（上游实收）→ `settle_usage_map`
  （按实际用量重跑渠道计费规则）→ 冻结金额兜底（绝不静默按 0 结算）。
- **用户令牌结算**：billing 只认用户令牌身份（跨用户 403）；冻结成功后令牌
  按 task_id 暂存 Redis（48h TTL，终态即清），Redis 丢失由 billing 冻结
  TTL 到期自动解冻兜底——资金不会锁死。
- **推进双通道**：上游支持 webhook 走回调（验签+去重），否则 taskiq 延迟探测
  （退避升档 5s→120s）；超 `GW_POLL_MAX_AGE_SECONDS` 转 FAILURE 并解冻。
- **提交有限重试**：仅换 key 可能改变结果的确定性拒绝（默认 401/403/429，
  `GW_SUBMIT_RETRYABLE_STATUS_CODES` 可配）换租约重打（≤3 次，只冻结一次）；
  模糊失败（超时/5xx）绝不重试（防双重创建双扣费）；任务级 4xx 立即判死。
- **HELD 挂起**：账户级故障（欠费/封禁，渠道 `error_classify` 显式配置才判定）
  → 任务挂起保留冻结、202 照常返回；sweep 续期保活（billing renew 只推
  expires_at 不动钱），补费后金丝雀排空（重新租约不钉渠道，1m→5m→15m 退避），
  超 `GW_HOLD_MAX_AGE_SECONDS`（默认 4h）判死解冻。
- **不亏本兜底**：孤儿任务（非终态且无 upstream_task_id 超 30min，`GW_ORPHAN_GRACE_SECONDS`）sweep 收口
  解冻；反向对账抽查"本地 FAILURE/已退 但上游 SUCCESS"告警台账；失败单按渠道
  `failed_billing: charge|absorb` 策略结算或解冻；渠道配 `cancel_path` 时取消/
  超时尽力调上游取消端点源头止损。
- **兜底收敛**：每分钟 sweep 巡检——在途任务停滞重投探测、终态未结算重发
  计费事件（billing 幂等，重发安全）、冻结临期续期、HELD 维护、队列积压/死信
  告警，`/ops/dlq/replay` 补号。
- **熔断与上报**：每 biz 失败计数熔断（30s/10 次）；探测按错误分类表接线——
  key 级失效上报 keypool 驱动换 key、429 按 Retry-After 拉长退避、账户级只告警
  不误报；提交 4xx/5xx 结果实时上报驱动坏 key 自动禁用。
- **观测**：日志统一走 loguru（`app/logging.py` 装配 stderr sink 并桥接
  stdlib，`GW_LOG_LEVEL` 控制级别，排障调 DEBUG 即可看全链路；令牌/上游
  key 绝不进日志）。`GET /ops/tasks/{task_id}` 提供任务诊断视图（含
  task_id → 令牌会话存在性/TTL——渠道侧轮询不带 sk 时的查询处）。
  `GW_LOGFIRE_ENABLED=true` 接入 logfire（web 由 main 装配，taskiq
  worker 由队列中间件装配）；**状态变化唯一记录点**是 statelog——Redis 去重，
  只在任务状态变化时发一条 `task_status_changed`（运行中连探多轮零事件）；
  队列中间件成功路径静默、失败才发 `taskiq_task_failed`（绝不带任务参数）；
  探测连续失败按 1/5/20 档升档告警（statelog 失败计数，成功清零）；
  `GW_TASKIQ_ADMIN_URL`/`GW_TASKIQ_ADMIN_API_TOKEN` 配好后 taskiq-admin
  看板（compose 内 `127.0.0.1:3000`）展示任务执行层（args 脱敏不上报）。

## 本地开发

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest tests/ -q      # 75 项断言：三服务契约 / 引擎 / 状态映射 /
                                          # 创建与结算链路 / 探测 / MiniMax-H3 端到端
.venv/bin/python -m ruff check app tests
```

单测不依赖真实 MySQL/Redis/上游/微服务（respx 拦截 HTTP，FakeRedis +
内存 taskstore）。

## 部署

```bash
cp .env.example .env       # 填三微服务地址与 GW_KEY_SVC_TOKEN
docker compose up --build  # gateway + taskiq worker + scheduler + mysql + redis
```

关键环境变量（全部 `GW_` 前缀，见 `.env.example` 注释）：
`GW_DATABASE_URL` / `GW_REDIS_URL` / `GW_BILLING_SVC_URL` /
`GW_PRICING_SVC_URL` / `GW_KEY_SVC_URL` + `GW_KEY_SVC_TOKEN` /
`GW_GATEWAY_PUBLIC_BASE_URL`（注入上游回调的公网基址）。
