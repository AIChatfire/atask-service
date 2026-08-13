# atask-service（异步 AI 网关）

异步任务型 AI 模型的统一网关：对上承接用户请求（鉴权/计费/幂等），对下适配
任意异步上游（视频生成等），与 new-api 生态共用用户体系、钱包与渠道配置。

**核心特点**

- **三微服务协同**：上游凭证与渠道配置走
  [keypool-service](https://github.com/AIChatfire/keypool-service)，计费规则走
  [pricing-service](https://github.com/AIChatfire/pricing-service)，资金走
  [newapi-billing-service](https://github.com/AIChatfire/newapi-billing-service)；
  网关本身无状态（自有状态全在 Redis），零自有 MySQL 表（只读写 new-api
  `tasks` 表）。
- **傻瓜式模型接入**：接入新模型 = 在 new-api 建一个渠道 + 在 pricing 配一
  条计费规则，**网关零代码改动、零路由文件**。
- **计费闭环**：提交顶格预估 freeze → 终态按实际用量 settle（多退少补）/
  cancel（全额解冻），全链路幂等。

## 架构

```
用户 ──► gateway（FastAPI）
          │  Authorization: Bearer sk-...（new-api 个人令牌）
          │
          ├─► newapi-billing-service   /api/v1/auth/inspect（身份内省）
          │                            /api/v1/billing/freeze|settle|cancel
          ├─► pricing-service          GET /v1/models/{model}（billing.rule 沙箱求值）
          ├─► keypool-service          POST /v1/keys/select（租约+渠道全量元数据）
          │                            POST /v1/keys/report（用量/失败上报，驱动自动禁启）
          │
          ├─► 上游（minimax / seedance / kling / …）提交 + 探测/回调
          │
          ├─► Redis（taskiq 队列、幂等键、令牌会话、熔断、缓存）
          └─► MySQL（new-api tasks 表，任务事实源）

后台：taskiq worker（探测/结算/通知执行）+ taskiq scheduler（延迟派发 + 每分钟 sweep 补数）
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
  `setting.gateway`、`other.gateway` 三处等价任选，优先级从高到低；
  装配请求头时会自动剥离，不会作为 HTTP 头透给上游）：

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

**② pricing-service 配计费规则**：`GET /v1/models/MiniMax-H3` 返回
`billing.rule`（Python 函数 `calulate(request)`，返回 USD 金额）与
`discountRate`（结算 = 规则值 × 折扣率）。

**③ 完成**。提交报文以用户请求体为基底原样透传（MiniMax 的
`content[]` 多模态结构——t2va/i2va/首尾帧/r2va——无需网关理解），网关自动：
渠道模型映射改写、参数覆盖合并、回调注入（`supports_callback` 时）、
按路径提取 task_id/状态/结果、终态用实际产出秒数重跑规则结算。

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
| `GET /ops/queue`、`POST /ops/requeue/{task_id}`、`POST /ops/dlq/replay` | 运维（`X-Admin-Token`） |
| `GET /healthz/live`、`GET /healthz/ready` | 探针 |

## 计费闭环与可靠性

- **幂等**：客户端 `Idempotency-Key` → 任务级去重（freeze 前短路，绝不重复
  冻结）；billing `request_id = task_id` 唯一约束兜底。
- **结算三档**：`actual_amount_path`（上游实收）→ `settle_usage_map`
  （按实际用量重跑 pricing 规则）→ 冻结金额兜底（绝不静默按 0 结算）。
- **用户令牌结算**：billing 只认用户令牌身份（跨用户 403）；冻结成功后令牌
  按 task_id 暂存 Redis（48h TTL，终态即清），Redis 丢失由 billing 冻结
  TTL 到期自动解冻兜底——资金不会锁死。
- **推进双通道**：上游支持 webhook 走回调（验签+去重），否则 taskiq 延迟探测
  （退避升档 5s→120s）；超 `GW_POLL_MAX_AGE_SECONDS` 转 FAILURE 并解冻。
- **兜底收敛**：每分钟 sweep 巡检——在途任务停滞重投探测、终态未结算重发
  计费事件（billing 幂等，重发安全）、队列积压/死信告警，`/ops/dlq/replay` 补号。
- **熔断与上报**：每 biz 失败计数熔断（30s/10 次）；上游 4xx/5xx 结果实时
  上报 keypool 驱动坏 key 自动禁用。
- **观测**：`GW_LOGFIRE_ENABLED=true` 接入 logfire（web 由 main 装配，taskiq
  worker 由队列中间件装配）；**状态变化唯一记录点**是 statelog——Redis 去重，
  只在任务状态变化时发一条 `task_status_changed`（运行中连探多轮零事件）；
  队列中间件成功路径静默、失败才发 `taskiq_task_failed`（绝不带任务参数）。

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
