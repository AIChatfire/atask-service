# async-gateway — 异步任务网关

Python + FastAPI + Gunicorn + Logfire + Taskiq 的异步任务网关。统一承接 Kling / Seedance 等上游异步任务，对接已有微服务体系（pricing 计费逻辑 / billing 计费 / KeyManager 密钥租约 / 配置中心）。

**核心特征：网关零建表** —— 复用 NewAPI 单实例 MySQL 的 `tasks` 表（读写），身份由 billing 服务内省提供（不读 tokens/users），Redis 承担缓存/限流/幂等与 taskiq 队列。

## 架构

```
Client → Ingress → Gateway API × N（FastAPI 无状态）
                      │  kiq 投递
                      ▼
              taskiq broker（Redis ListQueueBroker，支持延迟任务）
                      │
        ┌─────────────┼──────────────────┐
        ▼             ▼                  ▼
  taskiq worker × M  结算/通知/探测    taskiq scheduler（每分钟触发补数巡检）
                      │
        ┌─────────────┼──────────────────┐
        ▼             ▼                  ▼
     Redis        NewAPI MySQL      已有微服务
     缓存/限流/     tasks（读写）     billing（freeze/settle/cancel + TTL 兜底 + /auth/inspect）
     幂等/去重                      pricing（asteval 表达式下发）
                                  KeyManager（上游 key 租约）/ 配置中心（路由下发）
```

### 资金闭环（三重兜底）

1. **taskiq 任务**：终态 CAS → `billing_settle_task` / `billing_cancel_task`（失败指数退避重投，超限落死信 `gw:events:dlq`）→ 回填 `data.settled`
2. **补数巡检**：`sweep_task` 每分钟扫「终态但 settled 未落」重发结算任务；「非终态且过期」重新入探测队列——队列层任何丢失都从这里修复
3. **billing TTL**：freeze 超时自动退款，最后防线

关键不变量：`request_id = task_id`（billing 幂等锚点）；状态迁移全部 CAS（恰好一次）；freeze 同步、settle/cancel 异步；创建类上游调用绝不重试。

### 并发与补数

- **worker 并发**：`--max-async-tasks 100`（单进程协程上限），加副本即水平扩容；探测/通知/结算全部按任务并行
- **补数**：队列丢失由 sweep 巡检自动补；手动补单 `POST /ops/requeue/{task_id}`；死信重放 `POST /ops/dlq/replay`
- **每令牌并发上限**：Redis 计数（`GW_MAX_CONCURRENT_TASKS`），终态自动释放

### 队列阻塞处置（runbook）

1. **观测**：`GET /ops/queue` 返回 `{pending, delayed, dlq, tasks_by_status}`；sweep 巡检每分钟自动检查，`pending > GW_QUEUE_WARN_DEPTH`（默认 500）或 `dlq > 0` 时打告警日志 + Logfire `queue_backlog` 事件。
2. **判读**：
   - `pending` 涨、`delayed` 平 → worker 消费能力不足；
   - `delayed` 涨 → 上游大面积超时（探测/重试在退避），先查上游；
   - `dlq > 0` → 有毒消息，查死信内容再重放。
3. **补并发**：`docker compose up -d --scale worker=4`，或调大 `--max-async-tasks`（注意 MySQL 连接预算同步核算）。
4. **补号**：积压消化后确认无遗漏——`POST /ops/dlq/replay` 重放死信；个别任务 `POST /ops/requeue/{task_id}` 立即补探测；sweep 每分钟自动兜底。
5. **安全**：`/ops/*` 配置 `GW_ADMIN_TOKEN` 校验，或在 Ingress 限制内网访问。

## API 形态

| 形态 | 路径 | 鉴权 |
|---|---|---|
| 通用任务 | `POST /{biz}/v1/tasks`、`GET /{biz}/v1/tasks/{id}`、`POST .../cancel` | POST 需 Bearer；GET 免鉴权（task_id 即凭证） |
| NewAPI 兼容 | `POST /{biz}/v1/videos`、`GET /{biz}/v1/videos/{id}` | 同上 |
| 动态透传 | `/{biz}/{原生路径}` | 写操作计费透传；GET 免费（按 IP 限流） |
| 回调入站 | `POST /callback/{biz}/{task_id}` | HMAC-SHA256 验签（每 biz 独立密钥） |

注意：**无 list 接口**（task_id 凭证体系下枚举即泄露）；需要列表时先给 billing 加鉴权再开放。

## 依赖的既有服务接口（providers 适配层）

所有微服务调用收口在 `app/services/providers/`：**协议（Protocol）与实现分离**，网关主体只依赖 `providers.billing / providers.pricing / providers.keys` 三个端口。换实现只改 `GW_*_PROVIDER` 配置；新增实现在该目录加一个类并注册名字即可，调用方零改动。

```
# billing（GW_BILLING_PROVIDER=newapi-billing，newapi-billing-service）
POST {BASE}/api/v1/auth/inspect    Authorization: Bearer <用户token>
  → 200 {"valid": true, "user_id": 123, "token_id": 45} / 401
POST {BASE}/api/v1/billing/freeze  Authorization: Bearer <用户token>
  body {request_id, biz_type, metric, amount, ttl_seconds, attrs}
  → 200 {..., "user_id": 123} / 401 / 402 / 409（幂等成功）
POST {BASE}/api/v1/billing/settle  Authorization: Bearer <服务账号token>   body {request_id, actual_amount}
POST {BASE}/api/v1/billing/cancel  Authorization: Bearer <服务账号token>   body {request_id}

# 定价（GW_PRICING_PROVIDER=model-meta）：按模型取规则，asteval 沙箱执行
GET  {BASE}/v1/models/{model}
  → {"billing": {"rule": "def calulate(request):\n ... return 1", "type": "second", ...}, ...}
  规则是完整函数定义（函数名 calulate/calculate/calc/compute/price 自动识别），
  入参 request 为用户请求体 dict，返回值为冻结金额；纯表达式形态自动兜底。

# 密钥（GW_KEY_PROVIDER=keypool，keypool-service，服务级 token 认证）
POST {BASE}/v1/key:get     Authorization: Bearer <GW_KEY_SVC_TOKEN>
  body {"group": "default", "model": "<model>", "retry": 0}
  → 提取 key/api_key、channel_id、key_index、base_url（字段名以实际服务为准）
POST {BASE}/v1/key:report  Authorization: Bearer <GW_KEY_SVC_TOKEN>  Idempotency-Key: <uuid>
  body {"channel_id", "key_index", "success", "status_code", "error_message"}

# 配置中心微服务
GET  {BASE}/gateway/routes         → {"data": {"version": 12, "routes": {"<biz>": {…RouteConfig…}}}}
```

**扩展一个新 provider 示例**（比如自建计费）：`providers/` 下新建 `my_billing.py` 实现 `BillingProvider` 协议四个方法 → `__init__.py` 工厂里注册 `"my-billing"` → 配置 `GW_BILLING_PROVIDER=my-billing`，网关其他代码不动。

## 快速开始

```bash
cp .env.example .env          # GW_DATABASE_URL 指向 NewAPI 的 MySQL；Redis 由 compose 容器化
docker compose up -d          # redis + gateway + worker + scheduler（MySQL 复用 NewAPI 实例，不在编排内）
# 或手动：
gunicorn -c gunicorn.conf.py app.main:app
taskiq worker    app.queue:broker --max-async-tasks 100
taskiq scheduler app.queue:scheduler
```

## 配置

- 环境变量见 `.env.example`（`GW_` 前缀）

### 动态路由：三级来源（优先级从高到低）

1. **Redis 热覆盖**（应急操作，秒级生效）：
   ```
   HSET gw:routes:override <biz> '{"enabled": false}'   # 紧急下线某上游
   INCR gw:config_ver
   ```
2. **配置中心微服务**（主源）：按 `version` 增量合并；拉取失败保留最后成功快照。
3. **本地 `gateway-routes.yaml`**：兜底/bootstrap —— 进程重启且配置中心从未成功拉取时接管。

### 上游状态自动映射（兼容 NewAPI tasks 状态）

目标枚举：`SUBMITTED / QUEUED / IN_PROGRESS / SUCCESS / FAILURE / CANCELED`。三级映射：

1. 路由配置里的 `status_map`（显式覆盖，配置中心/Redis 可热更，免发版接入新上游）
2. **内置字典全枚举**：归一化后精确匹配（success/succeeded/completed/failed/expired/canceled/queued/processing/…）
3. **前缀猜测**：归一化（小写、折叠分隔符、剥离 `task_status_`/`state_` 等命名空间前缀）后前缀命中，兼容 `TASK_STATUS_SUCCEED`、`task.succeeded`、`State: Running` 等风格

未识别的状态：不崩溃、不错误推进——回调忽略 / 轮询下轮再探，并打 warning 日志（每进程每种只报一次）。**扩展流程**：日志发现未知状态 → 先在该 biz 的 `status_map` 应急 → 稳定后回流 `app/services/statusmap.py` 内置字典。

## 运维手册

1. **MySQL**：`transaction_isolation=READ-COMMITTED`、`sql_mode` 严格模式、开 binlog + 每日全量备份（单实例底线）。
2. **tasks 表索引**（一次性 DDL，升级 NewAPI 后复查）：
   ```sql
   ALTER TABLE tasks ADD INDEX idx_status_sweep (status, updated_at);
   ```
3. **NewAPI 共存**：网关任务以 `platform='gateway'` 写入，上线前实测 NewAPI 的后台任务逻辑不会捞取这些行。
4. **死信**：`gw:events:dlq` 有消息即告警，人工处理后重投（重新 kiq 对应任务）或补结算。
5. **连接数**：`(API 进程数 + worker 进程数) × (pool 20 + overflow 10) + NewAPI 占用 ≤ max_connections × 0.8`。
6. **日志纪律**：任务终态结构化记录 result（上游结果直链）便于排查；脱敏仅针对凭证类字段（Authorization/Bearer/sk-）。
7. **轮询降噪**：GET 状态查询、healthz/readyz 不产生 Logfire span；上游探测的 httpx 调用不埋点（控制面 billing/pricing/keyman/配置中心保留完整 trace）。任务状态**仅在发生变化时**记录一条 `task_status_changed`（GET、探测、回调三个观察者共享 Redis 去重）。
8. **扩缩容**：API 与 worker 独立伸缩；队列堆积先加 worker 副本，再调 `--max-async-tasks`。

## 上线前 checklist

- [ ] 核对 new-api `tasks` 表列名/类型与 `models.py` 注释一致
- [ ] billing 服务实现 `/api/v1/auth/inspect`，freeze 响应带 `user_id`，settle/cancel 支持服务账号
- [ ] 配置中心 `/gateway/routes` 下发每个 biz 的 `submit_path`/`probe_path`/`task_id_path`/`status_path` 与真实上游对齐；`gateway-routes.yaml` 作为兜底同步维护
- [ ] 用真实上游报文验证状态自动映射（各状态各打一条日志确认归类正确）
- [ ] videos 路由响应字段与你们 new-api 版本做契约测试
- [ ] 压测定 gunicorn workers 数（n~2n 起步）与 taskiq `--max-async-tasks`
- [ ] 演练：kill worker 在 settle 前 —— sweep 补数 + TTL 应自动收敛
