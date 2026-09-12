# stask-service 设计 — 同步转异步任务网关

**版本**：v1.2　**日期**：2026-09-12　**状态**：定稿评审

同步生成 API（出图/TTS）加 `/async` 前缀即任务化：毫秒返回本地 task_id，结果异步取回。计费零代码——资金操作全部在 new-api 原生 relay 内闭环。atask-service（异步任务网关）对外统一 `/batch/{上游路径}` 前缀（ADR-010），与本服务前缀互斥、共用同一 nginx 域名与 new-api tasks 表（隔离见 §3）。

## 1. 架构（三网关共域名）

```
nginx（同一域名）
├─ /batch/ → atask-service   （异步任务中继，ADR-010）
├─ /async/ → stask-service   （本服务：同步转异步）
└─ 其余    → new-api          （同步 relay 原生直连）

stask web   提交：幂等占位 + 余额额度占槽 → 落库 SUBMITTED → 令牌会话 → 入队 → 返回 task_id
stask worker 执行：派发锁 → 用户 sk 调 new-api relay → 响应即终态落库 → 释放槽/清会话/可选回调
new-api     渠道选择 / 配额扣费（预扣+实结）/ 限流 / 消费日志，零改动
```

状态机：`SUBMITTED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`。无冻结、无孤儿判死、无 HELD。

红线：禁止跨服务读库（余额/日志走 HTTP）；sk 不落库不进日志（仅 Redis 令牌会话，终态即清）；tasks 表时间列经 `as_unix_seconds` 归一；错误响应统一 `{"error": {...}}`；运维端点（`/ops/*`、`/healthz` 等）不进公网域名（nginx 单独 location 或内网端口，防被默认 location 打到 new-api）。

## 2. API

### 提交 `POST /async/{path}`（通配，仅 POST/PUT）

- 剥前缀后 path/query/body 原文存储原样转发（浅解析 body 只为提 model）
- 路径准入：`ST_ASYNC_ALLOW_PREFIXES` 白名单 + deny-list 硬拒 `/api/`、`/console/`、`/batch/`（atask 的地盘，走它自己的端点）；剥前缀后存储路径仍以 `/async/` 开头 → `400 double async prefix`（回环形态下 nginx 会把该路径路由回 stask 套建任务：外层终态响应退化为内层 202 受理回执、外层提前 SUCCESS、双占并发槽——静默语义破坏，必须准入即拒）
- `Idempotency-Key` SET NX 原子占位，真并发 409；可选 `X-Callback-Url` 头
- 响应：`202 + {task_id, status}` + `Location: /async/{path}/{task_id}` 头

### 查询 `GET /async/{path}/{task_id}`（task_id 取最后一段，形态正则预筛）

| 状态 | 响应 |
|---|---|
| SUBMITTED / IN_PROGRESS | `202` + `{task_id, status, created_at}`；支持 `?wait=30` 长轮询 |
| SUCCESS | `200` + 上游原生响应体字节级回放（gzip 解压，Content-Type 原样） |
| FAILURE / CANCELED | 重放上游错误状态码 + 原文；本地失败用 `{"error": {...}}` |

### 取消 `DELETE /async/{path}/{task_id}`

排队中 → CANCELED（零资金动作）；执行中 → 409（同步调用不可中止）。

## 3. 数据模型（复用 new-api tasks 表，与 atask 隔离）

**与 atask 双写隔离的三道闸**（atask 侧对称成立）：

1. **platform 列**：stask 行统一 `platform = 'stask'`（atask 行统一 `platform = 'atask'`，配置 `GATEWAY_PLATFORM`）——双方的 CAS / sweep / 计数全部带 `platform = :p` 过滤，天然互不可见；
2. **data.source**：stask 行 `source='stask'`（atask 的中继行 `source='batch'`），作为第二道过滤；
3. **视图校验**：查询/取消链路按 task_id 取到行后校验 `source='stask'`，不是自家任务 → `404`（atask 的 `get()` 是裸主键查询，跨家 task_id 打到对方端点必须靠这层挡住误读）。

task_id：固定前缀 `stask_{uuid4hex}`（同域名双网关并存，人眼可辨归属，不取 model slug）；channel_id 执行前 0、执行后尽力回填；data JSON：

```jsonc
{
  "source": "stask", "model": "...", "token_hash": "...",
  "idempotency_key": "...", "callback_url": "...",
  "request_method": "POST", "request_path": "/v1/images/generations",
  "request_query": "...", "request_headers": {...}, "request_body": {...},
  "upstream_base_url": "http://newapi:3000",
  "inflight_slot": true,                          // 终态清
  "upstream_response": "<gzip bytes>",            // 终态原文，保留期 24h
  "upstream_content_type": "...", "dispatch_epoch": 0
}
```

（无 `freeze_amount` / `settled` 字段——ADR-010 后 atask 的 sweep 按 platform + source 过滤，无需旧版"骗 sweeper"的占位字段。）

## 4. 提交流程（web，毫秒级）

```
token 限流 ‖ 幂等占位
→ 身份内省 + 余额（Redis 缓存 30s）
→ upstream 地址校验（§7）+ 路径准入
→ 余额额度占槽（Redis Lua：算额度→查占用→占槽/429+Retry-After）
→ 落库 → 令牌会话 → 入队 → 202
```

失败即回滚：CAS 归还占位 + 归还槽。

## 5. 执行流程（worker）

```
出队 → CAS SUBMITTED→IN_PROGRESS
→ 派发锁 SET NX（TTL=超时）；锁被占 → 不重发，转超时对账
→ 取 sk → 调 new-api relay（原样 method/path/query/body + X-Task-Id）
→ 分流 → 终态落库（gzip）→ 释放槽 + 清会话 → 可选回调
```

派发锁防双扣：队列 at-least-once，崩溃重投不得再次调用 new-api（"锁在 = 可能已扣费"）。

## 6. 余额并发额度

```
slots = clamp(floor(balance / ref_price), 1, ST_MAX_SLOTS)
```

- 语义 = 余额付得起几个在途任务（100/单价100 → 1 路，1000 → 10 路）
- 入队即占、终态释放；软限制（存量不回收）；槽键 TTL 兜底 + 定时校准
- 余额走 new-api HTTP + 30~60s 缓存

## 7. upstream 寻址（nginx 共域名）

优先级：`X-Upstream-Base-Url` 头（nginx 注入）> `ST_NEWAPI_BASE_URL`。提交时校验后随任务落库，worker 用落库值。

安全三防线（防 sk 打到野地址）：nginx `proxy_set_header` 无条件覆盖客户端同名气头；host 必须命中 `ST_UPSTREAM_ALLOWLIST` 否则 400；仅 http(s)、拒绝 URL userinfo。

```nginx
location /batch/ {
    proxy_pass http://atask:8000;
    proxy_set_header X-Upstream-Base-Url "http://newapi:3000";
}
location /async/ {
    proxy_pass http://stask:8000;
    proxy_set_header X-Upstream-Base-Url "http://newapi:3000";
    proxy_read_timeout 65s;
}
location / { proxy_pass http://newapi:3000; }
# atask/stask 的 /ops /admin /healthz 不进公网域名（内网端口直连），
# 否则会被默认 location 打到 new-api
```

## 8. 失败分流与超时对账

| 情形 | 处理 | 资金 |
|---|---|---|
| 2xx | SUCCESS | 已实结，零动作 |
| 4xx（401/402/429/参数） | FAILURE | 未扣/已回滚 |
| 5xx / 网络错 | 退避重试 ≤3 次 → FAILURE | 依赖开放问题① |
| 超时 | **绝不判死**，标记 `reconcile_pending` | 见下 |

超时对账：定时按 `X-Task-Id` 查 new-api 消费日志（new-api 记录该头为唯一改造请求；否则降级 token+时间窗模糊匹配）→ 有成功扣费记录：补记 SUCCESS（result 缺失告警）；无记录：FAILURE；查不到：挂起，超 `ST_RECONCILE_TTL` 转人工。

## 9. 结果存储

gzip 落库（b64 压缩率 70%+，10MB 上限）；保留 `ST_RESULT_TTL_SECONDS`（默认 24h）后定时清空 `upstream_response`，状态行保留。

## 10. 配置项（`ST_` 前缀，`.env.example` 全量样例）

```
ST_NEWAPI_BASE_URL          默认 upstream
ST_UPSTREAM_ALLOWLIST       允许的 host 列表
ST_ASYNC_ALLOW_PREFIXES     路径白名单（如 /v1/images,/v1/audio）
ST_ASYNC_DENY_PREFIXES      硬拒前缀（默认 /api/,/console/,/batch/）
ST_BALANCE_CACHE_TTL=30     余额缓存秒
ST_REF_PRICE_{MODEL}        参考单价
ST_MAX_SLOTS=10             单用户并发额度上限
ST_RATE_LIMIT               token 提交速率
ST_RETRY_MAX=3              5xx 重试次数
ST_WORKER_TIMEOUT=120       relay 超时秒
ST_RECONCILE_TTL=86400      挂起转人工秒
ST_RESULT_TTL_SECONDS=86400 结果保留秒
ST_RESPONSE_MAX_BYTES       响应体落库上限（默认 10MB）
ST_BODY_MAX_BYTES           提交体落库上限
ST_QUEUE_CONCURRENCY        worker 并发度
ST_CALLBACK_SECRET          回调签名密钥
ST_PLATFORM=stask           tasks 表 platform 列取值（与 atask 隔离的第一道闸）
```

## 11. 部署 / 测试 / 实施

- 部署：web（gunicorn+uvicorn）+ worker（taskiq）+ Redis（独立实例）；compose 单机起步，worker 可自由扩副本
- 测试：pytest + respx + FakeRedis。重点：幂等真并发、额度边界与 429、占位/占槽失败回滚、路径准入（白名单/deny/双前缀/`/batch/` 拒绝）与 upstream 头校验、四类分流、派发锁防重投、对账三态、202/200 回放与错误码重放、长轮询、跨家 task_id → 404（source 校验）
- 实施顺序：表契约与状态机 → 提交链路 → worker+派发锁+分流 → 查询/回放/取消 → 超时对账 → 回调与观测 → 结果清理 → 压测

## 12. 开放问题

1. new-api relay 5xx 预扣配额是否确定回滚？（决定重试安全性）
2. 超时误判的退款通道：new-api 管理 API 还是人工台账？
3. ref_price 来源：配置 vs new-api 定价接口？
4. channel_id 回填来源：响应头 vs 消费日志？
5. 超大响应是否外置对象存储？
6. 是否与 atask 共用 Redis 实例（倾向隔离）
