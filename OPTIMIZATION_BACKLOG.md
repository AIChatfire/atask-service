# atask-service 总改动清单（2026-08-13 讨论汇总）

> 性质：优化方案 backlog。**实施状态（2026-08-14）**：代码项全部完成并回归
> （138 测试通过）——[1][2][3][4][5][6][7网关侧][8][9][10][11][12][15]
> 已落地；[7] billing 侧在 billing 仓库完成；[13] 为运营项（非代码）。
> **[14] adapter 框架已移除（2026-08-14）：该项目只做同构适配，渠道间无语义差异，
> 声明式配置足够，不引入 adapter 插件层。**
> 资金来源纪律贯穿全清单：freeze 顶格 → settle 实收优先 → 只有确认上游没扣钱才 cancel；
> 绝不静默按 0 结算。
>
> **追加（2026-08-14 `/minimax/v2/video_generation` 失败根因诊断）**：[16]–[19] 新增，
> 来源为诊断报告"修复 + 其他建议"部分；[16] 为线上在炸级（裸 400 + 用户令牌泄漏上游）。

## 优先级与依赖总览

```
P0  [1] 鉴权 Header bug（线上在炸）
P0  [16] proxy 双 Authorization 头（裸 400 + sk 泄漏上游，补丁已备）
P1  [2] sk 鉴权统一 ────────────── 前置确认：new-api 轮询是否带 token
P1  [3] 错误分类表（账户级/任务级/key级）──┬─► [6] HELD 挂起 ──► [7] billing renew
                                            └─► [4] 探测 key 时效接线
P1  [4] 探测链路 key 时效接线（report/换 key/熔断）
P1  [8] 不亏本三窟窿（失败单计费/反向对账/孤儿任务）
P1  [15] billing.rule 从 keypool 渠道元数据取，废弃 pricing-service
P1  [17] 配置与密钥卫生（public base URL 错值 + change-me 占位密钥）
P2  [5] 提交有限重试（依赖 [3] 分类表）
P2  [9] logfire healthz 排除
P2  [10] 轮询降噪补强
P2  [18] 历史异常排查（轮询超时单位口径 + relay 空 host 报错）
P2  [19] GET 免费透传选渠道（model="" 一律 404）
P3  [11] taskiq-admin 看板
P3  [12] scheduler 合并部署
P3  [13] 运营项（双账号冗余/余额预警，非代码）
```

全局收尾：每项完成跑 `.venv/bin/python -m pytest tests/ -q` + `ruff check app tests`；
[1][2] 同文件（auth.py），顺序提交避免冲突。

---

## P0 · [1] 鉴权 `AttributeError: 'Header' object` 根除

根因：`proxy.py:41` 以普通函数调用 `preflight(biz, request)`，DI 不生效，
`authorization` 形参拿到默认值 `Header(None)` 的返回值（`params.Header` 实例）。

- [ ] `app/deps/preflight.py:49-54` 删 `authorization` / `idempotency_key` 两个
  `Header(...)` 形参，改从 `request.headers.get(...)` 读取（L55 前）
- [ ] `app/deps/preflight.py:13` import 移除 `Header`
- [ ] `app/deps/auth.py:27-30` 删除 `HeaderParam` isinstance "临时规避"
- [ ] `app/deps/auth.py:31` 改防御式判型：`not isinstance(authorization, str) or not startswith("Bearer ")` → 401
- [ ] 全局 grep 依赖函数被直接调用的同模式点（`await preflight(`、`require_token(`）
- [ ] 测试：proxy 计费透传无 Authorization → 401（非 500）；错误前缀 → 401；合法 sk → 走 inspect

## P0 · [16] proxy 双 Authorization 头（裸 400 + 用户 sk 泄漏上游）

来源：2026-08-14 `/minimax/v2/video_generation` 失败根因诊断。根因：
`proxy.py` `_forward_headers` 头合并 bug —— Starlette `request.headers.items()`
返回全小写头名，`auth_headers()` 注入大写 `Authorization`，dict 并集大小写敏感
→ 网线上两个 Authorization 头并存 → 上游边缘（Tengine/WAF）裸 400；
同时用户 sk- 令牌被透传上游（安全隐患）。已用"双头发 metaso 必 400、单头正常"
逐字节复现确认，补丁文件 `fix-proxy-duplicate-authorization.patch` 已备。

- [ ] 应用补丁：`HOP_BY_HOP` 增加 `authorization`（用户 sk 绝不透传上游）
- [ ] `_forward_headers` 合并前对 `extra` 小写归一，按小写名剔除客户端同义头，
  保证每个头恰好出现一次（extra 优先）——同时覆盖 `X-Api-Key`、`header_override`
  等一切大小写碰撞场景
- [ ] 测试：bearer / x-api-key 两种场景网线上恰好一个鉴权头（渠道凭证），用户凭证不透出
- [ ] 发版前临时绕行已验证可用：`POST /minimax/v1/tasks`（走 `upstream.submit()` 单头）+
  `GET /minimax/v1/tasks/{task_id}` 查询
- [ ] 发版后回归：复现用户 curl 原生路径应返回正常 JSON 而非 Tengine 400 HTML

## P1 · [2] sk 鉴权统一（与 billing 一致）

现状：创建类已走 sk→billing inspect；查询/取消/透传 GET 免鉴权（"task_id 即凭证"）。

- [ ] **前置确认**：new-api 渠道侧轮询任务状态是否带用户 sk（不带则需先校准客户端）
- [ ] `app/services/flow.py:159-164` `view_task` 加 `token_hash` 参数，与
  `data.token_hash` 比对，不匹配 → **404**（不泄露存在性）
- [ ] `app/services/flow.py:257-267` `cancel_task` 同上；L258 docstring 删"持有即凭证"
- [ ] `app/services/flow.py:31` `public_view` docstring 删"task_id 即凭证"
- [ ] `app/routers/tasks.py:22-29`、`videos.py:32-34` 加 `Depends(require_token)` + 传 hash
- [ ] `app/routers/proxy.py:61-67` 免费透传 GET 加 sk 有效性校验（不冻结、不归属校验）
- [ ] 灰度两步：第一期无 token 记 warning 放行（观察一周）；第二期强制 401
- [ ] 测试：owner sk → 200；他人 sk → 404；无 token → 401（二期）

## P1 · [3] 错误分类表（多项方案的共同前置）

渠道 `setting.gateway` 加分类配置（可被渠道覆盖，默认表内置）：

| 类型 | 判定 | 动作 |
|---|---|---|
| 任务级失败 | 4xx 业务错误码白名单 | FAILURE + cancel（现状） |
| key 级失效 | 401/403 invalid key | report(ok=False) + 同渠道换 key（key_index 轮换） |
| 账户级故障 | 403 欠费/特定错误码 | 任务保持/转 HELD + 渠道熔断 + logfire 告警 |
| 限流 | 429 + Retry-After | 不 report，按 hint 拉长退避 |
| 模糊失败 | 超时/5xx/连接中断 | 重试探测；**submit 绝不重试** |

- [ ] 内置默认分类表 + 渠道覆盖位；误判原则：拿不准一律按任务级（错杀 HELD 代价 >> 错放）
- [ ] 落点：`upstream.py` 错误产出处 + `polling.py` / `flow.py` 消费处

## P1 · [4] 探测链路 key 时效接线（复用 new-api 重试与禁用）

现状：探测零上报、钉死 channel_id、坏 key 空转 24h → 批量"上游已扣费本地已退款"。

- [ ] `app/services/upstream.py:207-223` probe 补 `breaker_report`（与 submit 对齐）
- [ ] `app/services/polling.py:59-64` `except Exception` 按 [3] 分类表拆四分支，
  key 级失效 → `providers.keys.report(ok=False, status_code)`（钉回 channel_id 不变，
  keypool 返回同渠道健康 key）
- [ ] `app/services/polling.py:50-53` KeyLeaseError 解析 `retry_after_ms` 用于重投延迟
- [ ] `app/services/providers/keypool.py:46-49` `KeyLeaseError` 加结构化 `retry_after_ms` 字段
- [ ] `app/deps/preflight.py:102-103` 503 响应透传 `Retry-After` 头
- [ ] 前置确认：keypool `/v1/keys/select` 的 `"retry"` 字段（keypool.py:59）是否为
  new-api 风格"第 N 次重试排除已返渠道"语义
- [ ] 测试：probe 401 → report 发出 + 下轮换 key_index；keypool 40001 → 重投延迟 ≥ hint

## P2 · [5] 提交有限重试（只对确定性拒绝）

- [ ] `app/services/flow.py:103-116` submit 失败外加重试循环：仅 [3] 的确定性拒绝
  （401/403/429/400）才重新 lease（排除已试 channel_id）换 key 再 submit，上限 3 次；
  模糊失败（超时/5xx）维持"绝不重试"（防双重建任务双扣费）
- [ ] `app/config.py` `submit_max_attempts: int = 3`、可配 `retryable_status_codes`
- [ ] 测试：首 key 401 → 换 key 成功 + 只冻结一次；超时 → 不重试 + FAILURE + cancel

## P1 · [6] HELD 挂起（欠费期间正常接单，补费即发）

新增内部状态 `HELD`（入 ACTIVE 集合，不进 TERMINAL），依赖 [3] 分类表。

- [ ] `app/schemas.py` `HELD` 常量 + ACTIVE 集合；`public_view` 映射对外 "queued"
- [ ] `app/services/flow.py:106-116` submit 失败按 [3] 分流：账户级 → CAS(ACTIVE→HELD)
  + logfire 事件 + 202 返回（不再 502）
- [ ] HELD 不钉渠道：恢复排空时重新 `keys.lease`（双账号红利：自动切健康账号）
- [ ] `app/queue.py` 新增 `resume_held` 任务：金丝雀策略（取最老 HELD 试提交，
  再撞账户级 → 退避 1m→5m→15m；成功 → 按渠道限速节奏排空）
- [ ] `app/services/reconcile.py` sweep 补：HELD 超 `hold_max_age`（默认 4h，可配）
  → FAILURE + cancel；HELD 无进展 → 触发 resume_held
- [ ] 重提交带 `client_request_id = task_id`（上游幂等，防双重提交）
- [ ] ratelimit：HELD 期间并发槽策略二选一（推荐：挂起即释放、提交前重新 acquire）
- [ ] **硬约束**：挂起上限 ≤ 冻结有效期 → 依赖 [7] 或大 TTL 过渡
- [ ] 测试：submit 撞欠费错误码 → HELD + 冻结在 + 202；模拟恢复 → 重新租约 + 提交 + 进探测

## P1 · [7] billing renew 接口 + 网关续期扫描

billing 服务（独立仓库 AIChatfire/newapi-billing-service）新增"只推 expires_at、
不动钱"的接口；现状无 renew，但 expires_at/sweeper/GET freeze 地基都在。

billing 侧：

- [ ] `POST /api/v1/billing/renew` `{request_id, ttl_seconds}` → 200 `{expires_at, ...}`
- [ ] 语义：条件 `UPDATE ... SET expires_at = NOW()+ttl WHERE request_id=? AND status='frozen'`；
  RowsAffected=0 → 400 且响应体带当前 status（"别再续了"信号）；跨用户 403
- [ ] 走同一用户锁 `lock:acct:{userID}`（与 sweeper 竞态互斥）；409 可重试
- [ ] ttl 按 `MAX_FREEZE_TTL_SECONDS` 截断；新增 `MAX_FREEZE_AGE_SECONDS`（如 7d）总量上限
- [ ] 只写 `billing_logs`（direction=renew），无流水（不动钱）
- [ ] 测试：frozen 续期成功；终态单 400；跨用户 403；与 sweeper 并发竞态；超总量上限 400

网关侧：

- [ ] `app/services/providers/billing_newapi.py` 新增 `renew()`；错误映射：400 非重试终态，
  409/5xx 可重试
- [ ] `app/deps/preflight.py:111-126` freeze 成功后 data 落 `freeze_expires_at`
- [ ] `app/services/reconcile.py` sweep 加续期扫描：`非终态 且 freeze_expires_at - now < margin`
  （默认 600s）→ 用 tokensession 的 user_sk 续期，每轮上限 100
- [ ] 失败矩阵：400 → 任务 FAILURE 止损 + 告警（钱已被 sweeper 退用户）；409/5xx → 下轮再来
- [ ] `app/config.py` `freeze_renew_margin_seconds=600`、`freeze_renew_batch=100`
- [ ] 覆盖范围：HELD + 全部非终态任务（顺手堵"长任务 freeze 过期 → settle 被 4xx 静默收口"窟窿）
- [ ] 灰度：先 HELD，logfire direction=renew 日志正常后放开全量
- [ ] 退路（过渡期）：可挂起 biz 的 freeze 用大 TTL 或不传 ttl（永不过期）——不建议长期

## P1 · [8] 不亏本三窟窿

- [ ] **失败单计费策略**：`finalize_task` FAILURE 分支（flow.py:244-245）先查
  `actual_amount_path` 实收再决定 settle/cancel；渠道配置 `failed_billing: charge|absorb`
  （按厂商商务条款逐渠道配）
- [ ] **反向对账**：reconcile 周期性比对"本地 FAILURE/已退 但 上游 SUCCESS"任务
  → logfire 告警 + 计渠道成本台账（钱追不回但要看得见亏在哪）
- [ ] **孤儿任务**：sweep 检测 `ACTIVE 且 无 upstream_task_id 且 超 N 分钟` → 告警 + 收口；
  根治：submit 带 `client_request_id = task_id`，崩溃后可反查补挂
- [ ] **超时收口尝试上游取消**：poll 超时转 FAILURE 时调上游取消端点
  （对照 flow.py:265 已有 TODO），取消成功即源头止损

## P1 · [15] billing.rule 从 keypool 渠道元数据取，废弃 pricing-service

背景：asteval 沙箱求值本来就在网关侧（modelmeta_pricing.py:34-54），
pricing-service 实际只是"规则存储 + status 开关 + discountRate"。
规则随渠道元数据下发后它是纯减法：**三微服务 → 两微服务**，
preflight 少一个 RTT，且定价从"按模型全局价"变"按渠道价"（贴渠道成本，不亏本加分）。

渠道配置形态（三处等价配置块均可，示例即验收模板）：

```python
"upstream": {
    "biz": "minimax", ...
    "discountRate": 1,          # 网关块顶层（与 pricing-service 字段名一致，缺省 1；报价必乘）
    "billing": {
        "rule": "def calulate(request):\n    return round(float(request.get('duration') or 5) * 0.026, 6)",
        "type": "second",
        "price": [],            # 透传字段（档位/分辨率等计费数据，供 billing.rule 求值消费）
    },
}
```

三个设计决策：

1. **fail-closed**：可计费任务取不到 `billing.rule` → 502 配置错误（立即可见）；
   免费必须显式 `rule: "0"` 或 `billing.type: "free"`——废弃 pricing 时代
   `rule 缺省 "0"` 的静默免费（modelmeta_pricing.py:63 的亏损口子）。
2. **时序**：quote 依赖 lease 结果，`preflight.py:89-93` 三路并行 gather
   拆为两路 `(identity, key)` → 构建 route → 本地求值（少一个 pricing RTT）。
3. **模型可用性**（pricing `status != 0` 职能）：由 keypool abilities 接管
   （模型不可用 = select 40001 → 503/400，语义等价）。

改动点：

- [ ] `app/services/registry.py:31-51` `_GATEWAY_DEFAULTS` 加 `"billing": {}` 和
  `"discountRate": 1`（块顶层，registry 映射为 `RouteConfig.discount_rate`）
- [ ] `app/schemas.py` `RouteConfig` 加 `billing_rule / billing_type / discount_rate / billing_price`
- [ ] 新建 `app/services/pricing_engine.py`：平移 `_eval_rule` + `_FN_NAMES`
  （asteval 每调用独立 Interpreter 的并发纪律原样保留），接口
  `quote_from_route(route, request) -> Quote`；rule 缺失 → fail-closed
- [ ] `app/deps/preflight.py:89-93` gather 拆两路，L106 route 构建后本地报价；
  异常映射：规则求值失败 → 503，缺 rule → 502
- [ ] `app/services/flow.py:183-199` `_settle_amount` ②档改 `quote_from_route(route, ...)`
  （route 本来就有）；重估失败回退冻结额纪律不变
- [ ] freeze 时把 `billing_rule` + 金额快照落 taskstore data（审计 + settle 一致性核对）
- [ ] 删除：`modelmeta_pricing.py`、providers/__init__.py 的 PricingProvider 端口+工厂
  （L68-74, 96-100, 111）、`config.py:54-57`、`redis.py:19` K_PRICING、
  preflight 的 PricingError/ModelUnavailableError import
- [ ] **AGENTS.md 架构段更新**：三微服务 → 两微服务（keypool 升为
  "渠道元数据 + 计费规则"唯一事实源）；README / .env.example 同步

迁移波次（双读过渡，绝不静默改价）：

- [ ] W-a：`quote_from_route` 上线，渠道无 rule 时回退 pricing-service
  （临时路径 + logfire 计数"pricing 回退仍在发生"）
- [ ] W-b：全部渠道在 keypool 补 `billing` 块，回退计数归零
- [ ] W-c：删回退路径 + pricing-service 全部代码/配置/文档

测试：渠道 rule 求值（函数形态 + 纯表达式兜底 + calulate 历史拼写兼容）；
缺 rule → 502；discount_rate 必乘；settle 重估走渠道 rule；W-a 回退路径生效。

## P1 · [17] 配置与密钥卫生（诊断发现的部署错值）

来源：2026-08-14 诊断报告"其他建议"1/2。

- [ ] `GW_GATEWAY_PUBLIC_BASE_URL=http://127.0.0.1:8000` 是错的，应改为
  `https://dev.aapi.cn`——当前 minimax 渠道 `supports_callback=false` 暂未受影响，
  但任何开启回调的渠道会把不可达的 127.0.0.1 回调地址注入上游
- [ ] 占位密钥换强随机值：`GW_CALLBACK_SIGN_SECRET=change-me`、
  `GW_TASKIQ_ADMIN_API_TOKEN=change-me`；`GW_KEY_SVC_TOKEN=change-me` 目前恰好
  与 keypool AUTH_TOKEN 一致（故能工作），建议两边一起换
- [ ] 检查清单沉淀：部署前校验脚本或 checklist 防"能跑但错值"类配置漂移

## P2 · [9] logfire 排除 healthz 等噪音路由

- [ ] `app/config.py:38` 后新增 `logfire_excluded_urls`（默认
  `/healthz/live,/healthz/ready,/ops/queue,/ops/requeue,/ops/dlq/replay`）
- [ ] `app/main.py:47` `instrument_fastapi(app, excluded_urls=...)`
- [ ] `.env.example` 加 `GW_LOGFIRE_EXCLUDED_URLS` 样例
- [ ] 验证：50 次 healthz + 1 次业务请求，面板只剩业务 span

## P2 · [10] 轮询降噪补强

- [ ] `app/services/statelog.py` 新增 `record_failure_escalated()`：Redis INCR 计数，
  仅 {1,5,20} 档 log.warning + logfire.warn；`reset_failure()` 成功后清零
- [ ] `app/services/polling.py:51,62` 两个 per-round `log.warning` 改走上面
- [ ] （可选）`task_status_changed` 事件 attributes 补 biz/channel_id
- [ ] 测试并入 `test_statelog_observability.py`：6 次连续失败 → 恰好 2 条事件；
  成功后计数清零
- [ ] 确认项（无需改）：statelog 唯一记录点、中间件成功路径 DEBUG、
  reconcile `_watch_queue` 仅超阈值发事件 ✅

## P2 · [18] 历史异常排查（tasks 表两条可疑记录）

来源：2026-08-14 诊断报告"其他建议"3（user_id=1, channel 5）。

- [ ] `task timeout after 1440 minutes` 但创建后数秒即"超时"——排查轮询超时判定
  的 created_at 秒/毫秒单位口径是否混用
- [ ] `I/O error on POST request for "": Target host is not specified`（Java 风格报错，
  疑似上游 relay 侧当时配置问题）——与 relay 维护方核对当时渠道配置

## P2 · [19] GET 免费透传选渠道（model="" 一律 404）

来源：2026-08-14 诊断报告"其他建议"4。现状：GET 免费透传路径固定用
`model=""` 选渠道，keypool 对空 model 返回 40010 → 一律 404 `unknown biz`。

- [ ] 方案二选一：渠道/路由侧提供默认 model；或按 task_id 钉渠道反查
- [ ] 与 [2] proxy GET 加 sk 校验的改动同文件（proxy.py），合并排期避免冲突

## P3 · [11] taskiq-admin 看板

- [ ] `pyproject.toml` 加 `taskiq-admin`
- [ ] `app/queue.py:127` broker 挂 `RedisAsyncResultBackend`（注意 result TTL 防膨胀；
  任务均返回 None，无敏感数据）
- [ ] `docker-compose.yml` + `docker-compose.dev.yml` 新增 taskiq-admin 服务，
  端口只绑 127.0.0.1，反代 basic auth
- [ ] 回归：全量 pytest，重点 `_retry_or_dlq` / `replay_dlq` 行为不变
- [ ] 职责划分：admin 看队列执行层，logfire 看业务语义层；`queue_stats()` 告警已有
  （reconcile `_watch_queue` ✅）

## P3 · [12] scheduler 合并部署

- [ ] `docker-compose.yml:46-55` 删独立 scheduler 服务；worker command 改
  `sh -c "taskiq scheduler app.queue:scheduler & exec taskiq worker app.queue:broker --max-async-tasks 100"`
- [ ] `docker-compose.dev.yml` 同步；`app/queue.py:11-13` 与 AGENTS.md 运行说明更新
- [ ] 约束写注释：scheduler 必须单副本，worker 扩 replicas 时拆回独立服务
- [ ] 验证：合并后 poll 重投 / settle 退避 / 每分钟 sweep 三链路事件齐全

## P3 · [13] 运营项（非代码）

- [ ] 双账号冗余：同模型挂两个火山账号渠道（同 group 同 model），keypool 健康度自动切换
- [ ] 余额预警：火山账号余额监控，欠费前告警（而非任务失败后才发现）
- [ ] keypool 渠道内多 key 管理（key 时效轮换的前提）

---

## 建议实施波次

| 波次 | 内容 | 理由 |
|---|---|---|
| W1 | [1] + [16] + [17] | 线上在炸（[16] 补丁已备，顺手堵 sk 泄漏）+ 配置错值即改 |
| W2 | [3] + [4] | 共同前置 + 止住探测空转亏损 |
| W3 | [8] + [5] + [15] W-a | 资金窟窿 + 提交重试（依赖 [3]）+ 渠道定价双读上线 |
| W4 | [2] 灰度一期 + [19] | 需先确认 new-api 轮询带 token；[19] 与 [2] 同改 proxy.py 合并排期 |
| W5 | [9] + [10] | 降噪小改 |
| W6 | [7] billing 侧先行 → 网关侧 | HELD 的硬约束 |
| W7 | [6] | 依赖 [3][7] |
| W8 | [15] W-b/W-c + [18] | 渠道补 billing 块后删 pricing-service；[18] 纯排查随波携带 |
| W9 | [11] + [12] + [13] | 运维增强排最后 |
