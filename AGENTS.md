# 项目说明（AI 入口）

## 这是什么
异步 AI 网关（atask-service）：异步任务型模型（视频生成等）的统一接入网关，
与 new-api 生态共用用户体系、钱包（users.quota）与渠道配置。外部协同只有两个微服务：

- **keypool-service**（上游凭证 + 渠道全量元数据 + 路由提取配置 + billing.rule
  计费规则的唯一事实源；计费规则随租约下发，网关本地沙箱求值，见 app/services/pricing.py）
- **newapi-billing-service**（身份内省 + freeze/settle/cancel 资金操作）

## 目录结构
- app/routers/   HTTP 入口（tasks/videos/callback/ops/proxy 通配透传）
- app/deps/      请求预检（鉴权内省、限流、preflight 报价+租约+冻结）
- app/services/  编排层：flow（生命周期，创建为异步提交：落库即返回本地
  task_id）/ submit（worker 侧上游提交）/ upstream（上游引擎）/ polling / reconcile
- app/services/providers/  两微服务适配层（端口 Protocol + 实现，换实现改 *_PROVIDER）
- app/services/registry.py 路由构建：keypool 渠道 setting.gateway → RouteConfig
- app/queue.py   taskiq 任务定义与发布门面（submit/settle/cancel/notify/poll/sweep）
- app/schemas.py 共享契约（状态常量、KeyLease、RouteConfig、Quote、UserIdentity）
- tests/         pytest（respx 拦 HTTP，FakeRedis + 内存 taskstore，无外部依赖）

## 常用命令
- 测试：`.venv/bin/python -m pytest tests/ -q`（含 mypy 类型检查，见
  tests/test_typecheck.py；单跑 `.venv/bin/python -m mypy app/`）
- Lint：`.venv/bin/python -m ruff check app tests`
- 安装：`.venv/bin/pip install -e ".[dev]"`
- 本地依赖：`docker compose up -d mysql redis`
- 运行：网关 `gunicorn -c gunicorn.conf.py app.main:app`；
  后台 `sh -c "taskiq scheduler app.queue:scheduler & exec taskiq worker app.queue:broker"`
  （scheduler 合并进 worker，必须单副本；worker 扩副本时拆回独立 scheduler）

## 关键约定
- **零路由文件**：上游配置全部在 keypool 渠道；全部渠道挂在统一分组
  （默认 `keypool`，`GW_KEY_GROUP` 可配）下，选渠道 = `select(group, model)`。
  **biz 从渠道取**（网关配置块 `biz` → 渠道 `name` → URL 段兜底），
  URL `/{biz}/` 只是入口标签。网关提取配置块可放 `header_override.upstream`
  或 `setting.gateway`（两处等价、优先级从高到低；装配请求头时自动剥离嵌套块，
  不透出为 HTTP 头）：submit_path/probe_path/status_path/result_path/
  settle_usage_map、billing（rule/type/discount_rate）…。接入新模型 =
  渠道挂进分组 + 配 gateway 块 billing 计费规则，不改代码。
- **渠道覆盖三层叠加**：route.default_params < 用户 body < channel.param_override；
  model_mapping 改写 model；用户自带 callback_url/webhook 一律摘除（用户回调由网关签名投递）。
- **任务级租约钉回精确到 key**（唯一入口 `app/services/leasing.py`）：探测 /
  取消 / 原生查询 / 回调 / 反向对账一律用 keypool 的 `channel_id + key_index`
  **单 key 精确直达**（`mode=direct`，跳过调度算法与 Redis）——同一渠道挂多个
  上游账号时，换 key 就查不到任务。key 级失败（40010 索引越界 / 40001 该 key
  被禁用）自动降级为渠道直达；40002 渠道不存在原样上抛。**提交链路不钉 key**
  （任务还没进上游，渠道内任意健康 key 都行；钉死反而在该 key 被禁时白等）。
  凭证治理（禁用/轮换/epoch）全留在 keypool，网关绝不缓存明文 key。
- **产物直链改写（转存/镜像）**：渠道配 `result_url_template`（如
  `https://myhost.com/{upstream_result_url}`）即让所有出口只出现网关地址——
  finalize 改写 `data.result`（原始链另存 `data.upstream_result`）→ tasks/videos
  视图与用户回调自动跟随；原生查询报文里的直链**字节级替换**（报文其余部分
  同构）。占位符见 `app/services/resulturl.py`（还有 `_encoded` / `_no_scheme` /
  `_host` / `_path` / `{task_id}`）。**网关只改地址、不搬字节**，回源由模板指向
  的服务负责；模板为空 = 不改写（默认零影响）。
- **原生路径拦截（透传形态的生命周期入口）**：通配 `/{biz}/{原生路径}` 默认同步
  透传，但命中渠道路径模板的三条路径被改写为网关语义（零硬编码、判定全来自
  渠道配置，见 app/services/nativeapi.py + app/routers/proxy.py）：
  `submit_path`（POST）→ 走 flow.create_task 异步受理，**零上游往返秒级返回**，
  响应体按 `task_id_path`（+ `ok_check` 信封）塑形为原生形状、值是本地 task_id；
  `probe_path`（GET）→ 按 URL 里的 id 反查 tasks 行（本地 id 主键直查、上游 id
  兜底反查），用 `channel_id` 钉回直达租约转发，响应缓冲后把上游 id 逐字节改写
  回本地 id（其余字节 100% 同构）；**终态零上游往返**——finalize 时把上游终态
  原始报文落 `data.upstream_snapshot`（≤8KB），查询直接回放（逐字段同构）；
  首探前/上游不可达时按配置反向构建快照（`probe_task_id_path` 指定快照里 id
  的字段路径，状态词优先用 `data.upstream_status` 上游原话），绝不 404；
  `cancel_path`（非 GET）→ 走本地 cancel 链路（解冻 + 尽力源头止损），绝不当
  新任务报价冻结。其余路径透传语义一字不改。
- **免费透传永不空 model 问 keypool**：`select(group, model)` 对空 model 直接拒
  （40010），所以免费 GET 按「Redis `biz→channel_id` 记忆（app/services/routecache.py，
  唯一写入点 = preflight 成功租约）→ 进程路由缓存」钉回 channel_id 直达租约，
  两级都落空才 404——一次浪费的出站都不发。
- **提交异步化**：创建接口 preflight+落库即返回本地 task_id（`{biz}_{uuid4hex}`），
  上游提交由 worker 执行（app/services/submit.py）。"上游已接单、落库前"崩溃
  存在双重提交窗口：渠道配 `client_request_id_param` 时提交体注入 task_id
  供上游幂等反查对账，孤儿收口（`GW_ORPHAN_GRACE_SECONDS`，默认 1800s）兜底。
  提交互斥锁 TTL 按路由动态派生（`submit_max_attempts` × 渠道 `timeout_sec`
  + `GW_SUBMIT_LOCK_BUFFER_SECONDS`，换渠道重打按新路由刷新），sweep 补投
  前查锁让路——锁先于在飞提交过期的双建窗口已根治。
- **幂等键原子占位**：preflight 以 SET NX 写占位（`pending`，短 TTL
  `GW_IDEM_PENDING_TTL_SECONDS`）把「先查后写」变原子——同 Idempotency-Key
  真并发只有占位者继续创建链路；其余短轮询等占位在同一键上回填为
  task_id（pending → task_id）后回放，超时/过期按 409 冲突（不放行重建，
  防双建双冻结）；创建链路失败 CAS 归还占位。
- **计费纪律**：freeze 用渠道 billing.rule 顶格预估；settle 三档（actual_amount_path →
  settle_usage_map 重估 → 冻结兜底），绝不静默按 0 结算；settle/cancel 用**用户令牌**。
- 错误响应统一 `{"error": {...}}`（app/errors.py 注册点）；内部状态常量以 app/schemas.py 为准。
- 日志统一 loguru：业务模块 `from app.logging import log`，装配点 app/logging.py
  （web 在 main、worker 在队列中间件 startup），级别 `GW_LOG_LEVEL`；
  只支持同构任务（新建任务数据形态唯一），不再兼容旧版配置位/旧任务。
- 配置全部环境变量 `GW_` 前缀（app/config.py；.env.example 为全量样例）。

## 红线
- 禁止跨服务读库（网关只读写 new-api tasks 表，零建表职责）
- 不要修改 deploy/prod/ 下的任何文件
- 第三方密钥只允许从 config/secrets.example.yaml 推断结构
- 用户令牌/上游 key 不落 tasks 表、不进日志（令牌会话只放 Redis，终态即清）
