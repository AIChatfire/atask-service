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
- app/services/  编排层：flow（生命周期）/ upstream（上游引擎）/ polling / reconcile
- app/services/providers/  两微服务适配层（端口 Protocol + 实现，换实现改 *_PROVIDER）
- app/services/registry.py 路由构建：keypool 渠道 setting.gateway → RouteConfig
- app/queue.py   taskiq 任务定义与发布门面（settle/cancel/notify/poll/sweep）
- app/schemas.py 共享契约（状态常量、KeyLease、RouteConfig、Quote、UserIdentity）
- tests/         pytest（respx 拦 HTTP，FakeRedis + 内存 taskstore，无外部依赖）

## 常用命令
- 测试：`.venv/bin/python -m pytest tests/ -q`
- Lint：`.venv/bin/python -m ruff check app tests`
- 安装：`.venv/bin/pip install -e ".[dev]"`
- 本地依赖：`docker compose up -d mysql redis`
- 运行：网关 `gunicorn -c gunicorn.conf.py app.main:app`；
  后台 `taskiq worker app.queue:broker` + `taskiq scheduler app.queue:scheduler`

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
