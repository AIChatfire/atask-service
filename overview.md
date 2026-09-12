# atask-service 架构换向交付概览

> 体例对齐同族仓库 stask-service 的 `overview.md`。
> 本文件是**一次专项的交付记录**，不是架构文档——决策看 `docs/decisions/`
> （本轮核心是本仓库 ADR-010：`docs/decisions/ADR-010-queue-path-zero-billing.md`），
> 架构总览看 `docs/ARCH-queue-relay-lifecycle.md`，契约看 `docs/SPEC.md`。
> 凡涉及具体数字，均标注采集时间与采集命令。

## 已完成

### 对外形态统一为 `/queue/{上游原生路径}`

- 唯一形态三件套（`app/routers/queue_task.py`，通配路由最后注册）：
  `POST /queue/{path}`（受理，`202 + {task_id, status}` + `Location` 头）、
  `GET /queue/{path}/{task_id}`（查询）、`DELETE /queue/{path}/{task_id}`（取消）。
- `{biz}` 段从 URL **彻底移除**：biz 本就由渠道元数据提供，URL 段只是入口标签，
  去掉不丢信息。**不做任何旧形态兼容**。
- **两层前缀**（2026-09-13 定）：对外统一 `/async`（与 stask 一致，客户端只记一个；
  nginx 按路径分流并重写），网关内部为 `/queue/{上游原生路径}`（与机制名同源）。
  早先的 `/batch` 前缀已废弃。见本仓库 ADR-010 §1。
- 旧原生透传形态（`/{biz}/v1/tasks`、`/{biz}/v1/videos`、`/{biz}/{原生路径}`）删除；
  免费 GET 透传并入 `GET /queue/{path}`（末段不是本地 id 时降级为原样转发）。

### 鉴权与计费全部下沉上游（网关零资金动作）

- **鉴权不做内省**：用户 token 以 `Authorization: Bearer` 原样透传上游；网关只做
  本地可做的事——限流（按 token hash）、幂等、并发上限（`app/deps/identity.py` /
  `app/deps/ratelimit.py`）。
- **计费零资金动作**：不 freeze / settle / cancel，`tasks.data` 不写
  `freeze_amount` / `settled`，配额由上游 new-api 原生 relay 扣减。
- 网关**不再持有上游 key**：凭证面从「上游 key + 服务级 token」缩到「无」，只剩链路转发。

### 渠道路由：按 new-api 约定零配置

- 上游寻址：`X-Upstream-Base-Url` 头（nginx 无条件覆盖注入）→ 回退 `UPSTREAM_BASE_URL`；
  host 必须命中 `UPSTREAM_ALLOWLIST`；仅 `http`/`https`、拒 URL userinfo、
  **白名单为空即全拒**（fail-closed，防用户 sk 被打到野地址）。三防线见
  `app/services/upstream_addr.py`。
- 渠道元数据（`task_id_path` / `probe_path` / `auth_type` / 渠道级 `timeout_sec` /
  `model_mapping` / `result_url_template` / `body_allowlist` 等）全部失去来源，
  一律按 new-api 约定硬编码：提交 `POST {base}{path}` 原样转发；提取任务 id 取 `id`、
  缺失回退 `task_id`；探测 `GET {base}{path}/{id}`；状态字段 `status`；固定 Bearer；
  超时降级为全局 `RELAY_TIMEOUT_SECONDS`。实现在 `app/services/relay.py`。

### 终态收敛与失败分流

- **单一终态收口点** `relayflow._finalize_queue`：视图探测 / 后台 sweep / worker 提交
  共用同一份实现——CAS 抢推进权 → 记一条状态迁移日志 → 落终态快照（≤8KB）→ 释放并发槽
  → 投递用户回调 → 清令牌会话。
- 后台 `queue_sweep_task`（cron 每分钟、独立重入锁）探测非终态任务，候选**最旧优先**
  （`updated_at ASC`）；令牌会话过期则跳过（不判死、不释槽）。
- 用户回调地址在受理时确定：`X-Callback-Url` 头优先，body 顶层 `callback_url` 兜底
  （上游 API 文档口径，如火山方舟 Seedance）；两者都过 `callback_addr` 的 fail-closed
  白名单（仅 http(s)、拒私网字面 IP、`CALLBACK_ALLOWLIST` 空即全拒）。终态经
  `app/services/notify.py` HMAC-SHA256 签名后投递（重试 + 死信）；**只推终态**。
  客户端对接契约见 `docs/CALLBACK-CONTRACT.md`。
- 提交失败收敛为**三档**：4xx → FAILURE + 还槽 + 清会话；5xx/传输错误 → 留活重试；
  2xx 缺 id → FAILURE。
- 保留不变的既有契约：复用 new-api `tasks` 表 + `platform='atask'` 隔离、
  时间列归一、幂等原子占位、按 token hash 的并发上限、原生报文同构与终态快照回放。

### 攒批放行（ADR-011）

把「上游提交」从受理时刻解耦：攒够 N 条或等够 T 秒才整批**提交上游**
（**不合并请求**——上游是 new-api 约定式异步接口，没有批量端点；这一条与 stask 一致）。

- 两个触发器一个放行点：N 触发（成员数达到 `BATCH_SIZE`，**只投递不同步放行**）、
  T 触发（`schedule_by_time` 排的延迟任务，不照搬 stask 的 cron 自旋）、外加 sweep 的
  超期兜底（跑在既有的每分钟 `queue_sweep_task` 上）；
- **等待期不占并发槽**，是唯一改变对外语义的一条：占槽点从受理搬到放行，因此攒批路径
  **受理不再因并发满而 429，改为排队**；放行时占不到槽则指数退避 + 抖动重排；
- 状态零新增：复用 `SUBMITTED` + `data.batch_state`
  （`waiting` / `releasing` / `released` / `requeued`），对外只暴露 `waiting` /
  `released`（`requeued` 漏出去会让客户端去等一个永不到达的批次事件）；
- 恰好一次的两道关：批次级 Redis 原子摘取（`LUA_BATCH_CLAIM`）+ 成员级 DB 条件更新
  （`taskstore.claim_for_release`）；还槽则有「谁把 `data.slot_flags` 置零谁去 DECR」
  （`taskstore.claim_slot_release`）；
- **默认关闭**：`BATCH_SIZE=0` 表示收到即提交，开箱行为与本特性之前逐字节一致。
- 新增用例 `tests/test_batching.py`，并对四处关键不变量做过变异测试自证（还槽不校验
  掩码 / 去掉等待态提交闸门 / 取消不退批 / 放行不抢放行权）。**第一轮抓到一条假绿**：
  原「已放行的成员只 SKIPPED」用例走的其实是快速路径、从未打到抢放行权那一关，已补
  `test_lost_claim_race_never_takes_slot_nor_submits` 专门覆盖那个 TOCTOU 窗口。

### 删除旧链路

旧链路模块已整体移除，仓库里不再有——`services/` 下的 providers、pricing、
leasing、held、registry、nativeproxy、passthrough、flow、submit、polling、
reconcile、resulturl、routecache、taskrecord、errclass，`deps/` 下的 preflight，
`routers/` 下的 tasks、videos、proxy、callback，以及根部的 models。
`app/` 规模从 7378 行降到实测 3724 行（`find app -name "*.py" | xargs wc -l`，
2026-09-12）。

### 工程化外壳（保留，未随换向改动）

- `Makefile`：统一开发入口；`make check` = ruff + mypy + pytest 三项门禁。
- 多阶段 `Dockerfile`（非 root、`HEALTHCHECK` 走 `/healthz/live`）、
  `docker-compose.yml`（gateway + worker + redis + taskiq-admin；MySQL 用共享实例）。
- `gunicorn.conf.py`：worker 数由「与 new-api 共享 MySQL 的连接预算」反推。
- `app/standalone.py` 单进程起全套；`app/observability.py` 可观测装配单点。
- `tests/test_static_gates.py`：结构断言（不写死条数，含「扫描范围非空」防假绿）。

## 验证结果

采集命令与结果（2026-09-12）：

```
$ .venv/bin/python -m ruff check app tests scripts gunicorn.conf.py
All checks passed!
$ .venv/bin/python -m mypy app/
Success: no issues found in 35 source files
$ .venv/bin/python -m pytest tests/ -q
183 passed in 15.68s
```

（复核时另出现一次 `1 failed, 182 passed`：唯一失败为静态门禁
`tests/test_static_gates.py::test_repo_has_no_real_secrets`，命中同期编辑中的
`docs/ARCH-queue-relay-lifecycle.md` 里的 userinfo 示例写法，属该文档的清理项；
本轮三个交付文件（`README.md` / `AGENTS.md` / 本文件）不在此门禁命中范围内。）

测试规模：16 个测试文件；单测**不依赖真实** MySQL / Redis / 上游
（respx 拦出站，FakeRedis + 内存 taskstore）。

**未做的事，如实说明**：本轮**没有**对真实依赖（MySQL / Redis / 真实上游）跑全链路
压测，因此**本文件不含任何 P99 / QPS / 收益百分比**。任何性能结论都必须现场采集，
不得引用本文件。

## 重要契约变化

1. **[破坏性] 对外形态从 `/{biz}/...` 改为 `/queue/{path}`**
   旧形态 `POST /{biz}/v1/tasks`、`/{biz}/v1/videos`、通配 `/{biz}/{原生路径}` 全部
   删除，`{biz}` 段不再出现在 URL 中；`DELETE` 取代旧的
   `POST /{biz}/v1/tasks/{task_id}/cancel` 取消形态。**不提供任何旧形态兼容**——
   客户端必须改打到 `/queue/{上游原生路径}`。

2. **[破坏性] 网关零资金动作，不再有计费接口**
   不再调用任何 freeze / settle / cancel；`tasks.data` 不再写
   `freeze_amount` / `settled`。计费权威完全在上游 relay，网关侧没有「资金兜底」。
   相应地，不再有 HELD 挂起、冻结续期、孤儿资金收口、解冻。

3. **[破坏性] 网关不再持有上游 key**
   旧架构从 keypool 取上游 key 并任务级租约钉 key；现改为按请求寻址、用户 token
   原样透传。依赖旧微服务的部署配置项（keypool / billing 的地址与令牌）**全部作废**，
   没有替代项。

4. **配置键名即 `Settings` 字段名大写、不带前缀**（既有契约）
   **不提供任何旧键名或别名兼容**：未知变量一律被静默忽略（`extra="ignore"`，
   刻意的），因此**写错键名不会有任何提示**，网关会带默认值启动——实测默认
   `DATABASE_URL` 指向 `root:root@127.0.0.1`，**现象是连库失败而不是配置报错**。

5. **上游寻址白名单是新的安全开关**
   `UPSTREAM_ALLOWLIST` **为空即全部拒绝**（fail-closed）。这与「未配 `ADMIN_TOKEN`
   时整个管理面 404」是同一纪律：失败方向恒为拒绝。

6. **管理面 fail-closed**（既有契约）
   `/ops/*` 与 `/admin/*` 共用 `X-Admin-Token`（配置项 `ADMIN_TOKEN`），
   **未配置密钥时整个管理面返回 404**——不是 401，也不依赖内网隔离。

7. **部署前置必填**（既有契约）
   `docker-compose.yml` 使用 `env_file: .env`；`TASKIQ_ADMIN_API_TOKEN` 使用 `:?`
   守卫，缺失即报错。

8. **[行为变更，仅攒批路径] 受理不再因并发满而 429，改为排队**
   攒批路径（`BATCH_SIZE>=2` 或客户端声明 `X-Batch-Size`）的并发槽占用点从**受理**
   搬到**放行**，因此并发满时不再拒绝，而是入批/退避重排（`ADR-011` §3）。非攒批路径
   行为不变。**依赖 429 做退避的客户端需要相应调整**（429 仍出现在限流
   `RATE_LIMIT_PER_MINUTE` 与免费 GET 的 IP 限流上）。另外新增 `X-Batch-Size` /
   `X-Batch-Wait` / `X-Batch-Key` 三个头：**非法值一律 400 且不留痕**（不落库、不占槽、
   `batch_enabled=false` 时同样报错——否则「关着不报错、打开才报错」会变成切换开关后
   才暴露的客户端 bug）。

9. **[部署动作] scheduler 需要 `--update-interval 1`**
   攒批的 T 触发走 `schedule_by_time`，而 taskiq 0.11 的 scheduler 默认按分钟对点唤醒
   （`next_run = now + 1min`），不设这一项时 `batch_wait` 的实际放行最坏晚约 60s。
   三个形态都要带上：`Makefile` 的 `make scheduler`、`docker-compose.yml` 的 worker
   命令、`app/standalone.py` 的 `run_scheduler_task(interval=...)`。
   **正确性不依赖它**（投递丢失/迟到都由 sweep 的超期兜底接住），只影响准点程度。

## 后续事项

1. **待轮换凭据**（凭据一旦泄露，改文件不等于止损）：
   `.env.example` 曾含生产 MySQL 公网口令、回调签名密钥、logfire token；
   这些值仍需在各自系统轮换。

2. **文档收尾**：
   - `README.md` / `AGENTS.md` / 本文件已按本仓库 ADR-010 更新；
   - `docs/decisions/README.md` 的 ADR 一览与阅读顺序仍按旧架构书写
     （ADR-002 / ADR-005 / ADR-006 / ADR-007 已被 ADR-010 取代），需同步；
   - `docs/SPEC.md` 与 `docs/ARCH-queue-relay-lifecycle.md` 同步更新中。

3. **线上（宝塔面板）环境变量须对齐**：无前缀键名 + `UPSTREAM_BASE_URL` /
   `UPSTREAM_ALLOWLIST`，并确认 nginx 无条件注入 `X-Upstream-Base-Url`。
   头依赖 nginx 配置正确是已知限制（本仓库 ADR-010）。

4. **真实环境验证待定**：验证上游改测目标待指定。红线不放松——真实发请求会触发
   真实计费，**不假定任何组合免费**；`scripts/bench_submit.py` 默认 dry-run，
   真发必须显式 `--execute`，且除非再加 `--yes` 会在终端二次确认。

5. **未决项**：`docs/decisions/OPEN-DECISIONS.md` 当前 **3 未决 / 4 已决**。
   其中 `deploy-drift-needs-ops-sync` 需要运维确认线上部署镜像已换到本仓库版本
   （线上曾长期运行一个 Spring Boot 重实现的旧网关，它把秒制 `submit_time` 当毫秒读，
   导致新任务瞬时超龄判死）。
