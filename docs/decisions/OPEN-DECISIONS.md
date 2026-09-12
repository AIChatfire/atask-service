# 悬而未决登记册（OPEN-DECISIONS）

> **仓库独立声明**：本仓库（atask-service）的 ADR 系列与 stask-service 的
> ADR 系列**编号独立、内容不同**——两边都有 ADR-001/006/008，但说的是不同
> 决策。交叉引用时**必须带仓库名**（如「stask-service ADR-002」），否则会
> 指错文件。同理，两仓库各有一份 `OPEN-DECISIONS.md`，本文件只登记
> **atask-service** 的未决项。
>
> 只追加、就地关闭。每次进入新阶段前先复现本表，逐条判断能否关闭。
> 当前：**7 未决 / 4 已决**（2026-09-13 新增
> `route-rename-external-sync`、`deploy-topology-confirm`、`redis-prefix-drain-required`、
> `external-prefix-doc-drift`；
> 2026-09-12：随 ADR-010 换向关闭
> `config-invariant-unenforced` / `acceptable-residual-window` / `design-asymmetry`；
> 此前已关闭 `doc-retirement-pending`）

| Date | Source | Open Item | Related Constraints | Current Leaning | Blocked By | Resolves When | Status |
|---|---|---|---|---|---|---|---|
| 2026-09-13 | 本次改名（ADR-010 §1） | 路由前缀 `/batch` → `/async` 需**仓库外三方同步**：nginx `location /batch/`→`/async/`（宝塔代管）、stask 的 `ST_ASYNC_DENY_PREFIXES` 里硬拒前缀 `/batch/`→`/async/`、客户端调用路径 | 本仓库已改净且不做别名兼容（既定立场）；三方未同步时**流量打不进新前缀** | 保持不兼容（干净的一侧）；同步窗口由运维安排 | 需运维（nginx）+ stask 仓库（deny-list）+ 客户端三方确认 | 线上域名下 `POST /async/{上游路径}` 可受理，且 stask 不再拦 `/async/` | OPEN（`route-rename-external-sync`） |
| 2026-09-13 | 本次优化复查 | gateway 发布端口默认改为**只绑回环**（`${GATEWAY_BIND_IP:-127.0.0.1}:8000:8000`），需确认线上反代与网关是否同栈 | 反代与网关同栈时宿主端口不必对外（nginx 走容器服务名 `atask:8000`，见 `docs/stask-service-design.md` §7）；跨主机才需要放宽 | 保持回环默认（安全侧）；确需经宿主 IP 访问时在 `.env` 设 `GATEWAY_BIND_IP=0.0.0.0` | 需运维确认部署拓扑（反代与网关是否同一宿主 / 同一 compose 网络） | 线上 `docker compose up -d` 后经域名访问 `/async/*` 仍正常 | OPEN（`deploy-topology-confirm`） |
| 2026-09-12 | `OPTIMIZATION_BACKLOG.md` KI-D | 提交/孤儿收口的配置不变量**无代码强制**：`orphan_grace_seconds > submit_max_attempts × max(渠道 timeout_sec) + submit_lock_buffer_seconds`，渠道 `timeout_sec` 配得过大（> ~580s）时孤儿收口可能在在飞提交期间判死 | 判死是不可逆资金动作（ADR-005） | 保持为运维不变量；调参时人工守住。可加启动期校验（用渠道 timeout 上界或保守常量） | 需要一个「渠道 timeout 上界」的可信来源（keypool 元数据扫描有成本） | **因本次架构换向失去意义（2026-09-12 关闭）**：`orphan_grace_seconds`、提交锁 TTL（及其 buffer）、渠道级 `timeout_sec` 三个前提配置项全部随旧链路（孤儿收口 / 提交锁 / keypool）删除；新链路无冻结、判死不再是不可逆资金动作，该不变量不再存在 | CLOSED (2026-09-12) |
| 2026-09-12 | `OPTIMIZATION_BACKLOG.md` KI-F | 幂等占位 TTL 极端窗口：占位 TTL 30s，preflight 超 30s 未完成（billing/keypool 长时间故障）时占位过期，后来同键请求可能重建任务 | billing `request_id` 唯一约束兜底（资金侧零风险，ADR-003）。注（2026-09-12）：原兜底约束 billing request_id 随 ADR-010 失效，该窗口的第二道唯一约束兜底已不存在（网关零资金动作） | 可选方案：占位心跳续期（创建链路按期 EXPIRE 续占位），本期不做。注（2026-09-12）：原「可接受」的两个依据都变了——触发原因（preflight 超过 30s 未完成所需的跨服务故障）随新链路受理段去掉跨服务调用（只做 token 哈希 → 限流 → 幂等占位 → 落库 → 入队）而基本消失，概率显著下降；但第二道唯一约束兜底随 billing 退场而消失，残余后果从「资金侧零风险」变为「可能重复消耗上游额度且网关无从补救」。综合判断：概率大降 vs 兜底消失，**当前仍判可接受**，但理由已从「有兜底」改为「概率足够低且无更廉价的防线」 | 需要确认续期策略不会把「卡死的创建链路」永久占位 | 真实流量出现同键双建（billing 侧可见 request_id 冲突）或占位过期率上升 | OPEN（`low-probability-window`） |
| 2026-09-12 | `OPTIMIZATION_BACKLOG.md`「观察项」 | 设计不对称：**proxy 纯透传落 tasks 行但不占并发槽**（`conc_acquire` 只覆盖 tasks/videos 与原生提交链路；纯透传是同步流式转发，占用语义不同） | 终态 `conc_release` 对未占槽任务 DECR 由 Lua 钳 0，无负槽风险（ADR-003 的并发模型） | 记录在案，不统一。若运营侧需要「按用户统一并发口径」再议 | 需要产品确认透传是否应计入用户并发额度 | **因本次架构换向失去意义（2026-09-12 关闭）**：旧的 `/{biz}` 纯透传形态已随 ADR-010 整体删除，不存在「落行但不占槽」的任务；新链路每个受理任务都占并发槽 | CLOSED (2026-09-12) |
| 2026-09-12 | `OPTIMIZATION_BACKLOG.md` 时间列结尾注 | 部署侧版本漂移：线上曾跑 Java 重实现的旧版网关（Spring Boot，端口 39600），写毫秒 `finish_time`；本地已修（ADR-004），**部署侧需同步本版本** | 共享表时间列不可信（ADR-004） | 运维侧同步部署版本；网关侧靠 `as_unix_seconds` / `_secs()` / 二次核龄三层防线兜底 | 需运维确认部署镜像已换到本仓库版本 | 线上不再出现毫秒时间列（`finish_time > 1e11` 的行归零） | OPEN（`deploy-drift-needs-ops-sync`） |
| 2026-09-12 | 本次 ADR 核实 | stask 仓库文档与代码不符：stask `ADR-001`/`SPEC.md` 声称其 `data` 恒写 `freeze_amount:0` + `settled:true` 让 atask sweeper 跳过，但**当前 stask 代码不写这两个键**；实际隔离靠 `platform` 过滤 | 共表隔离（ADR-001 / ADR-008） | atask 侧不依赖这两个键，无需改动；建议在 stask 仓库修正文档或补实现 | 跨仓库协作（stask 侧决策） | stask 仓库修正文档或落地该契约 | OPEN（`cross-repo-doc-drift`） |
| 2026-09-12 | `OPTIMIZATION_BACKLOG.md` KI-E | 锁 TTL 刷新残余窗口：重打换渠道时锁 TTL 用 `SET ... xx` 刷新，锁恰在重打间隙丢失（说明已超最坏窗口）仅告警、当次提交继续，存在极小并发窗 | sweep 补投前查锁 + `client_request_id` 对账兜底（ADR-003） | 可接受，不阻塞 | — | **因本次架构换向失去意义（2026-09-12 关闭）**：提交锁（及其重打换渠道刷新）随旧链路删除；新链路无提交锁，该残余窗口不再存在 | CLOSED (2026-09-12) |
| 2026-09-12 | `tests/test_static_gates.py` 注释 | 历史文档退役：`AI_TODO.md` / `OPTIMIZATION_BACKLOG.md` 的「无 emoji」门禁豁免。本次已去除两文件的 emoji 状态标记（对勾/红圆/黄圆标记分别改为 `[完成]` / `[高危]` / `[中危]`）并加归档说明，但门禁扫描范围仍只含 `app/*.py` 与 `docs/decisions/*.md` | 历史文档仅作记录，不应长期豁免门禁 | 待把这两文件纳入 `test_no_emoji_*` 扫描（或删除文件） | 门禁是否扩展扫描根目录 `.md` 的决策（team-lead 负责，见任务「把纪律做成测试门禁」） | **已满足（2026-09-12 核实）**：`test_no_emoji_in_docs` 的扫描范围早已是「仓库根级 `*.md` + `docs/**/*.md`」，`AI_TODO.md` / `OPTIMIZATION_BACKLOG.md` 均在扫描范围内且不含 emoji，该门禁全绿；无须再扩展 | CLOSED (2026-09-12) |

| 2026-09-13 | 攒批实现（ADR-011）+ Redis 键前缀改名 | **Redis 键前缀 `gw:` → `atask:` 需部署侧配合：切换前必须排空队列**。队列键本身也在改名范围内（`atask:taskiq` / `atask:sched:*` / `atask:events:dlq`），旧键不会被新进程读写 | 幂等键（24h）与令牌会话的丢失只影响在飞窗口（任务停在非终态，ADR-010 已知限制 L-1 的待遇）；**队列丢失是真丢任务**（积压消息、延迟任务、死信一并成孤儿键） | 保持不兼容（干净的一侧，与路由改名同一立场，不做双前缀并存） | 需运维：确认 `gw:taskiq` / `gw:sched:*` 队列深度为 0 后再切 | 线上跑 `atask:` 前缀后，`/ops/queue` 的 `pending`/`delayed` 从 0 正常增长，且无 `gw:taskiq` 残留消费 | OPEN（`redis-prefix-drain-required`） |
| 2026-09-13 | 本次攒批实现中的核对 | 文档口径不一：**对外路由前缀到底是 `/queue` 还是 nginx 层的 `/async`**？三处说法冲突——`docs/SPEC.md` §13 有一行写「`/batch` → `/async`」，同表上一行与 `route-rename-external-sync` 都写「`/batch` → `/queue`」，而 `docs/ARCH-queue-relay-lifecycle.md` §14 写「对外前缀共用 `/async`（由 nginx 按路径分流并重写，atask 内部端点是 `/queue`）」 | 仓库内代码只有 `/queue`（`app/routers/queue_task.py`）；若对外确实靠 nginx 重写成 `/async`，则「三件外部同步」里的客户端路径与 deny-list 写法都要按 `/async` 复核 | 以「仓库内 `/queue` + nginx 可选重写为 `/async`」并存理解，但**未擅自改任何一处**（改哪一处都会掩盖另外两处） | 需用户/运维确认对外形态 | 三处文档口径一致，且线上按确认后的前缀可受理 | OPEN（`external-prefix-doc-drift`） |

> **ADR-010 的 5 条已知限制不在本册重复登记**（令牌会话过期后任务无法自愈；
> 刻意不设 max-age 判死；`X-Upstream-Base-Url` 头的可信性依赖 nginx 配置；
> 取消语义退化；body 回调字段不拦截）。它们已经写在
> `ADR-010-queue-path-zero-billing.md` 的「已知限制」小节，**那里是唯一登记处**——
> 同一事实两处维护必然漂移，且读者无法判断哪份是事实源。


## 固定 slug 说明

（slug 会随登记项增加；标题不写死条数，避免「说三类、实际七个」这种自我漂移。）

- `config-invariant-unenforced`：文档/口头约定的配置不变量，代码未强制
  （**已无未决项引用**：2026-09-12 随 ADR-010 换向关闭，见上表；保留说明供追溯）
- `low-probability-window`：概率极低、且当前无第二道唯一约束兜底的并发窗口
- `design-asymmetry`：有意保留的设计不对称，仅记录
  （**已无未决项引用**：2026-09-12 随 ADR-010 换向关闭，见上表；保留说明供追溯）
- `deploy-drift-needs-ops-sync`：需运维同步部署版本
- `cross-repo-doc-drift`：跨仓库文档与代码不一致
- `acceptable-residual-window`：已评估可接受、不阻塞的残余窗口
  （**已无未决项引用**：2026-09-12 随 ADR-010 换向关闭，见上表；保留说明供追溯）
- `route-rename-external-sync`：本仓库内已改净、但需仓库外三方同步才能生效的改名
- `redis-prefix-drain-required`：本仓库内已改净、但需运维先排空队列才能安全切换的键空间改名
  （与上一条同族：都是「代码侧干净、外部动作未完成」）
- `external-prefix-doc-drift`：多份文档对同一对外形态各说一套，未确认前不擅改任何一处
- `deploy-topology-confirm`：部署拓扑待运维确认（发布端口 / 反代位置这类「代码侧只能选一侧默认值」的项）
- `doc-retirement-pending`：历史文档内容已沉淀，但退役收尾（门禁/删除）未完成。
  **本项已无未决项引用**（2026-09-12 关闭，见上表最后一行）；保留说明供追溯
