# atask-service 决策记录（ADR）索引

本目录是 atask-service 的**架构决策记录**（Architecture Decision Record）。
每篇 ADR 记录一个「已经定了、且以后不该随意改」的决策，重点写**为什么**
（设计动机、被否决的替代方案、真实踩过的坑、上游源码事实），而不是复述
代码「是什么」。

## 跨仓库编号冲突警示（先读这段）

atask-service 与 stask-service **各有一套独立的 ADR 编号，同一个编号在
两个仓库含义不同**。两仓库都有 ADR-001 / ADR-006 / ADR-008，但说的是完全
不同的决策，例如：

| 编号 | atask-service（本仓库） | stask-service（另一仓库） |
|---|---|---|
| ADR-001 | 复用 `tasks` 表，`platform='gateway'` | 复用 `tasks` 表，`platform='stask'`（约束细节不同） |
| ADR-006 | 任务级租约钉回精确到 key | 与 new-api 共存契约（轮询/超时清理防撞） |
| ADR-008 | 与 stask-service 的分工 | 上游中立化（AUTH_MODE）与热点路径优化 |

**引用任何 ADR 时必须写明仓库名**（如「stask-service ADR-002」），否则会
指到另一篇文件。两套编号**不合并、不对齐**——它们是两个独立演进的服务，
强行对齐编号只会制造误解。

## ADR 一览（按编号）

| 编号 | 标题 | Status | 一句话结论 |
|---|---|---|---|
| [ADR-001](ADR-001-reuse-newapi-tasks-table.md) | 复用 new-api `tasks` 表，网关零建表 | Accepted (2026-09-12) | 任务事实源 = new-api `tasks` 表，`platform='gateway'` 划分；网关零自有表，自有状态全在 Redis |
| [ADR-002](ADR-002-zero-route-files.md) | 零路由文件 | Superseded by ADR-010 (2026-09-12) | 接入新模型 = 渠道挂统一分组 + 配渠道 gateway 计费块；网关零代码、零配置文件（**历史**：已升级为 ADR-010 的零配置） |
| [ADR-003](ADR-003-async-create-and-orphan-closeout.md) | 创建接口异步化（**部分被取代**） | Accepted (2026-09-12)；孤儿收口一半由 ADR-010 删除 | 「落库即返回本地 `task_id`、上游提交交 worker」仍有效；**「双重提交窗口与孤儿收口」已删且无兜底**——该双建窗口新架构下无任何等价物 |
| [ADR-004](ADR-004-time-columns-normalized.md) | 共享表时间列不可信，一律归一 | Accepted (2026-09-12) | 一切时间计算走 `as_unix_seconds`，SQL 比较套 `_secs()`，判死类动作 finalize 前二次核龄 |
| [ADR-005](ADR-005-submit-failure-five-way-triage.md) | 提交失败五级分流——模糊失败绝不判死 | Superseded by ADR-010 (2026-09-12) | 账户级/限流 → HELD；任务级 → FAILURE+解冻；模糊失败 → 留活重试（**历史**：资金分支已删，现为 ADR-010 三档） |
| [ADR-006](ADR-006-task-lease-exact-key-pin.md) | 任务级租约钉回精确到 key | Superseded by ADR-010 (2026-09-12) | 探测/取消/原生查询/回调/对账用 `channel_id + key_index` 单 key 直达；**提交链路不钉 key**（**历史**：网关已不再持有上游 key） |
| [ADR-007](ADR-007-billing-rule-at-lease-local-sandbox.md) | 计费规则随租约下发、网关本地沙箱求值 | Superseded by ADR-010 (2026-09-12) | 规则唯一事实源 = keypool 渠道元数据；asteval 本地求值；结算 = 规则值 × `discount_rate`（**历史**：网关已零资金动作） |
| [ADR-008](ADR-008-division-with-stask-service.md) | 与 stask-service 的分工——不合并 | Accepted (2026-09-12) | stask「同步转异步」、atask「异步任务再网关」；分工按「**包哪种上游 + 占哪个 URL 前缀**」划分（`/async` vs `/batch`）；共用 `tasks` 表但 `platform` 不同；不要把两者合并 |
| [ADR-009](ADR-009-exception-layering-and-module-locality.md) | 异常分层——不引入统一基类 | Accepted (2026-09-12) | 端口契约异常定义在端口模块（`providers/__init__.py`，供各实现共享）、模块私有异常就近定义；**刻意不建统一基类**——异常处置是逐点决策（现行见 ADR-010 三档分流），继承层级帮不上忙 |
| [ADR-010](ADR-010-batch-path-zero-billing.md) | 对外形态统一 `/batch/{上游路径}`，鉴权与计费全部下沉上游 | Accepted (2026-09-12) | 唯一形态 `POST/GET/DELETE /batch/{path}`；用户 token 原样透传、网关零资金动作、渠道路由零配置；**supersedes ADR-002/005/006/007、rewrites ADR-008** |

配套文件：[`OPEN-DECISIONS.md`](OPEN-DECISIONS.md)（未决事项登记册，
只追加、就地关闭）。

## 阅读顺序建议（新人先读这四篇）

1. **ADR-010** —— 现行架构的唯一入口：`/batch/{上游路径}` 形态、鉴权下沉上游、
   零资金动作、上游寻址、终态收敛与已知限制。当前代码就是它的实现。
2. **ADR-008** —— 与 stask 的分工：两服务按「包哪种上游 + 占哪个 URL 前缀」
   区分（`/async` vs `/batch`），为什么不合并。跳过它会反复把 atask 与 stask
   搞混。
3. **ADR-001** —— 数据基座：任务事实源在共享 `tasks` 表、`platform`
   划分、零建表。后面所有 ADR 都建立在「任务行长什么样」之上。
4. **ADR-004** —— 共享表时间列纪律，共享表上最贵的坑（`as_unix_seconds` /
   `_secs()` / 判死前二次核龄），现行链路仍然有效。

**ADR-002 / ADR-005 / ADR-006 / ADR-007 已是历史记录**（均被 ADR-010 取代：
「网关持有上游凭证与资金动作」这一前提已被整体移除）——只用于追溯当时的权衡与
被否决的替代方案，**不要据其实现**。**ADR-003** 的「落库即返回本地 task_id」异步
受理思路被 ADR-010 继承，但其 preflight 与资金侧孤儿收口细节已随旧链路删除。
**ADR-009** 是写新代码前扫一眼的短约定（异常放哪、为什么不统一基类），仍然有效。

## 写作约定

- 中文；技术术语保留英文原文。
- 无 emoji（`tests/test_static_gates.py::test_no_emoji_in_decisions` 机械拦截）。
- 关键数字/顺序要解释「为什么不能改」并给出反例后果。
- 交叉引用具体文件路径（如 `app/services/relayflow.py`、`app/deps/identity.py`）；
  引用**行号**时务必谨慎——行号会随编辑漂移，能写符号名就写符号名。
- 引用别的仓库的 ADR 时带仓库名。
