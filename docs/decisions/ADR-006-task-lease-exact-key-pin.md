# ADR-006: 任务级租约钉回精确到 key——探测/取消/原生查询/回调/对账一律单 key 直达

> **已被取代（2026-09-12）**：本决策的前提（网关持有上游凭证与资金动作）已被
> **本仓库 ADR-010** 整体移除。**不要据本文实施**——保留本文仅为记录当时的权衡与
> 被否决的替代方案。取代原因见 ADR-010。
>
> **随之失效的结论**：任务级租约钉 key（`channel_id + key_index` 单 key 精确直达、
> key 级失败降级为渠道直达、`data.key_id` / `data.key_index` 持久化）随「网关不再持有
> 上游 key」整体消失——不再有「换 key 查不到任务」这个问题。**鉴权改为用户 token 原样
> 透传上游**（ADR-010 §2），上游账号由用户自己的 token 决定。

## Status: Superseded by 本仓库 ADR-010 (2026-09-12)

## Background

keypool 的一个**渠道**下可以挂多把属于**不同上游账号**的 key
（new-api channels 的 key 列表）。选渠道是 `select(group, model)`，
默认会走轮换批次 / 轮询游标 / usage 打分挑一把 key。

问题：任务在 A 账号那把 key 上创建。之后探测/取消/原生查询若换到 B 账号的
key，上游会返回 404 / 无权限——任务凭空「消失」，冻结只能等 sweep 兜底。
`app/services/leasing.py` 开篇把这个坑写得很直白：

> 一个 keypool 渠道下可以挂多把属于**不同上游账号**的 key。任务在 A 账号
> 创建，探测/取消/原生查询若换到 B 账号的 key，上游会返 404 / 无权限——
> 任务凭空"消失"，冻结只能等 sweep 兜底。

这个坑在旧 polling 实现里就存在（`AI_TODO.md`「keypool 精确直达」条目）。

## Decision

keypool `select` 支持 `channel_id + key_index` 的 **单 key 精确直达**
（`mode="direct"`：跳过轮换批次/轮询游标/usage 打分，且**不访问 Redis**）。
网关只递一个下标，凭证治理（禁用/轮换/epoch）完整留在 keypool 侧，
**网关绝不缓存明文 key**（红线）。

### 哪些操作钉 key，哪些不钉

| 链路 | 钉 key？ | 理由 |
|---|---|---|
| 探测（poll） | 钉 `channel_id + key_index` | 任务已在某把 key 的上游，换 key 查不到 |
| 取消（cancel） | 钉 | 同上，源头止损要打到同一账号 |
| 原生查询（probe 拦截） | 钉 | 同上 |
| 回调（callback） | 钉 | 终态核对要打同一账号 |
| 反向对账（reconcile） | 钉 | 多账号下换 key 会把「上游没这条任务」误读成对账通过，掩盖真实少收/多退 |
| **提交（submit）** | **不钉** | 任务还没进上游，渠道内**任意健康 key** 都可用；钉死一把反而在该 key 被禁时白等重试 |

提交链路的纪律（`submit._submit`，submit.py:90）：首打钉回**原渠道**
（`key_id` 直达，与 preflight 租约/对账口径一致），key 级确定性拒绝重打时
换**新鲜租约**（不钉渠道，keypool 剔除坏 key 后自动切健康渠道）。
`AI_TODO.md` 记同样的结论：「提交链路不钉 key（首打钉渠道 key_id 直达，
key 级确定性拒绝重打换新鲜租约）」。

### 降级纪律

精确直达失败时（`leasing.lease_for_task`，leasing.py:41）：

| 失败类型 | 表现 | 动作 |
|---|---|---|
| key 级失败 | 40010 索引越界（该 key 已被移出渠道，永久性）/ 40001 该 key 被禁用 | 自动降级为**渠道直达**（`key_id` + 不传 `key_index`），渠道内换一把健康 key 继续 |
| 渠道级失败 | 40002 渠道不存在等 | **原样上抛**，无从降级 |

为什么 key 级可以降级而渠道级不行：key 级失败时渠道仍存在，换渠道内健康
key 对单账号渠道完全无损；多账号渠道下这一跳可能查不到任务，但比直接放弃
更好。渠道级失败时连渠道都没了，没有降级目标。

`key_index` 必须搭配 `channel_id`，单独出现被忽略（`leasing._pin`，
leasing.py:32）：旧任务无 `key_index` 快照 → 自动退渠道直达。
`channel_id` 取 `data.key_id` 为主、`tasks.channel_id` 列兜底。

### 唯一入口

`app/services/leasing.py` 是唯一入口：`lease_for_task` / `route_for_task`。
polling / reconcile / callback / `flow.try_upstream_cancel` /
proxy 生命周期拦截全部迁入，钉 key 语义**一处维护**（`AI_TODO.md` K3）。

## Consequences

- 正面：同渠道多账号场景下任务不再「消失」，探测/取消/对账命中正确账号。
- 正面：提交链路保持灵活（任意健康 key），不被单把坏 key 拖住。
- 正面：凭证治理仍在 keypool 侧，网关不缓存明文 key（红线不破）。
- 负面：任务行需持久化 `data.key_id` / `data.key_index`，多一处内部状态。
  缓解：key_ids 不是敏感凭证（是渠道/下标），落 tasks.data 无红线问题。
- 负面：key 级降级到渠道直达时，多账号渠道下**可能查不到任务**（换到
  别的账号）。有意接受的取舍——精确 key 已被移出/禁用，没有更好的选择，
  比直接放弃好。
- 负面：提交不钉 key，意味着提交可能落到与 preflight 预估不同的 key 上；
  重打落到别的渠道时需同步 `key_id`/`key_index` 与 `channel_id`
  （submit.py:202），多一次 patch。

## Related ADRs

- **atask-service ADR-002**（零路由文件：渠道选择机制）
- **atask-service ADR-005**（key 级失败上报驱动 keypool 禁用坏 key）
- `app/services/leasing.py`；`app/services/polling.py`；
  `app/services/reconcile.py`；`app/services/flow.py::try_upstream_cancel`
