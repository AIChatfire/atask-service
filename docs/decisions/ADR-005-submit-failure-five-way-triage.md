# ADR-005: 提交失败五级分流——模糊失败绝不判死

> **已被取代（2026-09-12）**：本决策的前提（网关持有上游凭证与资金动作）已被
> **本仓库 ADR-010** 整体移除。**不要据本文实施**——保留本文仅为记录当时的权衡与
> 被否决的替代方案。取代原因见 ADR-010。
>
> **随之失效的结论**：五级分流中所有资金分支（`ACCOUNT_LEVEL` HELD 保留冻结 /
> `TASK_LEVEL` FAILURE + 解冻 / `KEY_LEVEL` 上报 keypool 换 key）随「零资金动作」
> 与「不再有上游 key」消失；分流收敛为 ADR-010 的**三档**（4xx 确定性拒绝 →
> FAILURE 释槽；5xx / 传输错误 → 留活重试；2xx 缺 id → FAILURE），且判死不再是
> 不可逆资金动作。**「模糊失败绝不判死」这条核心原则仍然成立**，见 ADR-010。

## Status: Superseded by 本仓库 ADR-010 (2026-09-12)

## Background

提交上游可能失败，失败原因分很多层。早期实现把「提交失败」粗分成
「同步 502」或「任务 FAILURE + 解冻」，这在两个方向上都错：

- **误判死**：基础设施故障（渠道 `base_url` 未配导致
  `Target host is not specified`、上游超时、5xx、熔断）被当成任务失败，
  任务瞬间终态 + 解冻。但这类失败里**上游可能已经接单**（超时场景），
  判死 = 上游真在跑的单「钱面裸奔」+ 把基础设施故障误伤成用户任务失败。
- **误挂起**：把任务级 4xx（内容审核拒绝）也挂起重试，白白占着冻结
  重试一个永远不会成功的报文。

核心原则：**判死是不可逆的资金动作**。只有「上游明确未接单且重试无意义」
的**确定性失败**才 FAILURE。

## Decision

提交失败按 **五级分流**（分类表见 `app/services/errclass.py`，
分流落地见 `submit._submit_rejected`，submit.py:231）：

| 层级 | 触发 | 动作 | 为什么 |
|---|---|---|---|
| `ACCOUNT_LEVEL` 账户级 | 欠费/封禁——**只在渠道 `error_classify` 显式配名单内才判定** | HELD 挂起，保留冻结 | 账户问题会恢复，判死等于把可恢复的任务烧掉；默认不认，防误伤 |
| `RATE_LIMITED` 限流 | 429（+ `Retry-After`） | HELD 挂起（固定 5m 退避，1h 判死） | 限流是时间窗口问题，重试有意义 |
| `KEY_LEVEL` key 级失效 | 默认 401/403 | 上报 keypool 驱动禁用坏 key，重打换 key | 单把 key 坏了，换 key 可能就好了 |
| `TASK_LEVEL` 任务级 | 4xx 业务错 / `ok_check` 信封业务错 | FAILURE + 解冻 | 上游**明确拒绝**这个报文，重打同一报文无意义 |
| `AMBIGUOUS` 模糊失败 | 599 网络/超时/base_url 缺失、5xx、熔断 | **留活重试**：保 SUBMITTED/QUEUED + 写 `data.last_submit_error` 观测，sweep 补投下轮重试 | 拿不准——上游可能已接单，也可能没发出；判死两头都可能错 |

### 五级分类的两个默认取向

`errclass.classify`（errclass.py:60）的默认表故意保守：

- **账户级故意留空**（`_DEFAULT_STATUS` 只有 401→key、403→key、429→限流）。
  欠费/封禁的措辞因厂商而异，必须渠道显式配置 `account_level` 状态码名单
  或 `account_level_messages` body 子串才生效——错杀账户级的代价远大于
  错放，宁可当成任务级。
- **拿不准的 4xx 一律按任务级**（errclass.py:79）。4xx 通常意味着「上游看懂
  了报文并拒绝」，重打无意义；5xx 与网络错归 `AMBIGUOUS`。

### 模糊失败的留活机制

模糊失败时（submit.py:276）：

- **不改状态**——任务保持 SUBMITTED/QUEUED（仍在 `ACTIVE`）；
- patch `data.last_submit_error` / `last_submit_error_at` 供 ops 观测；
- sweep 的 stale 补投负责下轮重试（reconcile.py:190）；
- 重复触发安全，靠三件事保证：`submit_one` 幂等短路（已有
  `upstream_task_id` 或终态直接 return）、提交互斥锁（ADR-003）、
  终态守卫 CAS（只允许活跃态落 QUEUED）；
- 持续失败的**上限**由孤儿收口兜底（ADR-003）——那才是「确实从未接单」
  的正确口径。

### 为什么持锁体内的 `UpstreamError` 也走同一分流

`_submit`（submit.py:110）捕获持锁体冒泡的 `UpstreamError`，交给
`_submit_rejected` 同一分流，而不是冒泡到 DLQ 了事。DLQ 是最后手段，
任务状态必须**诚实反映**「还没失败」——冒泡到 DLQ 会让任务停在
SUBMITTED 但没有任何重试路径。

### 提交阶段失败一律解冻，不走 `failed_billing=charge`

`_submit_rejected` 的任务级分支与 `finalize_task(..., failed_charge=False)`
（submit.py:300）显式区分：`failed_billing: charge|absorb` 策略只覆盖
**生成失败**的厂商条款收费，**不覆盖提交阶段拒绝**——提交阶段上游明确
未接单，一律解冻（`flow.finalize_task` 的 `failed_charge` 参数，
flow.py:227）。

## Consequences

- 正面：基础设施故障不再误伤任务，不再「秒失败 + 解冻」。
- 正面：账户级/限流可恢复问题挂起而非烧掉，补费后金丝雀排空。
- 正面：任务级确定性拒绝快速止损，不占冻结重试无望的报文。
- 负面：模糊失败会**多轮重试**同一提交，上游不支持幂等键时仍有极小的
  双重创建概率。缓解：`client_request_id_param` 幂等反查 + 孤儿收口。
- 负面：模糊失败的任务会长时间停在 SUBMITTED（直到重试成功或孤儿收口
  判死），客户端看到的是「排队中」。这是**有意**的——宁可显示久一点，
  也不把可能成功的任务提前判死。
- 负面：账户级判定依赖渠道显式配置，配错（把 403 配成账户级但当成本
  任务级）可能导致误挂起。缓解：默认不认账户级，需人工按厂商文档配。

## Related ADRs

- **atask-service ADR-003**（异步创建：模糊失败的窗口与孤儿收口）
- **atask-service ADR-004**（时间列归一：同期第二个「秒失败」根因）
- **atask-service ADR-006**（key 级失败上报与换 key）
- `app/services/errclass.py`；`app/services/submit.py`；
  `app/services/providers/`（keypool report）
