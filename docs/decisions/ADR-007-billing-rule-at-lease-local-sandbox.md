# ADR-007: 计费规则随租约下发、网关本地沙箱求值

> **已被取代（2026-09-12）**：本决策的前提（网关持有上游凭证与资金动作）已被
> **本仓库 ADR-010** 整体移除。**不要据本文实施**——保留本文仅为记录当时的权衡与
> 被否决的替代方案。取代原因见 ADR-010。
>
> **随之失效的结论**：freeze / settle / cancel 三段计费闭环、asteval 计费规则沙箱
> （`pricing.py`）、计费规则随租约下发、settle 三档降级、`failed_billing` 策略，
> 全部随「网关零资金动作」消失——配额改由上游 relay 一处扣减（ADR-010 §3），
> `tasks.data` 不再写 `freeze_amount` / `settled`。

## Status: Superseded by 本仓库 ADR-010 (2026-09-12)

## Background

网关自带计费闭环：提交顶格预估 `freeze` → 终态按实际用量 `settle`
（多退少补）/ `cancel`（全额解冻）。计费规则必须有个事实源。可选：

- (a) 网关本地维护一张「模型/渠道 → 规则」的配置表（或 YAML）；
- (b) 规则放 keypool 渠道元数据，随租约下发，网关本地求值；
- (c) 每次报价/结算都远程调 keypool 求值。

(a) 的问题是多渠道多规则会漂移——渠道元数据已经在 keypool 一处维护，
再在网关复制一份规则表，两边一旦不同步就会算错钱。(c) 的问题是报价/结算
是热路径（每次 preflight 与 settle 重估都走），远程调用会把计费可用性
绑死在 keypool 上，且增加 RTT。同理「settle 时再问 keypool」也违反
「租约已携带渠道全量元数据」的设计（**atask-service ADR-002**）。

## Decision

**渠道 `billing.rule` 的唯一事实源是 keypool 渠道元数据；随租约下发；
网关用 asteval 沙箱本地求值；零额外远程调用。结算 = 规则值 ×
`discount_rate`。**

配置形态（`app/services/pricing.py` 开篇）：

```json
"billing": {
  "rule": "def calulate(request):\n    return float(request.get('duration') or 5) * 0.026",
  "type": "second",
  "discount_rate": 1.0
}
```

- 随 keypool 租约（`include_channel=true`）下发 →
  `registry.route_from_channel` 摊平为 `RouteConfig.billing_rule /
  billing_type / discount_rate`（registry.py:87）；
- `rule` 是**完整 Python 函数定义**（约定函数名 `calulate`，历史拼写，
  兼容 `calculate/calc/compute/price`；纯表达式形态自动兜底）；
  入参为**完整请求体**，返回值为计费金额（USD）；
- 实际金额 = 规则返回值 × `discount_rate`（缺省 1，**折扣必乘**）；
- 渠道未配 `billing` → 报价 0（免费渠道，不产生冻结）。

### 为什么不做成本地配置表

规则的输入有渠道特异性（不同上游的用量字段名、计价维度都不同），且规则会
随商务条款频繁调整。把它复制进网关配置表 = 制造第二个事实源，两边同步靠
纪律，纪律总有一天会破。放 keypool 渠道元数据后，**改规则 = 改渠道，
零网关改动**（与 ADR-002 的「零路由文件」同一哲学）。

### 沙箱安全边界

计费规则是**外部输入**（运营在渠道上配的 Python 代码），必须按不可信代码
对待（`pricing.py`）：

1. **每次求值新建 `Interpreter`**（`_fresh_interpreter`，pricing.py:136）。
   为什么不能复用共享实例：asteval 的 symtable 在 eval 时可变，共享实例
   并发 eval 会**串账**——A 用户的 `request` 符号泄进 B 用户的求值。
   计费纪律红线：**语义与优化前完全一致，报价金额一个分都不能变**。
2. **语句长度上限** `max_statement_length`（默认 50000，与 asteval 默认
   对齐）：超限规则视为解析失败，回落原始完整求值路径，错误语义与优化前
   逐字节一致。
3. **符号表模板只读共享**：`_BASE_SYMTABLE`（`use_numpy=False`，123 个
   符号）模块级构建一次、只读共享；每次求值 `dict()` 浅拷贝后再注入
   `request` / `units` / 数值字段，**绝不原地修改**。
4. **解析缓存有界**（`ParsedRuleCache`，LRU 上限 256）：缓存的是
   **只读 AST**（asteval 求值只遍历不改写），可跨协程/线程安全共享；
   有界防异常渠道配置撑爆内存。缓存命中不缓存 Interpreter 实例——见第 1 条。
5. 求值失败抛 `PricingError`，**绝不静默按 0 计费**。

### settle 三档降级

终态实收金额的取值优先级（`flow._settle_amount`，flow.py:190）：

1. **`actual_amount_path`**：上游直接给出实收金额（最可信，如
   `task.usage.amount`）→ 直接用；
2. **`settle_usage_map` 重估**：终态报文提取实际用量覆盖原始请求体，
   **重跑同一份渠道计费规则**得实收。例：`duration ←
   task.usage.output_seconds`，把预估用的 `duration=5` 换成实际产出秒数；
3. **冻结金额兜底**（顶格预估即实收，多退少补语义退化为不补不退）。

**绝不静默按 0 结算**——重估失败（`PricingError`）时回退冻结额并告警
（flow.py:219），交由 sweeper 对账，而不是把用户的钱退成 0。

### 失败单计费

渠道可配 `failed_billing: charge|absorb`（schema.py:176）：`absorb`（默认）
失败全额解冻；`charge` 失败也收费——同样走上面的三档。但**提交阶段失败
永远 `absorb`**（ADR-005）：失败单 charge 只覆盖生成失败的厂商条款。

## Consequences

- 正面：规则单事实源（keypool），改规则零网关改动、零发版。
- 正面：报价/结算零额外远程调用（preflight 报价与 settle 重估用同一份
  本地规则），计费可用性不被 keypool 求值路径绑死。
- 正面：上游实收/用量重估/冻结兜底三档，最大限度贴近真实成本。
- 负面：沙箱执行外部代码有固有风险（ateval 是表达式求值器，能力受限，
  但没有 JVM 级隔离）。缓解：渠道元数据由运营/管理员维护（可信边界在
  keypool），语句长度上限，异常必抛不吞。
- 负面：规则在网关与 keypool 之间靠租约传递，若渠道刚改规则而租约未刷新，
  有最长 60s 的缓存窗口（ADR-002）。
- 负面：每次求值新建 Interpreter 有固定开销。缓解：解析缓存（LRU 256）
  缓存 AST，报价热路径的 `ast.parse` 被摊掉；Interpreter 构造本身轻量。
- 负面：`discount_rate` 漏乘会算错钱——代码层强制 `* route.discount_rate`
  （pricing.py:184），但配置里写错折扣仍会按错价计费。

## Related ADRs

- **atask-service ADR-002**（零路由文件：规则随租约下发的载体）
- **atask-service ADR-005**（提交阶段失败一律解冻，不走 failed_billing=charge）
- `app/services/pricing.py`；`app/services/registry.py`；
  `app/services/flow.py::_settle_amount`；`app/deps/preflight.py`
