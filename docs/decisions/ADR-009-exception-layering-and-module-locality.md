# ADR-009: 异常分层——不引入统一基类，端口契约异常与模块私有异常分置

> **仓库独立声明**：本 ADR 属于 **atask-service** 的决策系列。stask-service
> 的 ADR 系列编号独立、内容不同；引用任何 ADR 时必须带仓库名。

## Status: Accepted (2026-09-12)

> **修订（2026-09-13，随 ADR-010 换向核对）**：本 ADR 的**决策**（不引入统一
> 基类、异常就近定义）仍然有效，下文模块清单已更新为现行实现。原文用作例证的
> `providers/__init__.py`（端口契约异常）与「提交失败五级分流」已随旧链路删除：
> 端口层不存在，「端口契约异常」这一类**当前为空**；分流改为 ADR-010 的三档
> （网关零资金动作）。决策不受影响——「逐点决策而非按继承子树分派」这一理由在
> 新链路里同样成立。

## Background

随着 provider 适配层、上游引擎、通知等模块增多，本仓库出现了一批自定义异常。
曾有一个提案：**引入统一异常基类**（如 `GatewayError`），让「本服务的业务异常」
可以一次 `except GatewayError` 捕获，并宣称这样更整齐、跨模块好认。

核对参照物后否掉了这个提案，事实如下：

- **stask-service 自己也没有统一基类**。它有 7 个平铺的 `Exception` 子类
  （`SubmitConflict` / `SlotExhausted` / `BatchParamError` /
  `TaskExecutionError` / `UpstreamAuthError` / `AdmissionError` /
  `ScheduleError`），各自**就近定义在抛出它的模块**里，没有公共父类
  （stask-service `app/services/submit.py:39,43`、`batching.py:93`、
  `outcome.py:107`、`upstream.py:54`、`admission.py:40`、`schedule.py:54`）。
- **atask-service 现状同构**：ADR-010 换向后全仓只有 3 个自定义异常，全部
  **就近定义在抛出它的模块**，无公共父类——`RelayError` 在
  `app/services/relay.py`（中继出站失败）、`NotifyError` 在
  `app/services/notify.py`（回调投递）、`BreakerOpenError` 在
  `app/services/upstream.py`（熔断）。旧链路那个「端口模块因被多方共享而天然
  成为聚合点」的例子（`providers/__init__.py`）已随 provider 适配层删除。

也就是说，「统一基类」不是本项目演化中缺失的一环，而是**两个仓库都主动没做**
的选择。本 ADR 把这个选择正式记录下来。

## Decision

**不引入统一异常基类。** 异常按归属分两类，各自决定定义位置：

1. **端口契约异常**（跨实现共享、调用方按契约依赖的）：定义在**端口模块**。
   **本仓库当前这一类为空**——ADR-010 后 provider 端口层（`providers/`）已随
   旧链路整体删除，端口 `Protocol` 与实现类都不复存在。规则保留备用：将来若
   重新出现「一份端口契约、多个实现」的结构，异常就跟端口放同一个模块，
   **不要**退化成往公共 `exceptions.py` 里堆。
2. **模块私有异常**（只有抛出它的模块及其直接调用方关心的）：**就近定义在
   抛出它的模块**。现行三例：`RelayError` 在 `app/services/relay.py`；
   `NotifyError` 在 `app/services/notify.py`；`BreakerOpenError` 在
   `app/services/upstream.py`。

判断规则：**这个异常是否被当作跨实现/跨模块的契约？是 → 端口模块；
否 → 抛出模块。** 不额外造继承层级，不设「本服务全部业务异常」的公共祖先。

## 理由

### 1. 统一基类会强迫调用方按继承层级做 `except` 分派，而处置动作是逐点决策的

本仓库最有说服力的反例是**提交失败三档分流**
（`app/services/relayflow.py::submit_queue_task`，现行口径见
**atask-service ADR-010**）：同一个「上游返回了错误」要按**三个不同去向**分派——

| 分流 | 处置 |
|---|---|
| 上游 4xx | FAILURE：确定性拒绝 → 还并发槽、清令牌会话、按需回调 |
| 5xx / 传输错误 / 599 哨兵 | **留活**：保持 SUBMITTED/QUEUED，交队列退避重试 |
| 2xx 但缺任务 id | FAILURE：约定被违反，立即可见、不静默挂起 |

这些去向**逐点决策**，取决于「上游到底有没有接单」「重试还有没有意义」，
而不是「这个异常属于哪个继承子树」。如果异常被塞进统一基类、调用方写成
`except GatewayError`，就会把「判 FAILURE」与「留活重试」这两种**语义相反**的
处置混进一个分支——而它们的分界线恰恰是「上游有没有接单」这一事实，继承层级
根本表达不了。继承层级在这里**帮不上忙**，只会诱导出一个过粗的 `except`。

### 2. 就近定义让「谁抛谁定义」一眼可见

读者在 `upstream.py` 里看到 `raise UpstreamError(...)`，异常类就在同文件顶部；
不需要先跳到某个 `exceptions.py` 再回来。删除/重命名一个模块时，它的私有异常
跟着一起走，不会在公共基类文件里留下孤儿。这与本仓库既有的
「单一数据访问点 `taskstore.py`」是同一种取向：**把相关的东西收在一个可审的
位置**，而不是为了形式统一分散到中心文件。

### 3. 与参照物一致

stask-service 的 7 个异常都是「就近定义、无公共基类」。本仓库沿用同一取向，
跨仓库阅读时心智模型一致（两仓库本就共用一套工程外壳，见 **ADR-008**）。

## Consequences

- **正面**：`except` 分支必须显式列举它真正关心的异常类型，逼迫作者写出
  「这个失败要做什么」——而不是一个笼统的父类一把抓。资金动作的安全性因此提高。
- **正面**：新增/删除模块时异常随之移动，公共异常文件不会积累孤儿条目。
- **负面（如实记录）**：**无法一次性捕获「本服务的全部业务异常」**。需要
  「兜底记日志 / 兜底转 5xx」这类横切逻辑时，必须**枚举**所有自定义异常类型。
  缓解：本仓库的横切入口是固定的少数几处（FastAPI 错误处理器 `app/errors.py`、
  taskiq 队列中间件的失败路径），枚举成本可控；若某天枚举点扩散到多处，
  再考虑引入统一基类——**届时应新写一条 ADR 覆盖本决策，而不是悄悄加基类**。
- **负面**：类名风格靠人工维持（`*Error` 为主）。缓解：`tests/test_static_gates.py`
  已有门禁 `test_no_orphan_exception_classes`，断言 `app/` 里定义的每个自定义
  异常类**至少被 `raise` 一次**，防「定义了却没人抛」的孤儿异常（与既有的
  `test_no_orphan_service_functions` 同一思路）。

## Related ADRs

- **atask-service ADR-005**（提交失败五级分流：本决策最直接的动机——资金处置
  逐点决策，继承层级帮不上忙）
- **atask-service ADR-008**（与 stask-service 的分工：本决策对齐参照物的工程
  取向，但不合并两套编号）
- `app/services/relay.py`；`app/services/notify.py`；`app/services/upstream.py`
  （现行全部自定义异常的归属：就近定义、无公共基类）
- `tests/test_static_gates.py`（孤儿异常门禁，随本 ADR 的取向新增）
