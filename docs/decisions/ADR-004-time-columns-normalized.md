# ADR-004: 共享表时间列不可信，一律归一——不可逆动作前二次核龄

## Status: Accepted (2026-09-12)

## Background

`tasks` 是与 new-api 共享的表（**atask-service ADR-001**）。时间列的单位
不由本服务独占：new-api 原生任务模块用 **UnixMilli（毫秒）** 写法，历史行
与任何其他写入方都可能把毫秒值写进同一列。而网关的一切时间计算——探测
超龄、stale 判定、孤儿判死、HELD 判死、duration、对账窗口——都是**秒**
口径。

### 真实事故（部署漂移 + 单位混用）

线上出现「任务秒失败」：服务器上跑着一个**Java 重实现的旧版网关**
（Spring Boot，监听端口 39600）。这个版本把 `tasks.submit_time` 里本应是
**秒**的值当**毫秒**读，于是：

- 刚创建的新任务瞬间被算成「超龄」，秒判 FAILURE + 解冻；
- 超时文案出现 `task timeout after 1440 minutes`（把秒值按毫秒解释，
  算出的年龄被放大 1000 倍后换算成分钟）；
- 该版本写回的 `finish_time` 是**毫秒**（如 `1787199556676`），进一步污染
  共享列，让后续任何秒口径比较继续失真。

同一时期还有第二个根因（`OPTIMIZATION_BACKLOG.md`「提交期模糊失败不再
判死」）：渠道 `base_url` 缺失时 httpx 拿相对路径发请求，产生
`I/O error on POST request for "": Target host is not specified`，被当成
任务失败判死。这一条属于 ADR-005 的范畴，此处只记「毫秒」这条。

`OPTIMIZATION_BACKLOG.md` 末尾也点名了漂移：`finish_time=1787214476106`
（毫秒）与 `result_url` 列写入来自「部署版与本地仓库的漂移（2026-08-17
已发现）」，本地已修，部署侧需同步。

## Decision

**「写侧恒写秒、读侧统一归一、SQL 比较套归一表达式、不可逆动作前二次
核龄」四道纪律，缺一不可。**

1. **写侧恒写秒**：`taskstore.cas`（taskstore.py:155）终态一律用
   `_now()`（`int(time.time())`）刷 `finish_time`，并一律把 `progress`
   置 `100%`（不只 SUCCESS——失败/取消停在 `0%` 会让看板以为还在跑）。

2. **读侧统一归一**：`taskstore.as_unix_seconds`（taskstore.py:45）——
   值 `> 1e11` 视为毫秒折算秒，缺失/非法 → 0。`_row_to_dict`
   （taskstore.py:90）对全部时间列（`submit_time` / `start_time` /
   `finish_time` / `created_at` / `updated_at`）统一归一。消费方
   （`flow.duration_seconds` / `public_view` / polling / ops）拿到的
   永远是秒。

3. **SQL 比较必须套 `_secs(col)`**：`_secs(col)` =
   `IF(col > 1e11, col DIV 1000, col)`（taskstore.py:34）。为什么读侧的
   Python 归一**不够**：在 SQL 里做的 cutoff 比较（`col < :cutoff`，
   cutoff 恒为秒）遇到毫秒列会彻底失真——毫秒行永远躲过判死，
   而秒行若被拿去与毫秒口径比较就会被瞬间判死。所有时间比较统一套它：
   `stale_active`、`orphan_active`、`held_expired`、`reconcile_candidates`、
   `oldest_held` 排序全部包裹（taskstore.py:256、:340、:380、:439、:359）。

4. **判死类不可逆动作在 finalize 前二次核龄**：
   `reconcile._orphan_closeout`（reconcile.py:53）在 `finalize_task` 前按
   归一后的秒重算年龄，不足 grace 则跳过并告警。判死是不可逆资金动作，
   查询层被脏时间列骗过也还有最后一道防线（宁可这轮跳过下轮再来，
   绝不把刚创建的任务秒判失败）。`polling.poll_one`（polling.py:50）
   同样归一 + 负值钳 0 + `submit_time` 缺失回退 `created_at`，
   且超时文案带**实际配置值**（不再出现硬编码的「1440 minutes」类谎话）。

### 与 stask-service 的刻意不对称

stask-service 的 `ADR-008`（stask 仓库）做了**相反**的决定：
删除 `_secs()` 包裹、时间谓词裸列比较走索引。它成立的前提是
「写侧恒写秒 + 查询恒带 `platform='stask'`（只命中本服务的秒值行），
毫秒值只存在于 new-api 自己写的行，那些行我们碰不到」。

这个前提在 atask 这里**不成立**：atask 的 `platform='atask'` 行本身
就曾被 Java 漂移版写成毫秒（同一个 platform 值）。只要网关自己的行可能
含毫秒，裸列比较就会重演「任务秒失败」。**所以 atask 保留 `_secs()` 包裹，
不能照抄 stask 的 ADR-008**。代价是包裹列使 range 条件吃不到索引，
退化为按 `platform` 过滤后的扫描——这是为正确性付出的性能代价，
**有意接受**。

## Consequences

- 正面：单位混用不再导致误判死（钱的正确性优先于查询性能）。
- 正面：`duration` / 对外视图 / 对账窗口永远拿到秒，不产出天文数字。
- 负面：`_secs()` 包裹让时间谓词无法使用列索引，sweeper 的扫描成本
  高于裸列比较。缓解：所有扫描都带 `platform` + 状态 + 时间窗三重收敛
  并带 LIMIT。
- 负面：读侧归一是「猜测式」修复（`> 1e11` 阈值），无法区分「真毫秒」与
  「未来时间戳」。缓解：判死前二次核龄；对「未来时间」负值/年龄钳 0。
- 负面：这套纪律无法约束**别的写入方**（Java 漂移版、new-api 原生模块）
  继续写毫秒。根治需要部署侧统一版本或推动 new-api 侧改为秒——
  见 `OPEN-DECISIONS.md`（部署版本漂移）。

## Related ADRs

- **atask-service ADR-001**（复用共享 `tasks` 表——本决策的前提）
- **atask-service ADR-003**（孤儿收口：二次核龄的落点）
- **atask-service ADR-005**（模糊失败留活重试：同期第二个「秒失败」根因）
- stask-service ADR-008（stask 仓库，刻意相反的决定，不可照抄）
- `app/services/taskstore.py`；`app/services/reconcile.py`；
  `app/services/polling.py`；`OPTIMIZATION_BACKLOG.md`
