# ADR-002: 零路由文件——接入新模型 = 渠道挂进统一分组 + 配 gateway 计费块

> **已被取代（2026-09-12）**：本决策的前提（网关持有上游凭证与资金动作）已被
> **本仓库 ADR-010** 整体移除。**不要据本文实施**——保留本文仅为记录当时的权衡与
> 被否决的替代方案。取代原因见 ADR-010。
>
> **随之失效的结论**：keypool 渠道元数据作为路由唯一事实源、`biz` 优先级链、渠道覆盖三层
> 叠加、路由缓存与免费 path→channel 钉回、`result_url_template` 产物直链改写，全部随
> keypool 渠道块一并删除；「零路由文件」升级为 ADR-010 的**零配置**——渠道路由元数据
> 一律按 new-api 约定硬编码，不再有任何本地配置来源。

## Status: Superseded by 本仓库 ADR-010 (2026-09-12)

## Background

网关要对下适配「任意异步上游」（视频生成等）。传统写法是每个模型一族一个
Python 适配器 + 一份 YAML/路由表，接入新模型 = 提 PR 改代码改配置 + 发版。
这在渠道频繁增减的运营场景下不可接受：上游产品（视频生成 / 图像生成 /
语音合成 / …）由运营侧随时试接，网关不该为每个模型重新走一遍发版流程。

`app/schemas.py` 开篇的设计原则写得很直白：「**新增一个上游模型 = 一段路由
配置（RouteConfig）+ keypool 一个渠道，不写 Python 代码**」。

## Decision

**网关不维护任何路由文件、不硬编码任何模型知识。** 上游配置的唯一事实源
是 **keypool 渠道元数据**（new-api `channels` 表），随租约
（`KeyLease.channel`）下发，网关侧由 `registry.route_from_channel`
（app/services/registry.py:62）摊平为 `RouteConfig`。

接入新模型两步（`README.md`「傻瓜式接入新模型」）：

1. **渠道挂进统一分组**：全部渠道挂在同一个 group（默认 `keypool`，
   `KEY_GROUP` 可配）下。选渠道 = `select(group, model)`，**model 决定
   渠道**（new-api abilities 表映射），与 URL 路径解耦。
   URL `/{biz}/` 只是**入口标签**，不做路由依据。
2. **渠道配 gateway 块**：放 `header_override.upstream` 嵌套块或
   `setting.gateway`（两处等价、优先级从高到低），配 `submit_path` /
   `probe_path` / `status_path` / `result_path` / `settle_usage_map` /
   `billing` 等。缺省值见 `registry._GATEWAY_DEFAULTS`（registry.py:30）。

拼装请求头时会**自动剥离** `header_override.upstream` 嵌套块，绝不作为
HTTP 头透给上游（`providers/keypool.py` + `upstream.auth_headers`）。

### biz 的优先级链

```
setting.gateway.biz（或 header_override.upstream.biz）   ← 最高
  → 渠道 name
    → URL 段兜底
```

实现见 `registry.route_from_channel`（registry.py:99）。内部记录
（`tasks.data.biz`、回调路径、计费维度）一律用渠道给出的权威 biz，
不用 URL 段——URL 段只是兜底。

### 渠道覆盖三层叠加

提交报文以**用户请求体为基底**，按优先级从低到高叠加：

```
route.default_params  <  用户 body  <  channel.param_override
```

`model_mapping` 改写 `model`；用户自带的 `callback_url` / `webhook`
**一律摘除**（用户回调由网关签名投递，防止用户把回调指向任意地址）。

### 路由缓存与免费路径

`RouteRegistry`（进程内 TTL 60s，registry.py:115）在每次租约解析后回填
（`remember`）。无租约上下文的入口（如 callback）先读缓存，未命中再按
`channel_id` 直达租约重建。免费 GET 透传没有 model，keypool `select` 对空
model 必拒 40010，因此按「path 里的任务 id → 渠道」→ Redis `biz→channel_id`
记忆（`app/services/routecache.py`）→ 进程缓存三级钉回 `channel_id`，
**永不发起空 model 的 select**。

### 渠道配置位补充：产物直链改写（转存/镜像）

渠道 gateway 块可配 `result_url_template`（如
`https://myhost.com/{upstream_result_url}`），让所有出口只出现网关地址，
上游原始直链不外泄。纯函数零 I/O，实现 `app/services/resulturl.py`。

占位符 6 个（`resulturl._tokens`，resulturl.py:46）：`{upstream_result_url}`
（原样）/ `{upstream_result_url_encoded}`（百分号编码，当查询参数值时用）/
`{upstream_result_url_no_scheme}`（去 scheme，拼路径段时用）/
`{upstream_result_host}` / `{upstream_result_path}`（含查询串）/
`{task_id}`。未知占位符**原样保留**——响亮暴露配置错误，而不是静默产出坏链接。

生效范围全入口一致（客户端永远看不到上游直链）：

- `flow.finalize_task` 终态落库时改写 `data.result`，原始直链另存
  `data.upstream_result`（对账/回源用）→ `/v1/tasks` GET、videos 视图、
  用户回调载荷自动跟随（flow.py:243）；
- 原生查询拦截对报文里的直链做**字节级替换**（不重新序列化，报文其余
  部分 100% 同构，`resulturl.rewrite_bytes`），终态快照回放同理。

**网关只改地址、不搬字节**——回源/转存由模板指向的服务负责。模板为空 =
不改写（默认零影响）。这条「网关不做内容搬运」是长期边界，不是实现细节：
把字节搬进网关会引入存储、带宽、清理三类新问题。

## Consequences

- 正面：接入新模型零代码、零配置文件、零发版；渠道覆盖（base_url /
  model_mapping / param_override / header_override / status_code_mapping /
  proxy）在 new-api 渠道上配一次，网关自动消费。
- 正面：判定逻辑全部来自渠道配置，网关不认识任何具体上游；
  换模型只改 keypool 渠道。
- 负面：配置错误在运行期才暴露（如 `submit_path` 缺失）。
  缓解：创建链路在 `flow.create_task`（flow.py:109）与 worker 侧
  `submit._submit`（submit.py:92）都对 `submit_path` 做硬校验，
  缺失即响亮报错（502 / FAILURE），不静默挂起。
- 负面：渠道元数据是外部输入，配错会让 `RouteConfig` 构建异常。
  缓解：`RouteConfig.timeout_sec` 等字段在路由构建期做 pydantic 校验
  （如 `ge=0`，KI-C），负数在构建期响亮报错而非带到提交期。
- 负面：进程缓存有 60s 生效窗口——渠道热改后最多 60s 才全副本生效。
  **有意接受**：这些是运营旋钮，为强一致做 pub/sub 广播的复杂度远超收益。

## Related ADRs

- **atask-service ADR-007**（计费规则随租约下发、网关本地沙箱求值）
- **atask-service ADR-006**（任务级租约钉回精确到 key）
- `app/schemas.py:RouteConfig`；`app/services/registry.py`；
  `app/services/providers/keypool.py`；`app/services/resulturl.py`

> 来源：本 ADR 的「产物直链改写（转存/镜像）」一节由 `AI_TODO.md` 中已
> 收敛的有效内容提炼而成（该历史文档已于 2026-09-12 归档）。
