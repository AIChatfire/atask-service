# 用户回调（Callback）对接契约

面向**调用方**（把任务提交给本网关的客户端）。本文只描述网关对外的回调行为，
实现细节见 `docs/ARCH-queue-relay-lifecycle.md`，验收标准见 `docs/SPEC.md`。

## 1. 一句话

提交任务时给出回调地址（请求头或 body 字段），任务**到达终态**时网关向你
`POST` 一份与「查询任务」同构的报文，带 HMAC-SHA256 签名供验真。

## 2. 怎么给回调地址

两种方式，二选一或同时给：

| 方式 | 形态 | 优先级 |
|---|---|---|
| 请求头（推荐） | `X-Callback-Url: https://your-app.example.com/hook` | **优先** |
| 请求体字段 | `{"callback_url": "https://your-app.example.com/hook", ...}` | 兜底 |

- 头是网关专有契约，语义明确；body 字段是兼容上游 API 文档的等价通道。
- **两者同时给且不同时以头为准**；头为空白串时按缺失处理，回退 body。
- 都不给 = **不做任何投递**（不会「凭空」通知你），只能靠轮询 `GET` 取结果。

### 地址必须过白名单（否则整个提交被拒）

网关侧配置 `CALLBACK_ALLOWLIST`（逗号分隔的 host 列表），语义与上游寻址白名单
同一条纪律：**空白名单 = 全部拒绝**（fail-closed）。

- 地址不在白名单 → 整个提交请求返回 `400`，且**不留任何痕迹**（不落库、不占
  并发槽、不占幂等键）。**接入前请先与运维确认你的回调域名已列入白名单。**
- 仅接受 `http` / `https`；不接受 URL userinfo（`http://user@host` 形态，即主机名前带 `user:pass@` 前缀）。
- **字面 IP 只接受公网地址**：私网、回环、链路本地（含云元数据段）一律拒绝——
  即使白名单里显式列了它。请使用域名。

## 3. 什么时候推送

| 时机 | 是否推送 |
|---|---|
| 终态 `SUCCESS` / `FAILURE` / `CANCELED` | 是，**恰好一次** |
| 中间态 `SUBMITTED` / `QUEUED` / `IN_PROGRESS` | **否** |
| 你主动 `DELETE` 取消 | **否** |

三条必须知道的边界：

1. **只推终态，不推进度。** 网关的推进由你的轮询或后台巡检驱动，观测到的中间态
   是**抽样**而不是事件流——推它会有漏报与乱序，所以刻意不推。唯一确定的时刻是
   终态，且由数据库 CAS 保证「恰好一次」。
2. **主动取消不回调。** 取消是你自己发起的，结果你已经知道；不要依赖回调来确认
   取消是否成功（用 `DELETE` 的响应体判断）。
3. **默认模式下 body 里的 `callback_url` 会被摘除后再转发上游。** 这是为了消除
   双投递：若地址同时被转发给上游，上游可能也回调一次（那一份**没有**本网关的
   签名），你会收到两份通知。摘除只影响这一个字段，body 其余内容不变。

## 4. 推送什么

```
POST <你的回调地址>
Content-Type: application/json
X-Gateway-Signature: t=<unix 秒>,v1=<十六进制小写签名>
```

body 与「查询任务 API」的返回体**同构**——即上游原生报文的字段原样保留，其中
上游任务 id 已被逐字节改写为本地 `task_id`。形如：

```json
{
  "id": "queue_1f0c9a2b4d5e6f708192a3b4c5d6e7f8",
  "status": "succeeded",
  "progress": "100%",
  "result": { "...": "上游产物字段原样保留" }
}
```

本地判死（无上游报文，例如提交被确定性拒绝）时退化为最小报文：

```json
{"task_id": "queue_1f0c9a2b4d5e6f708192a3b4c5d6e7f8", "status": "failed"}
```

**不要按固定 schema 严格解析**：`status` 用上游原话，其余字段由上游能力决定。

## 5. 怎么验签

签名头格式（Stripe 风格）：

```
X-Gateway-Signature: t=1757712000,v1=3f2a...c9
```

计算方式：

```
v1 = HMAC_SHA256(CALLBACK_SIGN_SECRET, "{t}." + <原始 body 字节>)   的十六进制小写
```

`CALLBACK_SIGN_SECRET` 是网关侧配置的密钥，**由运维同步给你**（两侧必须一致）。

三条验签纪律：

1. **必须用原始 body 字节验签。** 不要 JSON 解析后再重新序列化——键序、空格、
   `ensure_ascii` 任一差异都会让签名不匹配。拿到 raw body 直接算。
2. **必须校验时间窗**（建议 5 分钟）。网关不做防重放窗口，这一步只能由你做，
   否则旧报文可被无限重放。
3. **必须用常量时间比较**，不要用 `==` 逐字节短路比较。

Python：

```python
import hashlib, hmac, json, time

def verify_callback(raw_body: bytes, sig_header: str, secret: str, skew: int = 300) -> dict:
    kv = dict(p.split("=", 1) for p in sig_header.split(","))
    ts, sig = int(kv["t"]), kv["v1"]
    if abs(time.time() - ts) > skew:
        raise ValueError("timestamp outside window")
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + raw_body,
                        hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise ValueError("bad signature")
    return json.loads(raw_body)
```

Node.js：

```javascript
const crypto = require("crypto");

function verifyCallback(rawBody, sigHeader, secret, skew = 300) {
  const kv = Object.fromEntries(sigHeader.split(",").map(p => p.split("=", 1)[0] && [p.slice(0, p.indexOf("=")), p.slice(p.indexOf("=") + 1)]));
  const ts = Number(kv.t);
  if (Math.abs(Date.now() / 1000 - ts) > skew) throw new Error("timestamp outside window");
  const expected = crypto.createHmac("sha256", secret)
    .update(`${ts}.`).update(rawBody).digest("hex");
  if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(kv.v1))) {
    throw new Error("bad signature");
  }
  return JSON.parse(rawBody.toString("utf8"));
}
```

## 6. 投递语义：至少一次

- **同一终态可能收到多次**，请按 `task_id` 幂等去重（`task_id` 是每次任务唯一且
  稳定的标识；不要按 body 哈希去重）。
- 你的端点返回 `2xx` 视为成功；返回 `>= 300` 或超时（网关出站超时 15 秒）会被
  **退避重投**。
- 重投上限为网关配置 `EVENT_MAX_ATTEMPTS`（默认 8 次）；超限后进入死信队列，
  需要运维介入重放。
- **回调投递不影响任务本身的终态**：回调失败不会把任务改回非终态，也不会重复释放
  并发额度。你可以随时用 `GET /async/{上游路径}/{task_id}` 兜底查询。

## 7. 上游透传模式

运维可以把网关切到 `CALLBACK_PASSTHROUGH_UPSTREAM=true`。此模式下：

- 网关**不摘除** body 里的 `callback_url`，也**不投递**任何回调；
- 回调由**上游**自己完成，其签名与重试语义**由上游决定**，本文第 4–6 节不适用。

采用本模式的前提是**上游自身实现了回调语义**。若上游其实不回调而你按本文对接，
会一个通知都收不到——所以切换前后请与运维确认模式。

## 8. 排障清单

收不到回调时，按顺序自查：

1. 提交是否返回了 `202`？`400` 说明地址没过白名单或协议不合规。
2. `GET` 查任务是否真的已到终态？非终态本就不会回调。
3. 任务是否是你自己 `DELETE` 取消的？取消不回调。
4. 你的端点是否对回调请求返回了 `2xx`？返回 `>= 300` 会被反复重投直至死信。
5. 验签是否失败？确认用的是**原始 body 字节**。
6. 是否处在上游透传模式？那种模式下网关不发任何回调。

## 9. 明确不做的事

- 不推中间态与进度（理由见第 3 节）。
- 不提供回调体的自定义模板（body 是上游原生报文的同构副本）。
- 不提供回调重放接口（网关侧无该入口；需要重放请运维从死信处理）。
- 不保证「恰好一次」，只保证「至少一次」——去重是你的责任。

## 10. 与火山方舟 Seedance 的对照

本网关的 callback 契约与火山方舟 Seedance 任务 API 的口径高度一致（按方舟文档写的
客户端可平滑接入），差异集中在下面三处。方舟口径依据其官方文档「创建视频生成任务」
（`volcengine.com/docs/508/1393047`）。

| 维度 | 方舟 Seedance | 本网关 |
|---|---|---|
| 地址传入 | body 顶层 `callback_url` | 头 `X-Callback-Url` 优先，body `callback_url` 兜底 |
| 回调体 | 与「查询任务 API」返回体一致 | 同（上游原生报文，id 改写为本地 `task_id`） |
| 状态词 | `queued` / `running` / `succeeded` / `failed` / `expired` | 上游 `status` **原话透传**，不翻译 |
| 触发时机 | **每次状态变化** | **仅终态** |
| 重试 | `succeeded` / `failed` 各再回调至多 3 次 | 退避重投至多 `EVENT_MAX_ATTEMPTS` 次 + 死信 |
| 签名 | 无 | HMAC-SHA256（`X-Gateway-Signature`） |
| 地址准入 | 要求 public HTTP(S) 端点 | 白名单 fail-closed + 拒私网/回环/链路本地 |

### 差异一（重要）：不推送中间态

方舟是任务的**执行者**，能在 `queued` / `running` 每一次跃迁时推送；本网关是**中继**，
只在你的轮询或后台巡检真正访问上游时才观测到状态——**观测是抽样，不是事件流**。

因此本网关**只推终态**。对你的实现意味着三件事：

- **不要等第一条通知来启动进度展示**：任务提交后可能长时间只有 `202` 回执，
  直到终态才有唯一一条通知。
- **保留轮询兜底**：`GET /async/{上游路径}/{task_id}` 始终可用，回调不应是你获取
  结果的唯一通道。
- 从方舟迁移而来且依赖 `queued` / `running` 通知的代码，需要改这两处逻辑。

### 差异二：多了一条签名头，可忽略

方舟不签名，本网关所有回调都带 `X-Gateway-Signature`。按方舟实现、不验签的客户端
**不受影响**（只是多一个请求头）；建议逐步启用验签（见第 5 节）。

### 差异三：没有 `expired` 通知

方舟在任务超过 `execution_expires_after`（默认 48 小时）时推 `expired`。本网关
**不设任务超时判死**——判死不可逆，会丢失一个可能已在上游成功的任务，收益为负。

唯一相关的边界：若你的令牌会话已过期（网关不持有长期凭证，见 `docs/SPEC.md` 的
L-1），该任务会永久停在非终态。此时请用 `DELETE` 取消或联系运维介入，
**不会收到自动通知**——所以不要把回调当作唯一的终态判据。

