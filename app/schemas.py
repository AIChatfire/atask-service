"""共享契约模型：状态常量、KeyLease、RouteConfig、UserIdentity、Quote。

设计原则（新模型「傻瓜式」接入）：

1. **新增一个上游模型 = 一段路由配置（RouteConfig）+ keypool 一个渠道**，
   不写 Python 代码。提交报文以用户请求体为准原样透传，网关只做三件事：
   渠道覆盖（model_mapping/param_override/header_override）、回调注入、
   按配置路径提取 task_id/状态/结果。
2. keypool 租约（KeyLease）携带渠道全量覆盖配置——上游侧差异（base_url、
   模型名映射、默认参数、自定义头、代理）在 new-api 渠道上配置一次，网关
   自动消费，不为单个模型开顶层字段。
3. 内部状态枚举与 new-api tasks 表状态口径一致（SUBMITTED/IN_PROGRESS/
   SUCCESS/FAILURE/CANCELED），``ACTIVE``/``TERMINAL`` 元组为唯一判断点。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 内部状态常量（与 new-api tasks.status 枚举对齐；大写）
# ---------------------------------------------------------------------------

SUBMITTED = "SUBMITTED"
QUEUED = "QUEUED"            # 网关自写：已提交上游、等待推进
IN_PROGRESS = "IN_PROGRESS"
HELD = "HELD"                # 网关自写：账户级故障（欠费/封禁）或上游限流（429）挂起，恢复后金丝雀排空
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
CANCELED = "CANCELED"

#: 活跃（非终态）状态集合——CAS 迁移的合法起点
#: （HELD 在列：挂起持有冻结，可被判死收口或被 resume 重新提交）
ACTIVE: tuple[str, ...] = (SUBMITTED, QUEUED, IN_PROGRESS, HELD)
#: 终态集合——不可逆，迟到快照丢弃
TERMINAL: tuple[str, ...] = (SUCCESS, FAILURE, CANCELED)


# ---------------------------------------------------------------------------
# 身份与报价
# ---------------------------------------------------------------------------


class UserIdentity(BaseModel):
    """billing 服务 ``/auth/inspect`` 的解析结果（令牌即用户身份）。"""

    user_id: int
    token_id: int = 0


class Quote(BaseModel):
    """计费报价：``amount`` 为冻结金额（USD，已乘 discount_rate）。

    规则唯一事实源 = keypool 渠道元数据（gateway 块 ``billing.rule``），
    随租约下发，网关本地沙箱求值（见 ``app.services.pricing``）。
    """

    amount: float
    metric: str = "default"   # billing.type（second/call/token...）
    logic: str = ""           # 命中的计费规则源码（审计用）


# ---------------------------------------------------------------------------
# keypool 租约（POST /v1/keys/select 的 data 投影）
# ---------------------------------------------------------------------------


class KeyLease(BaseModel):
    """上游密钥租约 + 渠道全量覆盖配置（傻瓜式适配的核心载体）。

    渠道覆盖字段全部来自 keypool 渠道元数据（new-api channels 表），
    网关消费顺序见 ``app.services.upstream``：
    ``base_url`` → 覆盖路由默认上游地址；``model_mapping`` → 改写请求体
    model；``param_override`` → 最高优先级合并进请求体；``header_override``
    → 合并进请求头；``status_code_mapping`` → 上游错误码重写；
    ``proxy`` → 渠道级代理客户端。
    """

    key_id: int = 0                    # channel_id（→ tasks.channel_id 对账口径）
    key_index: int = 0
    key: str
    base_url: str | None = None
    epoch: str = ""                    # key 集合指纹（report 携带，过期被忽略）
    lease_id: str = ""                 # usage 模式预扣租约（report 校正用量）
    # ---- 渠道覆盖（keypool channel 元数据；空 = 无覆盖）----
    model_mapping: dict[str, str] = Field(default_factory=dict)
    header_override: dict[str, str] = Field(default_factory=dict)
    param_override: dict[str, Any] = Field(default_factory=dict)
    status_code_mapping: dict[str, str] = Field(default_factory=dict)
    proxy: str | None = None           # channel.setting.proxy
    openai_organization: str | None = None
    # ---- 渠道原始元数据（keypool include_channel 投影；RouteConfig 构建源）----
    channel: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# 动态路由配置（新增模型只写这一段；配置中心/Redis/YAML 三源热更）
# ---------------------------------------------------------------------------


class RouteConfig(BaseModel):
    """一个 biz（上游产品/模型族）的全部声明式配置——由 keypool 渠道元数据
    构建（``app.services.registry.route_from_channel``），网关不维护路由文件。

    接入新模型只需在渠道元数据里放一块网关配置（``header_override.upstream``
    或 ``setting.gateway``，等价任选），例如 MiniMax-H3::

        "header_override": {"upstream": {
            "biz": "minimax",
            "submit_path": "/v2/video_generation",
            "probe_path": "/v2/query/video_generation/{upstream_task_id}",
            "status_path": "task.status",
            "result_path": "task.content.url",
            "settle_usage_map": {"duration": "task.usage.output_seconds"}
        }}

    其余字段按需渐进开启（回调、信封校验、显式状态映射……），缺省值见
    ``app.services.registry._GATEWAY_DEFAULTS``。
    """

    biz: str
    enabled: bool = True
    display_name: str = ""
    channel_id: int = 0                # 构建来源渠道 id（免费 GET 钉渠道反查用）

    # ---- 上游端点 ----
    upstream_base_url: str = ""        # 渠道 base_url 兜底；租约 base_url 字段优先
    submit_path: str = ""              # POST 提交路径（空 = 渠道未配，提交即报错）
    probe_path: str = ""               # GET 探测路径，``{upstream_task_id}`` 占位
    auth_type: str = "bearer"          # bearer | x-api-key | none
    timeout_sec: float = Field(default=60.0, ge=0)
    """上游 HTTP 超时（秒）。非负校验在路由构建期响亮报错（KI-C）——负数配置
    若带进提交期，锁 TTL 派生（submit_lock_ttl）与 Redis SET ex 才炸。"""

    # ---- 请求体塑形（用户 body 为基底，三层叠加）----
    default_params: dict[str, Any] = Field(default_factory=dict)
    """合并进提交 body 的默认参数（用户 body 同名字段优先于它）。"""
    body_allowlist: list[str] | None = None
    """可选白名单：只允许这些字段透传到上游（防客户端注入敏感参数）。"""

    # ---- 响应提取（点分路径，支持数字下标如 data.0.task_id）----
    task_id_path: str = "task_id"      # 提交响应中上游任务 id
    probe_task_id_path: str = ""
    """探测/查询报文中任务 id 的字段路径（如 ``task.id``）。仅用于**原生查询
    拦截**在上游还没接单时自造同构快照报文；空则回退 ``task_id_path``。"""
    status_path: str = "status"        # 探测/回调报文中状态
    result_path: str = ""              # 成功产物 URL
    result_url_template: str = ""
    """产物直链改写模板（转存/镜像；空 = 不改写）。占位符
    ``{upstream_result_url}`` / ``{upstream_result_url_encoded}`` /
    ``{upstream_result_url_no_scheme}`` / ``{upstream_result_host}`` /
    ``{upstream_result_path}`` / ``{task_id}``，例如
    ``https://myhost.com/{upstream_result_url}``。网关只改地址、不搬字节：
    终态落库时改写 ``data.result``（原始链另存 ``data.upstream_result``），
    原生查询报文里的直链同步字节级替换。见 ``app.services.resulturl``。"""
    error_path: str = ""               # 失败信息（如 task.error.message）
    actual_amount_path: str = ""       # 上游直接给出实收金额（结算最高优先）
    settle_usage_map: dict[str, str] = Field(default_factory=dict)
    """结算重估映射 ``{请求体字段: 终态报文路径}``：用终态实际用量覆盖原始
    请求体重跑渠道计费规则得出实收金额（如
    ``duration: task.usage.output_seconds``）。空且 actual_amount_path 空时
    按冻结金额结算。"""
    ok_check: dict[str, Any] | None = None
    """可选信封校验 ``{"path": "code", "equals": 0, "message_path": "message"}``：
    提交响应 HTTP 2xx 但业务码不匹配时按业务错误处理（kling 类信封上游）。"""

    # ---- 回调（上游 webhook；False = 纯探测推进，最省心）----
    supports_callback: bool = False
    callback_param: str = "callback_url"   # 注入提交 body 的回调参数名
    callback_secret: str | None = None     # 入站验签密钥（None = 不验签，仅内网）
    callback_sig_header: str = "X-Signature"

    # ---- 止损（不亏本纪律的渠道级配置）----
    failed_billing: str = "absorb"
    """失败单计费策略（按厂商商务条款逐渠道配）：``absorb``（默认）失败全额解冻；
    ``charge`` 失败也收费——先查 ``actual_amount_path`` 实收 → ``settle_usage_map``
    重估 → 冻结额兜底。"""
    cancel_path: str = ""
    """上游取消端点（``{upstream_task_id}`` 占位；空 = 上游不支持取消）。
    用户取消 / 探测超时收口时尽力调用（失败仅告警，不阻塞本地收口）。"""
    client_request_id_param: str = ""
    """提交体注入网关 task_id 的参数名（上游幂等反查/孤儿任务根治；空 = 不注入）。"""

    # ---- 计费（规则唯一事实源 = 本渠道 gateway 块 billing，随租约下发）----
    pricing_biz_type: str = ""         # freeze 的 biz_type；缺省用 biz
    billing_rule: str = ""             # billing.rule：asteval 沙箱求值，入参完整请求体
    billing_type: str = "default"      # billing.type（second/call/token...）→ freeze metric
    discount_rate: float = 1.0         # billing.discount_rate / discountRate（折扣必乘）
    status_map: dict[str, str] = Field(default_factory=dict)
    """显式状态映射（最高优先级，见 app.services.statusmap）：上游状态 →
    SUBMITTED/QUEUED/IN_PROGRESS/SUCCESS/FAILURE/CANCELED。"""
    error_classify: dict[str, list] = Field(default_factory=dict)
    """错误分类覆盖位（见 app.services.errclass）：``{"key_level": [401],
    "account_level": [403], "account_level_messages": [...], ...}``；
    空 = 内置默认表（401/403→key 级、429→限流、其余 4xx→任务级、5xx→模糊）。"""

    def callback_url_for(self, public_base: str, task_id: str) -> str:
        """注入上游的回调地址（task_id 即凭证；验签靠 callback_secret）。"""
        return f"{public_base.rstrip('/')}/callback/{self.biz}/{task_id}"


__all__ = [
    "ACTIVE",
    "CANCELED",
    "FAILURE",
    "HELD",
    "IN_PROGRESS",
    "QUEUED",
    "SUBMITTED",
    "SUCCESS",
    "TERMINAL",
    "KeyLease",
    "Quote",
    "RouteConfig",
    "UserIdentity",
]
