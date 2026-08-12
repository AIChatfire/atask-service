"""UpstreamAdapter 抽象契约（SPEC §3.2 / 架构 §3.3/§13.2）。

所有上游差异收敛到本接口；新增上游 = 实现协议 + ``register()``，
不改分发代码（G1「新增 biz 不改代码」由此保证）。

内部统一状态 ``TaskStatus`` 定义在 ``app.tasks.models``（与 DB 映射同源），
本模块 re-export 以便适配器单点导入。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, ClassVar, Protocol, runtime_checkable

from app.tasks.models import TaskStatus  # re-export：内部状态枚举唯一定义点

__all__ = [
    "CanonicalTaskRequest",
    "SubmitContext",
    "SubmitResult",
    "TaskSnapshot",
    "TaskStatus",
    "UpstreamAdapter",
    "UpstreamBizError",
    "UpstreamError",
    "UpstreamRateLimitError",
    "UsageEstimate",
    "get_adapter",
    "register",
    "registered_adapters",
]


# ---------------------------------------------------------------------------
# 数据类（SPEC §3.2.2；字段即契约，适配器与 TaskManager 双方依赖）
# ---------------------------------------------------------------------------


@dataclass
class CanonicalTaskRequest:
    """统一任务请求（videos 提交/remix 与透传 tracked 归一后的内部表示，§3.4）。

    纯内部模型，无对外 /v1/tasks 契约（v2.0）；对外模型见 app/schemas.py。
    ``action`` 对齐 new-api 动作枚举：generate/textGenerate/firstTailGenerate/
    referenceGenerate/remixGenerate（简报 C §三）；适配器内部可再映射为上游
    原生动作（kling text2video/image2video）。
    """

    model: str
    prompt: str
    action: str                       # text2video / image2video / remixGenerate / ...
    duration: float | None = None
    resolution: str | None = None     # 480p/720p/1080p/4k
    mode: str | None = None           # std/pro（kling）
    image: str | None = None
    n: int = 1
    generate_audio: bool = False
    callback_url: str | None = None   # 用户回调（metadata.callback_url 提取）
    extra: dict[str, Any] | None = None  # 供应商扩展（negative_prompt 等）


@dataclass
class SubmitContext:
    """一次提交/轮询的上下文。适配器不得跨调用缓存其中凭证以外的东西。"""

    biz: str
    task_id: str                      # 网关内部 id → kling external_task_id
    gateway_callback_url: str         # 网关注入上游的回调地址（含 capability token）
    upstream_base_url: str            # 来自 biz 注册表
    secrets: Mapping[str, str] = field(default_factory=dict)  # auth_secret_ref 解析后凭证
    action: str = ""                  # 轮询时从 tasks 行取回的动作（kling 旧版查询路径需要）


@dataclass
class SubmitResult:
    upstream_task_id: str             # 上游真实任务 ID（kling task_id / 方舟 cgt- 前缀）
    raw: dict[str, Any]               # 上游提交原始响应 → tasks.data 列（脱敏后）


@dataclass
class TaskSnapshot:
    """统一任务快照（poll 与 parse_callback 的共同产物）。"""

    upstream_status: str              # 上游原生状态字符串（审计/排查用）
    status: TaskStatus                # 已归一的内部状态（map_status 结果）
    result: dict[str, Any] | None     # {url, duration, resolution, format...}，成功时有值
    usage: dict[str, Any] | None      # 实收信号 {completion_tokens/actual_duration/upstream_amount}
    error: dict[str, Any] | None      # {code, message}
    event_id: str                     # 幂等去重键：provider:upstream_task_id:status:updated_at（§7.1）
    raw: dict[str, Any] | None = None  # 上游最新原始响应（脱敏后写 tasks.data）


@dataclass
class UsageEstimate:
    """顶格预估用量（freeze 金额依据，§5.4）。

    ``amount_usd`` 恒 0 占位——金额由 PricingEvaluator 按表达式求值得出，
    适配器只负责产出**求值输入上下文**（变量名契约见 SPEC §5.3）。
    """

    amount_usd: Decimal
    context: dict[str, float | str]


# ---------------------------------------------------------------------------
# 异常体系（SPEC §3.2.3）
# ---------------------------------------------------------------------------


class UpstreamError(RuntimeError):
    """上游调用错误基类。submit/poll 抛出的异常必须是其子类。"""


class UpstreamBizError(UpstreamError):
    """上游业务错误（如 kling 信封 code != 0）。

    语义纪律（§8.2）：**不重试**（4xx 业务错重试无意义）；
    是否计入熔断由调用方按 code 决定（默认计入）。
    """

    def __init__(self, message: str, *, code: int | str | None = None) -> None:
        super().__init__(message)
        self.code = code


class UpstreamRateLimitError(UpstreamError):
    """上游 429。

    语义纪律（§8.2）：**不计入熔断失败**（上游健康只是你太快）；
    重试须尊重 Retry-After；读 x-ratelimit-remaining-* 主动降速优先于撞 429。
    """

    def __init__(self, message: str = "upstream rate limited",
                 *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


# ---------------------------------------------------------------------------
# 适配器协议
# ---------------------------------------------------------------------------


@runtime_checkable
class UpstreamAdapter(Protocol):
    """上游适配器协议（SPEC §3.2.1）。

    实现要点：
    - submit/poll 内部使用 ``app.http_clients.upstream_client()`` 单例；
    - httpx.HTTPStatusError 4xx（除 429）→ UpstreamBizError；429 →
      UpstreamRateLimitError（携带 retry_after）；超时向上抛（调用方计熔断）；
    - parse_callback 为同步纯函数（验签在网关层做，§7.1）。
    """

    name: ClassVar[str]
    """适配器注册名（与 gateway_biz_registry.adapter 一致）：'kling' / 'seedance'。"""

    callback_capability: ClassVar[bool]
    """上游是否支持 webhook callback。False 时轮询是唯一推进通道（§4.3）。"""

    echoes_external_task_id: ClassVar[bool]
    """回调是否回显提交时注入的 external_task_id。

    True（kling 两代）→ 回调可直接取网关 task_id；False（seedance/方舟）
    → 必须按 (platform, upstream_task_id) 索引表反查（§4.4）。
    """

    async def submit(self, req: CanonicalTaskRequest, ctx: SubmitContext) -> SubmitResult:
        """把网关统一请求翻译为上游原生请求并提交，返回上游 task_id。

        必须注入 ``ctx.gateway_callback_url``（若上游支持 callback）与
        ``external_task_id=ctx.task_id``（若上游支持回显）——双向关联（§3.3）。
        """
        ...

    async def poll(self, upstream_task_id: str, ctx: SubmitContext) -> TaskSnapshot:
        """查询上游任务当前快照（状态/产物/用量）。轮询 worker 按 biz 分组调用。"""
        ...

    def parse_callback(self, raw_body: bytes, headers: Mapping[str, str]) -> TaskSnapshot:
        """解析上游 callback 报文为统一快照（验签在网关层做，见 §7.1）。

        行业惯例回调体 ≈ 任务查询响应（方舟已确认，kling 两代 V1/V2 建议验证），
        可与 poll 的响应解析共用。坏报文抛异常 → 网关返回 400（上游不应重试）。
        """
        ...

    def map_status(self, upstream_status: str) -> TaskStatus:
        """上游状态 → 统一状态机枚举（§4.1 映射表唯一权威）。

        必须吃掉同上游代际拼写差异（kling 旧版 'succeed' vs 3.0 'succeeded'）；
        未知状态映射为 RUNNING（不推进终态，等下一轮）。
        """
        ...

    def estimate_usage(self, req: CanonicalTaskRequest) -> UsageEstimate:
        """顶格预估求值上下文（freeze 金额依据，§5.4）。

        按用户所选参数的最高规格估（未指定的按最高档）；终态实收信号由
        ``TaskSnapshot.usage`` 提供。
        """
        ...

    def auth_headers(self, cfg: Any) -> Mapping[str, str]:
        """上游鉴权头生成（kling: AK/SK HS256 JWT；seedance: Bearer API Key）。

        凭证从 ``cfg.auth_secret_ref`` 指向的环境变量/Secret 解析；
        JWT 进程内缓存至过期前 60s 刷新，**缓存 key 必须含 AK**（§3.3）。
        """
        ...

    def rewrite_callback_url(self, raw_body: bytes, cfg: Any) -> bytes:
        """透传形态报文改写：摘除/改写用户自带 callback_url（§3.4）。

        默认收敛回网关回调地址（用户回调由 §7.2 透传）；biz 级开关
        ``allow_user_direct_callback`` 在路由层判断，不走这里。
        非 JSON 报文原样返回。
        """
        ...


# ---------------------------------------------------------------------------
# 注册表（进程内；应用启动时 import app.adapters 触发各适配器自注册）
# ---------------------------------------------------------------------------

_ADAPTERS: dict[str, UpstreamAdapter] = {}


def register(adapter: UpstreamAdapter) -> None:
    """注册适配器实例（模块级 ``register(KlingAdapter())`` 自注册模式）。"""
    _ADAPTERS[adapter.name] = adapter


def get_adapter(name: str) -> UpstreamAdapter:
    """按注册名取适配器；未注册抛 RuntimeError（biz 注册表配置错误的信号）。"""
    try:
        return _ADAPTERS[name]
    except KeyError:
        raise RuntimeError(f"adapter not registered: {name}") from None


def registered_adapters() -> dict[str, UpstreamAdapter]:
    """只读视图（测试断言用）。"""
    return dict(_ADAPTERS)
