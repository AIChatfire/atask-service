from pydantic import BaseModel, Field


class RouteConfig(BaseModel):
    """单个 biz 的动态路由配置（YAML 为源，Redis 可热覆盖）"""

    biz: str = ""
    upstream_base_url: str
    auth_type: str = "bearer"                    # bearer | x-api-key | none
    submit_path: str = "/v1/tasks"               # 任务提交端点
    probe_path: str = ""                         # 轮询端点模板，含 {upstream_task_id}
    task_id_path: str = "id"                     # 提交响应中上游任务 ID 的 JSON 路径（点分）
    status_path: str = "status"                  # 探测/回调报文中状态字段的 JSON 路径
    result_path: str = ""                        # 终态时结果字段的 JSON 路径
    actual_amount_path: str = ""                 # 可选：终态报文中实际用量金额的 JSON 路径（缺省按冻结额结算）
    status_map: dict[str, str] = Field(default_factory=dict)   # 上游状态 -> 内部状态
    timeout_sec: int = 600
    supports_callback: bool = False
    callback_secret: str | None = None           # 入站回调 HMAC-SHA256 密钥
    callback_sig_header: str = "X-Signature"
    pricing_biz_type: str = ""
    key_group: str = "default"                   # keypool 分组
    default_model: str = ""                      # 请求体未带 model 时的计费兜底模型
    enabled: bool = True


class Quote(BaseModel):
    """定价结果：金额 + 计费维度（billing.type，如 second）+ 规则快照（排障用）"""

    amount: float
    metric: str = "default"
    logic: str = ""


class KeyLease(BaseModel):
    key_id: int                                  # keypool channel_id
    key: str
    key_index: int = 0
    base_url: str | None = None                  # keypool 可覆盖上游地址


class UserIdentity(BaseModel):
    user_id: int
    token_id: int = 0


# ---- 内部任务状态机 ----
SUBMITTED = "SUBMITTED"
QUEUED = "QUEUED"
IN_PROGRESS = "IN_PROGRESS"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
CANCELED = "CANCELED"
TERMINAL = (SUCCESS, FAILURE, CANCELED)
ACTIVE = (SUBMITTED, QUEUED, IN_PROGRESS)
