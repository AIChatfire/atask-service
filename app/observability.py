"""可观测装配单点（logfire / OTel）——**全项目唯一允许 configure 的地方**。

为什么要有这个模块：web（``app.main``）与 worker（``app.queue`` 的中间件）
原本各自抄了一份 ``logfire.configure(...)``，两处的 service_name / scrubbing /
token 口径必须手工保持一致，改一处漏一处就会让 trace 分叉到两个服务名下。
配置面收敛到本模块后，进程形态差异只体现为传入的 ``component``。

**进程形态 → component 的对应**（service_name 与历史值保持一致，避免已有
logfire 看板/告警规则失联）：

| 进程 | component | service_name |
|---|---|---|
| web（gunicorn + uvicorn worker） | ``web`` | ``async-gateway`` |
| 队列 worker（taskiq worker） | ``worker`` | ``atask-worker`` |
| 单进程全套（app.standalone） | ``standalone`` | ``atask-standalone`` |

装配顺序不可换：``configure`` → ``instrument_*`` → ``attach_logfire_handler``。
先挂 loguru 桥接再 configure，启动期日志会被 logfire 的 no-op 吞掉（见
``app.logging.attach_logfire_handler`` 的说明）。

失败方向：**绝不影响业务**。``LOGFIRE_ENABLED=false``（默认）时本模块
整体短路；开启后任何异常都只记一条 warning，进程照常起。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from app.config import settings
from app.logging import attach_logfire_handler, log

if TYPE_CHECKING:
    from fastapi import FastAPI

#: 进程形态标识；``standalone`` 供 app.standalone 单进程模式使用
Component = Literal["web", "worker", "standalone"]

_SERVICE_NAMES: dict[str, str] = {
    "web": "async-gateway",
    "worker": "atask-worker",
    "standalone": "atask-standalone",
}

#: 敏感字段兜底 scrubbing——业务侧已保证令牌不进日志，这里是第二道防线
_SCRUB_PATTERNS = ["api_key", "access_token", "authorization", "sk-"]

#: configure 是否已成功执行（幂等守卫）。
#: 单进程模式下 web 与 worker 装配同进程，只有第一次调用真正 configure——
#: OTel 全局 provider 只允许设置一次，重复 configure 会打警告且后者大多不生效。
_configured: bool = False


def is_configured() -> bool:
    """本进程是否已成功 configure（供启动诊断与测试断言）。"""
    return _configured


def setup(component: Component, app: FastAPI | None = None) -> None:
    """装配 logfire（幂等；``LOGFIRE_ENABLED`` 关闭时零动作）。

    :param component: 进程形态，决定 service_name。
    :param app: 传入时额外 instrument FastAPI（web/standalone 形态）。
    """
    global _configured
    if not settings.logfire_enabled or _configured:
        return
    try:
        import logfire

        logfire.configure(
            service_name=_SERVICE_NAMES[component],
            service_version=settings.app_version,
            environment=settings.app_env,
            token=settings.logfire_token,
            send_to_logfire="if-token-present",
            scrubbing=logfire.ScrubbingOptions(extra_patterns=_SCRUB_PATTERNS),
            console=False,
        )
        # 探针与运维端点不进 trace 面板（高频、零诊断价值）
        if app is not None:
            logfire.instrument_fastapi(app, excluded_urls=settings.logfire_excluded_urls)
        # configure 成功后再挂 loguru→logfire 桥接（顺序颠倒会丢启动期日志）
        attach_logfire_handler()
        _configured = True
    except Exception:
        log.opt(exception=True).warning("logfire setup failed, continue without it")


def instrument_httpx(client: Any) -> None:
    """给共享 httpx 客户端挂 OTel instrumentation（幂等；未开启时零动作）。

    调用点是 ``httpc.new_client``——ADR-010 删除旧控制面协同后，出站只剩中继
    提交/探测这一条上游数据面，故统一挂埋点，不再有「控制面挂、数据面不挂」
    之分。若将来引入高频、零诊断价值的探针出站，应另建客户端并跳过本函数，
    避免把看板刷满、推高成本。
    """
    if not settings.logfire_enabled:
        return
    try:
        import logfire

        logfire.instrument_httpx(client)
    except Exception:
        log.debug("logfire instrument_httpx failed, continue without it")
