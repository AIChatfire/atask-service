"""Logfire 观测接入（SPEC §2 / 架构 §2.4）。

两行接入 + httpx/sqlalchemy/redis 串联 trace；生产 level_or_duration
组合采样（基线 10%，warning 及以上与慢 trace 100% 保留）；敏感数据
scrubbing（sk-/api_key/authorization 不进 trace/日志）。

采样权衡（架构 §2.4）：尾采样把整个 trace 的 span 缓冲在内存，网关单请求
span 数可控（HTTP→DB→Redis→httpx×2~3），内存风险可接受。
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any

import logfire

from app.config import settings

if TYPE_CHECKING:
    from fastapi import FastAPI


def _level_or_duration_tail(
    span_info: Any,
    *,
    level_threshold: str,
    duration_threshold: float,
    background_rate: float,
) -> float:
    """与 ``SamplingOptions.level_or_duration`` 内置闭包同语义的尾采样回调。

    集成修复：logfire 3.25.0 ``configure()`` 恒 patch ``ProcessPoolExecutor.submit``
    做跨进程 OTel context 传播，``serialize_config()`` 经 ``dataclasses.asdict``
    序列化全局配置——内置 ``level_or_duration`` 的 tail 是局部闭包不可 pickle，
    会打爆 W3 计费沙箱的子进程池。模块级函数 + ``partial`` 可安全 pickle，
    子进程 ``SamplingOptions(**dict)`` 反序列化后 tail 仍是可调用对象。
    """
    if span_info.duration > duration_threshold:
        return 1.0
    if span_info.level >= level_threshold:
        return 1.0
    return background_rate


def setup_telemetry(app: FastAPI) -> None:
    """在 app 创建后、路由注册前调用（W1 在 main.py lifespan 外调用一次）。"""
    logfire.configure(
        service_name="async-gateway",
        service_version=settings.app_version,
        environment=settings.app_env,
        token=settings.logfire_token,
        send_to_logfire="if-token-present",  # 无 token 时走本地/OTLP，不阻塞启动
        sampling=logfire.SamplingOptions(
            head=settings.logfire_sample_head,  # 基线 trace 采样率
            tail=partial(
                _level_or_duration_tail,        # 告警及以上 100% 保留 / 慢 trace 必留
                level_threshold="warning",
                duration_threshold=5.0,
                background_rate=settings.logfire_sample_head,
            ),
        ),
        scrubbing=logfire.ScrubbingOptions(
            extra_patterns=["api_key", "access_token", "authorization", "sk-"]
        ),
        console=False,
        inspect_arguments=False,
    )
    logfire.instrument_fastapi(app, request_attributes_mapper=_attr_mapper)
    logfire.instrument_httpx()       # 出站上游/计费调用串入同一 trace（架构 §9.1）
    logfire.instrument_sqlalchemy()
    logfire.instrument_redis()


def _attr_mapper(request: Any, attributes: dict[str, Any]) -> dict[str, Any] | None:
    """收窄记录属性：不记录解析后的业务参数（可能含 prompt 等敏感内容）。"""
    if attributes.get("errors"):
        return {"validation_error_count": len(attributes["errors"])}
    return {}
