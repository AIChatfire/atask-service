"""统一错误响应：OpenAI 风格 error 对象（SPEC §4.4 / 架构 §11.1）。

契约：所有非 2xx 响应体恒为
    ``{"error": {"message": str, "type": str, "param": str|None, "code": str|None}}``
任何模块抛 ``GatewayError`` 即可，由 W1 在 ``app/main.py`` 注册的
``gateway_exception_handler`` 统一序列化；HTTPException 也经同一处理器归一。
"""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


class GatewayError(Exception):
    """网关业务错误基类。子类/实例按场景覆写 status_code 与 type。"""

    status_code: int = 500
    error_type: str = "server_error"
    code: str | None = None

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_type: str | None = None,
        code: str | None = None,
        param: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        if error_type is not None:
            self.error_type = error_type
        if code is not None:
            self.code = code
        self.param = param
        self.headers = headers


def error_body(
    message: str,
    error_type: str,
    *,
    code: str | None = None,
    param: str | None = None,
) -> dict[str, Any]:
    """构造 OpenAI 风格错误体（SPEC §4.4 唯一格式）。"""
    return {"error": {"message": message, "type": error_type, "param": param, "code": code}}


# ---- 常用错误工厂（各模块直接抛，保证 type/code 口径一致） ----

def unauthorized(message: str = "invalid or expired token") -> GatewayError:
    return GatewayError(message, status_code=401, error_type="authentication_error")


def forbidden(message: str, *, code: str | None = None) -> GatewayError:
    return GatewayError(message, status_code=403, error_type="permission_error", code=code)


def not_found(message: str = "resource not found") -> GatewayError:
    return GatewayError(message, status_code=404, error_type="invalid_request_error")


def payment_required(message: str = "insufficient balance") -> GatewayError:
    # 计费服务 402 原样透传（SPEC §5.4 时序分支）
    return GatewayError(message, status_code=402, error_type="billing_error", code="insufficient_quota")


def rate_limited(retry_after: int) -> GatewayError:
    return GatewayError(
        "rate limit exceeded",
        status_code=429,
        error_type="rate_limit_error",
        headers={"Retry-After": str(retry_after)},
    )


def backpressure(retry_after: int = 5, message: str = "service busy, retry later") -> GatewayError:
    # 有界队列满 / 熔断 open：503 背压（SPEC §4.4 纪律：绝不无限排队）
    return GatewayError(
        message,
        status_code=503,
        error_type="server_error",
        code="backpressure",
        headers={"Retry-After": str(retry_after)},
    )


def upstream_error(message: str, *, status_code: int = 502) -> GatewayError:
    return GatewayError(message, status_code=status_code, error_type="upstream_error")


def idempotency_conflict(message: str = "Idempotency-Key reused with different payload") -> GatewayError:
    return GatewayError(message, status_code=409, error_type="idempotency_error")


async def gateway_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    """FastAPI 异常处理器（W1 在 main.py 注册，覆盖 GatewayError 与 HTTPException）。

    HTTPException 的 detail 若已是 ``{"error": {...}}`` 形制则原样透传，
    否则包装为 OpenAI 风格——保证历史 ``raise HTTPException(404, {...})`` 写法
    与新 ``GatewayError`` 写法输出一致。
    """
    from fastapi import HTTPException

    if isinstance(exc, GatewayError):
        return JSONResponse(
            status_code=exc.status_code,
            content=error_body(exc.message, exc.error_type, code=exc.code, param=exc.param),
            headers=exc.headers,
        )
    if isinstance(exc, HTTPException):
        detail = exc.detail if isinstance(exc.detail, dict) else None
        if detail and "error" in detail:
            body = detail
        else:
            body = error_body(
                str(exc.detail), "invalid_request_error" if exc.status_code < 500 else "server_error"
            )
        headers = dict(exc.headers) if exc.headers else None
        return JSONResponse(status_code=exc.status_code, content=body, headers=headers)
    return JSONResponse(
        status_code=500,
        content=error_body("internal server error", "server_error"),
    )
