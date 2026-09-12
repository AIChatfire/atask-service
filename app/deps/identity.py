"""Bearer 令牌提取（中性件）：把 ``Authorization`` 头解成 ``TokenCtx``。

本模块刻意**不** import ``providers`` / ``pricing`` / ``registry`` /
``preflight``——旧链路（keypool 取 key + billing 内省）删除后，
``extract_token`` 仍被新 ``/batch`` 链路使用（见 ``app/services/relayflow.py``），
故抽到中性位置，删旧模块不打断新链路。

安全红线：``TokenCtx.hash`` 是本地身份替身（限流/并发/幂等键）；
``TokenCtx.raw`` 是用户原始令牌——只进 Redis 会话（终态清除），
**绝不落 tasks 表、绝不进日志、绝不出现在任何响应里**。
"""

import hashlib
from dataclasses import dataclass

from fastapi import HTTPException


@dataclass
class TokenCtx:
    raw: str
    hash: str


def extract_token(authorization: str | None) -> TokenCtx:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing or invalid Authorization header")
    raw = authorization[7:].strip()
    if not raw:
        raise HTTPException(401, "empty token")
    return TokenCtx(raw=raw, hash=hashlib.sha256(raw.encode()).hexdigest())
