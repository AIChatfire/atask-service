"""管理面鉴权 —— ``/admin/*`` 与 ``/ops/*`` 的**唯一**鉴权入口。

## 统一语义：fail-closed（未配置密钥 ⇒ 404）

``require_admin`` 同时服务 ``/admin/*`` 与 ``/ops/*``。**未配置
``ADMIN_TOKEN`` 时一律 404**，不区分端点。

裁决与理由（2025 统一，此前 ``/ops`` 的本地实现是 fail-open）：
1. fail-open 意味着「忘配 token」+「反代多放开一条 /ops 路由」= 未鉴权即可
   改状态（``/ops/requeue``、``/ops/dlq/replay`` 是写操作）；
2. 同一个密钥两种行为是明确的陷阱，后来人必然踩。
**安全类开关的失败方向恒为拒绝；不得为兼容而回退 fail-open。**

未配置为什么是 404 而不是 401：404 不泄露「这里有个管理后台但你没密钥」；
默认不开启，忘配密钥不等于裸奔。

## 为什么不新造管理密钥

复用 ``settings.admin_token``（``/ops/*`` 与 ``/admin/*`` 同一 ``X-Admin-Token``
字段）。再引入第二个管理密钥，保护同一批高危操作的两把密钥**必然有一套忘记
轮换**，而忘记轮换的那把不会报错、只会静默失效——安全边际反而更低。

## 为什么用 secrets.compare_digest

普通 ``==`` 会在第一个不同字节处短路返回，攻击者攒够样本可以逐字节爆破。
"""

from __future__ import annotations

import secrets

from fastapi import HTTPException, Request

from app.config import settings


def admin_enabled() -> bool:
    """管理面是否启用（``ADMIN_TOKEN`` 非空）。"""
    return bool((settings.admin_token or "").strip())


async def require_admin(request: Request) -> None:
    """FastAPI 依赖：校验 ``X-Admin-Token``（未配置密钥则整个管理面 404）。"""
    expected = (settings.admin_token or "").strip()
    if not expected:
        raise HTTPException(404, "not found")

    provided = (request.headers.get("x-admin-token") or "").strip()
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(401, "invalid admin token")
