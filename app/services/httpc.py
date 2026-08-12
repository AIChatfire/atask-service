"""HTTP 客户端工厂：控制面（billing/pricing/keyman/配置中心）客户端按需挂 Logfire 埋点。
数据面（上游提交/探测）客户端不走这里 —— 探测调用高频，全量 trace 无意义，
状态变化由 statelog 负责记录。
"""

import httpx

from app.config import settings


def new_client(**kwargs) -> httpx.AsyncClient:
    client = httpx.AsyncClient(**kwargs)
    if settings.logfire_enabled:
        try:
            import logfire

            logfire.instrument_httpx(client)
        except Exception:
            pass
    return client
