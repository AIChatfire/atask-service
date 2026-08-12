"""动态路由注册表，三级来源（优先级从高到低）：
  1. Redis 热覆盖（应急操作：紧急下线/临时改配置，秒级生效）
  2. 配置中心微服务（主源：GET {GW_CONFIG_SVC_URL}/gateway/routes）
  3. 本地 YAML 文件（兜底/bootstrap：配置中心从未成功拉取时使用）

配置中心故障 → 保留最后一次成功拉取的快照；进程重启且中心不可达 → YAML 接管。
"""

import asyncio
import json
import logging
import time
from pathlib import Path

import yaml

from app.config import settings
from app.redis import K_CONFIG_VER, K_ROUTES_OVERRIDE, r
from app.schemas import RouteConfig
from app.services import httpc

log = logging.getLogger("gateway.registry")


class RouteRegistry:
    def __init__(self) -> None:
        self._file_base: dict[str, RouteConfig] = {}
        self._remote_base: dict[str, RouteConfig] | None = None   # None = 从未拉取成功
        self._routes: dict[str, RouteConfig] = {}
        self._redis_ver: int = -1
        self._remote_ver: int = -1
        self._last_pull: float = 0.0

    # ---------- 源 3：本地 YAML ----------
    def load_file(self) -> None:
        raw = yaml.safe_load(Path(settings.routes_file).read_text(encoding="utf-8")) or {}
        self._file_base = {biz: RouteConfig(biz=biz, **cfg) for biz, cfg in raw.items()}
        self._routes = dict(self._file_base)
        log.info("routes loaded from file: %s", list(self._file_base))

    # ---------- 源 2：配置中心微服务 ----------
    async def _pull_remote(self) -> bool:
        """到点拉取配置中心；返回 base 是否变化"""
        if not settings.config_svc_url:
            return False
        if time.monotonic() - self._last_pull < settings.config_pull_interval:
            return False
        self._last_pull = time.monotonic()
        try:
            async with httpc.new_client(timeout=settings.http_timeout) as client:
                resp = await client.get(f"{settings.config_svc_url}/gateway/routes")
                resp.raise_for_status()
                data = resp.json()["data"]
            version = int(data.get("version", 0))
            if version == self._remote_ver and self._remote_base is not None:
                return False
            routes = {
                biz: RouteConfig(biz=biz, **cfg)
                for biz, cfg in (data.get("routes") or {}).items()
            }
            self._remote_base = routes
            changed = version != self._remote_ver
            self._remote_ver = version
            log.info("routes pulled from config svc, version=%s, biz=%s", version, list(routes))
            return changed
        except Exception:
            log.warning("config svc pull failed (keep last good)", exc_info=True)
            return False

    # ---------- 合并与热更新 ----------
    def _base(self) -> dict[str, RouteConfig]:
        return self._remote_base if self._remote_base is not None else self._file_base

    async def refresh_loop(self) -> None:
        while True:
            try:
                remote_changed = await self._pull_remote()
                ver_raw = await r.get(K_CONFIG_VER)
                redis_ver = int(ver_raw) if ver_raw is not None else 0
                if remote_changed or redis_ver != self._redis_ver:
                    overrides = await r.hgetall(K_ROUTES_OVERRIDE)
                    merged = dict(self._base())
                    for biz, js in overrides.items():
                        cfg = json.loads(js)
                        if cfg.get("_delete"):
                            merged.pop(biz, None)
                            continue
                        base = merged.get(biz)
                        merged[biz] = RouteConfig(
                            biz=biz, **({**(base.model_dump() if base else {}), **cfg})
                        )
                    self._routes = merged
                    self._redis_ver = redis_ver
                    log.info("routes refreshed: redis_ver=%s remote_ver=%s overrides=%s",
                             redis_ver, self._remote_ver, list(overrides))
            except Exception:
                log.exception("route refresh failed (keep last good)")
            await asyncio.sleep(2)

    def get(self, biz: str) -> RouteConfig | None:
        route = self._routes.get(biz)
        if route and route.enabled:
            return route
        return None


registry = RouteRegistry()
