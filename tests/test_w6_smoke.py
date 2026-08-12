"""W6 运维件静态冒烟（不依赖其他 W 模块完成度；SPEC §7.3 的可静态部分）。

覆盖：
1. ``.env.example`` ↔ ``app.config.Settings`` 字段一致性（SPEC §3.7 清单口径）；
2. **零自有表断言（决策 A）**：ORM metadata 仅 new-api 共享 ``tasks`` 一张
   （非网关创建/迁移），migrations/alembic/alembic.ini 目录已删除，
   compose 无 initdb 挂载；
3. ``docker-compose.yml`` 为唯一部署形态：可解析、关键字段符合架构 §12.2
   （健康检查/AOF/优雅停机/profile 隔离），且无硬编码密钥。

集成冒烟（uvicorn 起服务、路由注册顺序、临时 MySQL 幂等建表）随 W1~W5
集成后在 tests/test_smoke.py 收口（SPEC §7.3），本文件不重复。
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# 1. .env.example ↔ config.Settings
# ---------------------------------------------------------------------------

# .env.example 中合法存在但不属于 Settings 的变量（SPEC §3.7：
# gunicorn.conf.py 直读 + 适配器按 auth_secret_ref 直读的上游凭证 +
# docker-compose.yml ${VAR:-dev} 插值引用的本地 MySQL 初始化账号）
_PASSTHROUGH_VARS = {
    "GUNICORN_WORKERS",
    "GUNICORN_TIMEOUT",
    "GUNICORN_GRACEFUL_TIMEOUT",
    "GUNICORN_LOG_LEVEL",
    "UPSTREAM_SECRET_KLING_AK",
    "UPSTREAM_SECRET_KLING_SK",
    "UPSTREAM_KEY_ARK",
    "MYSQL_ROOT_PASSWORD",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
}

_ENV_LINE_RE = re.compile(r"^\s*#?\s*([A-Z][A-Z0-9_]*)\s*=")


def _env_example_vars() -> dict[str, bool]:
    """{变量名: 是否取消注释（生效行）}。"""
    result: dict[str, bool] = {}
    for raw in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        m = _ENV_LINE_RE.match(raw)
        if m:
            result[m.group(1)] = not raw.lstrip().startswith("#")
    return result


def test_env_example_matches_settings() -> None:
    """.env.example 每个变量必须对应 Settings 字段或登记的 passthrough 变量。"""
    from app.config import Settings

    settings_env_names = {name.upper() for name in Settings.model_fields}
    env_vars = _env_example_vars()
    assert env_vars, ".env.example 未解析到任何变量"

    unknown = set(env_vars) - settings_env_names - _PASSTHROUGH_VARS
    assert not unknown, f".env.example 存在 Settings 未定义且未登记的变量: {sorted(unknown)}"

    # SPEC §3.7 核心变量必须在 .env.example 中生效（非注释）
    required_active = {
        "DATABASE_URL",
        "REDIS_URL",
        "BILLING_SERVICE_URL",
        "PRICING_SERVICE_URL",
        "QUOTA_PER_USD",
        "DEFAULT_TASK_TTL_SECONDS",
        "NEWAPI_TASK_TIMEOUT_MINUTES",
        "NEWAPI_SWEEP_MARGIN_SECONDS",
        "FREEZE_SHARD_TTL_SECONDS",
        "FREEZE_RENEW_WINDOW_SECONDS",
        "GATEWAY_PUBLIC_BASE_URL",
        "CALLBACK_SIGNING_SECRET_CURRENT",
        "BIND",
        "GUNICORN_WORKERS",
        "GUNICORN_TIMEOUT",
    }
    inactive = {v for v in required_active if not env_vars.get(v)}
    assert not inactive, f"核心变量在 .env.example 中缺失或被注释: {sorted(inactive)}"


# ---------------------------------------------------------------------------
# 2. 零自有表断言（决策 A：9 张 gateway_ 表全部删除，一张不留）
# ---------------------------------------------------------------------------


def test_zero_own_tables() -> None:
    """ORM metadata 只有 tasks 一张且不属于网关创建；迁移件已删除。"""
    from app.tasks.models import Base

    tables = Base.metadata.sorted_tables
    assert [t.name for t in tables] == ["tasks"], (
        f"零自有表纪律：metadata 只允许 new-api 共享 tasks，发现 {tables}"
    )
    assert not any(t.name.startswith("gateway_") for t in tables)
    # tasks 表归 new-api AutoMigrate：网关无任何建表入口
    import app.db as db_mod

    assert not hasattr(db_mod, "create_gateway_tables")
    # 迁移件整体删除
    assert not (REPO_ROOT / "migrations").exists()
    assert not (REPO_ROOT / "alembic").exists()
    assert not (REPO_ROOT / "alembic.ini").exists()
    # compose 不再挂载 initdb（无自有 DDL 可初始化）
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "docker-entrypoint-initdb.d" not in compose


# ---------------------------------------------------------------------------
# 3. gunicorn.conf.py 静态断言（架构 §2.2 关键参数）
# ---------------------------------------------------------------------------


def test_gunicorn_conf_static() -> None:
    spec = importlib.util.spec_from_file_location(
        "gunicorn_conf", REPO_ROOT / "gunicorn.conf.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert mod.worker_class == "uvicorn.workers.UvicornWorker"
    assert 1 <= mod.workers <= 16  # (2*CPU)+1 封顶 16
    assert mod.graceful_timeout == 30  # 与 compose stop_grace_period=60s 对齐
    assert mod.preload_app is True
    assert mod.worker_tmp_dir == "/dev/shm"
    assert mod.max_requests == 5000 and mod.max_requests_jitter == 500
    assert mod.limit_request_line == 4094
    assert mod.limit_request_fields == 100
    assert mod.limit_request_field_size == 8190
    for hook in ("on_starting", "post_fork", "worker_exit"):
        assert callable(getattr(mod, hook)), hook


# ---------------------------------------------------------------------------
# 4. docker-compose 唯一部署形态静态断言（架构 §12.2；k8s 清单已整体删除）
# ---------------------------------------------------------------------------


def _load_compose() -> dict:
    # PyYAML 非钉版依赖（SPEC §2 版本纪律，禁止未登记新增）：
    # 本地/CI 装 pyyaml 时执行完整断言，否则跳过（Docker/CI 镜像可装）。
    yaml = pytest.importorskip("yaml", reason="pyyaml 未安装，跳过 compose 静态断言")
    return yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def test_no_k8s_manifests() -> None:
    """compose 为唯一部署形态：deploy/（k8s 清单）已整体删除。"""
    assert not (REPO_ROOT / "deploy").exists(), "k8s 清单目录 deploy/ 应已删除"


def test_docker_compose_yaml_parseable() -> None:
    doc = _load_compose()
    services = doc["services"]
    assert {"gateway", "worker", "mysql", "redis"} <= set(services)
    assert services["mysql"]["image"] == "mysql:8.0"
    assert services["redis"]["image"] == "redis:7"
    assert services["gateway"]["env_file"] == ".env"
    assert services["worker"]["env_file"] == ".env"
    assert "healthcheck" in services["mysql"] and "healthcheck" in services["redis"]
    assert services["gateway"]["depends_on"]["mysql"]["condition"] == "service_healthy"
    # worker 与 gateway 同镜像不同 command（架构 §12.1 组件部署形态表）
    assert services["worker"]["command"] == "python -m app.worker"
    # gateway 优雅停机：SIGKILL 前窗口覆盖 gunicorn graceful_timeout=30
    assert services["gateway"]["stop_grace_period"] == "60s"
    # gateway 存活探针（compose healthcheck）
    assert "/healthz/live" in " ".join(services["gateway"]["healthcheck"]["test"])
    # Redis 持久化纪律：AOF everysec（零自有表后 Redis 承接自有状态）
    assert "everysec" in services["redis"]["command"]
    assert "appendonly yes" in services["redis"]["command"]
    # 计费占位服务仅在 profile 下启动
    assert services["billing-service"]["profiles"] == ["billing"]
    assert services["pricing-logic"]["profiles"] == ["billing"]


def test_compose_no_hardcoded_secrets() -> None:
    """compose 无硬编码密码：mysql 口令一律 ${VAR:-devonly} 占位（.env.example 登记）。"""
    raw = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "MYSQL_ROOT_PASSWORD: root" not in raw
    assert "MYSQL_PASSWORD: gw" not in raw
    assert "gw:gw@" not in raw  # DATABASE_URL 不再内嵌硬编码口令
    assert "-proot" not in raw  # healthcheck 不再内嵌硬编码 root 口令
    assert "${MYSQL_ROOT_PASSWORD:-devonly}" in raw
    assert "${MYSQL_PASSWORD:-devonly}" in raw
