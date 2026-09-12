"""管理面（/admin/*）与运行时热配置（dynconf）测试。

覆盖点：
1. 鉴权边界：未配 ``ADMIN_TOKEN`` 时整个管理面 404；错 token 401；
   正确 token 200；用户 sk / Bearer 不能当管理密钥。
2. 脱敏：详情与列表响应里不出现 ``token_hash``、原始 ``request_body``、
   上游 key 等。
3. requeue：非终态可重投；终态 409；未知任务 404。
4. dynconf：白名单外键被拒；非法值整批回滚（一个值都没变）；Redis 覆盖压过
   env；Redis 故障回落 env 且不抛异常。
5. 热生效证明：改 ``max_concurrent_tasks`` → 并发闸门真的变化；
   改 ``upstream_breaker_threshold`` → 熔断阈值真的变化。
6. 未配管理面时 /admin 与 /admin/api/* 全部 404。

所有 HTTP 断言都用真实 ASGI 请求（``httpx.ASGITransport``）打一遍，而不是只
断言 ``app.routes`` 里有这条路径——「端点写了但没挂上/被通配吞掉」是这类改动
最高频的失败模式。

不依赖真 MySQL/Redis：taskstore 用 conftest 的内存替身；dynconf 需要的 Redis
Hash 命令由本文件给 conftest 的 FakeRedis **就地补挂**（不改 conftest——它归
测试基建，本任务无修改权）。
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
import pytest

from app.config import settings
from app.main import app
from app.schemas import ACTIVE

ADMIN = "adm-token"
H = {"X-Admin-Token": ADMIN}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw.test")


# ---------------------------------------------------------------------------
# 给 conftest 的 FakeRedis 补 Hash 命令（dynconf 用）
# ---------------------------------------------------------------------------


def _enable_hash(redis: Any) -> None:
    """就地补 ``hgetall/hset/hdel``（FakeRedis 原只覆盖网关用到的命令子集）。"""

    async def hgetall(key: str) -> dict[str, str]:
        if not redis._alive(key):
            return {}
        value = redis._data.get(key)
        return dict(value) if isinstance(value, dict) else {}

    async def hset(key: str, mapping: dict | None = None, **kwargs: Any) -> int:
        fields = dict(mapping or {})
        fields.update(kwargs)
        if not (redis._alive(key) and isinstance(redis._data.get(key), dict)):
            redis._data[key] = {}
        redis._data[key].update({k: redis._s(v) for k, v in fields.items()})
        return len(fields)

    async def hdel(key: str, *fields: str) -> int:
        if not (redis._alive(key) and isinstance(redis._data.get(key), dict)):
            return 0
        removed = 0
        for field in fields:
            if field in redis._data[key]:
                del redis._data[key][field]
                removed += 1
        return removed

    redis.hgetall = hgetall
    redis.hset = hset
    redis.hdel = hdel


@pytest.fixture
def dynconf_redis(patch_redis, monkeypatch: pytest.MonkeyPatch):
    """把 dynconf 的 Redis 客户端指到补了 Hash 命令的 FakeRedis，并清本地缓存。"""
    import app.services.dynconf as dc

    _enable_hash(patch_redis)
    monkeypatch.setattr(dc, "r", patch_redis)
    dc._invalidate()
    yield patch_redis
    dc._invalidate()


def _install_fake_search(monkeypatch: pytest.MonkeyPatch, store: Any) -> None:
    """把 taskstore.search_tasks 挂成内存实现（镜像生产签名，验路由契约与脱敏）。

    真实后端已落在 ``app/services/taskstore.search_tasks``（返回
    ``(items, total)``，列投影白名单）。这里额外**故意多带** ``token_hash`` /
    ``request_body`` 两个敏感列，用来证明管理面这一层的白名单投影能兜住
    「数据层哪天被写宽了」的回归。
    """
    from app.services import taskstore

    async def fake_search(*, status: str = "", model: str = "", task_id: str = "",
                          since_seconds: int = 0, limit: int = 50,
                          offset: int = 0) -> tuple[list[dict], int]:
        cutoff = int(time.time()) - since_seconds if since_seconds else 0
        rows = []
        for row in store.rows.values():
            data = row.get("data") or {}
            if status and row["status"] != status:
                continue
            if model and data.get("model") != model:
                continue
            if task_id and row["task_id"] != task_id:      # 精确匹配，无前缀通配
                continue
            if cutoff and int(row.get("created_at") or 0) < cutoff:
                continue
            rows.append({
                "task_id": row["task_id"], "status": row["status"],
                "progress": row.get("progress"), "action": row.get("action"),
                "user_id": row.get("user_id"), "channel_id": row.get("channel_id"),
                "created_at": row.get("created_at"), "finish_time": row.get("finish_time"),
                "updated_at": row.get("updated_at"),
                "model": data.get("model"), "biz": data.get("biz"),
                "source": data.get("source"), "result": data.get("result"),
                "freeze_amount": data.get("freeze_amount"), "settled": data.get("settled"),
                # 故意越界的两列（见 docstring）
                "token_hash": data.get("token_hash"),
                "request_body": data.get("request_body"),
            })
        rows.sort(key=lambda r: int(r.get("created_at") or 0), reverse=True)
        return rows[offset:offset + limit], len(rows)

    monkeypatch.setattr(taskstore, "search_tasks", fake_search, raising=False)


async def _make_task(store: Any, task_id: str, *, status: str = "SUBMITTED",
                     data: dict | None = None) -> None:
    await store.create(
        task_id=task_id, user_id=42, channel_id=7, action="generate",
        data=data or {"source": "queue", "model": "MiniMax-H3"},
    )
    if status != "SUBMITTED":
        assert await store.cas(task_id, ACTIVE, status) is True


# ---------------------------------------------------------------------------
# 1 / 6. 鉴权边界
# ---------------------------------------------------------------------------


async def test_admin_404_when_token_unconfigured(monkeypatch: pytest.MonkeyPatch):
    """未配 ADMIN_TOKEN ⇒ 整个管理面 404（不是 401，不暴露端点存在）。"""
    monkeypatch.setattr(settings, "admin_token", None)
    paths = ["/admin", "/admin/", "/admin/api/overview", "/admin/api/tasks",
             "/admin/api/tasks/x", "/admin/api/config"]
    async with _client() as c:
        for path in paths:
            assert (await c.get(path)).status_code == 404, path
        assert (await c.put("/admin/api/config", json={"max_concurrent_tasks": 9})).status_code == 404
        assert (await c.post("/admin/api/config/reset")).status_code == 404
        assert (await c.post("/admin/api/tasks/x/requeue")).status_code == 404


async def test_admin_token_boundary(monkeypatch: pytest.MonkeyPatch, dynconf_redis,
                                    task_store):
    """错 token 401；正确 token 200；用户 sk / Bearer 都不是管理密钥。"""
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    _install_fake_search(monkeypatch, task_store)   # 概览的失败数探针走内存实现
    async with _client() as c:
        assert (await c.get("/admin/api/overview")).status_code == 401
        assert (await c.get("/admin/api/overview",
                            headers={"X-Admin-Token": "nope"})).status_code == 401
        # 用户 sk 当管理密钥用 → 拒
        assert (await c.get("/admin/api/overview",
                            headers={"X-Admin-Token": "sk-user-42"})).status_code == 401
        # Bearer 形式不被接受（管理面只认 X-Admin-Token，避免与用户面鉴权混淆）
        assert (await c.get("/admin/api/overview",
                            headers={"Authorization": f"Bearer {ADMIN}"})).status_code == 401
        ok = await c.get("/admin/api/overview", headers=H)
        assert ok.status_code == 200, ok.text


async def test_admin_dashboard_served_and_self_contained(monkeypatch: pytest.MonkeyPatch):
    """看板页面：启用时可取；单文件零外部依赖；令牌不落 URL。"""
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    async with _client() as c:
        resp = await c.get("/admin")
        assert resp.status_code == 200
        html = resp.text
        assert "atask-service" in html
        assert "<script src" not in html          # 不引外部脚本
        assert "cdn" not in html.lower()           # 不引 CDN
        assert "https://" not in html              # 无外部资源
        assert "sessionStorage" in html            # 密钥存会话
        assert "location.hash" not in html and "?token" not in html  # 不落 URL


# ---------------------------------------------------------------------------
# 2. 脱敏
# ---------------------------------------------------------------------------


SECRET_DATA = {
    "source": "queue",
    "model": "MiniMax-H3",
    "token_hash": "deadbeef" * 8,
    "request_body": {"prompt": "super-secret-prompt", "duration": 5},
    "upstream_task_id": "up-1",
    # 以下三个是 ADR-010 的退役字段（渠道分组 / keypool / 资金），**故意留在语料里**：
    # 它们现在的作用是证明白名单投影会把它们剥掉（而不是让它们以 null 形式透出）。
    "biz": "minimax",
    "key_id": 7,
    "key_index": 1,
    "freeze_amount": 0.13,
}


async def test_admin_task_detail_desensitized(monkeypatch: pytest.MonkeyPatch,
                                              patch_redis, task_store):
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    await _make_task(task_store, "minimax_detail", data=dict(SECRET_DATA))
    async with _client() as c:
        resp = await c.get("/admin/api/tasks/minimax_detail", headers=H)
        assert resp.status_code == 200, resp.text
        body = resp.text
        assert "deadbeef" not in body                # token_hash 不泄露
        assert "super-secret-prompt" not in body     # 原始请求体不泄露
        assert "request_body" not in body            # 连字段名都不出现
        assert "token_hash" not in body
        view = resp.json()
        assert view["task_id"] == "minimax_detail"
        assert view["data"]["model"] == "MiniMax-H3"
        # 详情投影同样不含退役字段（含旧链路的 key_index）
        assert not ({"biz", "key_index", "freeze_amount", "settled_amount"}
                    & set(view["data"]))
        # 令牌会话只给存在性与 TTL
        assert view["token_session"]["exists"] is False
        # 未知任务 404
        assert (await c.get("/admin/api/tasks/none", headers=H)).status_code == 404


async def test_admin_task_list_desensitized(monkeypatch: pytest.MonkeyPatch,
                                            patch_redis, task_store):
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    await _make_task(task_store, "minimax_one", data=dict(SECRET_DATA))
    await _make_task(task_store, "minimax_two",
                     data={**SECRET_DATA, "model": "Other-Model"})
    _install_fake_search(monkeypatch, task_store)
    async with _client() as c:
        resp = await c.get("/admin/api/tasks?limit=50", headers=H)
        assert resp.status_code == 200, resp.text
        body = resp.text
        assert "deadbeef" not in body
        assert "super-secret-prompt" not in body
        assert "request_body" not in body
        payload = resp.json()
        assert payload["total"] == 2
        assert {item["task_id"] for item in payload["items"]} == {"minimax_one", "minimax_two"}
        # 白名单投影真的落地：敏感键在 items 里根本不存在，业务键在
        first = payload["items"][0]
        assert "token_hash" not in first and "request_body" not in first
        assert first["model"] in ("MiniMax-H3", "Other-Model")
        assert first["source"] == "queue"
        # 退役字段（渠道分组 / 资金 / keypool）必须被投影剥掉，不是「回 null」
        assert "biz" not in first and "freeze_amount" not in first
        assert "settled" not in first

        # 精确匹配生效；不存在的 task_id 返回空
        exact = await c.get("/admin/api/tasks?task_id=minimax_one", headers=H)
        assert [i["task_id"] for i in exact.json()["items"]] == ["minimax_one"]
        none = await c.get("/admin/api/tasks?task_id=minimax", headers=H)
        assert none.json()["total"] == 0                # 前缀不通配

        # model / status 过滤
        by_model = await c.get("/admin/api/tasks?model=Other-Model", headers=H)
        assert [i["task_id"] for i in by_model.json()["items"]] == ["minimax_two"]


# ---------------------------------------------------------------------------
# 3. requeue
# ---------------------------------------------------------------------------


async def test_admin_requeue_non_terminal_ok_terminal_rejected(
    monkeypatch: pytest.MonkeyPatch, patch_redis, task_store, queue_events,
):
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    await _make_task(task_store, "t_active")                 # SUBMITTED（非终态）
    await _make_task(task_store, "t_done", status="SUCCESS")  # 终态
    async with _client() as c:
        ok = await c.post("/admin/api/tasks/t_active/requeue", headers=H)
        assert ok.status_code == 200, ok.text
        assert ok.json()["requeued"] is True
        assert queue_events["queue_submit"][-1] == "t_active"

        bad = await c.post("/admin/api/tasks/t_done/requeue", headers=H)
        assert bad.status_code == 409, bad.text               # 终态拒绝
        assert "terminal" in bad.text

        assert (await c.post("/admin/api/tasks/none/requeue", headers=H)).status_code == 404
        # 终态被拒时不得有任何重投副作用
        assert "t_done" not in queue_events["queue_submit"]


# ---------------------------------------------------------------------------
# 4. dynconf：白名单 / 整批回滚 / 优先级 / Redis 故障回落
# ---------------------------------------------------------------------------


async def test_dynconf_whitelist_only(dynconf_redis):
    import app.services.dynconf as dc

    # 白名单外（含安全项）一律拒绝，并给出原因
    for key in ("database_url", "admin_token", "callback_sign_secret",
                "upstream_allowlist", "totally_unknown"):
        with pytest.raises(ValueError):
            await dc.set_many({key: "x"})
    # 一个都没写进去
    assert (await dc.snapshot())["override_count"] == 0


async def test_dynconf_batch_rollback_all_or_nothing(dynconf_redis):
    """整批校验：任一项非法则整批拒绝，一个值都不改。"""
    import app.services.dynconf as dc

    before = settings.max_concurrent_tasks
    with pytest.raises(ValueError):
        # 第一项合法、第二项越界（min=1）——绝不能出现「第一项偷偷写进去」
        await dc.set_many({"max_concurrent_tasks": 9, "upstream_breaker_threshold": 0})
    assert (await dc.snapshot())["override_count"] == 0
    assert await dc.get_int("max_concurrent_tasks") == before


async def test_dynconf_priority_over_env_and_reset(dynconf_redis, monkeypatch):
    """读取优先级：Redis 覆盖 > env > 默认；reset 后回落 env。"""
    import app.services.dynconf as dc

    monkeypatch.setattr(settings, "max_concurrent_tasks", 5)
    dc._invalidate()
    assert await dc.get_int("max_concurrent_tasks") == 5            # env

    await dc.set_many({"max_concurrent_tasks": 11})
    assert await dc.get_int("max_concurrent_tasks") == 11           # Redis 覆盖压过 env

    snap = await dc.snapshot()
    assert snap["override_count"] == 1
    item = next(i for g in snap["groups"] for i in g["items"]
                if i["key"] == "max_concurrent_tasks")
    assert item["overridden"] is True and item["value"] == 11

    await dc.reset(["max_concurrent_tasks"])
    assert await dc.get_int("max_concurrent_tasks") == 5            # 回落 env
    assert (await dc.snapshot())["override_count"] == 0


async def test_dynconf_redis_down_falls_back_silently(monkeypatch: pytest.MonkeyPatch):
    """Redis 不可用：读回落 env、不抛异常；写明确失败（不谎报成功）。"""
    import app.services.dynconf as dc

    class DeadRedis:
        async def hgetall(self, key: str):
            raise ConnectionError("redis down")

        async def hset(self, key: str, mapping: dict | None = None):
            raise ConnectionError("redis down")

        async def hdel(self, key: str, *fields: str):
            raise ConnectionError("redis down")

        async def delete(self, *keys: str):
            raise ConnectionError("redis down")

    monkeypatch.setattr(dc, "r", DeadRedis())
    dc._invalidate()
    assert await dc.get_int("max_concurrent_tasks") == settings.max_concurrent_tasks
    snap = await dc.snapshot()                       # 不抛
    assert snap["override_count"] == 0


async def test_admin_config_endpoints_roundtrip(monkeypatch: pytest.MonkeyPatch,
                                                dynconf_redis):
    """HTTP 层：读全量视图 / 非法键 400 / 合法写生效 / reset 回落。"""
    import app.services.dynconf as dc

    monkeypatch.setattr(settings, "admin_token", ADMIN)
    async with _client() as c:
        read = await c.get("/admin/api/config", headers=H)
        assert read.status_code == 200
        keys = {i["key"] for g in read.json()["groups"] for i in g["items"]}
        assert keys == set(dc.MUTABLE)

        bad = await c.put("/admin/api/config", headers=H, json={"database_url": "nope"})
        assert bad.status_code == 400

        ok = await c.put("/admin/api/config", headers=H, json={"max_concurrent_tasks": 8})
        assert ok.status_code == 200, ok.text
        assert await dc.get_int("max_concurrent_tasks") == 8

        reset = await c.post("/admin/api/config/reset", headers=H)
        assert reset.status_code == 200
        assert await dc.get_int("max_concurrent_tasks") == settings.max_concurrent_tasks


# ---------------------------------------------------------------------------
# 5. 热生效证明（配置改了，行为真的变）
# ---------------------------------------------------------------------------


async def test_hot_effect_concurrency_limit(dynconf_redis, monkeypatch: pytest.MonkeyPatch):
    """改 max_concurrent_tasks → 并发闸门行为真的变化。"""
    import app.deps.ratelimit as rl
    import app.services.dynconf as dc

    monkeypatch.setattr(settings, "max_concurrent_tasks", 1)
    dc._invalidate()
    token_hash = "hot-effect-token"
    assert await rl.conc_try_acquire(token_hash) is True       # 用掉唯一名额
    assert await rl.conc_try_acquire(token_hash) is False      # 撞 env 上限 1

    await dc.set_many({"max_concurrent_tasks": 3})
    assert await rl.conc_try_acquire(token_hash) is True       # 上限放宽到 3
    assert await rl.conc_try_acquire(token_hash) is True
    assert await rl.conc_try_acquire(token_hash) is False      # 3 个名额用满

    await dc.set_many({"max_concurrent_tasks": 1})
    await rl.conc_release(token_hash)                          # 释放一个
    assert await rl.conc_try_acquire(token_hash) is False      # 收紧上限后重新生效


# ---------------------------------------------------------------------------
# 装配自检：端点真的可达（不是只存在于 app.routes）
# ---------------------------------------------------------------------------


async def test_admin_routes_actually_reachable(monkeypatch: pytest.MonkeyPatch,
                                               dynconf_redis, task_store, queue_events):
    """每个新增端点都真实请求一遍，确认没被通配路由吞掉、状态码符合预期。"""
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    _install_fake_search(monkeypatch, task_store)
    await _make_task(task_store, "minimax_r1", data=dict(SECRET_DATA))
    async with _client() as c:
        checks = [
            ("GET", "/admin", None, 200),
            ("GET", "/admin/api/overview", None, 200),
            ("GET", "/admin/api/tasks", None, 200),
            ("GET", "/admin/api/tasks/minimax_r1", None, 200),
            ("POST", "/admin/api/tasks/minimax_r1/requeue", None, 200),
            ("GET", "/admin/api/config", None, 200),
            ("PUT", "/admin/api/config", {"max_concurrent_tasks": 6}, 200),
            ("POST", "/admin/api/config/reset", None, 200),
        ]
        for method, path, body, expected in checks:
            if method == "GET":
                resp = await c.get(path, headers=H)
            elif method == "POST":
                resp = await c.post(path, headers=H, json=body)
            else:
                resp = await c.put(path, headers=H, json=body)
            assert resp.status_code == expected, f"{method} {path} -> {resp.status_code}: {resp.text}"

        # 配置写失败（Redis 挂）应当明确非 2xx，不谎报成功
        import app.services.dynconf as dc
        original = dc.r
        monkeypatch.setattr(dc, "r", _DeadHashRedis())
        failed = await c.put("/admin/api/config", headers=H, json={"max_concurrent_tasks": 7})
        assert failed.status_code == 503
        monkeypatch.setattr(dc, "r", original)


class _DeadHashRedis:
    async def hset(self, key: str, mapping: dict | None = None):
        raise ConnectionError("redis down")

    async def hgetall(self, key: str):
        raise ConnectionError("redis down")


# ---------------------------------------------------------------------------
# overview 内容
# ---------------------------------------------------------------------------


async def test_admin_overview_shape(monkeypatch: pytest.MonkeyPatch, dynconf_redis,
                                    task_store):
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    _install_fake_search(monkeypatch, task_store)
    await _make_task(task_store, "minimax_ok", status="SUCCESS", data=dict(SECRET_DATA))
    async with _client() as c:
        resp = await c.get("/admin/api/overview?window=3600", headers=H)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["tasks_by_status"].get("SUCCESS") == 1
        assert set(body["queue"]) == {"pending", "delayed", "dlq"}
        assert body["recent_failures"] == 0
        assert body["service"]["platform"] == settings.gateway_platform
        assert "deadbeef" not in json.dumps(body)


# ---------------------------------------------------------------------------
# 后端落地：taskstore.search_tasks（真实现，非降级分支）
# ---------------------------------------------------------------------------


class _FakeSession:
    """捕获 SQL/params 的假会话（不连 DB，验的是 search_tasks 自己构造的语句）。"""

    def __init__(self, captured: list[tuple[str, dict]]) -> None:
        self._captured = captured

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, stmt: Any, params: dict | None = None) -> _FakeResult:
        sql = str(stmt)
        self._captured.append((sql, dict(params or {})))
        if "COUNT(*)" in sql.upper():
            return _FakeResult(scalar=1)
        return _FakeResult(rows=[{"task_id": "t1", "status": "SUCCESS", "model": "M"}])


class _FakeResult:
    def __init__(self, *, rows: list[dict] | None = None, scalar: Any = None) -> None:
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[dict]:
        return self._rows

    def scalar(self) -> Any:
        return self._scalar


def test_taskstore_search_tasks_backend_present():
    """生产必须真的挂上 taskstore.search_tasks（否则列表端点只会 503）。"""
    from app.services import taskstore

    assert callable(getattr(taskstore, "search_tasks", None))


async def test_taskstore_search_tasks_sql_discipline(monkeypatch: pytest.MonkeyPatch):
    """真实现走通：platform 恒带、精确等值、列投影白名单、排序单位归一、分页钳制。"""
    from app.services import taskstore

    captured: list[tuple[str, dict]] = []
    monkeypatch.setattr(taskstore, "get_session_factory",
                        lambda: (lambda: _FakeSession(captured)))

    items, total = await taskstore.search_tasks(
        status="SUCCESS", model="M", task_id="t1", since_seconds=3600,
        limit=9999, offset=-5,
    )
    assert total == 1 and items[0]["task_id"] == "t1"
    assert len(captured) == 2                    # COUNT + SELECT

    joined = " ".join(sql for sql, _ in captured).lower()
    for frag in ("platform = :p", "status = :status", "task_id = :task_id",
                 "data ->> '$.model' = :model"):
        assert frag in joined, frag
    assert "select data" not in joined           # 绝不整列 SELECT data（token_hash 红线）
    assert "token_hash" not in joined and "request_body" not in joined
    assert " like " not in joined                # 无通配/前缀匹配
    assert "order by if(created_at >" in joined  # 排序走 _secs 单位归一

    count_params = dict(captured[0][1])
    item_params = dict(captured[1][1])
    shared = {"p", "status", "task_id", "model", "since"}
    assert shared <= set(count_params) and shared <= set(item_params)
    assert {k: count_params[k] for k in shared} == {k: item_params[k] for k in shared}
    assert item_params["lim"] == 200 and item_params["off"] == 0   # 钳制
    assert "lim" not in count_params             # count 查询不带分页参数


# ---------------------------------------------------------------------------
# 裁决 2：熔断阈值热生效（行为级）
# ---------------------------------------------------------------------------


async def test_hot_effect_breaker_threshold(dynconf_redis, monkeypatch: pytest.MonkeyPatch):
    """改 upstream_breaker_threshold → 熔断真的按新阈值开/关。"""
    import app.services.dynconf as dc
    from app.redis import K_BREAKER
    from app.services import upstream

    monkeypatch.setattr(settings, "upstream_breaker_threshold", 5)
    dc._invalidate()
    key = K_BREAKER.format(host="host-hot")
    await upstream.r.set(key, 3)

    await upstream.breaker_guard("host-hot")                    # 3 < 5：不打开

    await dc.set_many({"upstream_breaker_threshold": 2})
    with pytest.raises(upstream.BreakerOpenError):
        await upstream.breaker_guard("host-hot")                # 3 >= 2：打开

    await dc.set_many({"upstream_breaker_threshold": 10})
    await upstream.breaker_guard("host-hot")                    # 3 < 10：不再打开


# ---------------------------------------------------------------------------
# 裁决 3：/ops 与 /admin 统一 fail-closed
# ---------------------------------------------------------------------------


async def test_ops_fail_closed_when_admin_token_unconfigured(
    monkeypatch: pytest.MonkeyPatch, patch_redis, task_store,
):
    """未配 ADMIN_TOKEN 时 /ops/* 与 /admin/* 一样返回 404（不是 fail-open 放行）。"""
    monkeypatch.setattr(settings, "admin_token", None)
    async with _client() as c:
        assert (await c.get("/ops/queue")).status_code == 404
        assert (await c.get("/ops/tasks/x")).status_code == 404
        assert (await c.post("/ops/requeue/x")).status_code == 404   # 写操作更不允许裸奔
        assert (await c.post("/ops/dlq/replay")).status_code == 404
        assert (await c.get("/admin/api/overview")).status_code == 404


async def test_ops_configured_wrong_and_right_token(
    monkeypatch: pytest.MonkeyPatch, patch_redis, task_store,
):
    """配置密钥后：/ops/* 错 token 401、正确 token 200（已配置路径行为不变）。"""
    monkeypatch.setattr(settings, "admin_token", ADMIN)
    async with _client() as c:
        assert (await c.get("/ops/queue")).status_code == 401
        bad = await c.get("/ops/queue", headers={"X-Admin-Token": "nope"})
        assert bad.status_code == 401
        ok = await c.get("/ops/queue", headers=H)
        assert ok.status_code == 200, ok.text
