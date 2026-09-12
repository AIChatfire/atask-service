"""``app/services/taskstore`` 仍存活的查询：limit 夹紧 / 时间归一 / 禁 ``SELECT data``。

这些纪律无法靠「查得对」来验证——不夹紧 limit、漏掉毫秒归一、整列 ``SELECT data``
时功能照常工作，只是把生产库打爆或把用户令牌经管理面透出（本项目红线）。所以用
假会话**直接断言生成的 SQL 文本与绑定参数**。

覆盖：
- ``stale_queue_active``：``limit`` 夹到 1..200、时间比较走 ``_secs``、**无 ``SELECT data``**；
- ``search_tasks``：``limit`` 夹紧 / ``offset`` 非负、同样无 ``SELECT data``；
- ``as_unix_seconds`` / ``duration_seconds`` / ``_row_to_dict`` 的时间单位归一。
"""

from __future__ import annotations

import json
import time
from typing import Any

import pytest

from app.services import taskstore as ts


class _Result:
    def __init__(self, *, rows: list[dict] | None = None, scalar: Any = None) -> None:
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> _Result:
        return self

    def all(self) -> list[dict]:
        return self._rows

    def scalar(self) -> Any:
        return self._scalar


class _Session:
    def __init__(self, captured: list[tuple[str, dict]]) -> None:
        self._captured = captured

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, stmt: Any, params: dict | None = None) -> _Result:
        sql = str(stmt)
        self._captured.append((sql, dict(params or {})))
        if "COUNT(*)" in sql.upper():
            return _Result(scalar=0)
        return _Result(rows=[])


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    monkeypatch.setattr(ts, "get_session_factory", lambda: (lambda: _Session(out)))
    return out


# ---------------------------------------------------------------------------
# stale_queue_active
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("given", "expected"), [(0, 1), (-5, 1), (9999, 200), (50, 50)])
async def test_stale_queue_limit_is_clamped(captured, given, expected):
    await ts.stale_queue_active(stale_seconds=300, limit=given)
    assert captured[0][1]["lim"] == expected


async def test_stale_queue_sql_discipline(captured):
    await ts.stale_queue_active(stale_seconds=300, limit=50)
    sql, params = captured[0]
    assert ts._secs("updated_at") in sql
    assert "select data" not in sql.lower()               # token_hash 泄露红线
    assert "data ->> '$.token_hash'" in sql               # 只按名投影
    assert int(time.time()) - 400 < params["cutoff"] <= int(time.time()) - 299


# ---------------------------------------------------------------------------
# search_tasks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("limit", "offset", "exp_lim", "exp_off"),
                         [(0, 0, 1, 0), (-9, -9, 1, 0), (9999, 3, 200, 3)])
async def test_search_tasks_pagination_is_clamped(captured, limit, offset,
                                                  exp_lim, exp_off):
    await ts.search_tasks(limit=limit, offset=offset)
    assert captured[1][1]["lim"] == exp_lim
    assert captured[1][1]["off"] == exp_off
    assert "lim" not in captured[0][1]                    # COUNT 查询不带分页参数


async def test_search_tasks_never_selects_data_column(captured):
    await ts.search_tasks()
    sql = captured[1][0].lower()
    assert "select data" not in sql
    assert "token_hash" not in sql and "request_body" not in sql


# ---------------------------------------------------------------------------
# 时间单位归一
# ---------------------------------------------------------------------------


def test_as_unix_seconds_normalizes_millis():
    now = int(time.time())
    assert ts.as_unix_seconds(now) == now
    assert ts.as_unix_seconds(now * 1000) == now
    assert ts.as_unix_seconds(None) == 0
    assert ts.as_unix_seconds("not-a-number") == 0


def test_duration_seconds_uses_normalized_times():
    now = int(time.time())
    row = {"created_at": (now - 10) * 1000, "finish_time": now * 1000}
    assert ts.duration_seconds(row) == 10                 # 毫秒行也能算出正确秒差
    assert ts.duration_seconds({"created_at": now, "finish_time": 0}) == 0
    assert ts.duration_seconds({}) == 0


def test_row_to_dict_parses_json_and_normalizes_times():
    now = int(time.time())
    row = {
        "task_id": "t1",
        "data": json.dumps({"model": "m"}),
        "created_at": now * 1000,
        "finish_time": 0,
        "updated_at": now,
    }
    out = ts._row_to_dict(row)
    assert out["data"] == {"model": "m"}                  # JSON 串 → dict
    assert out["created_at"] == now                       # 毫秒 → 秒
    assert out["finish_time"] == 0
    assert ts._row_to_dict({"data": None})["data"] == {}  # 空 data 归一为 {}


def test_secs_expression_divides_millis():
    expr = ts._secs("created_at")
    assert expr.startswith("IF(created_at >")
    assert "DIV 1000" in expr
