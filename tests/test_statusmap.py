"""状态映射测试（statusmap）。

ADR-010 后渠道级 ``status_map`` 随 RouteConfig 一并放弃，``map_status`` 只吃
上游原话：内置字典精确枚举 / 前缀猜测 / 归一化，覆盖常见上游措辞。
"""

from __future__ import annotations

import pytest

from app.schemas import CANCELED, FAILURE, IN_PROGRESS, QUEUED, SUCCESS
from app.services import statusmap


@pytest.mark.parametrize(("raw", "expected"), [
    ("succeeded", SUCCESS), ("Success", SUCCESS), ("SUCCESS", SUCCESS),
    ("failed", FAILURE), ("Fail", FAILURE),
    ("cancelled", CANCELED), ("canceled", CANCELED),
    ("queued", QUEUED), ("pending", QUEUED), ("Preparing", QUEUED),
    ("running", IN_PROGRESS), ("processing", IN_PROGRESS),
    ("TASK_STATUS_SUCCEED", SUCCESS),          # 命名空间前缀剥离
    ("task.succeeded", SUCCESS),
    ("State: Running", IN_PROGRESS),
])
def test_map_status_builtin(raw, expected):
    assert statusmap.map_status(raw) == expected


def test_map_status_unknown_returns_none():
    assert statusmap.map_status("flibbertigibbet") is None
    assert statusmap.map_status("") is None
    assert statusmap.map_status(None) is None
