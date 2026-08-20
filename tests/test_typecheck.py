"""类型检查纳入测试套件：跑 `pytest tests/` 即跑 `mypy app/`。

mypy 配置以 pyproject.toml `[tool.mypy]` 为准（strict=false）；
增量缓存（.mypy_cache）让重复运行只需亚秒级。
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_mypy_clean():
    """`mypy app/` 零报错（配置见 pyproject.toml [tool.mypy]）。"""
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "app/"],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, (
        f"mypy 发现类型错误：\n{result.stdout}{result.stderr}"
    )
