"""本地 task_id 生成（中性件）。

``{biz_slug}_{uuid4hex}``——带 biz 前缀便于识别与分流；slug 化防渠道名含非法
字符，全链路（提交响应 / GET / 回调 / tasks 表）同值。

抽出到中性模块的理由同 ``app.deps.identity``：旧 ``deps/preflight`` 删除后，
``/batch`` 链路（``app/services/relayflow.py``）仍需生成 task_id。
"""

from __future__ import annotations

import re
import uuid

#: task_id 的 biz 前缀长度上限：{slug}_{uuid4hex} ≤ 20+1+32 = 53 字符，
#: 留足 tasks.task_id String(64) 余量（唯一索引长度不受影响）
_TASK_ID_PREFIX_MAX = 20


def new_task_id(biz: str) -> str:
    """本地 task_id：``{biz_slug}_{uuid4hex}``——带 biz 前缀便于识别与分流；
    slug 化防渠道名含非法字符，全链路（提交响应/GET/回调/tasks 表）同值。"""
    slug = re.sub(r"[^a-z0-9-]+", "-", biz.lower()).strip("-")[:_TASK_ID_PREFIX_MAX].strip("-")
    return f"{slug or 'task'}_{uuid.uuid4().hex}"
