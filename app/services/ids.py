"""本地 task_id 生成（中性件）。

``{prefix}_{uuid4hex}``——带固定前缀便于识别与分流（现行链路固定传 ``"queue"``，
故实际形态是 ``queue_{uuid4hex}``）；slug 化防前缀含非法字符，全链路
（受理响应 / GET / 回调 / tasks 表）同值。

抽出到中性模块的理由同 ``app.deps.identity``：旧 ``deps/preflight`` 删除后，
``/queue`` 链路（``app/services/relayflow.py``）仍需生成 task_id。
"""

from __future__ import annotations

import re
import uuid

#: task_id 的前缀长度上限：{prefix}_{uuid4hex} ≤ 20+1+32 = 53 字符，
#: 留足 tasks.task_id String(64) 余量（唯一索引长度不受影响）
_TASK_ID_PREFIX_MAX = 20


def new_task_id(prefix: str) -> str:
    """本地 task_id：``{prefix}_{uuid4hex}``——带前缀便于识别与分流；
    slug 化防前缀含非法字符，全链路（受理响应/GET/回调/tasks 表）同值。

    参数名不是 ``biz``：ADR-010 后 URL 里没有 biz 段，渠道分组概念也已退场，
    这里只是一个本地可读前缀（现行唯一调用方传 ``"queue"``）。
    """
    slug = re.sub(r"[^a-z0-9-]+", "-", prefix.lower()).strip("-")[:_TASK_ID_PREFIX_MAX].strip("-")
    return f"{slug or 'task'}_{uuid.uuid4().hex}"
