"""原生报文工具（中性件）：``/batch`` 链路用的纯函数。

- :func:`normalize`：URL 路径规整；
- :func:`is_local_id`：本地 task_id 形态判定（GET/DELETE 分派用）；
- :func:`rewrite_ids`：上游 id → 本地 id 的字节级改写（原生报文同构）；
- :func:`capture_snapshot`：终态上游报文的落库快照（≤8KB，供终态零上游往返回放）。

零 I/O、零硬编码模型知识；旧链路（RouteConfig 驱动）删除后本模块只保留
``/batch`` 链路实际使用的工具（见 ADR-010「原生报文同构」）。
"""

from __future__ import annotations

import json
import re

#: 终态上游报文快照的落库上限（超限不存——探测报文正常 <2KB，防 data 列膨胀）
SNAPSHOT_MAX_BYTES = 8192

#: 本地 task_id 形态：``{biz_slug}_{uuid4hex}``（见 app.services.ids.new_task_id）
LOCAL_ID_RE = re.compile(r"^[a-z0-9-]{1,20}_[0-9a-f]{32}$")


def normalize(path: str) -> str:
    """URL 路径规整为 ``/a/b`` 形态（补前导斜杠、去尾部斜杠）。"""
    return "/" + path.strip("/")


def is_local_id(segment: str) -> bool:
    """是否为网关本地 task_id 形态（主键直查，绝不触发反查扫描）。"""
    return bool(LOCAL_ID_RE.match(segment))


def capture_snapshot(raw: dict | None) -> dict | None:
    """终态上游报文的落库快照（供原生查询逐字段同构回放）。

    只收**小体积 JSON 对象**：``SNAPSHOT_MAX_BYTES`` 以内（探测报文正常
    <2KB，防 tasks.data 列膨胀）；空报文（本地取消）→ None（不落键）。
    """
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        size = len(json.dumps(raw, ensure_ascii=False).encode())
    except (TypeError, ValueError):
        return None
    return raw if size <= SNAPSHOT_MAX_BYTES else None


def rewrite_ids(body: bytes, from_id: str, to_id: str) -> bytes:
    """字节级 id 改写：上游 id → 本地 id。

    任务 id 是 ASCII 字母数字串，JSON 里不会被转义，逐字节替换即可在
    **不重新序列化**的前提下保持报文其余部分 100% 同构（字段顺序、未知字段、
    数值精度、浮点写法全部原样）。
    """
    if not body or not from_id or not to_id or from_id == to_id:
        return body
    return body.replace(from_id.encode(), to_id.encode())


__all__ = [
    "LOCAL_ID_RE",
    "SNAPSHOT_MAX_BYTES",
    "capture_snapshot",
    "is_local_id",
    "normalize",
    "rewrite_ids",
]
