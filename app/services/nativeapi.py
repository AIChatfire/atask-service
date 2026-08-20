"""原生形态（透传路径）语义拦截的纯函数工具集——零 I/O、零硬编码模型知识。

透传路由 ``/{biz}/{原生路径}`` 默认是**同步透传**语义（上游报文逐字节流回）。
但上游"提交任务 / 查询任务 / 取消任务"这三条路径是网关的生命周期入口，必须
改成 **异步受理 + 本地 task_id** 语义，同时保持请求/响应与原生接口 100% 同构：

- 提交（``route.submit_path``）：不触上游，落库即返回，响应体按
  ``route.task_id_path`` 塑形为原生形状，值换成本地 task_id（秒级返回）；
- 查询（``route.probe_path``）：客户端持本地 id 来查 → 把 URL 里的 id 换成上游
  id 转发探测，响应缓冲后把上游 id 原样替换回本地 id（其余字节不动）；
  已终态回放落库的上游原始报文（逐字段同构）、上游尚未接单（异步提交在飞）
  时按配置反向构建快照——两种情形都零上游往返；
- 取消（``route.cancel_path``）：走本地 cancel 链路（解冻 + 尽力源头止损），
  绝不当成"新任务"报价冻结。

判定全部由渠道配置（RouteConfig 的三个路径模板）驱动：换模型只改 keypool
渠道的 gateway 块，本模块不认识任何具体上游。
"""

from __future__ import annotations

import json
import re
from typing import Any

from app.schemas import (
    CANCELED,
    FAILURE,
    HELD,
    IN_PROGRESS,
    QUEUED,
    SUBMITTED,
    SUCCESS,
    TERMINAL,
    RouteConfig,
)

#: 终态上游报文快照的落库上限（超限不存——探测报文正常 <2KB，防 data 列膨胀）
SNAPSHOT_MAX_BYTES = 8192

#: 路径模板里的任务 id 占位符（与 upstream.probe/cancel 的 format 键一致）
PLACEHOLDER = "{upstream_task_id}"

#: 本地 task_id 形态：``{biz_slug}_{uuid4hex}``（见 deps/preflight.new_task_id）
LOCAL_ID_RE = re.compile(r"^[a-z0-9-]{1,20}_[0-9a-f]{32}$")
#: 上游任务 id 常见形态：长数字串 / 长 hex / UUID。用于 URL 段的"像不像 id"
#: 预筛——路径里没有 id 形态的段时零查库、零开销。
_UPSTREAM_ID_RE = re.compile(
    r"^(?:\d{6,}|[0-9a-fA-F]{16,64}"
    r"|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$"
)

#: 内部状态 → 原生报文里的小写状态词（仅"无上游可问"窗口的本地快照用；
#: statusmap 的逆向近似，客户端侧的正常报文一律来自上游原文）
_STATUS_WORDS: dict[str, str] = {
    SUBMITTED: "queued",
    QUEUED: "queued",
    HELD: "queued",
    IN_PROGRESS: "processing",
    SUCCESS: "succeeded",
    FAILURE: "failed",
    CANCELED: "canceled",
}


def status_word(route: RouteConfig, task: dict) -> str:
    """本地快照报文里的状态词，按"越接近上游原文越优先"三档取值：

    1. ``data.upstream_status``——poller/回调写入的**上游原话**（最忠实；
       但本地已终态而上游快照还是活跃态时不能用，否则报文自相矛盾）；
    2. 渠道 ``status_map`` 的逆映射——渠道显式配了上游措辞就用它的词汇表；
    3. 内置 ``_STATUS_WORDS`` 兜底（statusmap 的逆向近似）。
    """
    from app.services import statusmap   # 局部导入：statusmap 不依赖本模块，防环

    data = task.get("data") or {}
    status = str(task.get("status") or SUBMITTED)
    upstream_status = data.get("upstream_status")
    # 上游原话仅在与本地状态同档时可用：本地已终态而快照还停在活跃态
    # （取消/判死走的是本地收口），回显原话会自相矛盾
    if upstream_status and statusmap.map_status(route, upstream_status) == status:
        return str(upstream_status)
    for raw, mapped in (route.status_map or {}).items():
        if str(mapped).upper() == status:
            return str(raw)
    return _STATUS_WORDS.get(status, "queued")


# ---------------------------------------------------------------------------
# 路径规整与模板匹配
# ---------------------------------------------------------------------------


def normalize(path: str) -> str:
    """URL 路径规整为 ``/a/b`` 形态（补前导斜杠、去尾部斜杠）。"""
    return "/" + path.strip("/")


def _split(template: str) -> tuple[str, str]:
    """模板拆为（路径部分, 查询串部分）——占位符可能在任一侧。"""
    head, _, tail = (template or "").partition("?")
    return head, tail


def _query_param_name(template: str) -> str | None:
    """占位符位于查询串时的参数名（``/v1/status?id={upstream_task_id}`` → ``id``）。"""
    _, tail = _split(template)
    if PLACEHOLDER not in tail:
        return None
    for pair in tail.split("&"):
        name, _, value = pair.partition("=")
        if value.strip() == PLACEHOLDER and name:
            return name
    return None


def match_path(template: str, path: str) -> bool:
    """无占位符模板的精确匹配（提交端点用）。"""
    head, _ = _split(template)
    if not head or PLACEHOLDER in head:
        return False
    return normalize(path) == normalize(head)


def template_id(template: str, path: str, params: Any = None) -> str | None:
    """带占位符的模板命中时，返回 URL 里携带的任务 id；不命中 → None。

    支持两种形态：占位符在**路径段**（``/v2/query/x/{upstream_task_id}``）、
    占位符在**查询参数**（``/v1/status?id={upstream_task_id}``）。
    """
    if not template:
        return None
    head, tail = _split(template)
    if PLACEHOLDER in head:
        pattern = "^" + "([^/]+)".join(
            re.escape(part) for part in normalize(head).split(PLACEHOLDER)
        ) + "$"
        hit = re.match(pattern, normalize(path))
        return hit.group(1) if hit else None
    if PLACEHOLDER in tail and normalize(path) == normalize(head):
        name = _query_param_name(template)
        got = params.get(name) if (name and params is not None) else None
        return str(got) if got else None
    return None


def swap_id(template: str, path: str, params: dict[str, str],
            new_id: str) -> tuple[str, dict[str, str]]:
    """把 URL 里的任务 id 换成 ``new_id``（转发上游用）。

    返回 ``(转发路径, 转发查询参数)``；路径不带前导斜杠（与 FastAPI ``path``
    参数同形）。路径形态改路径段，查询形态改同名查询参数。
    """
    head, _ = _split(template)
    out_params = dict(params or {})
    if PLACEHOLDER in head:
        return normalize(head).replace(PLACEHOLDER, new_id).lstrip("/"), out_params
    name = _query_param_name(template)
    if name:
        out_params[name] = new_id
    return path.lstrip("/"), out_params


def match_submit(route: RouteConfig, path: str) -> bool:
    """命中渠道配置的提交端点。"""
    return match_path(route.submit_path, path)


def probe_id(route: RouteConfig, path: str, params: Any = None) -> str | None:
    """命中渠道配置的查询端点时返回 URL 里的任务 id。"""
    return template_id(route.probe_path, path, params)


def cancel_id(route: RouteConfig, path: str, params: Any = None) -> str | None:
    """命中渠道配置的取消端点时返回 URL 里的任务 id。"""
    return template_id(route.cancel_path, path, params)


# ---------------------------------------------------------------------------
# 任务 id 预筛（查库前的零成本判断）
# ---------------------------------------------------------------------------


def is_local_id(segment: str) -> bool:
    """是否为网关本地 task_id 形态（主键直查，绝不触发反查扫描）。"""
    return bool(LOCAL_ID_RE.match(segment))


def looks_like_task_id(segment: str) -> bool:
    """URL 段是否像任务 id（本地 id 或上游 id）。"""
    return bool(LOCAL_ID_RE.match(segment) or _UPSTREAM_ID_RE.match(segment))


def path_task_id_candidates(path: str, limit: int = 2) -> list[str]:
    """从路径里由后向前挑出"像 id"的段（最多 ``limit`` 个）。

    ``/v2/query/video_generation/2090071565996011520`` → ``["2090071565996011520"]``；
    ``/v2/models`` → ``[]``（不产生任何查库）。
    """
    out: list[str] = []
    for segment in reversed(normalize(path).split("/")):
        if segment and looks_like_task_id(segment):
            out.append(segment)
            if len(out) >= limit:
                break
    return out


# ---------------------------------------------------------------------------
# 报文塑形（按渠道配置的提取路径反向构建）
# ---------------------------------------------------------------------------


def set_path(root: dict, dotted: str, value: Any) -> dict:
    """按点分路径写入嵌套值：``task.status`` → ``{"task": {"status": ...}}``。

    只构建字典层级（数组下标形态的提取路径极少出现在提交/状态字段上，遇到时
    数字段按字符串键处理）。``dotted`` 为空则原样返回。
    """
    if not dotted:
        return root
    parts = dotted.split(".")
    cur: dict = root
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value
    return root


def _satisfy_envelope(route: RouteConfig, body: dict) -> dict:
    """渠道配了 ``ok_check`` 信封时，网关自造的响应也要满足同一信封形状
    （客户端很可能按 ``code == 0`` 判成功）。"""
    check = route.ok_check
    if not check:
        return body
    path = str(check.get("path") or "")
    if path:
        set_path(body, path, check.get("equals"))
    message_path = str(check.get("message_path") or "")
    if message_path:
        set_path(body, message_path, "ok")
    return body


def submit_body(route: RouteConfig, task_id: str) -> dict:
    """原生提交响应（同构）：``route.task_id_path`` 位置放**本地** task_id。"""
    body = set_path({}, route.task_id_path or "task_id", task_id)
    return _satisfy_envelope(route, body)


def capture_snapshot(raw: dict | None) -> dict | None:
    """终态上游报文的落库快照（供原生查询逐字段同构回放）。

    只收**小体积 JSON 对象**：``SNAPSHOT_MAX_BYTES`` 以内（探测报文正常
    <2KB，防 tasks.data 列膨胀）；空报文（本地取消/判死）→ None（不落键）。
    """
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        size = len(json.dumps(raw, ensure_ascii=False).encode())
    except (TypeError, ValueError):
        return None
    return raw if size <= SNAPSHOT_MAX_BYTES else None


def replay_snapshot(route: RouteConfig, task: dict) -> dict | None:
    """终态时落库的**上游原始终态报文**（``data.upstream_snapshot``）回放：
    把上游任务 id 换成本地 id、把上游产物直链换成改写后的直链，其余原样
    返回——100% 逐字段同构（usage、trace_id 等网关不认识的字段全都在），
    且零上游往返。

    未落快照（旧任务 / 报文超限 / 本地判死无上游报文）→ None（调用方回退
    ``snapshot_body`` 按配置反向构建）。
    """
    from app.services import resulturl   # 局部导入：resulturl 不依赖本模块，防环

    data = task.get("data") or {}
    snapshot = data.get("upstream_snapshot")
    if not isinstance(snapshot, dict) or not snapshot:
        return None
    upstream_task_id = str(data.get("upstream_task_id") or "")
    url_pairs = resulturl.pairs(route, data.get("upstream_result"), task["task_id"])
    if not upstream_task_id and not url_pairs:
        return snapshot
    try:
        raw = rewrite_ids(json.dumps(snapshot, ensure_ascii=False).encode(),
                          upstream_task_id, task["task_id"])
        replayed = json.loads(resulturl.rewrite_bytes(raw, url_pairs))
    except (TypeError, ValueError):
        return snapshot
    return replayed if isinstance(replayed, dict) else snapshot


def snapshot_body(route: RouteConfig, task: dict) -> dict:
    """本地快照的原生查询响应。

    终态且落过上游原始报文 → 直接回放（逐字段同构，见
    :func:`replay_snapshot`）；否则按渠道配置的 ``status_path`` /
    ``result_path`` / ``error_path`` 反向构建，id 字段路径取
    ``probe_task_id_path``（探测报文形态，如 ``task.id``），未配则回退
    ``task_id_path``。

    用于三种"无上游可问"的场景：上游还没接单（异步提交在飞 / HELD 挂起）、
    本地已终态（本地即权威，不必再问上游）、上游探测不可达。
    """
    data = task.get("data") or {}
    if str(task.get("status") or "") in TERMINAL:
        replayed = replay_snapshot(route, task)
        if replayed is not None:
            return replayed
    body = set_path({}, route.status_path or "status", status_word(route, task))
    set_path(body, route.probe_task_id_path or route.task_id_path or "task_id",
             task["task_id"])
    if route.result_path and data.get("result"):
        set_path(body, route.result_path, data["result"])
    if route.error_path and task.get("fail_reason"):
        set_path(body, route.error_path, str(task["fail_reason"]))
    return _satisfy_envelope(route, body)


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
    "PLACEHOLDER",
    "SNAPSHOT_MAX_BYTES",
    "cancel_id",
    "capture_snapshot",
    "is_local_id",
    "looks_like_task_id",
    "match_path",
    "match_submit",
    "normalize",
    "path_task_id_candidates",
    "probe_id",
    "replay_snapshot",
    "rewrite_ids",
    "set_path",
    "snapshot_body",
    "status_word",
    "submit_body",
    "swap_id",
    "template_id",
]
