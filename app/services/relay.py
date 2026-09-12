"""约定式上游交互（ADR-010）：提取纯函数 + 单出入口站。

ADR-010 把渠道路由配置整体放弃（``task_id_path`` / ``probe_path`` /
``auth_type`` / 渠道级 ``timeout_sec`` 等），改为**按 new-api 原生约定硬编码**：

- 提交响应里取 ``id``，缺失回退 ``task_id``（:func:`extract_upstream_task_id`）；
- 状态字段固定 ``status``（:func:`upstream_status`）；
- 鉴权固定 ``Authorization: Bearer <用户 token>``（不再有 x-api-key / none）；
- 超时降级为全局 ``RELAY_TIMEOUT_SECONDS``（不再有渠道级 ``timeout_sec``）。

## 为什么复用共享客户端与熔断，而不是新建 httpx client

每次请求新建 ``AsyncClient`` 都要重付一轮 TCP + TLS 握手（无 keep-alive），
中继链路是数据面热路径，握手开销会直接叠加到提交延迟；且自建 client 会绕过
``app.services.upstream`` 的 Redis 熔断计数，上游整体故障时网关仍会持续打靶。
故出站统一走 ``app.services.httpc.shared_client``（进程级连接池）并复用
``breaker_guard`` / ``breaker_report``。熔断键取上游 host（中继链路没有 biz）。

## 599 语义（照 ``app/services/upstream.py::_require_base_url`` 的理由）

``base_url`` 为空时 httpx 会拿相对路径发请求，报出与业务无关的传输层错误
（*Target host is not specified*）。这类失败是配置/基础设施问题，**不是任务
失败**，必须归到模糊类（599）走重试而非判死，所以这里显式先行拦下。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import httpx

from app.config import settings
from app.logging import log
from app.services import httpc, upstream
from app.services.upstream_addr import assert_upstream_allowed


class RelayError(Exception):
    """中继出站失败。``status`` 口径（本模块自定，见 ``call_upstream``）：

    - 4xx：上游**确定性拒绝**（重试无意义）→ 调用方判 FAILURE；
    - 5xx 与 599：**模糊失败**（基础设施/上游故障，或我方 base_url 缺失哨兵），
      上游可能已接单 → 调用方留活重试，绝不判死；
    - 传输错误（超时/连接失败）同样按 599 归入模糊失败。

    ``body`` 保留上游响应原文（仅用于日志与失败文案，绝不整段外发）。
    """

    def __init__(self, status: int, body: str = ""):
        super().__init__(f"relay upstream {status}: {body[:200]}")
        self.status = status
        self.body = body


def _scalar_id(value: object) -> str | None:
    """标量任务 id 归一：字符串/整数可用，空串、布尔、容器一律 None。"""
    if isinstance(value, bool):
        return None                       # bool 是 int 子类，必须先挡
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def extract_upstream_task_id(payload: object) -> str | None:
    """从提交/探测响应里取上游任务 id（ADR-010 约定：``id`` 优先，回退 ``task_id``）。

    纯函数（无 I/O，便于单测）。``id`` 缺失或显式为 ``null`` 时回退 ``task_id``；
    两者都不是标量 → ``None``。
    """
    if not isinstance(payload, dict):
        return None
    value: object = payload.get("id")
    if value is None:
        value = payload.get("task_id")
    return _scalar_id(value)


def upstream_status(payload: object) -> str | None:
    """从探测响应里取上游状态词（ADR-010 约定：固定 ``status`` 字段）。

    保留上游原话（去首尾空白），不做任何映射——映射由
    ``app.services.statusmap.map_status`` 在调用侧完成。
    """
    if not isinstance(payload, dict):
        return None
    value = payload.get("status")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _join_url(base_url: str, path: str, query: str = "") -> str:
    """拼出站 URL：``base_url.rstrip('/') + '/' + path.lstrip('/')``，query 原样附带。"""
    url = base_url.rstrip("/") + "/" + path.lstrip("/")
    return f"{url}?{query}" if query else url


def _breaker_key(base_url: str) -> str:
    """熔断计数键：取上游 host:port（中继链路没有 biz，用寻址目标做分组）。"""
    return urlsplit(base_url).netloc or base_url or "unknown"


async def call_upstream(
    method: str,
    base_url: str,
    path: str,
    *,
    token: str,
    query: str = "",
    body: bytes | None = None,
    content_type: str = "",
    timeout: float | None = None,  # noqa: ASYNC109 —— 契约要求的每调用超时覆盖，非自建超时循环
) -> tuple[int, bytes, str]:
    """按 new-api 约定出站一次，返回 ``(status_code, raw_body, content_type)``。

    出站口径（唯一事实源见 ADR-010 追加决策表）：

    - 鉴权固定 ``Authorization: Bearer <token>``，用户 token 原样透传；
    - method / query / body 原样转发；
    - 超时用全局 ``RELAY_TIMEOUT_SECONDS``（``timeout`` 显式传入时覆盖）；
    - 出站前 ``assert_upstream_allowed``（安全三防线，见 upstream_addr），
      并把空基址按 599 语义拦下。

    ``content_type`` 取上游响应的 ``Content-Type``，缺失时回退
    ``application/json``——免费转发可能是任意媒体类型（图片/二进制产物），
    透传它才能保真，绝不硬写 JSON。

    失败：传输层错误（含超时/连接失败）→ :class:`RelayError` 599；上游 4xx/5xx
    **不抛**，原样返回给调用方按状态分流（提交路由据此判确定性拒绝 vs 模糊失败）。
    """
    if not base_url:
        log.error("relay base url missing, cannot {} {}", method, path)
        raise RelayError(599, "upstream base_url missing (infrastructure, not task failure)")
    assert_upstream_allowed(base_url)

    url = _join_url(base_url, path, query)
    headers: dict[str, str] = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["content-type"] = content_type

    breaker = _breaker_key(base_url)
    await upstream.breaker_guard(breaker)

    timeout_value = float(timeout if timeout is not None else settings.relay_timeout_seconds)
    # 共享连接池按 (timeout) 缓存：同参数复用 keep-alive，免逐请求握手。
    client = httpc.shared_client(timeout=timeout_value)
    try:
        resp = await client.request(method, url, headers=headers, content=body)
    except httpx.HTTPError as exc:
        await upstream.breaker_report(breaker, ok=False)
        raise RelayError(599, str(exc)) from exc

    await upstream.breaker_report(breaker, ok=resp.status_code < 500)
    return (resp.status_code, resp.content,
            resp.headers.get("content-type") or "application/json")


def _declared_length(value: str | None) -> int | None:
    """解析上游声明的 ``Content-Length``：缺失 / 非法 / 负数一律视为「未声明」。"""
    if value is None:
        return None
    try:
        length = int(value)
    except ValueError:
        return None
    return length if length >= 0 else None


async def _closing_iter(response: httpx.Response) -> AsyncIterator[bytes]:
    """把 httpx 流式响应转成可迭代字节，并在迭代结束（含异常/取消）时关闭连接。

    关闭必须挂在迭代器自身的 ``finally``：调用方拿到的是 ``StreamingResponse``
    持有的迭代器，只有迭代才会走到这里；若不关闭，客户端提前断开时连接池会
    泄漏被占用的连接。
    """
    try:
        async for chunk in response.aiter_bytes():
            yield chunk
    finally:
        await response.aclose()


async def stream_upstream(
    method: str,
    base_url: str,
    path: str,
    *,
    token: str,
    query: str = "",
    body: bytes | None = None,
    content_type: str = "",
    timeout: float | None = None,  # noqa: ASYNC109 —— 契约要求的每调用超时覆盖，非自建超时循环
) -> tuple[int, str, AsyncIterator[bytes]]:
    """流式出站一次，返回 ``(status_code, content_type, body_iterator)``。

    与 :func:`call_upstream` 是**同一套守卫**（空基址 599 / 白名单 fail-closed /
    熔断护栏与上报 / 共享连接池 / Bearer 透传 / method·query·body 原样），唯一
    区别是**不把响应体读进内存**：用 ``client.send(..., stream=True)`` 打开响应，
    逐块产出给调用方经 ``StreamingResponse`` 直接转发。免费 GET 透传（取图片/
    二进制产物）走这条；需要完整报文才能逐字节改写 id / 落快照的探测路径**必须**
    继续走 :func:`call_upstream`，不要改成流式。

    ## 声明长度上限：只在**开始流式之前**拒绝（不中途截断）

    上游**声明**了 ``Content-Length`` 且超过 ``UPSTREAM_RESPONSE_MAX_BYTES`` 时，
    不开始流式，直接按 ``502`` 拒绝（连接随即关闭，body 一个字节都不消费）。
    这是「快路径拒绝」：在拿到响应头、还没开始把产物读进来时就把超限挡掉。

    未声明长度（chunked）时照常流式转发，**不设中途截断**。取舍：流式本身已把
    内存占用限成常数（与产物大小无关），中途截断并不能进一步省内存，却会给
    客户端一个**没有错误信号的截断体**（HTTP 200 + 少了一半的字节），比不截断
    更糟——客户端无法区分「产物就长这样」与「被网关砍了」。要拦大产物只能靠
    上游诚实地声明长度；上游不声明就信任流式本身。
    """
    if not base_url:
        log.error("relay base url missing, cannot {} {}", method, path)
        raise RelayError(599, "upstream base_url missing (infrastructure, not task failure)")
    assert_upstream_allowed(base_url)

    url = _join_url(base_url, path, query)
    headers: dict[str, str] = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["content-type"] = content_type

    breaker = _breaker_key(base_url)
    await upstream.breaker_guard(breaker)

    timeout_value = float(timeout if timeout is not None else settings.relay_timeout_seconds)
    # 共享连接池按 (timeout) 缓存：同参数复用 keep-alive，免逐请求握手。
    client = httpc.shared_client(timeout=timeout_value)
    request = client.build_request(method, url, headers=headers, content=body)
    try:
        response = await client.send(request, stream=True)
    except httpx.HTTPError as exc:
        await upstream.breaker_report(breaker, ok=False)
        raise RelayError(599, str(exc)) from exc

    declared = _declared_length(response.headers.get("content-length"))
    if declared is not None and declared > settings.upstream_response_max_bytes:
        await response.aclose()                       # 不开始流式：body 一个字节都不读
        # 熔断按**上游健康度**记账，与本地的尺寸策略拒绝无关：上游这里是**正常
        # 响应**（很可能就是 200），只是体量超了我方上限。若记成 ok=False，客户端
        # 只要反复要一个大产物就能把该上游对**所有租户**熔断——自伤式、跨租户的
        # DoS，比不加上限更糟。故与正常路径同口径：按状态码判健康。
        await upstream.breaker_report(breaker, ok=response.status_code < 500)
        log.warning(
            "upstream response declared length over limit, rejected: declared={} limit={}",
            declared, settings.upstream_response_max_bytes,
        )
        raise RelayError(
            502,
            f"upstream response too large: declared {declared} bytes > "
            f"limit {settings.upstream_response_max_bytes}",
        )

    # 熔断只在此刻（收到响应头后）按 status 记一次：**流式 body 中途的传输错误
    # 不再补记**——响应已成功建立，body 阶段断流属于「转发过程」而非「上游不可达」，
    # 且此时状态码早已回给客户端、中途也拿不到可靠的上游健康信号。这是有意的边界，
    # 不是漏了补报（若要对 body 完整性计熔断，需另设计，当前不做）。
    await upstream.breaker_report(breaker, ok=response.status_code < 500)
    # 不透传上游的 Content-Length（让 StreamingResponse 用 chunked 输出）：上游声明的
    # 长度可能不准，透传会让下游按错误长度读取。代价是**下游拿不到这个响应头**，
    # 若客户端强依赖它（如进度计算），需在此显式回填。
    return (response.status_code,
            response.headers.get("content-type") or "application/json",
            _closing_iter(response))
