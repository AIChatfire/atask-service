"""攒批：「攒够 N 条或等够 T 秒 → 整批放行上游提交」。

## 两个触发器，一个放行点

    受理 ──入批(LUA_BATCH_JOIN)──┬─ 成员数 ≥ N ──► queue.publish_batch_release()  ← N 触发
                                 └─ 否则等待
    T 触发 ── taskiq 延迟任务到期 ─────────────────► push 到 queue.batch_release_task
    兜底   ── sweep 扫超期未放行 ─────────────────► 逐条 release_batched_task

三条路径都只调 :func:`release`（N/T）或 ``relayflow.release_batched_task``（兜底单条）。
:func:`release` 内部**原子摘取**整批成员（``LUA_BATCH_CLAIM``：DEL 成员键 + ZREM 到期键
在同一次 EVAL 内），所以 N 触发与 T 触发并发时只有一方拿得到成员列表——**不需要
额外的放行锁**。

但这一层只保证**批次级**互斥：救不了「同一成员被两条路径各捞到一次」。摘到成员后
逐条走 ``relayflow.release_batched_task``，那里有 DB 级的条件更新
（``taskstore.claim_for_release``）兜住重复。两层（批次级 Redis 互斥 + 成员级 DB 幂等）
**都不能省**：前者防重复摘批，后者防重复下发上游。

## 事实源在 DB，Redis 只是可重建索引

Redis 掉一整个批次索引 = 成员在 DB 里仍是 ``batch_state='waiting'``、
``batch_due_at`` 还在，sweep 的「超期未放行」兜底按它捞回来。这是「Redis 只放可重建
索引」原则的直接应用——所以入批时 **deadline 必须落库**，不落库就没法重建 T 时刻。

## 按什么维度分批（可配置）

批次键 = **归组键**（``K_BATCH.format(key=...)``），由 :func:`group_key` 算出，
优先级 ``X-Batch-Key`` > ``BATCH_GROUP_BY`` 配置：

- ``model``（默认）：按归一化模型名，跨 token 合并。批次更大、N 更容易触发；
- ``token_model``：按 ``token_hash + model``，与并发维度对齐——「同一批放行的任务
  竞争同一个并发窗口」，凑批填满窗口才有意义。

两者都不改变「放行时各自占各自 token 的槽」这一事实，区别只在**谁和谁算同一批**。
跨模型混批默认不会发生（模型名是键的一部分），但客户端可用 ``X-Batch-Key`` 显式
指定来强制混批（显式优先）。

## 与 stask-service 的同构关系

本模块是 ``stask-service`` 的 ``app/services/batching.py`` 的移植，但**放行的内容不同**：
stask 的「放行」= 交出执行（同步上游调用）；本仓库的「放行」= 投递**上游提交**
（``queue_submit_task`` → ``relayflow.submit_queue_task``）。两者都不做「把 N 条合并成
一次上游请求」——上游是 new-api 约定式异步接口，没有批量端点。
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.config import settings
from app.logging import log
from app.redis import (
    K_BATCH,
    K_BATCH_DUE,
    LUA_BATCH_CLAIM,
    LUA_BATCH_JOIN,
    LUA_BATCH_LEAVE,
    r,
)

#: 批次键的 TTL 余量：到期时刻之后再留这么久，防「延迟任务卡顿一轮」时批次键先
#: 过期、成员索引凭空消失（DB 兜底能修，但要等一个 sweep 周期）。
_TTL_MARGIN = 3600

#: 单批上限（与 ``batch_size`` 的取值域一致；dynconf 白名单里也写死同一组数字）。
MAX_BATCH = 1000

#: ``X-Batch-Key`` 的长度上限。超长**截断不报错**，与 ``Idempotency-Key`` 的既有
#: 处理一致——一个只在长度上超界的键不值得让整个请求失败。
MAX_KEY_LEN = 64


def _as_text(member: Any) -> str:
    """Redis 成员归一为 str：真 redis 返回 bytes，FakeRedis 返回 str。"""
    return member.decode() if isinstance(member, bytes) else str(member)


def _now() -> int:
    return int(time.time())


# ---------------------------------------------------------------------------
# 客户端分批头（X-Batch-Size / X-Batch-Wait / X-Batch-Key）
# ---------------------------------------------------------------------------

HEADER_SIZE = "x-batch-size"
HEADER_WAIT = "x-batch-wait"
HEADER_KEY = "x-batch-key"

#: 归组键允许的字符集。归组键会**直接拼进 Redis 键名**（``atask:batch:{key}``），所以
#: 必须过一遍白名单：客户端可控的字符串不经约束地进键名会带来键空间污染（比如注入
#: ``:`` 故意与其他键碰撞）与不可读的键。非法字符替换为 ``_`` 而不是报错——归组键
#: 只是「谁和谁一批」的标记，语义不受个别字符影响，报错反而让客户端难以自查。
_KEY_SAFE = re.compile(r"[^A-Za-z0-9._:-]")


class BatchParamError(Exception):
    """分批头非法 → 400。

    带上 ``code`` / ``param`` 是为了让错误响应直接指出**是哪个头、错在哪**：这三个头
    都是可选的，客户端写坏一个而不自知时，只说「400 bad request」等于没说。
    """

    def __init__(self, message: str, code: str, param: str = "") -> None:
        super().__init__(message)
        self.status = 400
        self.message = message
        self.code = code
        self.param = param


@dataclass(frozen=True, slots=True)
class Overrides:
    """客户端在本次请求里声明的分批意图。``None`` = 未声明（交服务端配置）。"""

    size: int | None = None
    wait: int | None = None
    key: str | None = None


@dataclass(frozen=True, slots=True)
class Plan:
    """本次提交的攒批决策——**生效值**（客户端头叠加到服务端配置之后的结果）。

    ``size`` / ``wait`` 落库的意义就在于此：策略是热改的，在途任务要按创建时那一套
    走完生命周期；落「配置原值」而不是「生效值」会让排障时看到的参数与实际行为不符。
    """

    enabled: bool
    size: int
    wait: int
    key: str


def _parse_positive_int(raw: str, *, code: str, header: str) -> int:
    text = raw.strip()
    try:
        value = int(text, 10)
    except ValueError as exc:
        raise BatchParamError(
            f"{header} must be a positive integer, got {raw!r}", code, header,
        ) from exc
    if value < 1:
        raise BatchParamError(
            f"{header} must be a positive integer, got {value}", code, header,
        )
    return value


def sanitize_key(raw: str) -> str:
    """归组键归一：去空白 → 截断 :data:`MAX_KEY_LEN` → 非法字符换 ``_``。"""
    return _KEY_SAFE.sub("_", raw.strip()[:MAX_KEY_LEN])


def parse_overrides(headers: Mapping[str, str], *, max_wait: int) -> Overrides:
    """解析三个分批头。非法值抛 :class:`BatchParamError`（调用方转 400）。

    ``X-Batch-Size`` / ``X-Batch-Wait`` 都必须是**正整数**（0 与负数一律拒）：它们的
    语义是「凑够 N 条」与「等 T 秒」，声明 0 条或等 0 秒在语义上就是矛盾的，静默当成
    「不设」只会掩盖客户端的 bug。
    """
    size_raw = headers.get(HEADER_SIZE)
    wait_raw = headers.get(HEADER_WAIT)
    key_raw = headers.get(HEADER_KEY)

    size: int | None = None
    if size_raw is not None:
        size = _parse_positive_int(size_raw, code="invalid_batch_size", header=HEADER_SIZE)
        if size > MAX_BATCH:
            raise BatchParamError(
                f"X-Batch-Size {size} exceeds the maximum {MAX_BATCH}",
                "invalid_batch_size", HEADER_SIZE,
            )

    wait: int | None = None
    if wait_raw is not None:
        wait = _parse_positive_int(wait_raw, code="invalid_batch_wait", header=HEADER_WAIT)
        if wait > max_wait:
            raise BatchParamError(
                f"X-Batch-Wait {wait}s exceeds max_batch_wait_seconds ({max_wait}s)",
                "batch_wait_too_long", HEADER_WAIT,
            )

    key: str | None = None
    if key_raw is not None:
        # 全是不安全字符 → 归一后为空。当成「未声明」而不是报错：空键等价于
        # 「不指定」，回落默认维度即可。
        key = sanitize_key(key_raw) or None

    return Overrides(size=size, wait=wait, key=key)


def resolve_plan(
    headers: Mapping[str, str],
    *,
    model: str,
    token_hash: str,
    enabled: bool,
    size: int,
    wait: int,
    max_wait: int,
) -> Plan:
    """算出**生效的**攒批决策（客户端逐字段显式优先）。

    语义：
    - 客户端给 N 不给 T → T 取服务端配置；服务端也没配 → 取 ``max_wait`` 兜底。
      没兜底会让 ``due_at = now``（整批立刻到期）= 攒批静默失效，且任务还占着等待态。
    - 客户端可以**用头开启服务端未配的攒批**（把 N 从 0 抬到 >=2）：这是「客户端主动
      要求攒批」的正当用法，总闸门 ``batch_enabled`` 仍是最终闸门。
    - 反方向（客户端把 N 调小到 1）只让自己的批次更快放行，允许。

    ``batch_enabled=False`` 时**立刻返回未启用**（且不解析任何头？——不，仍解析）：
    这是防御性的第二道闸。上层已经拦过一次（受理链路读同一个开关），但那是「调用方
    记得拦」；把闸门同时做进函数里，才不会出现「新加一个调用方忘了拦」就绕过总开关。
    头仍照常解析，是为了让非法头在**任何开关状态下**都得到一致的 400——否则「关掉攒批
    时坏头不报错、打开后才报错」会成为一类只在切换开关后暴露的客户端 bug。
    """
    over = parse_overrides(headers, max_wait=max(1, int(max_wait)))
    if not enabled:
        return Plan(enabled=False, size=0, wait=0, key="")

    eff_size = over.size if over.size is not None else max(0, int(size))
    if eff_size < 2:
        return Plan(enabled=False, size=eff_size, wait=0, key="")

    eff_wait = over.wait if over.wait is not None else int(wait)
    if eff_wait <= 0:
        # 服务端配了 N 却没配 T（或配成 0）：必须兜底，理由见 docstring。
        eff_wait = max(1, int(max_wait))
    # 配置可能被写成「batch_wait_seconds > max_batch_wait_seconds」（两者都是 env，
    # 没有跨字段校验）。客户端头已在上游被拒，这里补一道路径无关的上限钳制。
    eff_wait = min(eff_wait, max(1, int(max_wait)))

    key = group_key("" if not over.key else over.key, model=model,
                    token_hash=token_hash, group_by=settings.batch_group_by)
    return Plan(enabled=True, size=eff_size, wait=eff_wait, key=key)


def group_key(batch_key: str, *, model: str, token_hash: str, group_by: str) -> str:
    """算出归组键——**谁和谁算同一批**的唯一判据。

    优先级：``X-Batch-Key``（客户端显式指定）> ``batch_group_by`` 配置的维度。

    ``batch_group_by`` 见 ``Settings``：``model``（默认，跨 token 合并）或
    ``token_model``（与并发维度对齐）。未知取值回落 ``model``；不在这里抛——归组
    维度配错不该让整个提交链路挂掉（它是 env-only 的结构性开关，启动时看不到就是写错了）。

    显式键**也过一遍** ``sanitize_key``（幂等，调用方通常已归过一次）：归组键会直接
    拼进 Redis 键名，这个不变量该由本函数自己保证，而不是靠每个调用方记得先归一——
    直接以原始字符串调用本函数的路径（测试、运维脚本、将来的新入口）就没有那道保护。
    """
    if batch_key:
        return sanitize_key(batch_key)
    # 模型名在函数内**自己归一**，不假设调用方已经归过：归组键是「谁和谁同一批」的
    # 判据，两个调用方对同一模型传了不同大小写 = 同一批被劈成两批，N 永远凑不齐。
    model = model.strip().lower()
    if group_by == "token_model":
        # 用 token_hash 前缀而非全量：它只是「同一批必须是同一个 token」的分隔符，
        # 前缀足够区分，也让键短一些便于人工排查。**不是凭证**——token_hash 是本地
        # 身份替身（sha256），明文 token 从不进 Redis 键名。
        return sanitize_key(f"{token_hash[:12]}:{model}")
    return sanitize_key(model)


#: 对外暴露的 ``batch_state`` 取值集。
#: 内部还有 ``releasing`` / ``requeued``：``releasing`` 是放行过程中的瞬态，对客户端
#: 而言「已经被放行」比「还在等」更接近事实；``requeued`` **也不能漏出去**——直接
#: 漏出去会让「靠 batch_state 判断是否在排队」的客户端把「占不到并发槽、正在退避重排」
#: 误判成在批次里等 N/T，从而去等一个永远不会到达的批次事件。
def public_state(state: str) -> str:
    """内部 ``batch_state`` → 可对外暴露的值（空串 = 不在批次里）。"""
    if state == "waiting":
        return "waiting"
    if state in ("releasing", "released", "requeued"):
        return "released"
    return ""


def _keys(key: str) -> tuple[str, str]:
    """成员 ZSET 与到期 ZSET（顺序即 Lua 的 KEYS 顺序，不可换）。"""
    return K_BATCH.format(key=key), K_BATCH_DUE


# ---------------------------------------------------------------------------
# 入批 / 退批 / 摘取
# ---------------------------------------------------------------------------


async def join(
    task_id: str, key: str, *, wait_seconds: int, now: int | None = None
) -> tuple[int, int, bool]:
    """入批。返回 ``(入批后成员数, 本批权威到期时刻, 本次是否写定了 deadline)``。

    到期时刻由**首个成员**用 ``ZADD NX`` 写定（见 ``LUA_BATCH_JOIN``）：T 是「自本批
    开始攒起」的窗口，若后续成员都刷新 deadline，涓涓细流会让批次永远等不到放行。

    返回值第二项是调用方**落库**用的（``data.batch_due_at``）——Redis 索引丢失后
    sweep 的兜底靠它判定「该放行了」。所以这里返回的必须是 **Redis 里真实生效的那个
    值**（脚本回读 ZSCORE），不是本调用者算出的 ``due_at``：NX 命中时后者偏晚，落库后
    一旦重建，整批放行时刻就会集体后移，T 语义失真。

    第三项 ``wrote_deadline`` 只给 T 触发的排程用：**只有真正写定 deadline 的调用者**
    才去排一个到期放行任务。若每个成员都排一次，一批 N 条就排 N 个延迟任务（N-1 个
    纯空转），而 taskiq 的调度源每轮都要读全量待派发任务——这是会被放大的浪费。
    """
    ts = _now() if now is None else now
    window = max(1, int(wait_seconds))
    due_at = ts + window
    members, due_key = _keys(key)
    raw = await r.eval(
        LUA_BATCH_JOIN, 2, members, due_key,
        task_id, str(ts), str(due_at), key, str(window + _TTL_MARGIN),
    )
    # Lua 返回 {成员数, 是否写定 deadline, 权威到期时刻}，**末项可能被 nil 截断**
    # （见 ``LUA_BATCH_JOIN`` 的注释：返回顺序就是为截断设计的）。补齐到 3 项再解包。
    parts: list[Any] = [*list(raw), None, None, None] if raw else [0, 0, None]
    count_raw, added_raw, score = parts[:3]
    count = int(count_raw or 0)
    # ZSCORE 返回字符串（可能是 '1789...' 或 '1.789e+09'），float 中转最稳。取不到
    # （理论不可能：ZADD 与 ZSCORE 在同一次 EVAL 内，键不可能被清）时回落本地值——
    # 宁可偏晚，也**不能返回 0**：0 会让兜底把整批判成「立刻该放行」而提前放行。
    real_due = int(float(score)) if score is not None else due_at
    wrote = bool(int(added_raw or 0))
    log.info("batch join: task_id={} key={} count={}/{} due_at={}",
             task_id, key, count, window, real_due)
    return count, real_due, wrote


async def leave(task_id: str, key: str) -> None:
    """成员退批（取消时用）。

    被取消的成员必须从计数里摘掉：一批声明 N=100 而其中 5 条被取消，计数就永远差
    5 条到不了 N，只能干等 T 兜底，等待时长凭空变长。

    失败只告警：残留成员被放行时 ``release_batched_task`` 会因状态已终态而 SKIPPED，
    不会重复下发上游。
    """
    members, due_key = _keys(key)
    try:
        await r.eval(LUA_BATCH_LEAVE, 2, members, due_key, task_id, key)
    except Exception:
        log.opt(exception=True).warning(
            "batch leave failed: task_id={} key={}", task_id, key)


async def claim(key: str) -> list[str]:
    """原子摘取整批成员。空列表 = 本批已被别人取走（或本来就是空批）。"""
    members, due_key = _keys(key)
    raw = await r.eval(LUA_BATCH_CLAIM, 2, members, due_key, key)
    return [_as_text(m) for m in (raw or [])]


# ---------------------------------------------------------------------------
# 放行（唯一入口）与退避重排
# ---------------------------------------------------------------------------


async def release(key: str, *, source: str = "batch") -> dict[str, int]:
    """放行一个归组键的整批任务。**这是攒批的唯一放行入口**。

    占不到并发槽的成员挂回重排通道（:func:`requeue`）而不是丢弃或失败——客户端要的是
    「帮我排队」，回 429 等于把重试逻辑推回给客户端，而此刻请求早已返回 202。

    有界并发（``BATCH_RELEASE_CONCURRENCY``）：一批若全并发放行，N 次 DB 条件更新 +
    N 次 Redis 往返会把连接池打满，反而拖慢正常提交。
    """
    from app.services import relayflow  # 延迟 import 破循环依赖

    task_ids = await claim(key)
    if not task_ids:
        return {"claimed": 0, "released": 0, "requeued": 0, "skipped": 0}

    gate = asyncio.Semaphore(max(1, int(settings.batch_release_concurrency)))
    tally = {"claimed": len(task_ids), "released": 0, "requeued": 0, "skipped": 0}

    async def one(task_id: str) -> None:
        async with gate:
            try:
                outcome = await relayflow.release_batched_task(task_id, source=source)
            except Exception:
                # 单条放行炸掉绝不能带走整批：剩下的成员已被 claim 摘出 Redis，只有
                # DB 里的等待态能救它们（sweep 的超期兜底）。**不上抛**——上抛会让
                # asyncio.gather 提前结束并丢失其余成员的处理结果。
                log.opt(exception=True).error(
                    "batch release: member failed: task_id={}", task_id)
                tally["requeued"] += 1
                return
            tally[outcome] = tally.get(outcome, 0) + 1

    await asyncio.gather(*(one(t) for t in task_ids))
    log.info("batch released: key={} source={} {}", key, source, tally)
    return tally


async def requeue(task_id: str, key: str = "", *, attempts: int) -> int:
    """放行时占不到并发槽 → 指数退避 + ±10% 抖动重排。返回下次可放行时刻。

    **抖动是必需的，不是锦上添花**：一批 200 条同时占不到槽，若都按固定延迟重排，
    下一轮又会同时涌向同一个满的闸门——整批惊群会一直重复，直到某轮恰好有槽空出来。
    抖动把它们摊开到一个窗口里。

    退避次数落库（``data.requeue_attempts``）而不是放 Redis：Redis 掉数据后次数归零 =
    退避重新从 1 秒起步，等于失去退避效果。

    ``batch_due_at`` 同时被改写为新的重试时刻：它是「本任务的**下一次**可放行时刻」，
    sweep 的超期兜底只需这一个谓词就能同时覆盖「批次到期」与「退避到点」。
    """
    from app import queue
    from app.services import taskstore

    attempt = max(1, int(attempts) + 1)
    ceiling = max(10, int(settings.batch_backoff_max_seconds))
    delay = min(ceiling, 2 ** min(attempt, 12))
    delay = max(1, int(delay * (1.0 + random.uniform(-0.1, 0.1))))
    due_at = _now() + delay
    await taskstore.patch_data(task_id, {
        "batch_state": "requeued",
        "requeue_attempts": attempt,
        "requeue_due_at": due_at,
        "batch_due_at": due_at,
    })
    await queue.publish_task_release(task_id, due_at)
    log.info("batch requeue: task_id={} key={} attempts={} delay={}s",
             task_id, key, attempt, delay)
    return due_at


# ---------------------------------------------------------------------------
# 观测（/ops/batches）
# ---------------------------------------------------------------------------


async def stats() -> dict[str, Any]:
    """攒批概览：每个归组键攒了多少条、还有多久到期。

    字段名是 ``key`` 而不是 ``model``：归组键可能是 ``token:model`` 或客户端自定义串
    （``X-Batch-Key``），叫 model 会误导排障的人。**归组键可能含 token 指纹**，所以该
    端点归管理面（``/ops/*``，需 ``X-Admin-Token``），不放用户面。
    """
    keys = await r.zrangebyscore(K_BATCH_DUE, "-inf", "+inf")
    now = _now()
    out: list[dict[str, Any]] = []
    for raw in keys or []:
        key = _as_text(raw)
        score = await r.zscore(K_BATCH_DUE, key)
        out.append({
            "key": key,
            "waiting": int(await r.zcard(K_BATCH.format(key=key)) or 0),
            "due_in": (int(float(score)) - now) if score is not None else None,
        })
    out.sort(key=lambda row: row["due_in"] if row["due_in"] is not None else 0)
    return {"batches": out}
