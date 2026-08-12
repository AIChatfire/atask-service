"""Redis 延迟队列通用原语（零自有表改造：dlv/obx 两个命名空间共用）。

数据结构（``ns`` 为命名空间前缀：``dlv`` 用户回调投递 / ``obx`` 计费 outbox）：

- ``{ns}:due``   ZSET，member=item_id，score=下次可领取时间（unix 秒）；
- ``{ns}:{id}``  HASH，条目事实源：``payload``（JSON 文本）/``state``
  （pending|delivering|dead）/``attempts``/``lease_until``/``dead_reason``
  + 各领域自有字段（dlv: task_id/user_id/url/event_type；obx: task_id/op/last_error）；
- ``{ns}:lease`` ZSET，member=item_id，score=租约到期时间——领取即写入，
  完成/重排/死信时摘除；副本死亡时由 ``reclaim_expired_leases`` 回收回 due；
- ``{ns}:dead``  ZSET，member=item_id，score=进死信时间（人工排查/重放清单）。

原子性：领取 / 租约回收 / 死信重放三段多步操作走 Lua（EVAL），多副本并发
互斥语义与原来 MySQL ``SELECT ... FOR UPDATE SKIP LOCKED`` 等价；单步操作
（done 删除、重排、死信落账）用普通命令——条目已由 lease 互斥归属本副本。

**持久化纪律**：该队列承接资金链路（outbox）与至少一次投递（dlv），Redis
必须开 AOF everysec（compose 已配）；丢失窗口 ≤1s，
对账任务（漏结算重入队）与轮询兜底通道负责收敛残差。
"""

from __future__ import annotations

import time
from typing import Any

# 领取：取到期项 → 置 delivering + lease_until → 移入 lease ZSET（原子）。
# KEYS: 1=due 2=lease；ARGV: 1=now 2=limit 3=lease_seconds 4=hash_prefix
LUA_CLAIM = """
-- gwq:claim
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local claimed = {}
for _, id in ipairs(ids) do
  if redis.call('ZREM', KEYS[1], id) == 1 then
    local deadline = tonumber(ARGV[1]) + tonumber(ARGV[3])
    redis.call('HSET', ARGV[4] .. id, 'state', 'delivering', 'lease_until', deadline)
    redis.call('ZADD', KEYS[2], deadline, id)
    table.insert(claimed, id)
  end
end
return claimed
""".strip()

# 租约回收：lease 过期项回 due（state 归 pending，清 lease_until；原子）。
# KEYS: 1=lease 2=due；ARGV: 1=now 2=limit 3=hash_prefix
LUA_RECLAIM = """
-- gwq:reclaim
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local reclaimed = {}
for _, id in ipairs(ids) do
  if redis.call('ZREM', KEYS[1], id) == 1 then
    redis.call('HSET', ARGV[3] .. id, 'state', 'pending', 'lease_until', '')
    redis.call('ZADD', KEYS[2], tonumber(ARGV[1]), id)
    table.insert(reclaimed, id)
  end
end
return reclaimed
""".strip()

# 死信重放：仅 state=='dead' 可重放（重置 attempts/dead_reason，dead→due；原子）。
# KEYS: 1=dead 2=due；ARGV: 1=id 2=now 3=hash_prefix
LUA_REPLAY = """
-- gwq:replay
local h = ARGV[3] .. ARGV[1]
if redis.call('HGET', h, 'state') ~= 'dead' then
  return 0
end
redis.call('HSET', h, 'state', 'pending', 'attempts', 0, 'dead_reason', '', 'last_error', '')
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 1
""".strip()


def _now() -> float:
    return time.time()


async def enqueue(
    redis: Any,
    ns: str,
    item_id: str,
    fields: dict[str, Any],
    *,
    delay_seconds: float = 0,
) -> None:
    """入队：HASH 落条目事实源 + ZADD due（score=now+delay）。

    ``fields`` 至少含领域字段与 ``payload``；state/attempts 由本函数初始化
    （调用方不得传入——重放/重排的状态推进归 queue 层管）。
    """
    data = {k: str(v) for k, v in fields.items()}
    data.setdefault("state", "pending")
    data.setdefault("attempts", "0")
    await redis.hset(f"{ns}:{item_id}", mapping=data)
    await redis.zadd(f"{ns}:due", {item_id: _now() + delay_seconds})


async def claim(
    redis: Any,
    ns: str,
    *,
    limit: int,
    lease_seconds: float,
) -> list[str]:
    """原子领取到期条目（Lua）：due → delivering + lease ZSET。"""
    return await redis.eval(  # type: ignore[misc]
        LUA_CLAIM, 2, f"{ns}:due", f"{ns}:lease",
        _now(), limit, lease_seconds, f"{ns}:",
    )


async def reclaim_expired_leases(
    redis: Any, ns: str, *, limit: int = 100
) -> list[str]:
    """调度器回收过期 lease 回 due（副本死亡兜底；Lua 原子）。"""
    return await redis.eval(  # type: ignore[misc]
        LUA_RECLAIM, 2, f"{ns}:lease", f"{ns}:due", _now(), limit, f"{ns}:",
    )


async def get_item(redis: Any, ns: str, item_id: str) -> dict[str, str] | None:
    """读取条目 HASH；不存在返回 None。"""
    data = await redis.hgetall(f"{ns}:{item_id}")
    return data or None


async def mark_done(redis: Any, ns: str, item_id: str) -> None:
    """成功收口：摘除 lease/due 占位 + 删除条目 HASH。"""
    await redis.zrem(f"{ns}:lease", item_id)
    await redis.zrem(f"{ns}:due", item_id)
    await redis.delete(f"{ns}:{item_id}")


async def reschedule(
    redis: Any,
    ns: str,
    item_id: str,
    *,
    delay_seconds: float,
    attempts: int | None = None,
    fields: dict[str, Any] | None = None,
) -> None:
    """失败重排：摘 lease 占位 → 更新 HASH → 回 due（score=now+delay）。

    ``attempts=None`` 表示不推进计数（reevaluate 挂起 / 域级熔断重排语义）。
    """
    update: dict[str, Any] = {"state": "pending", "lease_until": ""}
    if attempts is not None:
        update["attempts"] = attempts
    if fields:
        update.update(fields)
    await redis.zrem(f"{ns}:lease", item_id)
    await redis.hset(
        f"{ns}:{item_id}", mapping={k: str(v) for k, v in update.items()}
    )
    await redis.zadd(f"{ns}:due", {item_id: _now() + delay_seconds})


async def dead_letter(
    redis: Any,
    ns: str,
    item_id: str,
    *,
    reason: str,
    attempts: int | None = None,
    fields: dict[str, Any] | None = None,
) -> None:
    """死信：摘 lease/due 占位 → state=dead + dead_reason → 落 ``{ns}:dead`` ZSET。"""
    update: dict[str, Any] = {"state": "dead", "lease_until": "", "dead_reason": reason}
    if attempts is not None:
        update["attempts"] = attempts
    if fields:
        update.update(fields)
    await redis.zrem(f"{ns}:lease", item_id)
    await redis.zrem(f"{ns}:due", item_id)
    await redis.hset(
        f"{ns}:{item_id}", mapping={k: str(v) for k, v in update.items()}
    )
    await redis.zadd(f"{ns}:dead", {item_id: _now()})


async def replay_dead(redis: Any, ns: str, item_id: str) -> bool:
    """死信人工重放（Lua：仅 state=='dead' 命中，防并发竞态）；返回是否命中。"""
    res = await redis.eval(  # type: ignore[misc]
        LUA_REPLAY, 2, f"{ns}:dead", f"{ns}:due", item_id, _now(), f"{ns}:",
    )
    return bool(res)


__all__ = [
    "LUA_CLAIM",
    "LUA_RECLAIM",
    "LUA_REPLAY",
    "claim",
    "dead_letter",
    "enqueue",
    "get_item",
    "mark_done",
    "reclaim_expired_leases",
    "replay_dead",
    "reschedule",
]
