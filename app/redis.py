"""Redis 客户端与键规范。原则：Redis 只放"丢了能重建"的状态——
缓存、限流计数、幂等键（24h）、令牌会话、状态变更去重、批次成员索引。
任务队列由 taskiq 管理（app/queue.py）；事实源永远在 tasks 表。
"""

import redis.asyncio as aioredis

from app.config import settings

r = aioredis.from_url(settings.redis_url, decode_responses=True)

#: 全部 Redis 键的**唯一**前缀，也是键名的单一构造点（业务模块只 import 常量，
#: 不自己拼前缀）。
#:
#: 换前缀 = 换一整套键空间：改名后旧键不再被读写，等价于「Redis 侧状态从零开始」。
#: 队列本身（``atask:taskiq`` / ``atask:sched:*`` / ``atask:events:dlq``）也在其中，
#: 所以**换前缀必须先把队列排空**——幂等键与令牌会话的丢失只影响在飞窗口，而积压的
#: 待执行消息、延迟任务与死信一旦变成无人认领的孤儿键，就是真丢任务。
#:
#: 命名对齐 ``GATEWAY_PLATFORM='atask'``，与同族 stask-service 的
#: ``REDIS_KEY_PREFIX``（默认 ``st``）对称：两个服务共用同一 Redis 实例时，
#: 前缀是天然的第二道隔离。
KEY_PREFIX = "atask"


def _k(suffix: str) -> str:
    """``{KEY_PREFIX}:{suffix}``。"""
    return f"{KEY_PREFIX}:{suffix}"


# ---- 键规范 ----
K_IDEM = _k("idem:{token_hash}:{key}")        # 幂等键 -> task_id（24h）
K_RL = _k("rl:{subject}")                     # 滑动窗口限流（subject=token_hash 或 ip）
K_CONC = _k("conc:{token_hash}")              # 并发任务占用
K_BREAKER = _k("breaker:{host}")              # 上游熔断失败计数（分组键 = 目标 host；旧名叫 biz 是换向前的渠道分组，已无此概念）
K_QUEUE_SWEEP_LOCK = _k("queue_sweep_lock")   # /queue 收敛重入锁（慢轮防并发踩踏）
K_QSTATS = _k("queue_stats")                  # 队列观测快照缓存（JSON，短 TTL）
S_DLQ = _k("events:dlq")                      # 死信（taskiq 任务超限后落信）

# ---- 攒批（batching）键 ----
# 键名里的 ``{key}`` 是**归组键**（不是模型名）：谁和谁算同一批的唯一判据，
# 由 ``batching.group_key`` 算出，已过 ``sanitize_key`` 白名单（客户端可控字符串
# 会直接拼进键名，不过白名单会带来键空间污染）。
K_BATCH = _k("batch:{key}")                   # 批次成员索引（ZSET，member=task_id，score=入批时刻 → FIFO）
# 到期索引（ZSET，member=归组键，score=放行时刻）。**它只作可观测视图与可重建
# 索引，不是 T 触发源**：T 触发走 taskiq 延迟任务（app/queue.py），万一投递丢失
# 由 sweep 的「超期未放行」兜底按 DB 事实捞回来。留着它是因为 /ops/batches 要回答
# 「每个批次攒了多少条、还有多久到期」——那是 DB 说不清的事（DB 只有逐条的
# batch_due_at，没有「这一批现在几条」）。
K_BATCH_DUE = _k("batch:due")

# ---- 滑动窗口限流 ----
LUA_RATE_LIMIT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[2])
local n = redis.call('ZCARD', KEYS[1])
if n >= tonumber(ARGV[3]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1] .. ':' .. math.random(1e9))
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
"""

# ---- 并发占用（不超上限才 +1）----
# TTL 兜底（ARGV[2]）：进程在「占槽后、落库前」崩溃时槽位不再永久泄漏——
# 键整体过期后按 tasks 表事实重建。
LUA_CONC_ACQUIRE = """
local n = redis.call('INCR', KEYS[1])
if n > tonumber(ARGV[1]) then
  redis.call('DECR', KEYS[1])
  return 0
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
"""

LUA_CONC_RELEASE = """
local n = redis.call('DECR', KEYS[1])
if n < 0 then redis.call('SET', KEYS[1], 0) end
return 1
"""

# ---- CAS 删除（值匹配才删）：幂等占位释放专用，防误删并发回填的 task_id ----
LUA_CAS_DELETE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

# ---- 攒批 · 原子入批 ----
#
# KEYS = [成员 ZSET, 到期 ZSET]
# ARGV = [task_id, now, due_at, key, ttl]
# 返回 {入批后成员总数, 本次是否写定了 deadline, 本批权威到期时刻}
#
# 【返回顺序不是随意的】Lua 返回的表在**遇到 nil 处被截断**，所以可能为 nil 的
# ZSCORE 必须排在最后：排在中间会让「本批已有更早 deadline」（= ZADD NX 未命中）
# 这种再正常不过的情形把后面的元素一起吞掉，调用方于是既拿不到权威到期时刻、
# 又误判成「deadline 是我写的」而在每次都多排一个到期放行任务。
#
# 到期时刻用 **ZADD NX** 只由首个成员写定：T 是「自本批开始攒起」的窗口，后续成员
# 若都刷新 deadline，涓涓细流会让批次永远等不到放行。
#
# 入批与计数在同一次 EVAL 内完成，多副本并发提交时 ZCARD==N 不可能双触发
# （谁把计数推过阈值，谁就负责投递放行）。
#
# 第二个返回值（``added``）是给 T 触发的排程用的：**只有真正写定 deadline 的那个
# 调用者**才去排一个到期放行任务。若每个成员都排一次，一批 N 条就会排 N 个延迟
# 任务（N-1 个纯空转），而调度源每轮都要读全量待派发任务——那是会被放大的浪费。
#
# 必须把 **ZSCORE 的真实值**回给调用方，不能让它拿自己算的 due_at 落库：NX 命中
# （本批已有更早的 deadline）时两者不同，而 sweep 的超期兜底与重建正是按 DB 里的
# batch_due_at 判定——落了偏晚的值，整批放行时刻会集体后移，T 语义失真。
LUA_BATCH_JOIN = """
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
local added = redis.call('ZADD', KEYS[2], 'NX', ARGV[3], ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[5])
return {redis.call('ZCARD', KEYS[1]), added, redis.call('ZSCORE', KEYS[2], ARGV[4])}
"""

# ---- 攒批 · 原子摘取整批成员（放行的唯一入口）----
#
# KEYS = [成员 ZSET, 到期 ZSET]
# ARGV = [key]
# 返回被摘走的 task_id 列表；空列表 = 本批已被别人（或上一轮）取走。
#
# **摘取本身就是互斥**：DEL 成员键与 ZREM 到期键在同一 EVAL 内完成，N 触发与 T 触发
# 并发时只有一个能拿到成员列表——所以不需要额外的放行锁。但这一层只保证**批次级**
# 互斥，救不了「同一成员被两条路径捞到」，成员级幂等必须由 DB 条件更新补上
# （taskstore.claim_for_release），两层都不能省。
#
# 摘到空列表时也顺手清掉到期索引，避免空批次在 ZSET 里留残渣反复被扫。
LUA_BATCH_CLAIM = """
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return members
"""

# ---- 攒批 · 成员退批（取消时用）----
#
# KEYS = [成员 ZSET, 到期 ZSET]
# ARGV = [task_id, key]
# 被取消的成员必须从计数里摘掉：一批声明 N=100 而其中 5 条被取消，计数就永远差
# 5 条到不了 N，只能干等 T 兜底，等待时长凭空变长。
#
# 摘完若批次已空，顺手清掉到期索引——否则每轮都会捞到这个空批次并触发一次无成员的
# 放行（无害但纯浪费）。
LUA_BATCH_LEAVE = """
redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('ZCARD', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[2])
end
return 1
"""
