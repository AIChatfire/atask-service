"""Redis 客户端与键规范。原则：Redis 只放"丢了能重建"的状态——
缓存、限流计数、幂等键（24h）、令牌会话、状态变更去重。
任务队列由 taskiq 管理（app/queue.py）；事实源永远在 tasks 表。
"""

import redis.asyncio as aioredis

from app.config import settings

r = aioredis.from_url(settings.redis_url, decode_responses=True)

# ---- 键规范 ----
K_IDEM = "gw:idem:{token_hash}:{key}"        # 幂等键 -> task_id（24h）
K_RL = "gw:rl:{subject}"                     # 滑动窗口限流（subject=token_hash 或 ip）
K_CONC = "gw:conc:{token_hash}"              # 并发任务占用
K_BREAKER = "gw:breaker:{biz}"               # 上游熔断失败计数（中继链路以目标 host 做分组键）
K_BATCH_SWEEP_LOCK = "gw:batch_sweep_lock"   # /batch 收敛重入锁（慢轮防并发踩踏）
K_QSTATS = "gw:queue_stats"                  # 队列观测快照缓存（JSON，短 TTL）
S_DLQ = "gw:events:dlq"                      # 死信（taskiq 任务超限后落信）

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
