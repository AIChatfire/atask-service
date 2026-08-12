"""限流四层 + Redis 熔断器 + 幂等键 guard（SPEC §3.9.2 / 架构 §8）。

- 熔断器：状态放 Redis（Gunicorn 多进程共享，§8.2 已核实的坑）；
  ``fail_count>=5 → open(30s) → half-open 单探针(SET NX 抢权) → closed``；
  **429 不计失败**（调用方区分 UpstreamRateLimitError，§8.2 特殊纪律）。
- 限流分层（§8.3）：用户级 ZSET 滑动窗口 / biz 级令牌桶 / 上游并发信号量，
  均 Lua 原子化；超限 429 + Retry-After，容量满 503 背压（绝不无限排队）。
- 幂等键（§8.4）：``SET idem:{user}:{key} NX PX 24h`` 原子占位；同 key 同
  payload 回放首个响应（Stripe 式，含错误响应）；不同 payload 409。
- 欠费熔断名单（§5.6）：``debt:{user_id}`` 存在时提交类请求 402，查询类放行。
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import logfire
from fastapi import Request

from app import errors
from app.auth import TokenInfo
from app.redis_client import get_redis
from app.registry import BizConfig

IDEM_TTL_MS = 24 * 3600 * 1000  # 幂等占位 24h（§8.4）


# ---------------------------------------------------------------------------
# 熔断器（Redis 共享状态，§8.2）
# ---------------------------------------------------------------------------


class CircuitBreaker:
    """pybreaker 语义（closed→open→half-open），状态在 Redis HASH ``circuit:{target}``。

    target 维度：``upstream:{biz}`` / ``billing-logic`` / ``billing-service`` /
    ``user-callback:{domain}``。429 由调用方保证不调 ``on_failure``。
    """

    FAILURE_THRESHOLD = 5
    OPEN_SECONDS = 30
    PROBE_TTL_SECONDS = 30

    async def allow(self, target: str) -> bool:
        """是否放行。open 未冷却 → False；冷却期满 → SET NX 抢单探针权（half-open）。"""
        redis: Any = await get_redis()
        data = await redis.hgetall(f"circuit:{target}")
        if not data or data.get("state", "closed") != "open":
            return True
        opened_at = float(data.get("opened_at") or 0)
        if time.time() - opened_at < self.OPEN_SECONDS:
            return False
        # half-open：单探针（多副本只有一家抢到 NX）
        return bool(
            await redis.set(
                f"circuit:{target}:probe", "1", nx=True, ex=self.PROBE_TTL_SECONDS
            )
        )

    async def on_success(self, target: str) -> None:
        """成功（含探针成功）→ closed + 清零失败计数 + 释放探针权。"""
        redis: Any = await get_redis()
        pipe = redis.pipeline()
        pipe.hset(f"circuit:{target}", mapping={"state": "closed", "fail_count": 0})
        pipe.delete(f"circuit:{target}:probe")
        await pipe.execute()

    async def on_failure(self, target: str) -> None:
        """失败（5xx/超时；**429 绝不调用本方法**）。

        half-open 探针失败 → 立即重新 open；closed 累计 >=5 → open 并记 opened_at。
        """
        redis: Any = await get_redis()
        key = f"circuit:{target}"
        data = await redis.hgetall(key)
        now = time.time()
        if data and data.get("state") == "open":
            # 探针失败：重新 open（无论是否在冷却期，失败即再计时）
            await redis.hset(
                key, mapping={"state": "open", "opened_at": repr(now)}
            )
            await redis.delete(f"{key}:probe")
            return
        fail_count = int((data or {}).get("fail_count") or 0) + 1
        if fail_count >= self.FAILURE_THRESHOLD:
            await redis.hset(
                key,
                mapping={"state": "open", "fail_count": fail_count, "opened_at": repr(now)},
            )
            logfire.warning("circuit opened", target=target)
        else:
            await redis.hset(key, mapping={"state": "closed", "fail_count": fail_count})

    async def state(self, target: str) -> str:
        """当前状态（测试/排障用）。"""
        redis: Any = await get_redis()
        data = await redis.hgetall(f"circuit:{target}")
        return (data or {}).get("state", "closed")


circuit_breaker = CircuitBreaker()


# ---------------------------------------------------------------------------
# 限流（§8.3；Lua 原子化）
# ---------------------------------------------------------------------------

# 用户级滑动窗口（ZSET）：KEYS[1]=rl:user:{sk_hash}
# ARGV: now_ms, window_ms, limit, member → 0=放行; >0=retry_after_ms
_LUA_USER_WINDOW = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[2])
local count = redis.call('ZCARD', KEYS[1])
if count < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 0
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local retry = (tonumber(oldest[2]) + ARGV[2]) - ARGV[1]
if retry < 1 then retry = 1 end
return retry
"""

# biz 级令牌桶（STRING HASH 两字段 tokens/ts_ms）：KEYS[1]=rl:biz:{biz}
# ARGV: now_ms, capacity(=rpm), refill_per_ms → 1=放行; 0=拒绝
_LUA_BIZ_BUCKET = """
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts_ms')
local tokens = tonumber(data[1]) or tonumber(ARGV[2])
local ts = tonumber(data[2]) or tonumber(ARGV[1])
tokens = math.min(tonumber(ARGV[2]), tokens + (tonumber(ARGV[1]) - ts) * tonumber(ARGV[3]))
if tokens >= 1 then
  redis.call('HMSET', KEYS[1], 'tokens', tokens - 1, 'ts_ms', ARGV[1])
  return 1
end
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts_ms', ARGV[1])
return 0
"""

# 上游并发信号量（§8.3：Lua 原子 INCR+EXPIRE / 释放 DECR）：
# KEYS[1]=rl:upstream:{biz}  ARGV: limit, window_ms → 1=获得; 0=已满
_LUA_UPSTREAM_ACQUIRE = """
local c = redis.call('INCR', KEYS[1])
if c == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if c > tonumber(ARGV[1]) then
  redis.call('DECR', KEYS[1])
  return 0
end
return 1
"""

_LUA_UPSTREAM_RELEASE = """
local c = tonumber(redis.call('GET', KEYS[1]) or '0')
if c > 0 then
  redis.call('DECR', KEYS[1])
end
return 1
"""

DEFAULT_USER_RPM = 60          # cfg.rate_limit 未配置 user_rpm 时的兜底
UPSTREAM_WINDOW_MS = 10_000    # 并发信号量兜底窗口（崩副本计数自动过期）


async def check_user_rate_limit(token: TokenInfo, cfg: BizConfig | None) -> None:
    """用户级滑动窗口限流（ZSET）；超限抛 429 + Retry-After。"""
    rpm = DEFAULT_USER_RPM
    if cfg is not None and cfg.rate_limit.get("user_rpm"):
        rpm = int(cfg.rate_limit["user_rpm"])
    redis: Any = await get_redis()
    now_ms = int(time.time() * 1000)
    member = f"{now_ms}:{token.sk_hash}:{time.monotonic_ns()}"
    retry_ms = await redis.eval(
        _LUA_USER_WINDOW, 1, f"rl:user:{token.sk_hash}",
        str(now_ms), "60000", str(rpm), member,
    )
    retry_ms = int(retry_ms)
    if retry_ms > 0:
        logfire.info("rate limit hit", scope="user", sk_hash=token.sk_hash)
        raise errors.rate_limited(max(1, (retry_ms + 999) // 1000))


async def check_biz_rate_limit(cfg: BizConfig) -> None:
    """biz 级令牌桶限流；``rate_limit.biz_rpm`` 未配置则不限。超限抛 429。"""
    biz_rpm = cfg.rate_limit.get("biz_rpm")
    if not biz_rpm:
        return
    redis: Any = await get_redis()
    allowed = await redis.eval(
        _LUA_BIZ_BUCKET, 1, f"rl:biz:{cfg.biz}",
        str(int(time.time() * 1000)), str(int(biz_rpm)), repr(int(biz_rpm) / 60_000.0),
    )
    if not int(allowed):
        logfire.info("rate limit hit", scope="biz", biz=cfg.biz)
        raise errors.rate_limited(1)


async def acquire_upstream_slot(cfg: BizConfig) -> bool:
    """上游并发信号量（Lua 原子化，§8.3）。``upstream_concurrency`` 未配置恒放行。

    返回 False 表示已满——调用方抛 503 背压（绝不无限排队）；
    成功获得者**必须**在 finally 中调 ``release_upstream_slot``。
    """
    limit = cfg.rate_limit.get("upstream_concurrency")
    if not limit:
        return True
    redis: Any = await get_redis()
    got = await redis.eval(
        _LUA_UPSTREAM_ACQUIRE, 1, f"rl:upstream:{cfg.biz}",
        str(int(limit)), str(UPSTREAM_WINDOW_MS),
    )
    return bool(int(got))


async def release_upstream_slot(biz: str) -> None:
    """释放上游并发信号量（DECR，防负值）。"""
    try:
        redis: Any = await get_redis()
        await redis.eval(_LUA_UPSTREAM_RELEASE, 1, f"rl:upstream:{biz}")
    except Exception:
        logfire.exception("release upstream slot failed", biz=biz)


# ---------------------------------------------------------------------------
# 幂等键 guard（§8.4）
# ---------------------------------------------------------------------------


class IdempotentReplay(Exception):
    """同 key 同 payload 命中首个响应——由 W1 main.py 注册的处理器统一回放缓存响应。"""

    def __init__(self, status_code: int, body: dict[str, Any]) -> None:
        super().__init__("idempotent replay")
        self.status_code = status_code
        self.body = body


def _idem_redis_key(user_id: int, key: str) -> str:
    return f"idem:{user_id}:{key}"


async def idempotency_guard(request: Request, token: TokenInfo) -> str | None:
    """提交端点依赖（§8.4）。无 ``Idempotency-Key`` 头 → None（不启用幂等）。

    占位值 JSON：``{payload_hash, state, status_code, body}``。
    - 占位成功（首个请求）→ 返回 key（供落 private_data，完成后须调
      ``idempotency_complete`` 回填响应，失败须调 ``idempotency_release`` 释放）；
    - 同 payload 且已有响应 → 抛 ``IdempotentReplay``（回放首个响应）；
    - 同 payload 但首请求仍在途 → 409（并发同 key 拒绝，Stripe 语义）；
    - 不同 payload → ``errors.idempotency_conflict``。
    """
    key = request.headers.get("Idempotency-Key")
    if not key:
        return None
    body = await request.body()
    payload_hash = hashlib.sha256(
        request.method.encode() + b" " + request.url.path.encode() + b" " + body
    ).hexdigest()
    redis: Any = await get_redis()
    placeholder = json.dumps(
        {"payload_hash": payload_hash, "state": "pending", "status_code": 0, "body": None},
        ensure_ascii=False,
    )
    ok = await redis.set(_idem_redis_key(token.user_id, key), placeholder,
                         nx=True, px=IDEM_TTL_MS)
    if ok:
        return key
    blob = await redis.get(_idem_redis_key(token.user_id, key))
    try:
        stored = json.loads(blob) if blob else None
    except Exception:
        stored = None
    if not isinstance(stored, dict):
        # 占位值损坏：fail-closed 拒绝而非放行双写
        raise errors.idempotency_conflict("corrupted idempotency record")
    if stored.get("payload_hash") != payload_hash:
        raise errors.idempotency_conflict()
    if stored.get("state") == "done" and isinstance(stored.get("body"), dict):
        raise IdempotentReplay(int(stored["status_code"]), stored["body"])
    raise errors.idempotency_conflict(
        "request with the same Idempotency-Key is still in progress"
    )


async def idempotency_complete(
    token: TokenInfo, key: str, *, payload_hash: str | None = None,
    status_code: int, body: dict[str, Any],
) -> None:
    """首个请求完成后回填响应（供后续同 key 请求回放；含错误响应也回放，Stripe 式）。"""
    try:
        redis: Any = await get_redis()
        rkey = _idem_redis_key(token.user_id, key)
        blob = await redis.get(rkey)
        stored = json.loads(blob) if blob else {}
        if not isinstance(stored, dict):
            stored = {}
        stored.update({"state": "done", "status_code": status_code, "body": body})
        if payload_hash is not None:
            stored["payload_hash"] = payload_hash
        ttl_ms = await redis.pttl(rkey)
        await redis.set(rkey, json.dumps(stored, ensure_ascii=False),
                        px=ttl_ms if ttl_ms and ttl_ms > 0 else IDEM_TTL_MS)
    except Exception:
        # 回放增强失败不影响主链路（占位 24h 后自动过期）
        logfire.exception("idempotency complete failed", key=key)


async def idempotency_release(token: TokenInfo, key: str) -> None:
    """提交失败（如 402 不落库）时释放占位，允许客户端修正后重试（§3.10.1）。"""
    try:
        redis: Any = await get_redis()
        await redis.delete(_idem_redis_key(token.user_id, key))
    except Exception:
        logfire.exception("idempotency release failed", key=key)


# ---------------------------------------------------------------------------
# 欠费熔断名单（§5.6）
# ---------------------------------------------------------------------------


async def check_debt_block(token: TokenInfo) -> None:
    """``debt:{user_id}`` 存在 → 402（提交类端点调用；查询类不调用，§5.6）。"""
    redis: Any = await get_redis()
    if await redis.get(f"debt:{token.user_id}"):
        logfire.info("debt blocked submit", user_id=token.user_id)
        raise errors.payment_required(
            "account in debt, please top up before submitting new tasks"
        )
