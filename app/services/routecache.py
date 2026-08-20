"""biz → 最近成功使用的 channel_id（Redis 跨进程共享，进程缓存之上再兜一层）。

用途只有一个：**免费透传（GET）路径的选渠道**。免费请求不带 model，keypool
的 ``select(group, model)`` 对空 model 直接拒绝（40010），所以网关不去问它——
按 biz 记住"上次这个 biz 用的是哪个渠道"，需要时以 ``channel_id`` 直达租约
（keypool 直达分支不校验 model）。

为什么不只用进程内 RouteRegistry 缓存：进程缓存 TTL 60s 且每个 worker 各自
一份，重启/扩容/冷启动即空 → 免费 GET 直接 404。Redis 里这一条 KV 丢了也只是
回落 404（可重建，符合 Redis 只放"丢了能重建"的纪律）。

写入点：任何一次成功的租约解析（preflight / 提交 / 探测 / 透传）都可以顺手
remember；读取点只有免费透传兜底。
"""

from __future__ import annotations

from app.config import settings
from app.logging import log
from app.redis import K_ROUTE_CHANNEL, r


async def remember(biz: str, channel_id: int) -> None:
    """记住 biz 最近使用的渠道（fire-and-forget 语义：失败只 debug）。"""
    if not biz or not channel_id:
        return
    try:
        await r.set(K_ROUTE_CHANNEL.format(biz=biz), str(channel_id),
                    ex=settings.route_channel_ttl_seconds)
    except Exception:
        log.opt(exception=True).debug("route channel cache write failed: biz={}", biz)


async def get(biz: str) -> int | None:
    """取 biz 最近使用的渠道 id；无记录/Redis 故障 → None。"""
    if not biz:
        return None
    try:
        value = await r.get(K_ROUTE_CHANNEL.format(biz=biz))
    except Exception:
        log.opt(exception=True).debug("route channel cache read failed: biz={}", biz)
        return None
    try:
        return int(value) if value else None
    except (TypeError, ValueError):
        return None
