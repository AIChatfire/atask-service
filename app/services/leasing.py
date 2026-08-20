"""任务级租约钉回（唯一入口）：**同一把 key** 打全生命周期。

为什么必须精确到 key 而不只是渠道：一个 keypool 渠道下可以挂多把属于**不同
上游账号**的 key。任务在 A 账号创建，探测/取消/原生查询若换到 B 账号的 key，
上游会返 404 / 无权限——任务凭空"消失"，冻结只能等 sweep 兜底。

keypool 的 ``select`` 支持 ``channel_id + key_index`` **单 key 精确直达**
（``mode="direct"``：跳过轮换批次/轮询游标/usage 打分，且不访问 Redis），
正好是这个问题的正解——网关只递一个下标，凭证治理（禁用/轮换/epoch）仍完整
留在 keypool 侧，绝不在网关缓存明文 key。

降级纪律（精确直达失败时）：

- **key 级失败**（40010 索引越界 = 该 key 已被移出渠道；40001 = 该 key 被禁用）
  → 退回 ``channel_id`` 渠道直达，换渠道内一把健康 key 继续（对单账号渠道
  完全无损；多账号渠道下这一跳可能查不到任务，但比直接放弃更好）；
- **渠道级失败**（40002 渠道不存在等）→ 原样上抛，无从降级。

提交链路（``submit``）**不**用本模块：任务还没进上游，渠道内任意健康 key 都
可用，钉死一把反而在该 key 被禁时白等重试。
"""

from __future__ import annotations

from app.logging import log
from app.schemas import KeyLease, RouteConfig
from app.services import providers
from app.services.providers import KeyLeaseError
from app.services.registry import registry, route_from_lease


def _pin(data: dict, task: dict | None = None) -> tuple[int, int | None]:
    """从任务快照取 (channel_id, key_index)：``data.key_id`` 为主，
    ``tasks.channel_id`` 列兜底；``key_index`` 缺失（旧任务）→ None（退渠道直达）。"""
    channel_id = int(data.get("key_id") or (task or {}).get("channel_id") or 0)
    raw_index = data.get("key_index")
    key_index = int(raw_index) if isinstance(raw_index, int) else None
    return channel_id, key_index


async def lease_for_task(biz: str, data: dict, task: dict | None = None) -> KeyLease:
    """按任务快照钉回租约：精确直达（channel_id + key_index）→ 渠道直达降级。

    ``biz`` 仅用于日志与路由构建标签，渠道由 ``data`` 里的 id 决定。
    全部失败时抛 ``KeyLeaseError``（调用方按各自退避语义处理）。
    """
    channel_id, key_index = _pin(data, task)
    model = str(data.get("model") or "")
    if channel_id and key_index is not None:
        try:
            return await providers.keys.lease(
                biz, model=model, key_id=channel_id, key_index=key_index)
        except KeyLeaseError as exc:
            if not exc.key_level:
                raise
            # 该 key 已被移出/禁用：退渠道直达（同渠道换健康 key）
            log.warning("exact key pin-back failed, fall back to channel: "
                        "biz={} channel_id={} key_index={} code={} err={}",
                        biz, channel_id, key_index, exc.code, exc)
    return await providers.keys.lease(biz, model=model, key_id=channel_id or None)


async def route_for_task(biz: str, data: dict, task: dict | None = None,
                         ) -> tuple[KeyLease, RouteConfig]:
    """``lease_for_task`` + 路由构建并回填进程缓存（最常见的组合调用）。"""
    key = await lease_for_task(biz, data, task)
    return key, registry.remember(route_from_lease(biz, key))


__all__ = ["lease_for_task", "route_for_task"]
