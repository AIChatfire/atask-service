"""Callback 子系统（SPEC §3.5/§7；W4 全部文件）。

- ``receiver``：上游回调接收（POST /callbacks/{biz}/{provider}/{capability}，
  验签→防重放→去重→入队→202，顺序不可颠倒）+ 消费侧驱动状态机。
- ``dispatcher``：用户回调可靠投递（lease + SKIP LOCKED 领取、指数退避、
  410/4xx 死信、X-Signature Stripe 形制签名）。
"""

from app.callbacks.dispatcher import DeliveryDispatcher, replay_dead_delivery, sign_headers
from app.callbacks.receiver import (
    CallbackQueueConsumer,
    process_upstream_callback,
    router,
    set_task_manager,
)

__all__ = [
    "CallbackQueueConsumer",
    "DeliveryDispatcher",
    "process_upstream_callback",
    "replay_dead_delivery",
    "router",
    "set_task_manager",
    "sign_headers",
]

