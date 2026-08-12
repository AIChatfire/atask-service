"""后台 worker 装配入口（SPEC §3.9.4）：``python -m app.worker``。

asyncio.gather 启动各长驻组件的 ``run_forever()``：
    W2 PollWorker / W3 OutboxWorker / W3 FreezeRenewer /
    W4 CallbackQueueConsumer / W4 DeliveryDispatcher。
每个任务包 try/except 重启循环 + logfire.exception；SIGTERM/SIGINT 时 cancel
并等待（优雅停机 §8.5）。

装配所需的 BillingServiceClient / PricingEvaluator / TaskManager /
session_factory 在此构造一次并注入（W2/W3/W4 未交付的组件自动跳过并告警，
交付后无需改本文件）。
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

import logfire

from app.db import close_db, get_session_factory
from app.http_clients import close_http_clients
from app.redis_client import close_redis

_RESTART_DELAY_SECONDS = 5.0

RunForever = Callable[[], Awaitable[Any]]


async def _supervise(name: str, run_forever: RunForever) -> None:
    """单组件重启循环：崩溃记 logfire.exception 后延迟重启；Cancel 正常退出。"""
    while True:
        try:
            await run_forever()
            logfire.warning("worker component exited unexpectedly, restarting",
                            component=name)
        except asyncio.CancelledError:
            raise
        except Exception:
            logfire.exception("worker component crashed, restarting", component=name)
        await asyncio.sleep(_RESTART_DELAY_SECONDS)


def _build_components() -> list[tuple[str, RunForever]]:
    """构造一次并注入共享组件；未交付模块（ImportError/AttributeError）跳过。"""
    components: list[tuple[str, RunForever]] = []
    session_factory = get_session_factory()

    billing: Any = None
    pricing: Any = None
    task_manager: Any = None
    try:
        from app.billing.client import BillingServiceClient
        from app.billing.pricing import PricingEvaluator
        from app.tasks.manager import TaskManager

        billing = BillingServiceClient()
        pricing = PricingEvaluator(session_factory)
        task_manager = TaskManager(billing, pricing)

        # 与 web 进程同一注入点（worker 内嵌提交链路时口径一致）
        from app.routing import dynamic_router, videos

        videos.set_task_manager(task_manager)
        dynamic_router.set_passthrough_billing(pricing, billing)
    except (ImportError, AttributeError) as exc:
        logfire.warning("task manager stack unavailable, skipping",
                        error=str(exc))

    if task_manager is not None:
        try:
            from app.tasks.poller import PollWorker

            poller = PollWorker(task_manager, session_factory)
            components.append(("poller", poller.run_forever))
        except (ImportError, AttributeError) as exc:
            logfire.warning("poller unavailable, skipping", error=str(exc))

    if billing is not None:
        try:
            from app.billing.outbox import OutboxWorker

            outbox = OutboxWorker(billing, pricing, session_factory)
            components.append(("outbox", outbox.run_forever))
        except (ImportError, AttributeError) as exc:
            logfire.warning("outbox worker unavailable, skipping", error=str(exc))
        try:
            from app.billing.renewer import FreezeRenewer

            renewer = FreezeRenewer(billing, session_factory,
                                    task_manager=task_manager)
            components.append(("freeze_renewer", renewer.run_forever))
        except (ImportError, AttributeError) as exc:
            logfire.warning("freeze renewer unavailable, skipping", error=str(exc))

    if task_manager is not None:
        try:
            from app.callbacks.receiver import (
                CallbackQueueConsumer,
            )
            from app.callbacks.receiver import (
                set_task_manager as receiver_set_task_manager,
            )

            receiver_set_task_manager(task_manager)
            consumer = CallbackQueueConsumer(task_manager, session_factory)
            components.append(("callback_consumer", consumer.run_forever))
        except (ImportError, AttributeError) as exc:
            logfire.warning("callback consumer unavailable, skipping", error=str(exc))

    try:
        from app.callbacks.dispatcher import DeliveryDispatcher

        dispatcher = DeliveryDispatcher()  # 零自有表：Redis 队列，无需 DB session
        components.append(("delivery_dispatcher", dispatcher.run_forever))
    except (ImportError, AttributeError) as exc:
        logfire.warning("delivery dispatcher unavailable, skipping", error=str(exc))

    return components


async def _amain() -> None:
    from app.config import settings

    logfire.configure(
        service_name="async-gateway-worker",
        service_version=settings.app_version,
        environment=settings.app_env,
        token=settings.logfire_token,
        send_to_logfire="if-token-present",
        console=False,
    )
    # biz 注册表（决策 B：env/file 为唯一事实源；worker 进程同样加载 + watch）
    from app.registry import registry

    registry.reload()
    components = _build_components()
    components.append(("biz_watch", registry.invalidate_loop))
    if not components:
        logfire.warning("no worker components available, exiting")
        return

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):  # 非 Unix 平台无信号支持
            loop.add_signal_handler(sig, stop.set)

    logfire.info("worker started", components=[name for name, _ in components])
    tasks = [
        asyncio.create_task(_supervise(name, run), name=name)
        for name, run in components
    ]
    try:
        await stop.wait()
    finally:
        logfire.info("worker shutting down")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_http_clients()
        await close_redis()
        await close_db()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
