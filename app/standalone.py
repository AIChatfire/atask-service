"""单进程模式：web + worker + scheduler 跑在同一个事件循环里。

用途：本地开发、单机试用、小规格部署。``python -m app.standalone`` 一条命令
起全套（``make standalone``），不必拉 compose、不必开三个终端。

为什么可以合在一起：worker 的活儿是 ``await`` 上游 HTTP（IO 密集），与 web
请求共享事件循环不会互相饿死。真正的限制是**无法独立水平扩展**——web 与
worker 需要按不同倍数扩副本时，必须拆回 compose。scheduler 必须单副本
（多份会重复触发每分钟 sweep），单进程模式天然满足。

拆分标准（到了就该换 compose）：

- 提交 QPS > 200，或
- 在途任务常态 > 500，或
- 需要滚动重启 web 而不中断在途任务的执行与探测。

与 compose 形态的差异只在进程拓扑：配置、状态机、失败分流完全一致，
所以本地在此形态下跑通的链路对线上有参考价值。
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

import uvicorn

from app import observability
from app.config import settings
from app.logging import log, setup_logging


async def _run_worker() -> None:
    """在当前事件循环里跑 taskiq worker。

    用 ``run_receiver_task`` 而不是 spawn ``taskiq worker`` 子进程：子进程要
    各自建 DB / Redis 连接池，单机模式下白白多一倍连接（而连接数正是本项目与
    new-api 共享 MySQL 时最紧的资源，见 gunicorn.conf.py 的预算推导）。
    """
    from taskiq.api import run_receiver_task

    from app.queue import broker

    await broker.startup()
    try:
        await run_receiver_task(broker, max_async_tasks=settings.queue_max_async_tasks)
    finally:
        with contextlib.suppress(Exception):
            await broker.shutdown()


async def _run_scheduler() -> None:
    """在当前事件循环里跑 taskiq scheduler（延迟派发 + 每分钟 sweep）。

    ``interval=1s``：攒批的 T 触发走 ``schedule_by_time``（延迟派发），而 scheduler
    默认**按分钟对点唤醒**（taskiq 0.11 ``run_scheduler_loop``）。不设这一项时，
    ``batch_wait`` 的实际放行最坏会晚 60s——正确性由 sweep 的超期兜底保证（最坏也是
    多等一个周期），但「配了 30s 却要等到下一分钟」会让攒批看起来没生效。

    与 compose 形态的 ``taskiq scheduler ... --update-interval 1`` 是同一件事，
    两处都要改（这是第三个部署形态：Makefile 的 make scheduler / compose / 本文件）。
    """
    from datetime import timedelta

    from taskiq.api import run_scheduler_task

    from app.queue import scheduler

    await run_scheduler_task(scheduler, interval=timedelta(seconds=1))


async def _run_web(stop: asyncio.Event) -> None:
    """在当前事件循环里跑 uvicorn（不 fork，不用 gunicorn）。"""
    config = uvicorn.Config(
        "app.main:app",
        host=_host(),
        port=_port(),
        log_config=None,       # 日志统一走 loguru（setup_logging 已装配）
        access_log=False,      # 访问日志由 gunicorn/反代侧负责；此处只留业务日志
        timeout_keep_alive=15,
    )
    server = uvicorn.Server(config)
    # 信号由本模块统一处理——让 uvicorn 也装一套会互相抢，出现「Ctrl-C 只停了
    # web、worker 还在跑」的半死状态（请求照收、任务永不执行，最坏的一种）
    setattr(server, "install_signal_handlers", lambda: None)  # noqa: B010
    task = asyncio.create_task(server.serve())
    await stop.wait()
    server.should_exit = True
    await task


def _bind() -> str:
    """监听地址来源：``BIND``（配置单例，与 ``Settings.bind`` 同名同义）。

    不直读 ``os.environ``——本项目纪律是配置只有一个入口（``app.config``），
    唯一例外是 ``gunicorn.conf.py``（它在 pydantic 单例之前由 master 加载，
    ``tests/test_static_gates.py`` 用结构断言钉死这条边界）。
    """
    return settings.bind


def _host() -> str:
    return _bind().rsplit(":", 1)[0]


def _port() -> int:
    parts = _bind().rsplit(":", 1)
    try:
        return int(parts[1])
    except (IndexError, ValueError):
        return 8000


async def main() -> None:
    setup_logging()
    # 观测装配必须在 app.main 被 import **之前**：main.py 在模块级
    # create_app() 里会调 observability.setup("web")，而 configure 是全局
    # 一次性的（幂等守卫）——先到先得，抢先声明 standalone 形态才能在 logfire
    # 上把它与标准 web 部署区分开
    observability.setup("standalone")

    log.info(
        "standalone starting: env={} bind={} platform={} "
        "max_async_tasks={} logfire={}",
        settings.app_env, _bind(), settings.gateway_platform,
        settings.queue_max_async_tasks,
        "on" if settings.logfire_enabled else "off",
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    tasks = [
        asyncio.create_task(_run_web(stop), name="web"),
        asyncio.create_task(_run_worker(), name="worker"),
        asyncio.create_task(_run_scheduler(), name="scheduler"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # 任一组件退出即整体收摊：半死状态（web 活着但 worker 没了）比直接退出更糟
    # ——请求照收、任务永远不执行，客户端只能看到 202 然后一路超时
    for task in done:
        if (exc := task.exception()) is not None:
            log.opt(exception=exc).error("component crashed: {}", task.get_name())
    stop.set()
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    log.info("standalone stopped")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
