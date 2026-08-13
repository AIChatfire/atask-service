"""Gunicorn 配置（架构 §2.2；SPEC §3.7：本文件直读 GUNICORN_* 环境变量）。

- ``preload_app``：应用 master 预加载，引擎/Redis/HTTP 客户端全部惰性创建
  （post-fork 安全，见 app/db.py / app/redis_client.py / app/http_clients.py）；
- ``max_requests`` + jitter：防内存缓慢泄漏，错峰重启；
- ``graceful_timeout=30`` 与 compose ``stop_grace_period: 60s`` 对齐（SIGKILL
  前留足优雅停机窗口）；
- worker 公式 ``(2×CPU)+1`` 封顶 16（SPEC §2 约束表）。
"""

import multiprocessing
import os


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _default_workers() -> int:
    return min(16, 2 * multiprocessing.cpu_count() + 1)


bind = os.environ.get("BIND", "0.0.0.0:8000")
workers = max(1, min(16, _env_int("GUNICORN_WORKERS", _default_workers())))
worker_class = "uvicorn.workers.UvicornWorker"

worker_tmp_dir = "/dev/shm"  # 心跳文件放内存盘，防 worker 被误杀
keepalive = 5
timeout = _env_int("GUNICORN_TIMEOUT", 120)
graceful_timeout = _env_int("GUNICORN_GRACEFUL_TIMEOUT", 30)
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info")

preload_app = True
max_requests = 5000
max_requests_jitter = 500

# 请求行/头上限对齐常见反向代理默认值（防 431/414 与慢速攻击面）
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190


def on_starting(server):
    """master 启动钩子：记录生效 worker 数（公式封顶结果可观测）。"""
    server.log.info("gunicorn starting", extra={"workers": workers})


def post_fork(server, worker):
    """worker fork 后钩子：惰性单例保证引擎/连接池在子进程内新建。"""
    server.log.info("worker spawned (pid=%s)", worker.pid)


def worker_exit(server, worker):
    """worker 退出钩子（观测口径：异常退出计数由上层日志采集告警）。"""
    server.log.info("worker exited (pid=%s)", worker.pid)
