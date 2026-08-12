import multiprocessing
import os

bind = "0.0.0.0:8000"
# 异步 worker：1~2 倍 CPU 核数起步，按压测调；不要用同步经验的 2n+1
workers = int(os.environ.get("GW_WEB_WORKERS", multiprocessing.cpu_count()))
worker_class = "uvicorn.workers.UvicornWorker"

worker_tmp_dir = "/dev/shm"          # 心跳文件放内存盘，防 worker 被误杀
keepalive = 5
timeout = 120
graceful_timeout = 60                # 任务提交即 202，不等长请求
max_requests = 10000                 # 防内存缓慢泄漏，配合 jitter 错峰重启
max_requests_jitter = 1000
