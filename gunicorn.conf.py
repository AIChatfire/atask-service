"""Gunicorn 配置（架构 §2.2；SPEC §3.7：本文件直读 GUNICORN_*/DB_* 环境变量）。

**本文件是全项目唯一允许直读 os.environ 的地方**——master 进程在 app 之前加载，
此时 pydantic-settings 单例（app/config.py）还不存在。这条纪律有测试门禁盯着
（tests/test_static_gates.py::test_os_environ_only_in_gunicorn），别把 os.environ
挪到别的模块。

高可用三条硬约束（下面每个数字都由它推导，别写死数字）：
1. **worker 数由 DB 连接预算反推**——本项目的 MySQL 是与 new-api 共用的同一个实例，
   打爆 max_connections 会级联拖垮 new-api，属于跨服务故障。CPU 核心数只是上限、
   不是依据。每个 worker 独占一个连接池（preload 只加载代码不建连接），单 worker
   占用 = DB_POOL_SIZE + DB_MAX_OVERFLOW，**必须与 app/config.py 的
   db_pool_size / db_max_overflow 同字段同步**：改一处就要改另一处，否则这里反推
   出的 worker 数与真实连接占用脱节（真实连接数 = workers × 单 worker 占用）。
2. **timeout 由最长合法请求反推**——原生查询（probe）是缓冲转发，会 await 上游到
   全局 RELAY_TIMEOUT_SECONDS（app/config.py 默认 60s）；timeout 小于它就等于
   把正常请求当成卡死 worker 杀掉。
3. **graceful_timeout 必须 <= timeout - 5**（防两者倒挂），且必须 < compose 的
   stop_grace_period，否则 Docker 先 SIGKILL，优雅停机窗口形同虚设（见
   docker-compose.yml gateway.stop_grace_period 的同步说明）。

preload_app：应用 master 预加载，引擎/Redis/HTTP 客户端全部惰性创建（post-fork
安全，见 app/db.py / app/redis.py / app/httpc.py）。
max_requests + jitter：防内存缓慢泄漏，错峰重启。
"""

import multiprocessing
import os


def _env_int(name: str, default: int) -> int:
    """读整型 env；非法值回落默认（运维手敲错了不至于直接起不来）。"""
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    """读浮点 env；非法值回落默认。"""
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default


bind = os.environ.get("BIND", "0.0.0.0:8000")
worker_class = "uvicorn.workers.UvicornWorker"
# 进程名进 `ps`，运维一眼能区分 web / worker / new-api，排障不用翻 cgroup
proc_name = os.environ.get("GUNICORN_PROC_NAME", "atask-service")

# ---- 约束 1：worker 数由 DB 连接预算反推 ----
# 单 worker 的连接池上限 = pool_size + max_overflow（app/config.py 同名默认 20 + 10）。
# 用「与 app.config.Settings 字段同名的大写」读：这两个值就是该 Settings 的字段，
# 改 app 侧默认值要同步这里。
_DB_PER_WORKER = max(1, _env_int("DB_POOL_SIZE", 20) + _env_int("DB_MAX_OVERFLOW", 10))
# MySQL 实例的 max_connections 预算。默认 200 是「与 new-api 共存」的保守值：
# 它同时要养 new-api 自己的连接池与管理连接，网关不能按实例上限全占。
_DB_BUDGET = _env_int("DB_MAX_CONNECTIONS", 200)
# web 可占的预算比例。默认 0.6——余下 0.4 留给 taskiq worker 进程（同样各持连接池）、
# new-api 自身与管理连接；worker 的并发上限见 QUEUE_MAX_ASYNC_TASKS。
_DB_WEB_SHARE = _env_float("DB_WEB_SHARE", 0.6)
_BY_BUDGET = max(1, int(_DB_BUDGET * _DB_WEB_SHARE) // _DB_PER_WORKER)
# CPU 只是**上限**不是依据：IO 密集的网关吃不到那么多核，且必须为 DB 预算让路
_BY_CPU = min(16, multiprocessing.cpu_count() * 2 + 1)
# 最少 2 个：单 worker 在 max_requests 回收或崩溃重启的窗口里就是单点
workers = _env_int("GUNICORN_WORKERS", 0) or max(2, min(_BY_CPU, _BY_BUDGET))

# ---- 约束 2：timeout 由最长合法请求反推 ----
# 最长合法请求 = 原生查询缓冲转发等待上游的时长，取全局 RELAY_TIMEOUT_SECONDS
# 默认值 60s（app/schemas.py:136，按渠道可覆盖；调高渠道默认要同步这里）。
_REQ_MAX = _env_int("GUNICORN_REQ_MAX_SECONDS", 60)
# +120 给上游/DB 抖动留余量：60s 的转发 → timeout 180s。没有明确的长轮询配置，
# 所以下限锁 180，保证任何正常请求都不会被当成卡死 worker 杀掉。
timeout = _env_int("GUNICORN_TIMEOUT", 0) or max(180, _REQ_MAX + 120)

# ---- 约束 3：graceful_timeout 由 timeout 推导且防倒挂 ----
# 默认 75s：足够让在飞的原生查询（最长 ~60s）走完再退出；再夹一层 timeout-5，
# 保证 graceful < timeout。它还必须 < compose 的 stop_grace_period（当前 90s），
# 否则 Docker 会在 gunicorn 优雅停机完成前先 SIGKILL。
_GRACEFUL_DEFAULT = 75
graceful_timeout = min(
    _env_int("GUNICORN_GRACEFUL_TIMEOUT", 0) or _GRACEFUL_DEFAULT,
    max(5, timeout - 5),
)

# 突发排队的接纳队列；反代那层也要接得住，否则排队发生在内核而非这里
backlog = _env_int("GUNICORN_BACKLOG", 2048)
keepalive = _env_int("GUNICORN_KEEPALIVE", 15)

# /dev/shm 上的心跳文件：容器里 /tmp 可能是慢速 overlay，会导致 worker 被误杀。
# 兜底：/dev/shm 不存在或不可写时退回 /tmp，别让目录探测直接崩掉 master。
_tmp_dir = "/dev/shm"
if not (os.path.isdir(_tmp_dir) and os.access(_tmp_dir, os.W_OK)):
    _tmp_dir = "/tmp"
worker_tmp_dir = _tmp_dir

# 周期性重启 worker，兜住任何未发现的内存增长；jitter 防同时重启造成流量凹陷
max_requests = _env_int("GUNICORN_MAX_REQUESTS", 5000)
max_requests_jitter = _env_int("GUNICORN_MAX_REQUESTS_JITTER", 500)

# 请求行/头上限对齐常见反向代理默认值（防 431/414 与慢速攻击面）
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

# 反向代理后的真实客户端 IP（只影响访问日志；业务按 token_hash 走，不依赖它）。
# 默认只信回环——设为 "*" 等于信任任意伪造的 X-Forwarded-For。
forwarded_allow_ips = os.environ.get("FORWARDED_ALLOW_IPS", "127.0.0.1")

# 访问日志默认关（高频探针会刷屏），需要时 GUNICORN_ACCESS_LOG=1 开；错误日志始终 stderr
accesslog = "-" if os.environ.get("GUNICORN_ACCESS_LOG", "0") == "1" else None
errorlog = "-"
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info").lower()


def on_starting(server):
    """master 启动钩子：把推导结果打出来（公式封顶后真实生效值可观测）。"""
    server.log.info(
        "atask-service master starting: workers=%s (cpu上限=%s db预算上限=%s "
        "每worker连接=%s) bind=%s timeout=%s graceful=%s",
        workers, _BY_CPU, _BY_BUDGET, _DB_PER_WORKER, bind, timeout, graceful_timeout,
    )
    # 倒挂告警：graceful 逼近 timeout 时，慢请求可能还没走完就被强杀
    if graceful_timeout + 5 > timeout:
        server.log.warning(
            "graceful_timeout(%s) 逼近 timeout(%s)：慢请求可能还没走完就被强杀",
            graceful_timeout, timeout,
        )


def post_fork(server, worker):
    """worker fork 后钩子：惰性单例保证引擎/连接池在子进程内新建。"""
    server.log.info("worker spawned (pid=%s)", worker.pid)


def worker_exit(server, worker):
    """worker 退出钩子（观测口径：异常退出计数由上层日志采集告警）。"""
    server.log.info("worker exited (pid=%s)", worker.pid)
