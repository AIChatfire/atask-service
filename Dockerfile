FROM python:3.12-slim AS builder

# 依赖钉版唯一处是 pyproject.toml。历史上仓库根还有一份未钉版的 requirements.txt，
# 已删除——两份清单必然漂移，且 Docker 只认 pyproject（见 README「本地开发」）。
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# ---- 为什么需要 builder 阶段：asyncmy 在 linux/arm64 上没有预编译 wheel ----
# asyncmy 0.2.10 发布的 wheel 覆盖 macOS(x86_64/arm64)、Windows、以及
# manylinux/musllinux 的 **i686/x86_64**——**唯独没有 linux arm64**。
# 因此在 Apple Silicon（默认构建 linux/arm64）或 arm64 服务器上，pip 会退回用
# sdist 现场编译 Cython 扩展，而 python:3.12-slim 不带 C 编译器 → 构建硬失败
# （实测报错：Failed to build wheel for asyncmy，exit status 1）。
# CI 跑在 ubuntu-latest（x86_64）有 wheel 可用，所以这个缺陷长期没暴露。
#
# 解法：把编译器留在 builder 阶段，只把编好的 wheel 带进运行阶段——
# 既让 arm64 能建，又不把 build-essential 塞进最终镜像。
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
# 只放 pyproject + README（setuptools 构建时 pyproject 引用了 readme）。
# 此时 app/ 尚未 COPY：packages.find(include=["app*"]) 匹配到空集合，setuptools
# 仍能构建出仅含元数据的空包（已实测：退出码 0、依赖装齐、不生成 app 包）。
COPY pyproject.toml README.md ./

# 把**全部依赖**（含传递依赖）编成 wheel，并额外导出依赖清单供运行阶段离线安装。
# 运行阶段不装本项目自身——那份空包没有意义，应用代码在分层 2 里以 editable 装。
RUN pip wheel --wheel-dir /wheels . \
 && python -c "import pathlib, tomllib; \
deps = tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['dependencies']; \
pathlib.Path('/wheels/deps.txt').write_text(chr(10).join(deps) + chr(10))"


FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

# ---- 分层 1：依赖层（缓存稳定，改代码不触发重装）----
# --no-index + --find-links：完全离线，只从 builder 编好的 wheel 装。
# 注意这里按 deps.txt 装**依赖**、而不是 `pip install .`——后者会触发 PEP 517
# 构建隔离去拉 setuptools，与 --no-index 冲突（实测报错：pip subprocess to install
# build dependencies did not run successfully），本项目自身留到分层 2 再装。
# 装完即删 wheel 目录：体积可观且运行期无用。
COPY --from=builder /wheels /wheels
# pyproject/README 必须留在最终镜像里：分层 2 的 editable 安装要靠 pyproject 才能
# 认出「这是一个 Python 项目」（实测缺它会报 neither 'setup.py' nor 'pyproject.toml'
# found）。放在依赖层之前，pyproject 变更会正确失效依赖层。
COPY pyproject.toml README.md ./
RUN pip install --no-index --find-links=/wheels -r /wheels/deps.txt \
 && rm -rf /wheels

# ---- 分层 2：应用代码层（只在这一层 COPY 代码）----
# --no-deps：依赖已在上一层装好，避免 COPY 代码后重新解析依赖树、破坏分层缓存。
# -e（editable）：代码以路径方式生效，改代码只需重建容器、无需重装包。
# 本层不禁止联网（仅取构建后端，体积可忽略）：一旦加 --no-index 又会撞上
# 上面那条 PEP 517 构建隔离的坑。
COPY app ./app
COPY gunicorn.conf.py ./
RUN pip install --no-deps -e .

# 以非 root 运行：容器内进程被攻破时的爆炸半径更小。
# gunicorn 的 /dev/shm 心跳目录默认 world-writable，非 root 也能写，无需额外放权。
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser \
 && chown -R appuser:appuser /srv
USER appuser

EXPOSE 8000

# 探针：python:3.12-slim 没有 curl，用标准库 urllib 打存活端点
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/healthz/live', timeout=2)"

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app.main:app"]
