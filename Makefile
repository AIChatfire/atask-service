.DEFAULT_GOAL := help
PY := .venv/bin/python
SHELL := /bin/bash

# 参与静态检查的全部路径（与 CI 保持同一份清单，避免「本地绿、CI 红」）
LINT_PATHS := app tests scripts gunicorn.conf.py

.PHONY: help setup check test lint type fix run worker scheduler standalone \
        up down logs build clean bench admin-token

help:  ## 显示可用命令
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk -F':.*?## ' '{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup:  ## 建虚拟环境、装依赖、生成 .env
	@command -v uv >/dev/null 2>&1 || { echo "需要 uv：curl -LsSf https://astral.sh/uv/install.sh | sh"; exit 1; }
	uv venv --python 3.12 .venv
	uv pip install -e ".[dev]" --python $(PY)
	@test -f .env || { cp .env.example .env && echo "已生成 .env，请填 DATABASE_URL / REDIS_URL / CALLBACK_SIGN_SECRET / UPSTREAM_BASE_URL / UPSTREAM_ALLOWLIST"; }

check: lint type test  ## 三项门禁全跑（提交前必须绿）

lint:  ## ruff 静态检查
	$(PY) -m ruff check $(LINT_PATHS)

type:  ## mypy 类型检查
	$(PY) -m mypy app/

test:  ## 运行测试套件（不依赖 MySQL / Redis / 上游 / 微服务）
	$(PY) -m pytest tests/ -q

fix:  ## ruff 自动修复
	$(PY) -m ruff check $(LINT_PATHS) --fix

run:  ## 本地起 web（需另开 worker）
	$(PY) -m gunicorn -c gunicorn.conf.py app.main:app

worker:  ## 本地起 worker（上游提交/探测/通知/sweep）
	.venv/bin/taskiq worker app.queue:broker --max-async-tasks 10240

scheduler:  ## 本地起 scheduler（延迟派发 + 每分钟 sweep；必须单副本）
	# --update-interval 1 让「延迟派发」有秒级精度：攒批的 T 触发走 schedule_by_time，
	# scheduler 默认按分钟对点唤醒（taskiq 0.11 的 run_scheduler_loop），不设它会让
	# batch_wait 的实际放行最坏晚 60s（正确性由 sweep 的超期兜底保证，只是慢）。
	.venv/bin/taskiq scheduler app.queue:scheduler --update-interval 1

standalone:  ## 本地单进程起全套（web + worker + scheduler，免 .env 也可跑）
	$(PY) -m app.standalone

up:  ## compose 起全套（gateway + worker + redis + 看板）
	docker compose up -d --build
	@echo "等待就绪..." && sleep 3
	@curl -sf http://127.0.0.1:8000/healthz/ready | head -c 400 || echo "尚未就绪，看 make logs"

down:  ## 停止并移除容器
	docker compose down

logs:  ## 跟随日志
	docker compose logs -f --tail=100

build:  ## 构建镜像
	docker build -t atask-service:local .

# 变量名**不能用 PATH**：make 会从环境继承 PATH，`$(PATH)` 会展开成整条
# 系统 PATH（实测把 --path 传成一长串目录）。用 UPSTREAM_PATH。
bench:  ## 压测创建链路（需 TOKEN=sk-xxx [UPSTREAM_PATH=v1/tasks MODEL=your-model]）
	@test -n "$(TOKEN)" || { echo "用法：make bench TOKEN=sk-xxx [UPSTREAM_PATH=v1/tasks MODEL=your-model]"; exit 1; }
	$(PY) scripts/bench_submit.py --token $(TOKEN) \
		$(if $(UPSTREAM_PATH),--path $(UPSTREAM_PATH),) $(if $(MODEL),--model $(MODEL),)

admin-token:  ## 生成 /ops 与 /admin 的管理令牌并写入 .env
	@token=$$($(PY) -c "import secrets;print(secrets.token_urlsafe(32))"); \
	if grep -q '^ADMIN_TOKEN=' .env 2>/dev/null; then \
		sed -i.bak "s|^ADMIN_TOKEN=.*|ADMIN_TOKEN=$$token|" .env && rm -f .env.bak; \
	else echo "ADMIN_TOKEN=$$token" >> .env; fi; \
	echo "ADMIN_TOKEN=$$token"

clean:  ## 清理构建与缓存产物
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
