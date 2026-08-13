FROM python:3.12-slim

WORKDIR /srv

# 依赖钉版在 pyproject.toml（SPEC §2 版本纪律；requirements.txt 已删除）
COPY . .
RUN pip install --no-cache-dir .

EXPOSE 8000
CMD ["gunicorn", "-c", "gunicorn.conf.py", "app.main:app"]
