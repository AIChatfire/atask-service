"""创建链路压测脚本（手动运行，不是 pytest 用例）。

    make bench TOKEN=sk-xxx                                                  # 默认 dry-run：只探活 + 打印计划
    make bench TOKEN=sk-xxx UPSTREAM_PATH=v1/tasks MODEL=your-model

提交目标形如 ``{base-url}/queue/{上游路径}``——本仓库对外只有 ``/queue/{上游路径}``
一种形态（ADR-010：异步转异步的统一中继；前缀叫 ``/queue`` 而非 ``/queue``，因为
``/queue/`` 在 nginx 上已归 stask-service）。

注意：**真实提交会创建真实任务并消耗上游真实额度**（配额由上游 relay 扣减，
网关本身零资金动作，不做任何预扣 / 结算）。本脚本因此默认只做 dry-run；要真正
打流量必须显式加 ``--execute``，且除非再加 ``--yes``，否则会在终端二次确认。

用法::

    .venv/bin/python scripts/bench_submit.py --token sk-xxx --execute
    .venv/bin/python scripts/bench_submit.py --token sk-xxx --execute --yes \
        --base-url https://dev.aapi.cn --path v1/tasks --model your-model \
        --concurrency 20 --requests 200

口径说明：

- 只压**创建接口的同步段**（受理 + 落库即返回 local task_id），不打探测/终态
  收敛链路——这是网关自身的同步段耗时，也是限流与幂等真正的竞争面。
- 每个成功请求都会创建一个真实任务并消耗上游真实额度。**跑完记得清理**；
  脚本结束会打印全部 task_id。
- 分位数用线性插值（与 numpy.percentile 默认口径一致），不引第三方依赖。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from typing import Any

import httpx

# 注意：path/model 只是占位符，跑之前必须替换为真实存在的上游路径与模型。
DEFAULT_BODY: dict[str, Any] = {"model": "your-model", "prompt": "bench", "duration": 5}


def _target_url(base_url: str, path: str) -> str:
    """受理 URL：``{base}/queue/{上游路径}``（对外唯一形态，见 ADR-010）。"""
    return f"{base_url.rstrip('/')}/queue/{path.lstrip('/')}"


def _percentile(samples: list[float], pct: float) -> float:
    """线性插值分位数；样本为空返回 0。"""
    if not samples:
        return 0.0
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * pct / 100.0
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


async def _one(
    client: httpx.AsyncClient,
    url: str,
    token: str,
    body: dict[str, Any],
    sem: asyncio.Semaphore,
    latencies: list[float],
    results: dict[str, Any],
) -> None:
    async with sem:
        started = time.perf_counter()
        try:
            resp = await client.post(
                url,
                json=body,
                headers={"Authorization": f"Bearer {token}"},
            )
        except Exception as exc:  # 网络层异常单独归类，不算服务端失败
            results["transport_error"] += 1
            results["last_error"] = f"{type(exc).__name__}: {exc}"
            return
        latencies.append((time.perf_counter() - started) * 1000.0)

        if resp.status_code == 202:
            results["accepted"] += 1
            payload = resp.json() if resp.content else {}
            task_id = payload.get("task_id")
            if task_id:
                results["task_ids"].append(task_id)
        elif resp.status_code == 429:
            results["throttled"] += 1
        else:
            results["failed"] += 1
            results["last_error"] = f"HTTP {resp.status_code}: {resp.text[:200]}"


async def _run(args: argparse.Namespace) -> int:
    url = _target_url(args.base_url, args.path)
    body = dict(DEFAULT_BODY)
    if args.model:
        body["model"] = args.model

    async with httpx.AsyncClient(timeout=args.timeout) as client:
        # 先探活：连不上就别打流量，免得把「地址填错」读成「容量不够」
        try:
            ready = await client.get(f"{args.base_url.rstrip('/')}/healthz/ready")
        except Exception as exc:
            print(f"探活失败（{type(exc).__name__}: {exc}）——检查 --base-url 与网络", file=sys.stderr)
            return 2
        if ready.status_code != 200:
            print(f"未就绪：/healthz/ready 返回 {ready.status_code} {ready.text[:200]}", file=sys.stderr)
            return 2
        print(f"探活 OK。目标 {url}  并发 {args.concurrency}  总量 {args.requests}")

        latencies: list[float] = []
        results: dict[str, Any] = {
            "accepted": 0,
            "throttled": 0,
            "failed": 0,
            "transport_error": 0,
            "task_ids": [],
            "last_error": "",
        }
        sem = asyncio.Semaphore(args.concurrency)
        started = time.perf_counter()
        await asyncio.gather(
            *(_one(client, url, args.token, body, sem, latencies, results) for _ in range(args.requests))
        )
        wall = time.perf_counter() - started

    ok = results["accepted"] + results["throttled"] + results["failed"] + results["transport_error"]
    print("\n===== 结果 =====")
    print(f"耗时          {wall:.2f}s（{ok / wall:.1f} req/s）")
    print(f"202 受理      {results['accepted']}")
    print(f"429 限流      {results['throttled']}")
    print(f"其他失败      {results['failed']}")
    print(f"传输层错误    {results['transport_error']}")
    print(
        "延迟 ms       "
        f"p50={_percentile(latencies, 50):.1f}  "
        f"p90={_percentile(latencies, 90):.1f}  "
        f"p99={_percentile(latencies, 99):.1f}  "
        f"max={max(latencies) if latencies else 0:.1f}"
    )
    if results["last_error"]:
        print(f"最后错误      {results['last_error']}")

    if results["task_ids"]:
        print(f"\n本次创建了 {len(results['task_ids'])} 个真实任务（已消耗上游额度，请核账/取消）：")
        for task_id in results["task_ids"]:
            print(f"  {task_id}")
        print(
            "\n取消示例（本地置 CANCELED + 尽力源头止损）：\n"
            f"  curl -X DELETE {_target_url(args.base_url, args.path)}/<task_id>"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="atask-service 创建链路压测")
    parser.add_argument("--token", required=True, help="new-api 用户令牌 sk-xxx")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    # 默认值只是占位符，跑之前必须替换为真实存在的上游路径与模型。
    parser.add_argument("--path", default="v1/tasks", help="上游原生路径（受理目标 /queue/{path}）")
    parser.add_argument("--model", default="your-model")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--execute", action="store_true", help="真正提交（会触发真实计费）")
    parser.add_argument("--yes", action="store_true", help="跳过交互确认")
    args = parser.parse_args()

    if not args.execute:
        print("dry-run：未提交任何请求。")
        print(f"将向 {_target_url(args.base_url, args.path)} 提交 {args.requests} 次")
        print(f"body={DEFAULT_BODY if not args.model else {**DEFAULT_BODY, 'model': args.model}}")
        print("\n确认无误后加 --execute 真正执行（会触发真实计费）。")
        return 0

    if not args.yes:
        print("[注意] 这会创建真实任务并消耗上游真实额度。")
        if input("输入 yes 继续：").strip().lower() != "yes":
            print("已取消。")
            return 1

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
