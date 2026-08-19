"""httpc 共享客户端池：同参复用（keep-alive 连接池）/ 已关闭重建 / close_all 释放。

复用是提交链路性能前提：preflight 每次提交的 inspect/lease/freeze 控制面
调用若各自新建 AsyncClient，会逐请求付出完整 TCP+TLS 握手。
"""

import asyncio

import app.services.httpc as httpc


async def test_shared_client_reuses_instance_per_kwargs():
    a = httpc.shared_client(base_url="http://a.test", timeout=1)
    b = httpc.shared_client(base_url="http://a.test", timeout=1)
    c = httpc.shared_client(base_url="http://a.test", timeout=2)
    assert a is b                     # 同构造参数 → 同一实例（连接池复用）
    assert a is not c                 # 参数不同 → 独立池
    await httpc.close_all()
    assert a.is_closed and c.is_closed
    assert not httpc._shared


async def test_shared_client_rebuilds_after_close():
    a = httpc.shared_client(timeout=1)
    await a.aclose()
    b = httpc.shared_client(timeout=1)
    assert b is not a and not b.is_closed
    await httpc.close_all()


async def test_shared_client_concurrent_gather_single_instance():
    """并发 gather 下同参调用必须收敛到同一实例（函数内无 await，检查-赋值
    之间不会被事件循环切入，不存在双建竞争）。"""
    clients = await asyncio.gather(*(_wrap_shared_client(timeout=3) for _ in range(64)))
    assert len({id(c) for c in clients}) == 1
    assert not clients[0].is_closed
    await httpc.close_all()


async def _wrap_shared_client(**kwargs):
    # 包一层协程让 gather 真正并发调度（shared_client 本身是同步函数）
    return httpc.shared_client(**kwargs)


async def test_close_all_idempotent_and_tolerates_failing_client(monkeypatch):
    """close_all 幂等；单个 client aclose 抛错不影响其余释放与清表。"""
    a = httpc.shared_client(base_url="http://a.test", timeout=1)
    b = httpc.shared_client(base_url="http://b.test", timeout=1)

    async def _boom():
        raise RuntimeError("close boom")

    monkeypatch.setattr(a, "aclose", _boom)
    await httpc.close_all()            # 不抛出
    assert not httpc._shared           # 表已清
    assert b.is_closed                 # 其余客户端照常释放
    await httpc.close_all()            # 二次调用空转（幂等）


async def test_shared_client_rebuilds_after_close_all():
    """close_all 清表后，同参调用拿到全新实例（关闭期间不复用旧 client）。"""
    a = httpc.shared_client(timeout=1)
    await httpc.close_all()
    b = httpc.shared_client(timeout=1)
    assert b is not a and a.is_closed and not b.is_closed
    await httpc.close_all()
