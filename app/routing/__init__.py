"""路由层（SPEC §3.1/§4.3；W1 全部文件）。

注册顺序即 Starlette 匹配顺序（不可妥协的不变量）：
healthz/callbacks/docs → /{biz}/v1/videos → catch-all 永远最后。
- ``videos``：new-api 兼容视频形态（提交/查询/content/remix，精确路由）。
- ``dynamic_router``：原生透传 catch-all ANY /{biz}/{native_path:path}。
"""
