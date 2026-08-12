"""计费子系统（SPEC §3.5/§5；W3 全部文件）。

- ``client``：BillingServiceClient——freeze/settle/cancel/charge（计费服务契约，
  request_id={task_id}:{seq} 幂等，402/409 语义，透传用户 sk-）。
- ``pricing``：PricingLogic 获取三级缓存（L1 进程内/L2 Redis/L3 逻辑服务）+
  fail-closed 降级。
- ``sandbox``：asteval 安全求值（四层防御，子进程池 + rlimit + AST 预检）。
- ``outbox``：事务性 outbox 补偿 worker（退避重放、死信）。
- ``renewer``：FreezeRenewer 分片续期（request_id={task_id}:{seq}）。
- ``reconcile``：每日对账任务入口。
"""
