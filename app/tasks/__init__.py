"""任务域（SPEC §3.3/§4）。

- ``models``：共享契约（骨架交付）——new-api ``tasks`` 表映射 + 状态枚举/映射 +
  ``gateway_`` 自有表模型。
- ``manager``（W2）：TaskManager——submit_task 提交全链路 + transition()
  status-CAS 唯一仲裁点 + 终态事务性 outbox 副作用。
- ``poller``（W2）：轮询 worker——扫 ``platform LIKE 'gw\\_%'`` 在途行
  （SKIP LOCKED 批量领取、next_poll_at 退避、deadline 收敛 timeout）。
"""
