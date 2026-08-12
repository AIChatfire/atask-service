"""骨架冒烟测试：共享契约层可导入（SPEC §6 集成冒烟标准的 pytest 形态）。"""


def test_shared_contracts_importable() -> None:
    import app.config
    import app.db
    import app.errors
    import app.http_clients
    import app.redis_client
    import app.registry
    import app.schemas  # noqa: F401
    from app.adapters import base  # noqa: F401
    from app.tasks import models  # noqa: F401


def test_status_mapping_contract() -> None:
    """状态映射与架构 §4.1 映射表逐行一致（跨模块共享口径）。"""
    from app.tasks.models import (
        TaskStatus,
        db_progress,
        db_status,
        db_to_internal,
        new_task_id,
        platform_for,
        to_video_status,
    )

    assert db_status(TaskStatus.QUEUED) == "SUBMITTED"
    assert db_progress(TaskStatus.QUEUED) == "10%"
    assert db_status(TaskStatus.RUNNING) == "IN_PROGRESS"
    assert db_status(TaskStatus.SUCCEEDED) == "SUCCESS"
    assert db_status(TaskStatus.TIMEOUT) == "FAILURE"
    assert to_video_status("SUBMITTED") == "queued"
    assert to_video_status("IN_PROGRESS") == "in_progress"
    assert to_video_status("SUCCESS") == "completed"
    assert to_video_status("FAILURE") == "failed"
    assert db_to_internal("SUBMITTED") is TaskStatus.QUEUED
    assert platform_for("kling") == "gw_kling"
    tid = new_task_id()
    assert tid.startswith("task_") and len(tid) == 37


def test_tasks_model_columns_match_brief_c() -> None:
    """tasks 表模型列集与简报 C §一 逐列一致（含 `group` 保留字处理）。"""
    from app.tasks.models import Task

    cols = {c.name for c in Task.__table__.columns}
    expected = {
        "id", "created_at", "updated_at", "task_id", "platform", "user_id",
        "group", "channel_id", "quota", "action", "status", "fail_reason",
        "submit_time", "start_time", "finish_time", "progress",
        "properties", "private_data", "data",
    }
    assert cols == expected
