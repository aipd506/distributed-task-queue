"""Tests for DeadLetterQueue: isolation, inspection, and redrive."""

import pytest
from taskspool.dlq import DeadLetterQueue, TaskNotFoundInDLQError
from taskspool.queue import Task, TaskState


def test_dlq_push_and_inspect():
    dlq = DeadLetterQueue()
    task = Task(task_id="bad_1", task_name="poison_pill", payload={}, retry_count=3)

    dlq.push(task, error="NullPointerException", traceback_str="Traceback...")
    assert dlq.count() == 1
    assert task.state == TaskState.DLQ

    fetched = dlq.get_task("bad_1")
    assert fetched is not None
    assert len(fetched.error_history) == 1
    assert fetched.error_history[0]["error"] == "NullPointerException"


def test_dlq_redrive_single():
    dlq = DeadLetterQueue()
    task = Task(task_id="bad_1", task_name="p", payload={}, retry_count=3, state=TaskState.DLQ)
    dlq._dlq_tasks["bad_1"] = task

    redriven = dlq.redrive(task_id="bad_1", now=1000.0)
    assert len(redriven) == 1
    assert redriven[0].task_id == "bad_1"
    assert redriven[0].state == TaskState.PENDING
    assert redriven[0].retry_count == 0
    assert redriven[0].run_at == 1000.0
    assert dlq.count() == 0


def test_dlq_redrive_all():
    dlq = DeadLetterQueue()
    t1 = Task(task_id="b1", task_name="p", payload={})
    t2 = Task(task_id="b2", task_name="p", payload={})
    dlq.push(t1, "err1")
    dlq.push(t2, "err2")

    redriven = dlq.redrive(all_tasks=True, now=2000.0)
    assert len(redriven) == 2
    assert dlq.count() == 0


def test_dlq_redrive_not_found():
    """Edge Case #25: Redriving a non-existent task_id must raise TaskNotFoundInDLQError."""
    dlq = DeadLetterQueue()
    with pytest.raises(TaskNotFoundInDLQError):
        dlq.redrive(task_id="non_existent_uuid")
