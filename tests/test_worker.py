"""Tests for worker leases, retry backoff with jitter, and task execution."""

from taskspool.dlq import DeadLetterQueue
from taskspool.queue import PriorityTaskQueue, Task, TaskState
from taskspool.worker import (
    TaskWorker,
    WorkerLeaseCoordinator,
    calculate_backoff_delay,
)


def test_exponential_backoff_jitter():
    # Attempt 0: delay in [0, 1.0]
    for _ in range(50):
        d0 = calculate_backoff_delay(0, base_delay=1.0, max_delay=300.0)
        assert 0.0 <= d0 <= 1.0

    # Attempt 3: delay in [0, 8.0]
    for _ in range(50):
        d3 = calculate_backoff_delay(3, base_delay=1.0, max_delay=300.0)
        assert 0.0 <= d3 <= 8.0


def test_exponential_backoff_overflow_and_cap():
    """Edge Case #24: High attempt counts (e.g. 50) must clamp to max_delay and not raise OverflowError."""
    delay = calculate_backoff_delay(50, base_delay=1.0, max_delay=300.0)
    assert 0.0 <= delay <= 300.0

    delay_huge = calculate_backoff_delay(1000, base_delay=1.0, max_delay=60.0)
    assert 0.0 <= delay_huge <= 60.0


def test_worker_lease_coordinator():
    coord = WorkerLeaseCoordinator(default_lease_timeout=30.0)
    now = 1000.0
    task = Task(task_id="t1", task_name="t", payload={}, state=TaskState.PENDING)

    # Acquire
    assert coord.acquire_lease(task, worker_id="w1", now=now) is True
    assert task.state == TaskState.RUNNING
    assert task.lease_worker == "w1"
    assert task.lease_expires_at == 1030.0

    # Renew
    assert coord.renew_lease(task, worker_id="w1", extension=20.0, now=1010.0) is True
    assert task.lease_expires_at == 1030.0

    # Unauthorized worker renew fails
    assert coord.renew_lease(task, worker_id="w2", now=1010.0) is False

    # Expiration reclamation (Edge Case #23)
    expired_list = coord.reclaim_expired([task], now=1050.0)
    assert len(expired_list) == 1
    assert expired_list[0].task_id == "t1"


def test_task_worker_execution_success():
    queue = PriorityTaskQueue()
    dlq = DeadLetterQueue()
    worker = TaskWorker(worker_id="w1", queue=queue, dlq=dlq)

    task = Task(task_id="t_ok", task_name="ok_task", payload={"should_fail": False})
    queue.enqueue(task)

    assert worker.process_one(now=1000.0) is True
    assert task.state == TaskState.COMPLETED
    assert dlq.count() == 0


def test_task_worker_retry_and_dlq():
    queue = PriorityTaskQueue()
    dlq = DeadLetterQueue()
    worker = TaskWorker(worker_id="w1", queue=queue, dlq=dlq)

    task = Task(
        task_id="t_fail",
        task_name="fail_task",
        payload={"should_fail": True, "error_message": "DB connection failed"},
        max_retries=2,
    )
    queue.enqueue(task)

    # Attempt 1 -> state RETRYING
    assert worker.process_one(now=1000.0) is True
    assert task.state == TaskState.RETRYING
    assert task.retry_count == 1
    assert dlq.count() == 0

    # Re-process for Attempt 2 -> exceeds max_retries -> moves to DLQ
    # Set run_at to now for pop_ready
    task.run_at = 2000.0
    assert worker.process_one(now=2000.0) is True
    assert task.state == TaskState.DLQ
    assert task.retry_count == 2
    assert dlq.count() == 1
    assert dlq.get_task("t_fail") is not None
