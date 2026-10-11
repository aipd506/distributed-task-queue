"""Worker Lease Coordinator, Exponential Backoff with Jitter, and Task Execution Engine.
"""

from __future__ import annotations

import random
import threading
import time
import traceback
from typing import Any, Callable

from taskspool.dlq import DeadLetterQueue
from taskspool.queue import PriorityTaskQueue, Task, TaskState
from taskspool.wal import WALRecordType, WriteAheadLog


def calculate_backoff_delay(
    retry_count: int,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    multiplier: float = 2.0,
) -> float:
    """Calculates retry backoff delay with randomized full jitter to prevent thundering herds.

    cap_delay = min(max_delay, base_delay * (multiplier ** min(retry_count, 30)))
    sleep_delay = random.uniform(0.0, cap_delay)
    """
    safe_exponent = min(max(0, retry_count), 30)
    cap_delay = min(max_delay, base_delay * (multiplier**safe_exponent))
    return random.uniform(0.0, cap_delay)


class WorkerLeaseCoordinator:
    """Coordinates worker leases, renewals, and expiration reclamation."""

    def __init__(self, default_lease_timeout: float = 30.0) -> None:
        self.default_lease_timeout = default_lease_timeout

    def acquire_lease(
        self,
        task: Task,
        worker_id: str,
        lease_duration: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Attempts to acquire a lease on a task."""
        current_time = time.time() if now is None else float(now)
        duration = lease_duration or self.default_lease_timeout

        can_acquire = False
        if task.state in (TaskState.PENDING, TaskState.RETRYING):
            can_acquire = True
        elif task.state == TaskState.RUNNING and task.lease_expires_at <= current_time:
            # Lease expired; reclaimable
            can_acquire = True

        if can_acquire:
            task.state = TaskState.RUNNING
            task.lease_worker = worker_id
            task.lease_expires_at = current_time + duration
            return True
        return False

    def renew_lease(
        self,
        task: Task,
        worker_id: str,
        extension: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Extends the lease expiration for an actively owned task."""
        current_time = time.time() if now is None else float(now)
        ext = extension or self.default_lease_timeout

        if task.state == TaskState.RUNNING and task.lease_worker == worker_id:
            task.lease_expires_at = current_time + ext
            return True
        return False

    def release_lease(self, task: Task, worker_id: str) -> bool:
        """Releases the lease on a task."""
        if task.lease_worker == worker_id:
            task.lease_worker = None
            task.lease_expires_at = 0.0
            return True
        return False

    def reclaim_expired(
        self,
        tasks: list[Task],
        now: float | None = None,
    ) -> list[Task]:
        """Finds running tasks whose leases have expired."""
        current_time = time.time() if now is None else float(now)
        reclaimed: list[Task] = []
        for t in tasks:
            if t.state == TaskState.RUNNING and t.lease_expires_at <= current_time:
                reclaimed.append(t)
        return reclaimed


class TaskWorker:
    """Worker process / runner that acquires, executes, and finalizes tasks."""

    def __init__(
        self,
        worker_id: str,
        queue: PriorityTaskQueue,
        dlq: DeadLetterQueue,
        wal: WriteAheadLog | None = None,
        lease_coordinator: WorkerLeaseCoordinator | None = None,
        handler: Callable[[Task], Any] | None = None,
        lease_timeout: float = 30.0,
    ) -> None:
        self.worker_id = worker_id
        self.queue = queue
        self.dlq = dlq
        self.wal = wal
        self.lease_coord = lease_coordinator or WorkerLeaseCoordinator(lease_timeout)
        self.handler = handler or self._default_handler
        self.lease_timeout = lease_timeout
        self.stop_event = threading.Event()

    def _default_handler(self, task: Task) -> Any:
        """Default handler: checks if task payload has 'fail'=True for testing."""
        if task.payload.get("should_fail"):
            raise RuntimeError(task.payload.get("error_message", "Simulated failure"))
        return {"status": "ok", "task_id": task.task_id}

    def process_one(self, now: float | None = None) -> bool:
        """Attempts to pop, lease, execute, and record one ready task. Returns True if a task was processed."""
        current_time = time.time() if now is None else float(now)
        task = self.queue.pop_ready(now=current_time)
        if not task:
            return False

        # Acquire lease
        if not self.lease_coord.acquire_lease(task, self.worker_id, self.lease_timeout, now=current_time):
            # Could not acquire; put back
            self.queue.enqueue(task)
            return False

        if self.wal:
            self.wal.append(
                WALRecordType.TASK_ACQUIRE,
                {
                    "task_id": task.task_id,
                    "worker_id": self.worker_id,
                    "lease_expires_at": task.lease_expires_at,
                },
            )

        try:
            result = self.handler(task)
            task.state = TaskState.COMPLETED
            task.lease_worker = None
            task.lease_expires_at = 0.0

            if self.wal:
                self.wal.append(
                    WALRecordType.TASK_COMPLETE,
                    {"task_id": task.task_id, "result": result},
                )
            return True

        except Exception as exc:
            tb_str = traceback.format_exc()
            task.retry_count += 1

            if task.retry_count >= task.max_retries:
                # Move to DLQ
                self.dlq.push(task, error=str(exc), traceback_str=tb_str)
                if self.wal:
                    self.wal.append(
                        WALRecordType.TASK_DLQ,
                        {
                            "task_id": task.task_id,
                            "error": str(exc),
                            "retry_count": task.retry_count,
                        },
                    )
            else:
                # Schedule retry with backoff jitter
                delay = calculate_backoff_delay(task.retry_count)
                task.state = TaskState.RETRYING
                task.run_at = current_time + delay
                task.lease_worker = None
                task.lease_expires_at = 0.0
                self.queue.enqueue(task)

                if self.wal:
                    self.wal.append(
                        WALRecordType.TASK_RETRY,
                        {
                            "task_id": task.task_id,
                            "retry_count": task.retry_count,
                            "run_at": task.run_at,
                            "error": str(exc),
                        },
                    )
            return True

    def run_batch(self, max_tasks: int = 10, now: float | None = None) -> int:
        """Processes up to max_tasks from the queue."""
        processed = 0
        while processed < max_tasks:
            if not self.process_one(now=now):
                break
            processed += 1
        return processed
