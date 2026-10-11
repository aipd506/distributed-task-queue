"""Dead-Letter Queue (DLQ) for Poison-Pill Task Isolation and Redrive.

Preserves failure trace history and provides safe redrive capabilities.
"""

from __future__ import annotations

import time
from typing import Any

from taskspool.queue import Task, TaskState


class TaskNotFoundInDLQError(Exception):
    """Raised when attempting to redrive a task that is not in the Dead-Letter Queue."""
    pass


class DeadLetterQueue:
    """Isolated store for poison-pill tasks that exceeded max retries."""

    def __init__(self) -> None:
        self._dlq_tasks: dict[str, Task] = {}

    def push(self, task: Task, error: str, traceback_str: str = "") -> None:
        """Isolates a failed task into the DLQ with full error history."""
        task.state = TaskState.DLQ
        task.error_history.append(
            {
                "timestamp": time.time(),
                "error": str(error),
                "traceback": traceback_str,
                "retry_count": task.retry_count,
            }
        )
        task.lease_worker = None
        task.lease_expires_at = 0.0
        self._dlq_tasks[task.task_id] = task

    def get_task(self, task_id: str) -> Task | None:
        return self._dlq_tasks.get(task_id)

    def list_tasks(self) -> list[Task]:
        return list(self._dlq_tasks.values())

    def count(self) -> int:
        return len(self._dlq_tasks)

    def redrive(
        self,
        task_id: str | None = None,
        all_tasks: bool = False,
        now: float | None = None,
    ) -> list[Task]:
        """Redrives specified or all DLQ tasks back into active PENDING state.

        Resets retry_count to 0 and clears lease.
        """
        current_time = time.time() if now is None else float(now)
        redriven: list[Task] = []

        if task_id:
            if task_id not in self._dlq_tasks:
                raise TaskNotFoundInDLQError(
                    f"Task '{task_id}' was not found in the Dead-Letter Queue."
                )
            target_ids = [task_id]
        elif all_tasks:
            target_ids = list(self._dlq_tasks.keys())
        else:
            raise ValueError("Must specify either task_id or all_tasks=True.")

        for tid in target_ids:
            task = self._dlq_tasks.pop(tid)
            task.state = TaskState.PENDING
            task.retry_count = 0
            task.run_at = current_time
            task.lease_worker = None
            task.lease_expires_at = 0.0
            redriven.append(task)

        return redriven
