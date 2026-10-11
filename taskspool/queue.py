"""Thread-Safe Priority Task Queue with Deferred Scheduling.

Min-heap ordered by (priority, run_at, counter) with monotonic tie-breaker.
"""

from __future__ import annotations

import heapq
import itertools
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DLQ = "DLQ"


@dataclass
class Task:
    task_id: str
    task_name: str
    payload: dict[str, Any]
    priority: int = 5  # 0 (Critical/Highest) to 9 (Lowest)
    max_retries: int = 3
    retry_count: int = 0
    state: TaskState = TaskState.PENDING
    enqueue_time: float = 0.0
    run_at: float = 0.0
    lease_worker: str | None = None
    lease_expires_at: float = 0.0
    error_history: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "payload": self.payload,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "state": self.state.value if isinstance(self.state, TaskState) else str(self.state),
            "enqueue_time": self.enqueue_time,
            "run_at": self.run_at,
            "lease_worker": self.lease_worker,
            "lease_expires_at": self.lease_expires_at,
            "error_history": self.error_history,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Task:
        raw_state = data.get("state", TaskState.PENDING.value)
        state_enum = TaskState(raw_state) if isinstance(raw_state, str) else raw_state
        return cls(
            task_id=data.get("task_id", str(uuid.uuid4())),
            task_name=data.get("task_name", "unnamed_task"),
            payload=data.get("payload", {}),
            priority=int(data.get("priority", 5)),
            max_retries=int(data.get("max_retries", 3)),
            retry_count=int(data.get("retry_count", 0)),
            state=state_enum,
            enqueue_time=float(data.get("enqueue_time", 0.0)),
            run_at=float(data.get("run_at", 0.0)),
            lease_worker=data.get("lease_worker"),
            lease_expires_at=float(data.get("lease_expires_at", 0.0)),
            error_history=data.get("error_history", []),
        )


class PriorityTaskQueue:
    """Thread-safe priority min-heap queue with deferred scheduling."""

    def __init__(self) -> None:
        self._heap: list[tuple[int, float, int, str]] = []
        self._tasks: dict[str, Task] = {}
        self._lock = threading.RLock()
        self._counter = itertools.count()

    def enqueue(self, task: Task) -> None:
        """Enqueues or re-enqueues a task into the min-heap."""
        with self._lock:
            self._tasks[task.task_id] = task
            count = next(self._counter)
            # Tuple: (priority, run_at, count, task_id) prevents Task comparison
            heapq.heappush(self._heap, (task.priority, task.run_at, count, task.task_id))

    def pop_ready(self, now: float | None = None) -> Task | None:
        """Pops and returns the highest priority task ready for execution (run_at <= now).

        Skips tasks not in PENDING or RETRYING states.
        """
        current_time = time.time() if now is None else float(now)
        with self._lock:
            temp_skipped: list[tuple[int, float, int, str]] = []
            ready_task: Task | None = None

            while self._heap:
                prio, run_at, count, task_id = heapq.heappop(self._heap)
                task = self._tasks.get(task_id)

                if not task:
                    continue  # Task was removed

                if task.state not in (TaskState.PENDING, TaskState.RETRYING):
                    continue  # Task is RUNNING, COMPLETED, or DLQ

                if run_at > current_time:
                    # Deferred task not ready yet; skip and continue searching
                    temp_skipped.append((prio, run_at, count, task_id))
                    continue

                # Found ready task
                ready_task = task
                break

            for item in temp_skipped:
                heapq.heappush(self._heap, item)

            return ready_task

    def peek(self, now: float | None = None) -> Task | None:
        """Returns the task at the top of the heap ready for execution without popping."""
        current_time = time.time() if now is None else float(now)
        with self._lock:
            temp_skipped: list[tuple[int, float, int, str]] = []
            peeked_task: Task | None = None

            while self._heap:
                prio, run_at, count, task_id = heapq.heappop(self._heap)
                task = self._tasks.get(task_id)
                if not task or task.state not in (TaskState.PENDING, TaskState.RETRYING):
                    continue
                if run_at > current_time:
                    temp_skipped.append((prio, run_at, count, task_id))
                    continue
                peeked_task = task
                temp_skipped.append((prio, run_at, count, task_id))
                break

            for item in temp_skipped:
                heapq.heappush(self._heap, item)

            return peeked_task

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            return self._tasks.get(task_id)

    def update_task(self, task: Task) -> None:
        with self._lock:
            self._tasks[task.task_id] = task

    def remove_task(self, task_id: str) -> Task | None:
        with self._lock:
            return self._tasks.pop(task_id, None)

    def size(self) -> int:
        with self._lock:
            return len(self._tasks)

    def counts_by_state(self) -> dict[str, int]:
        with self._lock:
            counts = {s.value: 0 for s in TaskState}
            for task in self._tasks.values():
                val = task.state.value if isinstance(task.state, TaskState) else str(task.state)
                counts[val] = counts.get(val, 0) + 1
            return counts
