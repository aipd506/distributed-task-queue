"""Tests for PriorityTaskQueue: min-heap ordering, deferred scheduling, tie-breaking."""

import threading
import time
from taskspool.queue import PriorityTaskQueue, Task, TaskState


def test_priority_ordering():
    queue = PriorityTaskQueue()
    t_low = Task(task_id="low", task_name="t", payload={}, priority=9)
    t_high = Task(task_id="high", task_name="t", payload={}, priority=0)
    t_med = Task(task_id="med", task_name="t", payload={}, priority=5)

    queue.enqueue(t_low)
    queue.enqueue(t_high)
    queue.enqueue(t_med)

    popped1 = queue.pop_ready(now=time.time())
    assert popped1 is not None and popped1.task_id == "high"

    popped2 = queue.pop_ready(now=time.time())
    assert popped2 is not None and popped2.task_id == "med"

    popped3 = queue.pop_ready(now=time.time())
    assert popped3 is not None and popped3.task_id == "low"


def test_priority_tie_breaking():
    """Edge Case #22: Identical priority & run_at must disambiguate without comparing dicts."""
    queue = PriorityTaskQueue()
    t1 = Task(task_id="t1", task_name="a", payload={"data": 123}, priority=5, run_at=100.0)
    t2 = Task(task_id="t2", task_name="b", payload={"data": 456}, priority=5, run_at=100.0)

    queue.enqueue(t1)
    queue.enqueue(t2)

    p1 = queue.pop_ready(now=200.0)
    p2 = queue.pop_ready(now=200.0)
    assert p1 is not None and p2 is not None
    assert {p1.task_id, p2.task_id} == {"t1", "t2"}


def test_deferred_scheduling():
    queue = PriorityTaskQueue()
    now = 1000.0
    future_task = Task(task_id="future", task_name="f", payload={}, priority=1, run_at=now + 60)
    immediate_task = Task(task_id="imm", task_name="i", payload={}, priority=5, run_at=now)

    queue.enqueue(future_task)
    queue.enqueue(immediate_task)

    # At now = 1000, only immediate task should pop
    popped = queue.pop_ready(now=now)
    assert popped is not None and popped.task_id == "imm"

    # Future task is not ready
    assert queue.pop_ready(now=now) is None

    # At now = 1060, future task is ready
    popped_later = queue.pop_ready(now=now + 60)
    assert popped_later is not None and popped_later.task_id == "future"


def test_thread_safety_concurrency():
    queue = PriorityTaskQueue()
    num_tasks = 200

    def enqueuer():
        for i in range(num_tasks):
            t = Task(task_id=f"t_{threading.get_ident()}_{i}", task_name="task", payload={}, priority=i % 10)
            queue.enqueue(t)

    threads = [threading.Thread(target=enqueuer) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert queue.size() == num_tasks * 4
