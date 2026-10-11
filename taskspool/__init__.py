"""taskspool: Crash-Safe Write-Ahead Log Priority Task Queue with Worker Leases and DLQ.

Zero-dependency standard library implementation.
"""

__version__ = "0.1.0"

from taskspool.wal import (
    WALRecordType,
    WALFrame,
    WriteAheadLog,
    WALChecksumError,
    WALCorruptError,
)
from taskspool.queue import Task, TaskState, PriorityTaskQueue
from taskspool.worker import (
    calculate_backoff_delay,
    WorkerLeaseCoordinator,
    TaskWorker,
)
from taskspool.dlq import DeadLetterQueue, TaskNotFoundInDLQError

__all__ = [
    "WALRecordType",
    "WALFrame",
    "WriteAheadLog",
    "WALChecksumError",
    "WALCorruptError",
    "Task",
    "TaskState",
    "PriorityTaskQueue",
    "calculate_backoff_delay",
    "WorkerLeaseCoordinator",
    "TaskWorker",
    "DeadLetterQueue",
    "TaskNotFoundInDLQError",
]
