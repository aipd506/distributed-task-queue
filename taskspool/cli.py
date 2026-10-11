"""CLI Entry Point for taskspool.

Subcommands: enqueue, worker, status, retry-dlq, wal-recover.
Exit codes: 0 = Success, 1 = Operational/Task failure, 2 = CLI argument error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from taskspool.dlq import DeadLetterQueue, TaskNotFoundInDLQError
from taskspool.queue import PriorityTaskQueue, Task, TaskState
from taskspool.wal import (
    WALCorruptError,
    WALFrame,
    WALRecordType,
    WriteAheadLog,
)
from taskspool.worker import TaskWorker


def _resolve_wal_path(wal_arg: str | None, wal_dir_arg: str | None) -> Path:
    if wal_arg:
        p = Path(wal_arg)
        if p.is_dir():
            return p / "tasks.wal"
        return p
    if wal_dir_arg:
        d = Path(wal_dir_arg)
        d.mkdir(parents=True, exist_ok=True)
        return d / "tasks.wal"
    return Path("tasks.wal")


def _replay_state(wal: WriteAheadLog) -> tuple[PriorityTaskQueue, DeadLetterQueue, dict[str, Task]]:
    """Replays the WAL into in-memory queue and DLQ."""
    queue = PriorityTaskQueue()
    dlq = DeadLetterQueue()
    tasks_by_id: dict[str, Task] = {}

    try:
        frames = list(wal.read_frames())
    except Exception:
        frames, _ = wal.recover_and_truncate()

    for frame in frames:
        p = frame.payload
        tid = p.get("task_id")
        rec_type = frame.record_type

        if rec_type == WALRecordType.TASK_ENQUEUE:
            task = Task.from_dict(p)
            tasks_by_id[task.task_id] = task
        elif tid in tasks_by_id:
            task = tasks_by_id[tid]
            if rec_type == WALRecordType.TASK_ACQUIRE:
                task.state = TaskState.RUNNING
                task.lease_worker = p.get("worker_id")
                task.lease_expires_at = float(p.get("lease_expires_at", 0.0))
            elif rec_type == WALRecordType.TASK_COMPLETE:
                task.state = TaskState.COMPLETED
                task.lease_worker = None
                task.lease_expires_at = 0.0
            elif rec_type == WALRecordType.TASK_RETRY:
                task.state = TaskState.RETRYING
                task.retry_count = int(p.get("retry_count", task.retry_count + 1))
                task.run_at = float(p.get("run_at", 0.0))
                task.lease_worker = None
                task.lease_expires_at = 0.0
            elif rec_type == WALRecordType.TASK_DLQ:
                task.state = TaskState.DLQ
                task.lease_worker = None
                task.lease_expires_at = 0.0

    now = time.time()
    for task in tasks_by_id.values():
        if task.state == TaskState.DLQ:
            dlq.push(task, error="Recovered from WAL")
        elif task.state == TaskState.RUNNING and task.lease_expires_at <= now:
            # Lease expired before crash; restore to PENDING
            task.state = TaskState.PENDING
            task.lease_worker = None
            task.lease_expires_at = 0.0
            queue.enqueue(task)
        elif task.state in (TaskState.PENDING, TaskState.RETRYING):
            queue.enqueue(task)

    return queue, dlq, tasks_by_id


def _print_output(data: Any, as_json: bool = False, text_fallback: str | None = None) -> None:
    if as_json:
        print(json.dumps(data, indent=2, default=str))
    elif text_fallback is not None:
        print(text_fallback)
    elif isinstance(data, dict):
        for k, v in data.items():
            print(f"{k}: {v}")
    elif isinstance(data, list):
        for item in data:
            print(item)
    else:
        print(str(data))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taskspool",
        description="Crash-Safe WAL Priority Task Queue with Worker Leases and DLQ",
    )
    parser.add_argument("--version", action="version", version="taskspool 0.1.0")

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # 1. enqueue
    enq_p = subparsers.add_parser("enqueue", help="Enqueue task into priority queue")
    enq_p.add_argument("--task", "--name", dest="task_name", required=True, help="Task name")
    enq_p.add_argument("--payload", default="{}", help="Task payload JSON string or file path")
    enq_p.add_argument("--priority", type=int, default=5, help="Priority (0 highest, 9 lowest)")
    enq_p.add_argument("--delay", type=float, default=0.0, help="Delay execution by N seconds")
    enq_p.add_argument("--max-retries", type=int, default=3, help="Maximum retry attempts")
    enq_p.add_argument("--wal", help="Path to WAL file")
    enq_p.add_argument("--wal-dir", help="Path to WAL directory")
    enq_p.add_argument("--json", action="store_true", help="Output as JSON")

    # 2. worker
    work_p = subparsers.add_parser("worker", help="Run worker to execute tasks")
    work_p.add_argument("--id", "--worker-id", dest="worker_id", help="Worker ID")
    work_p.add_argument("--concurrency", type=int, default=1, help="Concurrency workers")
    work_p.add_argument("--lease-timeout", type=float, default=30.0, help="Lease timeout seconds")
    work_p.add_argument("--max-tasks", type=int, default=10, help="Maximum tasks to execute")
    work_p.add_argument("--once", action="store_true", help="Run once then terminate")
    work_p.add_argument("--wal", help="Path to WAL file")
    work_p.add_argument("--wal-dir", help="Path to WAL directory")
    work_p.add_argument("--json", action="store_true", help="Output as JSON")

    # 3. status
    stat_p = subparsers.add_parser("status", help="Display queue and task status")
    stat_p.add_argument("--wal", help="Path to WAL file")
    stat_p.add_argument("--wal-dir", help="Path to WAL directory")
    stat_p.add_argument("--format", choices=["json", "table"], default="table", help="Output format")
    stat_p.add_argument("--json", action="store_true", help="Output as JSON")

    # 4. retry-dlq
    dlq_p = subparsers.add_parser("retry-dlq", help="Redrive tasks from Dead-Letter Queue")
    dlq_group = dlq_p.add_mutually_exclusive_group(required=True)
    dlq_group.add_argument("--task-id", help="Specific task ID to redrive")
    dlq_group.add_argument("--all", action="store_true", help="Redrive all DLQ tasks")
    dlq_p.add_argument("--wal", help="Path to WAL file")
    dlq_p.add_argument("--wal-dir", help="Path to WAL directory")
    dlq_p.add_argument("--json", action="store_true", help="Output as JSON")

    # 5. wal-recover
    rec_p = subparsers.add_parser("wal-recover", help="Recover and repair WAL log")
    rec_p.add_argument("--wal", help="Path to WAL file")
    rec_p.add_argument("--wal-dir", help="Path to WAL directory")
    rec_p.add_argument("--verify-only", action="store_true", help="Verify without modifying")
    rec_p.add_argument("--compact", action="store_true", help="Compact completed records")
    rec_p.add_argument("--json", action="store_true", help="Output as JSON")

    return parser


def handle_enqueue(args: argparse.Namespace) -> int:
    wal_path = _resolve_wal_path(args.wal, args.wal_dir)
    wal = WriteAheadLog(wal_path)

    # Parse payload
    payload_str = args.payload.strip()
    if os.path.exists(payload_str):
        payload_data = json.loads(Path(payload_str).read_text(encoding="utf-8"))
    else:
        try:
            payload_data = json.loads(payload_str)
        except json.JSONDecodeError:
            payload_data = {"data": payload_str}

    now = time.time()
    task = Task(
        task_id=str(uuid.uuid4()),
        task_name=args.task_name,
        payload=payload_data,
        priority=args.priority,
        max_retries=args.max_retries,
        state=TaskState.PENDING,
        enqueue_time=now,
        run_at=now + args.delay,
    )

    wal.append(WALRecordType.TASK_ENQUEUE, task.to_dict())
    _print_output(
        task.to_dict(),
        as_json=args.json,
        text_fallback=f"Enqueued task '{task.task_name}' (ID: {task.task_id}) with priority {task.priority}",
    )
    return 0


def handle_worker(args: argparse.Namespace) -> int:
    wal_path = _resolve_wal_path(args.wal, args.wal_dir)
    wal = WriteAheadLog(wal_path)
    queue, dlq, _ = _replay_state(wal)

    worker_id = args.worker_id or f"worker-{os.getpid()}"
    worker = TaskWorker(
        worker_id=worker_id,
        queue=queue,
        dlq=dlq,
        wal=wal,
        lease_timeout=args.lease_timeout,
    )

    max_tasks = 1 if args.once else args.max_tasks
    processed = worker.run_batch(max_tasks=max_tasks)

    # Check if any new tasks moved to DLQ during this execution
    has_dlq = dlq.count() > 0
    _print_output(
        {"worker_id": worker_id, "tasks_processed": processed, "dlq_count": dlq.count()},
        as_json=args.json,
        text_fallback=f"Worker {worker_id} processed {processed} task(s). DLQ size: {dlq.count()}",
    )
    return 1 if has_dlq else 0


def handle_status(args: argparse.Namespace) -> int:
    wal_path = _resolve_wal_path(args.wal, args.wal_dir)
    wal = WriteAheadLog(wal_path)
    queue, dlq, tasks = _replay_state(wal)

    counts = {s.value: 0 for s in TaskState}
    for t in tasks.values():
        st = t.state.value if isinstance(t.state, TaskState) else str(t.state)
        counts[st] = counts.get(st, 0) + 1

    summary = {
        "wal_path": str(wal_path),
        "total_tasks": len(tasks),
        "counts": counts,
        "queue_ready": queue.size(),
        "dlq_size": dlq.count(),
    }
    use_json = args.json or (args.format == "json")
    _print_output(summary, as_json=use_json)
    return 0


def handle_retry_dlq(args: argparse.Namespace) -> int:
    wal_path = _resolve_wal_path(args.wal, args.wal_dir)
    wal = WriteAheadLog(wal_path)
    queue, dlq, _ = _replay_state(wal)

    try:
        redriven = dlq.redrive(task_id=args.task_id, all_tasks=args.all)
        redriven_ids = []
        for t in redriven:
            wal.append(WALRecordType.TASK_ENQUEUE, t.to_dict())
            redriven_ids.append(t.task_id)

        _print_output(
            {"redriven_count": len(redriven_ids), "task_ids": redriven_ids},
            as_json=args.json,
            text_fallback=f"Redrove {len(redriven_ids)} tasks from DLQ: {', '.join(redriven_ids)}",
        )
        return 0
    except TaskNotFoundInDLQError as err:
        sys.stderr.write(f"Error: {err}\n")
        return 1


def handle_wal_recover(args: argparse.Namespace) -> int:
    wal_path = _resolve_wal_path(args.wal, args.wal_dir)
    wal = WriteAheadLog(wal_path)

    if args.verify_only:
        try:
            frames = list(wal.read_frames())
            _print_output(
                {"status": "VALID", "frame_count": len(frames), "wal_path": str(wal_path)},
                as_json=args.json,
                text_fallback=f"WAL is valid: {len(frames)} frames verified cleanly.",
            )
            return 0
        except Exception as err:
            _print_output(
                {"status": "CORRUPT", "error": str(err), "wal_path": str(wal_path)},
                as_json=args.json,
                text_fallback=f"WAL verification failed: {err}",
            )
            return 1
    else:
        verified_frames, truncated_bytes = wal.recover_and_truncate()
        _print_output(
            {
                "status": "RECOVERED",
                "verified_frames": len(verified_frames),
                "truncated_bytes": truncated_bytes,
                "wal_path": str(wal_path),
            },
            as_json=args.json,
            text_fallback=f"WAL recovered: {len(verified_frames)} valid frames kept, {truncated_bytes} bytes truncated.",
        )
        return 0


def main(args: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        parsed = parser.parse_args(args)
    except SystemExit as exc:
        return exc.code

    if parsed.subcommand == "enqueue":
        return handle_enqueue(parsed)
    elif parsed.subcommand == "worker":
        return handle_worker(parsed)
    elif parsed.subcommand == "status":
        return handle_status(parsed)
    elif parsed.subcommand == "retry-dlq":
        return handle_retry_dlq(parsed)
    elif parsed.subcommand == "wal-recover":
        return handle_wal_recover(parsed)

    return 2


if __name__ == "__main__":
    sys.exit(main())
