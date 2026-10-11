# distributed-task-queue (`taskspool`)

Crash-Safe Write-Ahead Log (WAL) Priority Task Queue with Worker Leases, Exponential Backoff with Full Jitter, and Dead-Letter Queue (DLQ) Redrive. Pure Python standard library implementation.

## Features

- **Crash-Resilient WAL**: 14-byte framed append-only log with IEEE 802.3 CRC32 checksums, transactional fsync flushing, and automatic sequential crash recovery replayer that safely handles truncated/corrupted writes.
- **Priority Task Scheduling**: Thread-safe min-heap queue with priorities 0 (highest) to 9 (lowest) and deferred timestamp scheduling (`run_at`). Disambiguated with monotonic sequence counters.
- **Worker Leases & Heartbeats**: Atomic lease acquisition, periodic heartbeat renewal, and automatic reclamation of tasks abandoned by crashed workers.
- **Exponential Backoff with Full Jitter**: Prevents thundering herds using randomized full jitter ($[0, \text{cap\_delay}]$) with overflow-safe exponents.
- **Dead-Letter Queue Isolation**: Poison pills that exceed maximum retry thresholds are quarantined with full exception stack traces, allowing targeted or bulk redrive.

## Installation

```bash
pip install -e .
```

## CLI Usage

### Enqueue Tasks
```bash
taskspool enqueue --name sync_users --payload '{"tenant_id": 42}' --priority 2 --wal-dir ./wal
taskspool enqueue --name send_digest --payload ./digest.json --delay 30 --wal-dir ./wal
```

### Run Worker
```bash
taskspool worker --wal-dir ./wal --id worker-1 --lease-timeout 15 --max-tasks 10
taskspool worker --wal-dir ./wal --once
```

### Inspect Queue Status
```bash
taskspool status --wal-dir ./wal
```

### DLQ Inspection & Redrive
```bash
# Redrive a specific failed task
taskspool retry-dlq --wal-dir ./wal --task-id 4a7b9c1d-8e2f-4a3b-9c1d-8e2f4a3b9c1d

# Redrive all quarantined tasks
taskspool retry-dlq --wal-dir ./wal --all
```

### WAL Integrity Verification & Repair
```bash
taskspool wal-recover --wal-dir ./wal --verify-only
taskspool wal-recover --wal-dir ./wal
```

## License

MIT License. Copyright (c) 2026 aipd506.
