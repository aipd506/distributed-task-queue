"""Tests for taskspool CLI entry point and exit codes."""

import json
from taskspool.cli import main


def test_cli_help():
    try:
        rc = main(["--help"])
    except SystemExit as exc:
        rc = exc.code
    assert rc == 0


def test_cli_invalid_command():
    rc = main(["invalid_cmd"])
    assert rc == 2


def test_cli_enqueue_and_status(tmp_path, capsys):
    wal_dir = tmp_path / "wal"
    rc = main([
        "enqueue",
        "--name", "send_email",
        "--payload", '{"to": "alice@example.com"}',
        "--priority", "1",
        "--wal-dir", str(wal_dir),
        "--json",
    ])
    assert rc == 0
    captured = capsys.readouterr()
    task_data = json.loads(captured.out)
    assert task_data["task_name"] == "send_email"
    assert task_data["priority"] == 1

    # Check status
    rc_stat = main(["status", "--wal-dir", str(wal_dir), "--json"])
    assert rc_stat == 0
    captured_stat = capsys.readouterr()
    stat_data = json.loads(captured_stat.out)
    assert stat_data["total_tasks"] == 1
    assert stat_data["counts"]["PENDING"] == 1


def test_cli_worker_process(tmp_path, capsys):
    wal_dir = tmp_path / "wal"
    main([
        "enqueue",
        "--name", "task_1",
        "--payload", '{"should_fail": false}',
        "--wal-dir", str(wal_dir),
    ])

    rc = main(["worker", "--wal-dir", str(wal_dir), "--once"])
    assert rc == 0


def test_cli_wal_recover(tmp_path, capsys):
    wal_dir = tmp_path / "wal"
    main([
        "enqueue",
        "--name", "task_recover",
        "--wal-dir", str(wal_dir),
    ])
    capsys.readouterr()  # Clear stdout from enqueue

    rc = main(["wal-recover", "--wal-dir", str(wal_dir), "--verify-only", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["status"] == "VALID"
    assert data["frame_count"] >= 1


def test_cli_retry_dlq_missing_task(tmp_path):
    wal_dir = tmp_path / "wal"
    rc = main([
        "retry-dlq",
        "--wal-dir", str(wal_dir),
        "--task-id", "non-existent-uuid",
    ])
    assert rc == 1
