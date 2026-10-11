"""Tests for crash-safe Write-Ahead Log (WAL) and 14-byte framed CRC32 checks."""

import struct
import zlib
from pathlib import Path
import pytest
from taskspool.wal import (
    FRAME_HEADER_FORMAT,
    FRAME_HEADER_SIZE,
    MAGIC_TSPL,
    VERSION,
    WALChecksumError,
    WALCorruptError,
    WALRecordType,
    WriteAheadLog,
)


def test_wal_header_size():
    assert FRAME_HEADER_SIZE == 14
    assert struct.calcsize(FRAME_HEADER_FORMAT) == 14


def test_wal_append_and_read(tmp_path):
    wal_file = tmp_path / "test.wal"
    wal = WriteAheadLog(wal_file)

    offset1 = wal.append(WALRecordType.TASK_ENQUEUE, {"task_id": "1", "name": "task1"})
    offset2 = wal.append(WALRecordType.TASK_ACQUIRE, {"task_id": "1", "worker_id": "w1"})

    assert offset1 == 0
    assert offset2 > 0

    frames = list(wal.read_frames())
    assert len(frames) == 2
    assert frames[0].record_type == WALRecordType.TASK_ENQUEUE
    assert frames[0].payload["task_id"] == "1"
    assert frames[1].record_type == WALRecordType.TASK_ACQUIRE
    assert frames[1].payload["worker_id"] == "w1"


def test_wal_checksum_error(tmp_path):
    wal_file = tmp_path / "corrupt.wal"
    wal = WriteAheadLog(wal_file)
    wal.append(WALRecordType.TASK_ENQUEUE, {"task_id": "good"})

    # Tamper with 1 byte of the payload
    with open(wal_file, "r+b") as f:
        f.seek(FRAME_HEADER_SIZE + 2)
        f.write(b"X")

    with pytest.raises(WALChecksumError):
        list(wal.read_frames())


def test_wal_recovery_from_truncated_payload(tmp_path):
    """Edge Case #21: Process crashed while writing a payload; scanner truncates partial frame."""
    wal_file = tmp_path / "crash.wal"
    wal = WriteAheadLog(wal_file)

    wal.append(WALRecordType.TASK_ENQUEUE, {"task_id": "good_1"})
    wal.append(WALRecordType.TASK_ENQUEUE, {"task_id": "good_2"})

    good_size = wal_file.stat().st_size

    # Simulate an incomplete trailing write: 14 byte header claiming 100 bytes, but only 20 written
    payload_fake = b"truncated payload..."
    crc_fake = zlib.crc32(payload_fake) & 0xFFFFFFFF
    partial_header = struct.pack(
        FRAME_HEADER_FORMAT,
        MAGIC_TSPL,
        VERSION,
        int(WALRecordType.TASK_ENQUEUE),
        100,  # Claims 100 bytes
        crc_fake,
    )
    with open(wal_file, "ab") as f:
        f.write(partial_header)
        f.write(payload_fake)

    # Verification fails on truncated log
    with pytest.raises(WALCorruptError):
        list(wal.read_frames())

    # recover_and_truncate cleanly removes trailing incomplete write
    verified, truncated_bytes = wal.recover_and_truncate()
    assert len(verified) == 2
    assert truncated_bytes == (len(partial_header) + len(payload_fake))
    assert wal_file.stat().st_size == good_size

    # After recovery, read_frames succeeds
    recovered_frames = list(wal.read_frames())
    assert len(recovered_frames) == 2
