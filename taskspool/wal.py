"""Crash-Safe Write-Ahead Log (WAL) with 14-byte Framed Records and CRC32 Integrity.

Header Layout (14 Bytes):
+--------------------------------------------------------------------------+
| Magic (4B) | Version (1B) | Type (1B) | Length (4B) | CRC32 (4B) | Payload |
| 0x5453504C | 0x01         | 0x01..0x08| uint32 BE   | uint32 BE  | JSON... |
+--------------------------------------------------------------------------+
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Any, BinaryIO, Generator

MAGIC_TSPL = 0x5453504C  # 'TSPL'
MAGIC_WALT = 0x57414C54  # 'WALT'
VALID_MAGICS = {MAGIC_TSPL, MAGIC_WALT}
VERSION = 1
FRAME_HEADER_FORMAT = "!IBBII"
FRAME_HEADER_SIZE = struct.calcsize(FRAME_HEADER_FORMAT)  # 14 bytes


class WALRecordType(IntEnum):
    TASK_ENQUEUE = 0x01
    TASK_ACQUIRE = 0x02
    TASK_HEARTBEAT = 0x03
    TASK_COMPLETE = 0x04
    TASK_FAIL = 0x05
    TASK_RETRY = 0x06
    TASK_DLQ = 0x07
    CHECKPOINT = 0x08


class WALChecksumError(Exception):
    """Raised when CRC32 checksum verification fails on a WAL frame."""
    pass


class WALCorruptError(Exception):
    """Raised when a WAL frame contains an invalid header or truncated payload."""
    pass


@dataclass
class WALFrame:
    offset: int
    record_type: WALRecordType
    payload: dict[str, Any]
    payload_raw: bytes


class WriteAheadLog:
    """Append-only, crash-safe Write-Ahead Log manager."""

    def __init__(self, log_path: str | Path, auto_flush: bool = True) -> None:
        self.log_path = Path(log_path)
        self.auto_flush = auto_flush
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        # Ensure file exists
        if not self.log_path.exists():
            self.log_path.touch()

    def append(self, record_type: WALRecordType | int, payload: dict[str, Any]) -> int:
        """Appends a new frame with 14-byte header and CRC32 checksum. Returns start offset."""
        payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        crc32 = zlib.crc32(payload_bytes) & 0xFFFFFFFF
        rec_type_val = int(record_type)

        header = struct.pack(
            FRAME_HEADER_FORMAT,
            MAGIC_TSPL,
            VERSION,
            rec_type_val,
            len(payload_bytes),
            crc32,
        )

        with open(self.log_path, "a+b") as f:
            f.seek(0, os.SEEK_END)
            offset = f.tell()
            f.write(header)
            f.write(payload_bytes)
            if self.auto_flush:
                f.flush()
                os.fsync(f.fileno())

        return offset

    def read_frames(self) -> Generator[WALFrame, None, None]:
        """Sequentially reads and verifies all valid frames from the WAL."""
        if not self.log_path.exists():
            return

        with open(self.log_path, "rb") as f:
            while True:
                offset = f.tell()
                header_bytes = f.read(FRAME_HEADER_SIZE)
                if not header_bytes:
                    break  # Clean EOF

                if len(header_bytes) < FRAME_HEADER_SIZE:
                    raise WALCorruptError(
                        f"Truncated frame header at byte offset {offset} (read {len(header_bytes)} of {FRAME_HEADER_SIZE} bytes)."
                    )

                magic, ver, rec_type, payload_len, expected_crc = struct.unpack(
                    FRAME_HEADER_FORMAT, header_bytes
                )

                if magic not in VALID_MAGICS:
                    raise WALCorruptError(
                        f"Invalid magic {hex(magic)} at offset {offset}; expected {hex(MAGIC_TSPL)}."
                    )

                payload_bytes = f.read(payload_len)
                if len(payload_bytes) < payload_len:
                    raise WALCorruptError(
                        f"Truncated frame payload at byte offset {offset} (read {len(payload_bytes)} of {payload_len} bytes)."
                    )

                actual_crc = zlib.crc32(payload_bytes) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    raise WALChecksumError(
                        f"CRC32 mismatch at offset {offset}: expected {hex(expected_crc)}, got {hex(actual_crc)}."
                    )

                try:
                    payload_json = json.loads(payload_bytes.decode("utf-8"))
                except json.JSONDecodeError as err:
                    raise WALCorruptError(f"Malformed JSON payload at offset {offset}: {err}") from err

                yield WALFrame(
                    offset=offset,
                    record_type=WALRecordType(rec_type),
                    payload=payload_json,
                    payload_raw=payload_bytes,
                )

    def recover_and_truncate(self) -> tuple[list[WALFrame], int]:
        """Scans the WAL, keeps all verified frames, and cleanly truncates any partial/corrupted trailing write.

        Returns (verified_frames, truncated_bytes_count).
        """
        verified_frames: list[WALFrame] = []
        last_valid_offset = 0

        if not self.log_path.exists():
            return verified_frames, 0

        file_size = self.log_path.stat().st_size
        if file_size == 0:
            return verified_frames, 0

        with open(self.log_path, "rb") as f:
            while True:
                offset = f.tell()
                header_bytes = f.read(FRAME_HEADER_SIZE)
                if not header_bytes:
                    last_valid_offset = offset
                    break

                if len(header_bytes) < FRAME_HEADER_SIZE:
                    # Truncated header
                    last_valid_offset = offset
                    break

                magic, ver, rec_type, payload_len, expected_crc = struct.unpack(
                    FRAME_HEADER_FORMAT, header_bytes
                )

                if magic not in VALID_MAGICS:
                    last_valid_offset = offset
                    break

                payload_bytes = f.read(payload_len)
                if len(payload_bytes) < payload_len:
                    last_valid_offset = offset
                    break

                actual_crc = zlib.crc32(payload_bytes) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    last_valid_offset = offset
                    break

                try:
                    payload_json = json.loads(payload_bytes.decode("utf-8"))
                except Exception:
                    last_valid_offset = offset
                    break

                verified_frames.append(
                    WALFrame(
                        offset=offset,
                        record_type=WALRecordType(rec_type),
                        payload=payload_json,
                        payload_raw=payload_bytes,
                    )
                )
                last_valid_offset = f.tell()

        truncated_bytes = file_size - last_valid_offset
        if truncated_bytes > 0:
            with open(self.log_path, "r+b") as f:
                f.truncate(last_valid_offset)
                f.flush()
                os.fsync(f.fileno())

        return verified_frames, truncated_bytes
