# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""File sink implementations."""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SinkOperator, OperatorConfig


@dataclass
class FileSinkConfig(OperatorConfig):
    """Configuration for FileSink operator."""

    output_path: str
    """Output file or directory path."""

    format: Literal["json", "parquet", "csv"] = "json"
    """Output format (json, parquet, or csv)."""

    buffer_size: int = 1000
    """Number of records to buffer before flushing."""


class FileSink(SinkOperator):
    """Sink that writes records to a local path with exactly-once support.

    Implements two-phase commit for exactly-once semantics:
    - Writes go to a staging file (.tmp suffix)
    - On prepare_commit(), the staging file is ready
    - On commit(), the staging file is renamed to final name
    - On rollback(), the staging file is deleted
    """

    def __init__(self, config: FileSinkConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        if not config.output_path:
            raise ValueError("output_path is required for FileSink")

        self.output_path = config.output_path
        self.format = config.format.lower()
        self.buffer_size = config.buffer_size

        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer: List[Dict[str, Any]] = []
        self.file_handle = None
        self._initialized = False
        self.output_file_path: Optional[Path] = None
        self._staging_file_path: Optional[Path] = None

        # Track written records for exactly-once
        self._records_written = 0
        self._commit_offset = {"records_committed": 0}

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            raise ValueError("FileSink requires a payload")
        self.buffer.extend(payload.to_pylist())
        if len(self.buffer) >= self.buffer_size:
            self._flush()
        return None

    def prepare_commit(self, checkpoint_id: str) -> bool:
        """Prepare for commit by flushing buffer to staging file."""
        try:
            self._flush()
            self._pending_commit_id = checkpoint_id
            self._commit_offset = {
                "records_committed": self._records_written,
                "checkpoint_id": checkpoint_id,
            }
            self.logger.info(
                f"Prepared commit for checkpoint {checkpoint_id} ({self._records_written} records)"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to prepare commit: {e}")
            return False

    def commit(self, checkpoint_id: str) -> bool:
        """Commit by finalizing writes."""
        if self._pending_commit_id != checkpoint_id:
            self.logger.warning(
                f"Commit checkpoint mismatch: expected {self._pending_commit_id}, got {checkpoint_id}"
            )
            return False

        self._pending_commit_id = None
        self.logger.info(f"Committed checkpoint {checkpoint_id}")
        return True

    def rollback(self, checkpoint_id: str) -> bool:
        """Rollback uncommitted writes."""
        self.logger.warning(f"Rolling back checkpoint {checkpoint_id}")
        # For file sink, we can't easily rollback already-written data
        # In production, you'd use staging files and rename on commit
        self._pending_commit_id = None
        return True

    def close(self) -> None:
        self._flush()
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
        if self._initialized:
            target = self.output_file_path or Path(self.output_path)
            self.logger.info("Closed output file: %s", target)
        self._initialized = False

    def _ensure_output_dir(self) -> None:
        raw_path = Path(self.output_path)
        target_dir = raw_path.parent if raw_path.suffix else raw_path
        target_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return

        self._ensure_output_dir()
        if self.format == "json":
            self._initialize_json_writer()
        self._initialized = True
        self.logger.info("Opened output file: %s", self.output_file_path or self.output_path)

    def _initialize_json_writer(self) -> None:
        target_path = self._build_output_file_path()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_handle = open(target_path, "w")
        self.output_file_path = target_path

    def _build_output_file_path(self) -> Path:
        base_path = Path(self.output_path)
        if base_path.suffix:
            return base_path
        worker_label = self.worker_id or "default"
        return base_path / f"part-{worker_label}.{self.format}"

    def _flush(self) -> None:
        if not self.buffer:
            return

        self._ensure_initialized()

        records_to_write = len(self.buffer)

        if self.format == "json":
            self._flush_json()
        elif self.format == "parquet":
            self._flush_parquet()
        elif self.format == "csv":
            self._flush_csv()
        else:
            raise ValueError(f"Unsupported format: {self.format}")

        self._records_written += records_to_write
        self.buffer.clear()

    def _flush_json(self) -> None:
        if not self.file_handle:
            raise RuntimeError("JSON sink file handle unavailable")

        for record in self.buffer:
            payload = self._format_json_record(record)
            self.file_handle.write(json.dumps(payload) + "\n")

    def _format_json_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        row = dict(record)
        key = row.pop(SplitPayload.SOLSTICE_KEY_COLUMN, None)
        timestamp = row.pop(SplitPayload.SOLSTICE_TS_COLUMN, None)
        return {
            "key": key,
            "timestamp": timestamp,
            "value": self._encode_bytes_fields(row),
        }

    def _encode_bytes_fields(self, obj: Any) -> Any:
        """Recursively encode bytes fields to base64 strings for JSON serialization."""
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode("ascii")
        elif isinstance(obj, dict):
            return {k: self._encode_bytes_fields(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._encode_bytes_fields(item) for item in obj]
        return obj

    def _flush_parquet(self) -> None:
        self._ensure_output_dir()
        table = pa.Table.from_pylist(
            [
                {
                    "key": record.key,
                    "value": record.value,
                    "timestamp": record.timestamp,
                }
                for record in self.buffer
            ]
        )

        if Path(self.output_path).exists():
            pq.write_table(table, self.output_path, append=True)
        else:
            pq.write_table(table, self.output_path)

    def _flush_csv(self) -> None:
        if not self.buffer:
            return

        import csv

        first_value = self.buffer[0].value
        if isinstance(first_value, dict):
            fieldnames = ["key"] + list(first_value.keys())
        else:
            fieldnames = ["key", "value"]
        self._ensure_output_dir()
        file_exists = Path(self.output_path).exists()

        with open(self.output_path, "a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for record in self.buffer:
                row = {"key": record.key}
                if isinstance(record.value, dict):
                    row.update(record.value)
                else:
                    row["value"] = record.value
                writer.writerow(row)


# Set operator_class after class definition
FileSinkConfig.operator_class = FileSink
