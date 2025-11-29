"""File sink implementations."""

from __future__ import annotations

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
    """Sink that writes records to a local path."""

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

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            raise ValueError("FileSink requires a payload")
        self.buffer.extend(payload.to_pylist())
        if len(self.buffer) >= self.buffer_size:
            self._flush()
        return None

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

        if self.format == "json":
            self._flush_json()
        elif self.format == "parquet":
            self._flush_parquet()
        elif self.format == "csv":
            self._flush_csv()
        else:
            raise ValueError(f"Unsupported format: {self.format}")

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
            "value": row,
        }

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
