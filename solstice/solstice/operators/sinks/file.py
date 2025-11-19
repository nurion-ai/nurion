"""File sink implementations."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SinkOperator


class FileSink(SinkOperator):
    """Sink that writes records to a local path."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.output_path = cfg.get("output_path")
        self.format = cfg.get("format", "json").lower()
        self.buffer_size = cfg.get("buffer_size", 1000)

        if not self.output_path:
            raise ValueError("output_path is required for FileSink")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer: List[Dict[str, Any]] = []
        self.file_handle = None
        self._initialized = False

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if batch is None:
            raise ValueError("FileSink requires a batch")
        self.buffer.extend(batch.to_pylist())
        if len(self.buffer) >= self.buffer_size:
            self._flush()
        return None

    def close(self) -> None:
        self._flush()
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
        if self._initialized:
            self.logger.info(f"Closed output file: {self.output_path}")
        self._initialized = False

    def _ensure_output_dir(self) -> None:
        output_dir = Path(self.output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return

        self._ensure_output_dir()
        if self.format == "json":
            self.file_handle = open(f"{self.output_path}/part-{self.worker_id}.{self.format}", "w")
        self._initialized = True
        self.logger.info(f"Opened output file: {self.output_path}")

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
            self.file_handle.write(json.dumps(record) + "\n")

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
