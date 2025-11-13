"""File sink implementations."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from solstice.core.models import Record
from solstice.operators.sinks.base import Sink


class FileSink(Sink):
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
        self.buffer: List[Record] = []
        self.file_handle = None

    def open(self, context) -> None:
        super().open(context)
        output_dir = Path(self.output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        if self.format == "json":
            self.file_handle = open(self.output_path, "w")

        self.logger.info(f"Opened output file: {self.output_path}")

    def write(self, record: Record) -> None:
        self.buffer.append(record)
        if len(self.buffer) >= self.buffer_size:
            self._flush()

    def close(self) -> None:
        self._flush()
        if self.file_handle:
            self.file_handle.close()
        self.logger.info(f"Closed output file: {self.output_path}")

    def _flush(self) -> None:
        if not self.buffer:
            return

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
            raise RuntimeError("JSON sink not opened")

        for record in self.buffer:
            payload = {
                "key": record.key,
                "value": record.value,
                "timestamp": record.timestamp,
                "metadata": record.metadata,
            }
            self.file_handle.write(json.dumps(payload) + "\n")

    def _flush_parquet(self) -> None:
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

