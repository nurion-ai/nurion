"""Lance sink implementation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
from lance.dataset import write_dataset

from solstice.core.models import Record
from solstice.operators.sinks.base import Sink


class LanceSink(Sink):
    """Sink that writes records to a Lance table."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.table_path = cfg.get("table_path")
        self.mode = cfg.get("mode", "append")
        self.buffer_size = cfg.get("buffer_size", 1000)

        if not self.table_path:
            raise ValueError("table_path is required for LanceSink")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer: List[Dict[str, Any]] = []
        self.table = None

    def open(self, context) -> None:
        super().open(context)
        Path(self.table_path).parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Initialized Lance sink: {self.table_path}")

    def write(self, record: Record) -> None:
        self.buffer.append(record.value)
        if len(self.buffer) >= self.buffer_size:
            self._flush()

    def close(self) -> None:
        self._flush()
        self.logger.info(f"Closed Lance sink: {self.table_path}")

    def _flush(self) -> None:
        if not self.buffer:
            return

        table = pa.Table.from_pylist(self.buffer)
        write_dataset(table, self.table_path, mode=self.mode if self.table is None else "append")
        if self.table is None:
            self.mode = "append"
        self.logger.info(f"Flushed {len(self.buffer)} records to Lance table")
        self.buffer.clear()
