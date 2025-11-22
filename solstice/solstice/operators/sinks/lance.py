"""Lance sink implementation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
from lance.dataset import write_dataset

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SinkOperator

class LanceSink(SinkOperator):
    """Sink that writes records to a Lance table."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        cfg = config or {}
        self.table_path = cfg.get("table_path")
        self.mode = cfg.get("mode", "append")
        self.buffer_size = cfg.get("buffer_size", 1000)

        if not self.table_path:
            raise ValueError("table_path is required for LanceSink")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer: List[Dict[str, Any]] = []
        self.table = None

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if batch is None:
            raise ValueError("LanceSink requires a batch")
        self.buffer.extend(batch.to_pylist())
        if len(self.buffer) >= self.buffer_size:
            self._flush()
        return None

    def _flush(self) -> None:
        if not self.buffer:
            return

        table = pa.Table.from_pylist(self.buffer)
        write_dataset(table, self.table_path, mode=self.mode if self.table is None else "append")
        if self.table is None:
            self.mode = "append"
        self.logger.info(f"Flushed {len(self.buffer)} records to Lance table")
        self.buffer.clear()