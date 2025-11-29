"""Lance sink implementation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

import pyarrow as pa
from lance.dataset import write_dataset

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SinkOperator, OperatorConfig


@dataclass
class LanceSinkConfig(OperatorConfig):
    """Configuration for LanceSink operator."""

    table_path: str
    """Path to the Lance table."""

    mode: Literal["create", "append", "overwrite"] = "append"
    """Write mode for the table."""

    buffer_size: int = 1000
    """Number of records to buffer before flushing."""


class LanceSink(SinkOperator):
    """Sink that writes records to a Lance table."""

    def __init__(self, config: LanceSinkConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        if not config.table_path:
            raise ValueError("table_path is required for LanceSink")

        self.table_path = config.table_path
        self.mode = config.mode
        self.buffer_size = config.buffer_size

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


# Set operator_class after class definition
LanceSinkConfig.operator_class = LanceSink
