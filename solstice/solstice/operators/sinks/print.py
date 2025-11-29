"""Sink that prints records to stdout."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import json
from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SinkOperator, OperatorConfig


@dataclass
class PrintSinkConfig(OperatorConfig):
    """Configuration for PrintSink operator."""
    pass  # No configuration needed for PrintSink


class PrintSink(SinkOperator):
    """Sink that prints records to stdout."""

    def __init__(self, config: PrintSinkConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.count = 0

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if batch is None:
            raise ValueError("PrintSink requires a batch")
        self.logger.info(f"Printing {len(batch)} records")
        for record in batch.to_records():
            self.logger.info(json.dumps(record.to_dict()))
        return None


# Set operator_class after class definition
PrintSinkConfig.operator_class = PrintSink
