"""Sink that prints records to stdout."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import json
from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SinkOperator


class PrintSink(SinkOperator):
    """Sink that prints records to stdout."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, worker_id: Optional[str] = None):
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
