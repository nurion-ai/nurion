"""Sink that prints records to stdout."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from solstice.core.models import Record
from solstice.operators.sinks.base import Sink


class PrintSink(Sink):
    """Sink that prints records to stdout."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.count = 0

    def write(self, record: Record) -> None:
        self.count += 1
        print(f"[{self.count}] Key: {record.key}, Value: {record.value}")

    def close(self) -> None:
        self.logger.info(f"Printed {self.count} records")

