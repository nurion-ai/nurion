"""Base operator interface"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional

from solstice.core.models import Record, Batch

class Operator(ABC):
    """Base class for all operators"""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.config = config or {}

    @abstractmethod
    def process(self, record: Record) -> Iterable[Record]:
        """Process a single record and emit zero or more output records"""
        pass

    def process_batch(self, batch: Batch) -> Batch:
        """Process a batch of records (can be overridden for batch optimization)"""
        output_records = []
        for record in batch.to_records():
            output_records.extend(self.process(record))

        return Batch.from_records(
            output_records,
            batch_id=batch.batch_id,
            source_split=batch.source_split,
        )

    def close(self) -> None:
        """Clean up operator resources"""
        pass


class SourceOperator(Operator):
    """Base class for source operators"""

    @abstractmethod
    def read(self) -> Iterable[Record]:
        """Read records from source"""
        pass

    def process(self, record: Record) -> Iterable[Record]:
        """Sources don't process records"""
        raise NotImplementedError("Source operators should use read() method")


class SinkOperator(Operator):
    """Base class for sink operators"""

    @abstractmethod
    def write(self, record: Record) -> None:
        """Write a record to sink"""
        pass

    def process(self, record: Record) -> Iterable[Record]:
        """Write record and pass through"""
        self.write(record)
        return [record]
