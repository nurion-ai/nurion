"""Base operator interface"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional

from solstice.core.models import Record, Batch


class OperatorContext:
    """Context provided to operators during execution"""

    def __init__(
        self,
        task_id: str,
        stage_id: str,
        worker_id: str,
        checkpoint_id: Optional[str] = None,
    ):
        self.task_id = task_id
        self.stage_id = stage_id
        self.worker_id = worker_id
        self.checkpoint_id = checkpoint_id
        self._state: Dict[str, Any] = {}

    def get_state(self, key: str, default: Any = None) -> Any:
        """Get operator state"""
        return self._state.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """Set operator state"""
        self._state[key] = value

    def get_all_state(self) -> Dict[str, Any]:
        """Get all operator state"""
        return self._state.copy()

    def restore_state(self, state: Dict[str, Any]) -> None:
        """Restore operator state from checkpoint"""
        self._state = state.copy()


class Operator(ABC):
    """Base class for all operators"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self._context: Optional[OperatorContext] = None

    def open(self, context: OperatorContext) -> None:
        """Initialize operator with context"""
        self._context = context

    @abstractmethod
    def process(self, record: Record) -> Iterable[Record]:
        """Process a single record and emit zero or more output records"""
        pass

    def process_batch(self, batch: Batch) -> Batch:
        """Process a batch of records (can be overridden for batch optimization)"""
        output_records = []
        for record in batch.records:
            output_records.extend(self.process(record))

        return Batch(
            records=output_records,
            batch_id=batch.batch_id,
            source_shard=batch.source_shard,
        )

    def close(self) -> None:
        """Clean up operator resources"""
        pass

    def checkpoint(self) -> Dict[str, Any]:
        """Return operator state for checkpointing"""
        if self._context:
            return self._context.get_all_state()
        return {}

    def restore(self, state: Dict[str, Any]) -> None:
        """Restore operator from checkpoint"""
        if self._context:
            self._context.restore_state(state)


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
