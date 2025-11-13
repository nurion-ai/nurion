"""Base operator interface"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional

from solstice.core.models import Record, Batch


class OperatorContext:
    """Context provided to operators during execution.

    The context is intentionally lightweight so that operators can be
    instantiated and exercised outside the distributed runtime (e.g. in unit
    tests) without having to mock worker identifiers. Runtime components can
    still inject richer metadata/state managers when available.
    """

    def __init__(
        self,
        task_id: Optional[str] = None,
        stage_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
        state_manager: Optional[Any] = None,
    ):
        self.task_id = task_id
        self.stage_id = stage_id
        self.worker_id = worker_id
        self.checkpoint_id = checkpoint_id
        self._state_manager = state_manager
        self._state: Dict[str, Any] = {}

    def get_state(self, key: str, default: Any = None) -> Any:
        """Get operator state"""
        if self._state_manager:
            operator_state = self._state_manager.get_operator_state()
            return operator_state.get(key, default)
        return self._state.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """Set operator state"""
        if self._state_manager:
            self._state_manager.update_operator_state({key: value})
        else:
            self._state[key] = value

    def get_all_state(self) -> Dict[str, Any]:
        """Get all operator state"""
        if self._state_manager:
            return self._state_manager.get_operator_state().copy()
        return self._state.copy()

    def restore_state(self, state: Dict[str, Any]) -> None:
        """Restore operator state from checkpoint"""
        if self._state_manager:
            self._state_manager.update_operator_state(state.copy())
        else:
            self._state = state.copy()

    def attach_state_manager(self, state_manager: Any) -> None:
        """Attach a state manager after construction"""
        self._state_manager = state_manager


class Operator(ABC):
    """Base class for all operators"""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        context: Optional[OperatorContext] = None,
    ):
        self.config = config or {}
        self._context: OperatorContext = context or OperatorContext()

    def open(self, context: Optional[OperatorContext] = None) -> None:
        """Initialize operator with context"""
        if context:
            self._context = context
        elif self._context is None:
            self._context = OperatorContext()

    @property
    def context(self) -> OperatorContext:
        return self._context

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
