"""Operator Master interface for operator-specific control logic."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional

from solstice.core.models import Split


class OperatorMaster(ABC):
    """Base class for operator-specific master logic.
    
    OperatorMaster handles operator-specific control logic that doesn't belong
    in StageMaster. It uses an event-driven interface with on_xxx methods.
    
    This is a regular class (not a Ray actor) to keep the API simple for users.
    """

    @abstractmethod
    def initialize(self) -> None:
        """Initialize the operator master."""
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown the operator master."""
        pass

    def on_split_requested(self, max_count: int = 1) -> Iterator[Split]:
        """Event handler: Called when StageMaster needs more splits.
        
        This is called periodically by StageMaster when there's capacity
        for more splits. Operators that generate splits should override this.
        
        Args:
            max_count: Maximum number of splits to return
        
        Yields:
            Split objects to be enqueued
        """
        # Default implementation: no splits generated
        return
        yield  # Make it a generator function (unreachable, but makes it a generator)

    def on_split_completed(self, split_id: str) -> None:
        """Event handler: Called when a split is completed.
        
        This is called after a split has been fully processed and state cleared.
        
        Args:
            split_id: ID of the completed split
        """
        pass

    def on_split_failed(self, split_id: str, error: Exception) -> None:
        """Event handler: Called when a split processing fails.
        
        Args:
            split_id: ID of the failed split
            error: The exception that occurred
        """
        pass


class SourceOperatorMaster(OperatorMaster):
    """Master for source operators that handles split planning and generation."""

    def __init__(
        self,
        job_id: str,
        stage_id: str,
        operator_class: type,
        operator_config: Dict[str, Any],
    ):
        self.job_id = job_id
        self.stage_id = stage_id
        self.operator_class = operator_class
        self.operator_config = operator_config
        
        import logging
        self.logger = logging.getLogger(f"SourceOperatorMaster-{stage_id}")
        
        # Create operator instance for planning
        self.operator = operator_class(operator_config)
        
        # Planned splits
        self._planned_splits: List[Split] = []
        self._source_split_counter = 0
        self._source_finished = False
        
        # Initialize and plan splits
        self.initialize()

    def initialize(self) -> None:
        """Initialize and plan splits."""
        from solstice.core.operator import SourceOperator
        
        if not isinstance(self.operator, SourceOperator):
            raise TypeError(f"Expected SourceOperator, got {type(self.operator)}")
        
        # Phase 1: Plan splits
        self._planned_splits = self.operator.plan_splits()
        self.logger.info(
            "Source operator master planned %d splits for stage %s",
            len(self._planned_splits),
            self.stage_id,
        )

    def on_split_requested(self, max_count: int = 1) -> Iterator[Split]:
        """Event handler: Generate splits when requested by StageMaster."""
        if self._source_finished:
            return
        
        remaining = min(max_count, len(self._planned_splits) - self._source_split_counter)
        
        for _ in range(remaining):
            split = self._planned_splits[self._source_split_counter]
            self._source_split_counter += 1
            
            # Ensure split has correct stage_id
            if split.stage_id != self.stage_id:
                split = Split(
                    split_id=split.split_id,
                    stage_id=self.stage_id,
                    data_range=split.data_range,
                    parent_split_ids=split.parent_split_ids,
                    attempt=split.attempt,
                    status=split.status,
                    assigned_worker=split.assigned_worker,
                    retry_count=split.retry_count,
                    created_at=split.created_at,
                    updated_at=split.updated_at,
                    metadata=split.metadata,
                    record_count=split.record_count,
                    is_terminal=split.is_terminal,
                )
            
            yield split
        
        if self._source_split_counter >= len(self._planned_splits):
            self._source_finished = True

    def get_planned_count(self) -> int:
        """Get the total number of planned splits."""
        return len(self._planned_splits)

    def shutdown(self) -> None:
        """Shutdown the operator master."""
        self.logger.info("Shutting down SourceOperatorMaster for stage %s", self.stage_id)
        try:
            self.operator.close()
        except Exception as exc:
            self.logger.error("Error closing operator: %s", exc)


class SinkOperatorMaster(OperatorMaster):
    """Master for sink operators that handles output propagation control."""

    def __init__(
        self,
        job_id: str,
        stage_id: str,
        operator_class: type,
        operator_config: Dict[str, Any],
    ):
        self.job_id = job_id
        self.stage_id = stage_id
        self.operator_class = operator_class
        self.operator_config = operator_config
        
        import logging
        self.logger = logging.getLogger(f"SinkOperatorMaster-{stage_id}")

    def initialize(self) -> None:
        """Initialize the sink operator master."""
        self.logger.info("Sink operator master initialized for stage %s", self.stage_id)

    def shutdown(self) -> None:
        """Shutdown the operator master."""
        self.logger.info("Shutting down SinkOperatorMaster for stage %s", self.stage_id)

