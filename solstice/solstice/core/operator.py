"""Base operator interface"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional

from solstice.core.models import Record, Batch, Split
import logging

class Operator(ABC):
    """Base class for all operators"""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.config = config or {}
        self._operator_master: Optional[Any] = None  # Ray actor handle
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_operator_master(
        self,
        job_id: str,
        stage_id: str,
        operator_class: type,
        operator_config: Dict[str, Any],
    ) -> Optional[Any]:
        """Create an operator master for this operator type.
        
        Default implementation returns None. Subclasses can override to provide
        operator-specific control logic.
        
        Args:
            job_id: Job identifier
            stage_id: Stage identifier
            operator_class: Operator class
            operator_config: Operator configuration
        
        Returns:
            Ray actor handle for OperatorMaster, or None if not needed
        """
        return None

    def get_master(self) -> Optional[Any]:
        """Get the operator master actor handle.
        
        Returns:
            Ray actor handle for OperatorMaster, or None if not set
        """
        return self._operator_master

    def set_master(self, master: Optional[Any]) -> None:
        """Set the operator master actor handle.
        
        Args:
            master: Ray actor handle for OperatorMaster to set
        """
        self._operator_master = master

    def process(self, record: Record) -> Iterable[Record]:
        """Process a single record (optional helper method).
        
        Subclasses can override this for record-by-record processing.
        The default process_split() implementation uses this.
        """
        raise NotImplementedError("Subclasses must implement process_split()")

    def process_split(self, split: Split, batch: Optional[Batch] = None) -> Optional[Batch]:
        """Process a split and return output batch.
        
        Default implementation calls process() for each record in the batch.
        Subclasses should override this for batch optimization.
        
        Args:
            split: Split metadata containing information about the data to process
            batch: Input batch (required for non-source operators)
        
        Returns:
            Output batch, or None if no output
        """
        if batch is None:
            raise ValueError("Non-source operators require batch")
        
        output_records = []
        for record in batch.to_records():
            try:
                output_records.extend(self.process(record))
            except NotImplementedError:
                raise NotImplementedError(
                    f"{self.__class__.__name__} must implement process_split()"
                )

        if not output_records:
            return None

        return Batch.from_records(
            output_records,
            batch_id=batch.batch_id,
            source_split=batch.source_split,
        )

    def close(self) -> None:
        """Clean up operator resources"""
        pass


class SourceOperator(Operator):
    """Base class for source operators.
    
    Source operators work in two phases:
    1. plan_splits(): Get file list/table metadata and plan splits
    2. read(split): Read actual data for a given split
    """

    def create_operator_master(
        self,
        job_id: str,
        stage_id: str,
        operator_class: type,
        operator_config: Dict[str, Any],
    ) -> Optional[Any]:
        """Create a SourceOperatorMaster for managing split planning."""
        from solstice.core.operator_master import SourceOperatorMaster
        
        return SourceOperatorMaster(
            job_id=job_id,
            stage_id=stage_id,
            operator_class=operator_class,
            operator_config=operator_config,
        )

    @abstractmethod
    def plan_splits(self) -> List[Split]:
        """Plan splits by getting file list/table metadata.
        
        Returns:
            List of Split objects. Each Split should contain:
            - split_id: Unique identifier for the split
            - stage_id: Stage identifier
            - data_range: Information about what data to read (file path, offset, etc.)
            - metadata: Optional metadata about the split
            - record_count: Optional estimated record count
        """
        pass

    @abstractmethod
    def read(self, split: Split) -> Optional[Batch]:
        """Read data for a specific split.
        
        Args:
            split: Split object containing all metadata needed to read data
                  (data_range, metadata, etc.)
        
        Returns:
            Batch containing the data, or None if no data available
        """
        pass

    def process_split(self, split: Split, batch: Optional[Batch] = None) -> Optional[Batch]:
        """Process a split for source operators.
        
        For source operators, batch is None and split contains all metadata.
        This method calls read() with the split.
        """
        if batch is not None:
            raise ValueError("Source operators should not receive batch, only split")
        
        # Call read() with the split
        result = self.read(split)
        
        if result is None:
            return None
        
        # Ensure batch_id and source_split are set correctly
        if not result.batch_id:
            result = result.with_new_data(
                data=result.to_table(),
                batch_id=f"{split.stage_id}_batch_{split.split_id}",
                source_split=split.split_id,
            )
        elif result.source_split != split.split_id:
            result = result.with_new_data(
                data=result.to_table(),
                batch_id=result.batch_id,
                source_split=split.split_id,
            )
        
        return result


class SinkOperator(Operator):
    """Base class for sink operators"""

    @abstractmethod
    def write(self, record: Record) -> None:
        """Write a record to sink"""
        pass

    def create_operator_master(
        self,
        job_id: str,
        stage_id: str,
        operator_class: type,
        operator_config: Dict[str, Any],
    ) -> Optional[Any]:
        """Create a SinkOperatorMaster for controlling output propagation."""
        from solstice.core.operator_master import SinkOperatorMaster
        
        return SinkOperatorMaster(
            job_id=job_id,
            stage_id=stage_id,
            operator_class=operator_class,
            operator_config=operator_config,
        )

    def process_split(self, split: Split, batch: Optional[Batch] = None) -> Optional[Batch]:
        """Process a split for sink operators.
        
        Sink operators write all records from the batch and return None (no output).
        """
        if batch is None:
            raise ValueError("Sink operators require batch")
        
        for record in batch.to_records():
            self.write(record)
        return None  # Sinks don't produce output
