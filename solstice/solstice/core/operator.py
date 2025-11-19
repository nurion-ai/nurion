"""Base operator interface"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional

from solstice.core.models import Record, SplitPayload, Split
import logging


class Operator(ABC):
    """Base class for all operators"""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        worker_id: Optional[str] = None,
    ):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)


    @abstractmethod
    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        pass

    def close(self) -> None:
        """Clean up operator resources"""
        pass


class SourceOperator(Operator):

    @abstractmethod
    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read data for a specific split.

        Args:
            split: Split object containing all metadata needed to read data
                  (data_range, metadata, etc.)

        Returns:
            SplitPayload containing the data, or None if no data available
        """
        pass

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
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

        # Ensure split_id matches the split metadata
        if result.split_id != split.split_id:
            result = result.with_new_data(
                data=result.to_table(),
                split_id=split.split_id,
            )

        return result


class SinkOperator(Operator):
    """Base class for sink operators"""

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
