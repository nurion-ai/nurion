"""Map operator for transformations"""

from dataclasses import dataclass
from typing import Any, Callable, Optional

from solstice.core.operator import Operator, OperatorConfig
from solstice.core.models import Record, Split, SplitPayload


@dataclass
class MapOperatorConfig(OperatorConfig):
    """Configuration for MapOperator."""

    map_fn: Callable[[Any], Any]
    """Function to apply to each record's value."""


class MapOperator(Operator):
    """Operator that applies a function to each record"""

    def __init__(self, config: MapOperatorConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)

        if not callable(config.map_fn):
            raise ValueError("map_fn must be a callable")
        self.map_fn = config.map_fn

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Apply map function to record"""
        try:
            # Apply transformation
            new_data = []
            for record in batch.to_records():
                new_value = self.map_fn(record.value)
                new_data.append(
                    Record(
                        key=record.key,
                        value=new_value,
                    )
                )
            return batch.with_new_data(new_data, split_id=f"{split.split_id}_{self.worker_id}")
        except Exception as e:
            self.logger.error(f"Error mapping split {split.split_id}: {e}")
            return None


# Set operator_class after class definition
MapOperatorConfig.operator_class = MapOperator


@dataclass
class MapBatchesOperatorConfig(OperatorConfig):
    """Configuration for MapBatchesOperator."""

    map_batches_fn: Callable[[Any], Any]
    """Function to apply to the entire batch (Arrow table)."""

    skip_on_error: bool = False
    """If True, return empty payload on error instead of raising."""


class MapBatchesOperator(Operator):
    """Operator that applies a function to entire batches"""

    def __init__(self, config: MapBatchesOperatorConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)

        if not callable(config.map_batches_fn):
            raise ValueError("map_batches_fn must be a callable")
        self.map_batches_fn = config.map_batches_fn
        self.skip_on_error = config.skip_on_error

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Apply map function to entire batch"""
        try:
            # Apply transformation
            new_data = self.map_batches_fn(batch.to_table())
            if len(new_data) != len(batch):
                raise ValueError(
                    "map_batches_fn must return the same number of records as the input batch"
                )
            return batch.with_new_data(new_data, split_id=f"{split.split_id}_{self.worker_id}")
        except Exception as e:
            self.logger.error(f"Error mapping batch {batch.split_id}: {e}")
            if self.skip_on_error:
                return SplitPayload.empty(split_id=batch.split_id, schema=batch.schema)
            else:
                raise


# Set operator_class after class definition
MapBatchesOperatorConfig.operator_class = MapBatchesOperator


@dataclass
class FlatMapOperatorConfig(OperatorConfig):
    """Configuration for FlatMapOperator."""

    flatmap_fn: Callable[[Any], Any]
    """Function to apply to the batch, returning multiple records."""


class FlatMapOperator(Operator):
    """Operator that applies a function that returns multiple records"""

    def __init__(self, config: FlatMapOperatorConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)

        if not callable(config.flatmap_fn):
            raise ValueError("flatmap_fn must be a callable")
        self.flatmap_fn = config.flatmap_fn

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Apply flatmap function to record"""
        try:
            # Apply transformation - should return iterable
            new_data = self.flatmap_fn(batch.to_table())
            return batch.with_new_data(new_data, split_id=f"{split.split_id}_{self.worker_id}")

        except Exception as e:
            self.logger.error(f"Error flatmapping split {split.split_id}: {e}")
            return None


# Set operator_class after class definition
FlatMapOperatorConfig.operator_class = FlatMapOperator
