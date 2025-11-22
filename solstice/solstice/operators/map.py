"""Map operator for transformations"""

from typing import Any, Dict, Optional

from solstice.core.operator import Operator
from solstice.core.models import Record, Split, SplitPayload


class MapOperator(Operator):
    """Operator that applies a function to each record"""

    def __init__(self, config: Optional[Dict[str, Any]] = None, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)

        # The map function can be provided as a config parameter
        self.map_fn = config.get("map_fn")
        if not callable(self.map_fn):
            raise ValueError("map_fn must be a callable")

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


class MapBatchesOperator(Operator):
    """Operator that applies a function to entire batches"""

    def __init__(self, config: Optional[Dict[str, Any]] = None, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)

        self.map_batches_fn = config.get("map_batches_fn")
        if not callable(self.map_batches_fn):
            raise ValueError("map_batches_fn must be a callable")

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
            if self.config.get("skip_on_error", False):
                return SplitPayload.empty(split_id=batch.split_id, schema=batch.schema)
            else:
                raise


class FlatMapOperator(Operator):
    """Operator that applies a function that returns multiple records"""

    def __init__(self, config: Optional[Dict[str, Any]] = None, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)

        self.flatmap_fn = config.get("flatmap_fn")
        if not callable(self.flatmap_fn):
            raise ValueError("flatmap_fn must be a callable")

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Apply flatmap function to record"""
        try:
            # Apply transformation - should return iterable
            new_data = []
            new_data = self.flatmap_fn(batch.to_table())
            return batch.with_new_data(new_data, split_id=f"{split.split_id}_{self.worker_id}")

        except Exception as e:
            self.logger.error(f"Error flatmapping split {split.split_id}: {e}")
            return None
