"""Batch processing operators"""

from collections.abc import Iterable
from typing import Any, Dict, Optional

import pyarrow as pa

from solstice.core.operator import Operator
from solstice.core.models import Batch, Record, Split


class MapBatchesOperator(Operator):
    """Operator that applies a function to entire batches"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        # The batch map function
        self.map_batches_fn = config.get("map_batches_fn")
        if not callable(self.map_batches_fn):
            raise ValueError("map_batches_fn must be a callable")

    def process_split(self, split: Split, batch: Optional[Batch] = None) -> Optional[Batch]:
        """Apply map function to entire batch (optimized for Arrow data)."""
        if batch is None:
            raise ValueError("MapBatchesOperator requires batch")
        
        try:
            # Apply transformation. The function can return a Batch, Arrow object,
            # or an iterable of Record/dict for compatibility.
            result = self.map_batches_fn(batch)

            if isinstance(result, Batch):
                return result

            if isinstance(result, (pa.Table, pa.RecordBatch)):
                return batch.with_new_data(result)

            raise TypeError(
                "map_batches_fn must return one of Batch, pyarrow.Table, "
                "pyarrow.RecordBatch, Iterable[Record], Iterable[dict] or "
                "Iterable[pyarrow.RecordBatch]"
            )

        except Exception as e:
            self.logger.error(f"Error mapping batch {batch.batch_id}: {e}")
            if self.config.get("skip_on_error", False):
                # Return empty batch on error
                return Batch.empty(
                    batch_id=batch.batch_id,
                    source_split=batch.split_id,
                    schema=batch.schema,
                )
            else:
                raise

    def process(self, record):
        """Not used - batch processing is more efficient"""
        raise NotImplementedError(
            "MapBatchesOperator uses process_split(). "
            "Use MapOperator for record-by-record processing."
        )
