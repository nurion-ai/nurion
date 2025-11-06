"""Batch processing operators"""

from typing import Any, Callable, Dict, Optional

from solstice.core.operator import Operator
from solstice.core.models import Batch


class MapBatchesOperator(Operator):
    """Operator that applies a function to entire batches"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        
        # The batch map function
        self.map_batches_fn = config.get('map_batches_fn')
        if not callable(self.map_batches_fn):
            raise ValueError("map_batches_fn must be a callable")
    
    def process_batch(self, batch: Batch) -> Batch:
        """Apply map function to entire batch (optimized for batch processing)"""
        try:
            # Apply transformation to entire batch
            output_records = self.map_batches_fn(batch.records)
            
            # Return new batch with transformed records
            return Batch(
                records=output_records,
                batch_id=batch.batch_id,
                source_shard=batch.source_shard,
            )
            
        except Exception as e:
            import logging
            logger = logging.getLogger(self.__class__.__name__)
            logger.error(f"Error mapping batch {batch.batch_id}: {e}")
            
            if self.config.get('skip_on_error', False):
                # Return empty batch on error
                return Batch(
                    records=[],
                    batch_id=batch.batch_id,
                    source_shard=batch.source_shard,
                )
            else:
                raise
    
    def process(self, record):
        """Not used - batch processing is more efficient"""
        raise NotImplementedError(
            "MapBatchesOperator uses process_batch(). "
            "Use MapOperator for record-by-record processing."
        )

