"""Filter operator"""

from typing import Any, Dict, Optional

from solstice.core.operator import Operator
from solstice.core.models import Split, SplitPayload


class FilterOperator(Operator):
    """Operator that filters records based on a predicate"""

    def __init__(self, config: Optional[Dict[str, Any]] = None, worker_id: Optional[str] = None):
        super().__init__(config)

        self.filter_fn = config.get("filter_fn")
        if not callable(self.filter_fn):
            raise ValueError("filter_fn must be a callable returning bool")

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Filter record based on predicate"""
        try:
            # Apply filter
            new_data = []
            for record in batch.to_records():
                if self.filter_fn(record.value):
                    new_data.append(record)
            return batch.with_new_data(new_data, split_id=f"{split.split_id}_{self.worker_id}")

        except Exception as e:
            self.logger.error(f"Error filtering split {split.split_id}: {e}")
            return None
