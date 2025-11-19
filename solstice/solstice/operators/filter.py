"""Filter operator"""

from typing import Any, Dict, Iterable, Optional

from solstice.core.operator import Operator
from solstice.core.models import Split, SplitPayload


class FilterOperator(Operator):
    """Operator that filters records based on a predicate"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        self.filter_fn = config.get("filter_fn")
        if not callable(self.filter_fn):
            raise ValueError("filter_fn must be a callable returning bool")

    def process_split(self, split: Split) -> Optional[SplitPayload]:
        """Filter record based on predicate"""
        try:
            # Apply filter
            if self.filter_fn(record.value):
                return [record]
            else:
                return []

        except Exception as e:
            import logging

            logger = logging.getLogger(self.__class__.__name__)
            logger.error(f"Error filtering record {record.key}: {e}")

            if self.config.get("skip_on_error", False):
                return []
            else:
                raise
