"""Map operator for transformations"""

from typing import Any, Dict, Iterable, Optional

from solstice.core.operator import Operator
from solstice.core.models import Record


class MapOperator(Operator):
    """Operator that applies a function to each record"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        # The map function can be provided as a config parameter
        self.map_fn = config.get("map_fn")
        if not callable(self.map_fn):
            raise ValueError("map_fn must be a callable")

    def process(self, record: Record) -> Iterable[Record]:
        """Apply map function to record"""
        try:
            # Apply transformation
            new_value = self.map_fn(record.value)

            # Create new record with transformed value
            output_record = Record(
                key=record.key,
                value=new_value,
                timestamp=record.timestamp,
                metadata=record.metadata.copy(),
            )

            return [output_record]

        except Exception as e:
            # Log error and optionally skip record
            import logging

            logger = logging.getLogger(self.__class__.__name__)
            logger.error(f"Error mapping record {record.key}: {e}")

            if self.config.get("skip_on_error", False):
                return []
            else:
                raise


class FlatMapOperator(Operator):
    """Operator that applies a function that returns multiple records"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        self.flatmap_fn = config.get("flatmap_fn")
        if not callable(self.flatmap_fn):
            raise ValueError("flatmap_fn must be a callable")

    def process(self, record: Record) -> Iterable[Record]:
        """Apply flatmap function to record"""
        try:
            # Apply transformation - should return iterable
            results = self.flatmap_fn(record.value)

            # Create output records
            output_records = []
            for result in results:
                output_record = Record(
                    key=record.key,  # Keep same key or could extract from result
                    value=result,
                    timestamp=record.timestamp,
                    metadata=record.metadata.copy(),
                )
                output_records.append(output_record)

            return output_records

        except Exception as e:
            import logging

            logger = logging.getLogger(self.__class__.__name__)
            logger.error(f"Error flatmapping record {record.key}: {e}")

            if self.config.get("skip_on_error", False):
                return []
            else:
                raise


class KeyByOperator(Operator):
    """Operator that extracts/assigns keys to records"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        self.key_fn = config.get("key_fn")
        if not callable(self.key_fn):
            raise ValueError("key_fn must be a callable")

    def process(self, record: Record) -> Iterable[Record]:
        """Extract key from record"""
        try:
            # Extract key
            new_key = self.key_fn(record.value)

            # Create new record with updated key
            output_record = Record(
                key=str(new_key) if new_key is not None else None,
                value=record.value,
                timestamp=record.timestamp,
                metadata=record.metadata.copy(),
            )

            return [output_record]

        except Exception as e:
            import logging

            logger = logging.getLogger(self.__class__.__name__)
            logger.error(f"Error extracting key from record: {e}")

            if self.config.get("skip_on_error", False):
                return []
            else:
                raise
