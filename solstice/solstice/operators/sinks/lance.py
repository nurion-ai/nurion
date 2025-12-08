"""Lance sink implementation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Set

import pyarrow as pa
from lance.dataset import write_dataset

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SinkOperator, OperatorConfig


@dataclass
class LanceSinkConfig(OperatorConfig):
    """Configuration for LanceSink operator."""

    table_path: str
    """Path to the Lance table (local or S3)."""

    mode: Literal["create", "append", "overwrite"] = "append"
    """Write mode for the table."""

    buffer_size: int = 1000
    """Number of records to buffer before flushing."""

    blob_columns: List[str] = field(default_factory=lambda: ["slice_binary"])
    """Columns to store as Lance blobs (large binary with blob encoding)."""

    storage_options: Optional[Dict[str, str]] = None
    """Storage options for S3/cloud backends (e.g., aws_access_key_id, endpoint_url)."""


class LanceSink(SinkOperator):
    """Sink that writes records to a Lance table."""

    def __init__(self, config: LanceSinkConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        if not config.table_path:
            raise ValueError("table_path is required for LanceSink")

        self.table_path = config.table_path
        self.mode = config.mode
        self.buffer_size = config.buffer_size
        self.blob_columns: Set[str] = set(config.blob_columns)

        # Auto-configure storage options for S3 paths
        if config.storage_options:
            self.storage_options = config.storage_options
        elif self.table_path.startswith("s3://"):
            # Extract bucket from s3://bucket/path
            from solstice.utils.remote import get_lance_storage_options

            bucket = self.table_path[5:].split("/")[0]
            self.storage_options = get_lance_storage_options(bucket)
        else:
            self.storage_options = None

        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer: List[Dict[str, Any]] = []
        self.table = None

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if batch is None:
            raise ValueError("LanceSink requires a batch")
        self.buffer.extend(batch.to_pylist())
        if len(self.buffer) >= self.buffer_size:
            self._flush()
        return None

    def _flush(self) -> None:
        if not self.buffer:
            return

        # Filter out reserved Lance column names
        reserved_columns = {"_rowid", "_rowaddr"}
        filtered_buffer = []
        for record in self.buffer:
            filtered_record = {k: v for k, v in record.items() if k not in reserved_columns}
            filtered_buffer.append(filtered_record)

        # Create table from pylist first
        table = pa.Table.from_pylist(filtered_buffer)

        # Check if we need to add blob metadata to any columns
        has_blob_columns = any(col in self.blob_columns for col in table.column_names)

        if has_blob_columns:
            # Rebuild schema with blob metadata for binary columns
            new_fields = []
            for field in table.schema:
                if field.name in self.blob_columns:
                    # Add Lance blob encoding metadata
                    metadata = dict(field.metadata) if field.metadata else {}
                    metadata[b"lance-encoding:blob"] = b"true"
                    new_field = pa.field(field.name, pa.large_binary(), metadata=metadata)
                    new_fields.append(new_field)
                else:
                    new_fields.append(field)

            new_schema = pa.schema(new_fields)

            # Cast table to new schema with blob columns
            new_columns = []
            for i, field in enumerate(table.schema):
                col = table.column(i)
                if field.name in self.blob_columns:
                    # Cast to large_binary for blob storage
                    col = col.cast(pa.large_binary())
                new_columns.append(col)

            table = pa.table(dict(zip(table.column_names, new_columns)), schema=new_schema)

        write_dataset(
            table,
            self.table_path,
            mode=self.mode if self.table is None else "append",
            storage_options=self.storage_options,
        )
        if self.table is None:
            self.mode = "append"

        blob_info = (
            f" (blob columns: {list(self.blob_columns & set(table.column_names))})"
            if has_blob_columns
            else ""
        )
        self.logger.info(f"Flushed {len(self.buffer)} records to Lance table{blob_info}")
        self.buffer.clear()

    def close(self) -> None:
        """Flush remaining buffered records when closing."""
        self._flush()


# Set operator_class after class definition
LanceSinkConfig.operator_class = LanceSink
