"""Core data models for the streaming framework"""

import json
import time
import warnings
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import pyarrow as pa


class SplitStatus(str, Enum):
    """Status of a split"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class CheckpointStatus(str, Enum):
    """Status of a checkpoint"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Split:
    """Represents a logical split of data for processing."""

    split_id: str
    stage_id: str
    data_range: Dict[str, Any]  # offset, file path, key range, etc.
    parent_split_ids: List[str] = field(default_factory=list)
    attempt: int = 0
    status: SplitStatus = SplitStatus.PENDING
    assigned_worker: Optional[str] = None
    retry_count: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def lineage(self) -> Dict[str, Any]:
        """Return lineage metadata for downstream operators."""
        return {
            "split_id": self.split_id,
            "stage_id": self.stage_id,
            "parents": list(self.parent_split_ids),
            "attempt": self.attempt,
            "metadata": dict(self.metadata),
        }


@dataclass
class WorkerMetrics:
    """Metrics reported by a worker"""

    worker_id: str
    stage_id: str
    processing_rate: float  # records/sec
    backlog_size: int
    key_distribution: Dict[str, int] = field(default_factory=dict)
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    timestamp: float = field(default_factory=time.time)


@dataclass
class CheckpointHandle:
    """Handle to a split-scoped checkpoint stored remotely."""

    checkpoint_id: str
    stage_id: str
    split_id: str
    split_attempt: int
    state_path: str  # S3/DFS path
    offset: Dict[str, Any]
    size_bytes: int
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    worker_id: Optional[str] = None


@dataclass
class Barrier:
    """Checkpoint barrier marker"""

    barrier_id: str
    checkpoint_id: str
    stage_id: str
    timestamp: float = field(default_factory=time.time)
    upstream_stages: List[str] = field(default_factory=list)
    downstream_stages: List[str] = field(default_factory=list)


@dataclass
class BackpressureSignal:
    """Signal for backpressure propagation"""

    from_stage: str
    to_stage: str
    slow_down_factor: float  # 0.0 to 1.0, where 0.0 means pause
    reason: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class Record:
    """A single record flowing through the pipeline"""

    key: Optional[str] = None
    value: Any = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Batch:
    """Arrow-backed batch of records.

    The authoritative payload is stored as a :class:`pyarrow.Table` to enable
    zero-copy operations and efficient integration with the Arrow ecosystem.
    Legacy record access is still available through the ``records`` property,
    which materializes Python ``Record`` objects on demand.
    """

    data: Union[pa.Table, pa.RecordBatch]
    batch_id: str
    source_split: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_materialized: bool = field(default=False, init=False, repr=False)

    _table: pa.Table = field(init=False, repr=False)
    _records_cache: Optional[List[Record]] = field(default=None, init=False, repr=False)

    SOLSTICE_KEY_COLUMN = "__solstice_key"
    SOLSTICE_TS_COLUMN = "__solstice_timestamp"
    SOLSTICE_METADATA_COLUMN = "__solstice_metadata_json"

    def __post_init__(self) -> None:
        if isinstance(self.data, pa.RecordBatch):
            self._table = pa.Table.from_batches([self.data])
        elif isinstance(self.data, pa.Table):
            self._table = self.data
        else:
            raise TypeError(
                "Batch payload must be a pyarrow.Table or pyarrow.RecordBatch, "
                f"got {type(self.data)!r}"
            )

        # Normalise metadata dict
        self.metadata = dict(self.metadata)
        # Ensure timestamp column exists if provided as metadata
        self.data = self._table

    def __len__(self) -> int:
        return self._table.num_rows

    @property
    def schema(self) -> pa.Schema:
        return self._table.schema

    @property
    def column_names(self) -> List[str]:
        return list(self._table.column_names)

    @property
    def records(self) -> List[Record]:
        """Materialize Python ``Record`` objects from the Arrow payload.

        Accessing this property incurs a copy; callers that can operate on Arrow
        data should prefer :meth:`to_table`, :meth:`column` or other zero-copy APIs.
        """
        warnings.warn(
            "Batch.records materializes Python objects and defeats zero-copy benefits. "
            "Prefer operating on Arrow tables directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._records_cache is None:
            self._records_cache = self.to_records()
        return list(self._records_cache)

    def to_table(self) -> pa.Table:
        return self._table

    def to_record_batch(self) -> pa.RecordBatch:
        return pa.RecordBatch.from_struct_array(self._table.to_struct_array())

    def to_pylist(self) -> List[Dict[str, Any]]:
        return self._table.to_pylist()

    def to_records(self) -> List[Record]:
        rows: List[Record] = []
        key_col_present = self.SOLSTICE_KEY_COLUMN in self._table.column_names
        ts_col_present = self.SOLSTICE_TS_COLUMN in self._table.column_names
        metadata_col_present = self.SOLSTICE_METADATA_COLUMN in self._table.column_names

        for row in self._table.to_pylist():
            key = row.pop(self.SOLSTICE_KEY_COLUMN, None) if key_col_present else None
            timestamp = row.pop(self.SOLSTICE_TS_COLUMN, None) if ts_col_present else self.timestamp
            metadata_json = (
                row.pop(self.SOLSTICE_METADATA_COLUMN, None) if metadata_col_present else None
            )
            if isinstance(metadata_json, str) and metadata_json:
                metadata = json.loads(metadata_json)
            elif isinstance(metadata_json, dict):
                metadata = metadata_json
            else:
                metadata = {}
            rows.append(
                Record(
                    key=key,
                    value=row,
                    timestamp=timestamp if timestamp is not None else time.time(),
                    metadata=metadata,
                )
            )
        return rows

    def replace(
        self,
        data: Union[pa.Table, pa.RecordBatch],
        *,
        batch_id: Optional[str] = None,
        source_split: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Batch":
        """Return a new batch with the provided Arrow payload and optional overrides."""
        return Batch(
            data=data,
            batch_id=batch_id or self.batch_id,
            source_split=self.source_split if source_split is None else source_split,
            metadata=metadata or dict(self.metadata),
        )

    def select(self, columns: Sequence[str]) -> "Batch":
        """Return a batch containing only the specified columns."""
        missing = set(columns) - set(self.column_names)
        if missing:
            raise ValueError(f"Columns {missing} not found in batch schema")
        return self.replace(self._table.select(columns))

    def column(self, name: str) -> pa.ChunkedArray:
        return self._table.column(name)

    def with_metadata(self, **metadata: Any) -> "Batch":
        merged = dict(self.metadata)
        merged.update(metadata)
        return Batch(
            data=self._table,
            batch_id=self.batch_id,
            source_split=self.source_split,
            metadata=merged,
        )

    def is_empty(self) -> bool:
        return len(self) == 0

    @classmethod
    def from_arrow(
        cls,
        data: Union[pa.Table, pa.RecordBatch, Iterable[pa.RecordBatch]],
        *,
        batch_id: str,
        source_split: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Batch":
        """Construct a batch from Arrow data."""
        if isinstance(data, pa.Table):
            table = data
        elif isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        else:
            # Assume iterable of record batches
            table = pa.Table.from_batches(list(data))
        return cls(
            data=table,
            batch_id=batch_id,
            source_split=source_split,
            metadata=metadata or {},
        )

    @classmethod
    def from_records(
        cls,
        records: Sequence[Union[Record, Dict[str, Any]]],
        *,
        batch_id: str,
        source_split: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        schema: Optional[pa.Schema] = None,
    ) -> "Batch":
        """Materialize an Arrow batch from Python ``Record`` objects or dictionaries."""
        rows: List[Dict[str, Any]] = []
        for record in records:
            if isinstance(record, Record):
                row: Dict[str, Any] = {}
                if isinstance(record.value, dict):
                    row.update(record.value)
                else:
                    row["value"] = record.value
                row[cls.SOLSTICE_KEY_COLUMN] = record.key
                row[cls.SOLSTICE_TS_COLUMN] = record.timestamp
                if record.metadata:
                    row[cls.SOLSTICE_METADATA_COLUMN] = json.dumps(record.metadata)
                rows.append(row)
            else:
                rows.append(dict(record))

        if rows:
            table = pa.Table.from_pylist(rows, schema=schema)
        elif schema is not None:
            table = pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in schema], schema
            )
        else:
            table = pa.table({})

        batch = cls(
            data=table,
            batch_id=batch_id,
            source_split=source_split,
            metadata=metadata or {},
        )
        batch._records_cache = (
            list(records) if rows and all(isinstance(r, Record) for r in records) else None
        )
        batch.is_materialized = bool(rows)
        return batch

    @classmethod
    def empty(
        cls,
        *,
        batch_id: str,
        schema: Optional[pa.Schema] = None,
        source_split: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Batch":
        """Create an empty batch with an optional schema."""
        if schema:
            arrays = [pa.array([], type=field.type) for field in schema]
            table = pa.Table.from_arrays(arrays, schema=schema)
        else:
            table = pa.table({})
        return cls(
            data=table,
            batch_id=batch_id,
            source_split=source_split,
            metadata=metadata or {},
        )
