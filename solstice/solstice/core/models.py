# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Core data models for the streaming framework"""

import time
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

import pyarrow as pa


@dataclass
class Split:
    """Represents a logical split of data for processing.

    Each split tracks the scheduling metadata for a *single* data batch. The actual
    payload lives separately in :class:`SplitPayload` instances; the runtime associates
    splits with batches via identifiers/object references.
    """

    split_id: str
    stage_id: str
    data_range: Dict[str, Any]  # offset, file path, key range, etc.
    parent_split_ids: List[str] = field(default_factory=list)

    def lineage(self) -> Dict[str, Any]:
        """Return lineage metadata for downstream operators."""
        return {
            "split_id": self.split_id,
            "stage_id": self.stage_id,
            "parents": list(self.parent_split_ids),
        }

    def derive_output_split(
        self,
        target_split_id: str,
        target_stage_id: Optional[str] = None,
        data_range: Optional[Dict[str, Any]] = None,
    ) -> "Split":
        """Produce a new split metadata object for downstream consumption."""
        derived_stage_id = target_stage_id or self.stage_id
        derived_split_id = target_split_id or self.split_id

        return Split(
            split_id=derived_split_id,
            stage_id=derived_stage_id,
            data_range=data_range or {},
            parent_split_ids=[self.split_id],
        )


@dataclass
class WorkerMetrics:
    """Metrics reported by a worker"""

    worker_id: str
    stage_id: str
    input_records: int
    output_records: int
    processing_time: float
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "input_records": self.input_records,
            "output_records": self.output_records,
            "processing_time": self.processing_time,
            "cpu_usage": self.cpu_usage,
            "memory_usage": self.memory_usage,
            "timestamp": self.timestamp,
        }


@dataclass
class PartitionMetrics:
    """Metrics for a single partition"""

    partition_id: int
    latest_offset: int
    committed_offset: int
    lag: int  # latest_offset - committed_offset

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "latest_offset": self.latest_offset,
            "committed_offset": self.committed_offset,
            "lag": self.lag,
        }


@dataclass
class StageMetrics:
    """Metrics reported by a stage master"""

    stage_id: str
    worker_count: int
    input_records: int
    output_records: int
    total_processing_time: float  # seconds
    pending_splits: int
    inflight_results: int
    output_buffer_size: int = 0  # Size of output buffer (Pull model)
    backpressure_active: bool = False
    uptime_secs: float = 0.0
    timestamp: float = field(default_factory=time.time)
    partition_metrics: Dict[int, PartitionMetrics] = field(
        default_factory=dict
    )  # partition_id -> metrics
    skew_detected: bool = False
    skew_ratio: float = 0.0  # max_lag / avg_lag (if > 1.0, indicates skew)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "stage_id": self.stage_id,
            "worker_count": self.worker_count,
            "input_records": self.input_records,
            "output_records": self.output_records,
            "total_processing_time": self.total_processing_time,
            "pending_splits": self.pending_splits,
            "inflight_results": self.inflight_results,
            "output_buffer_size": self.output_buffer_size,
            "backpressure_active": self.backpressure_active,
            "uptime_secs": self.uptime_secs,
            "partition_metrics": {pid: pm.to_dict() for pid, pm in self.partition_metrics.items()},
            "skew_detected": self.skew_detected,
            "skew_ratio": self.skew_ratio,
            "timestamp": self.timestamp,
        }


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

    key: str = field(default="")
    value: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
        }


@dataclass
class SplitPayload:
    """Arrow-backed payload of records tied to a split.

    The authoritative payload is stored as a :class:`pyarrow.Table` to enable
    zero-copy operations and efficient integration with the Arrow ecosystem.
    """

    data: pa.Table
    split_id: str
    timestamp: float = field(default_factory=time.time)

    SOLSTICE_KEY_COLUMN = "__solstice_key"
    SOLSTICE_TS_COLUMN = "__solstice_timestamp"

    def __len__(self) -> int:
        return self.data.num_rows

    @property
    def schema(self) -> pa.Schema:
        return self.data.schema

    @property
    def column_names(self) -> List[str]:
        return list(self.data.column_names)

    @property
    def records(self) -> List[Record]:
        """Materialize Python ``Record`` objects from the Arrow payload.

        Accessing this property incurs a copy; callers that can operate on Arrow
        data should prefer :meth:`to_table`, :meth:`column` or other zero-copy APIs.
        """
        warnings.warn(
            "SplitPayload.records materializes Python objects and defeats zero-copy benefits. "
            "Prefer operating on Arrow tables directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        return list(self.to_records())

    def to_table(self) -> pa.Table:
        return self.data

    def to_pylist(self) -> List[Dict[str, Any]]:
        """Return the payload as a list of Python dictionaries."""
        return self.data.to_pylist()

    def to_records(self) -> List[Record]:
        rows: List[Record] = []
        key_col_present = self.SOLSTICE_KEY_COLUMN in self.data.column_names
        ts_col_present = self.SOLSTICE_TS_COLUMN in self.data.column_names
        for row in self.data.to_pylist():
            key = row.pop(self.SOLSTICE_KEY_COLUMN, None) if key_col_present else None
            timestamp = row.pop(self.SOLSTICE_TS_COLUMN, None) if ts_col_present else self.timestamp
            rows.append(
                Record(
                    key=key or "",
                    value=row,
                    timestamp=timestamp if timestamp is not None else time.time(),
                )
            )
        return rows

    def with_new_data(
        self,
        data: Union[pa.Table, pa.RecordBatch, Sequence[Record]],
        split_id: Optional[str] = None,
    ) -> "SplitPayload":
        """Return a new batch with the provided Arrow payload and optional overrides."""
        if isinstance(data, pa.Table):
            table = data
        elif isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        elif isinstance(data, Sequence):
            if not all(isinstance(item, Record) for item in data):
                raise TypeError("Expected an iterable of Record instances")
            table = pa.Table.from_pylist(self._rows_from_records(data))
        else:
            raise TypeError(
                f"data must be a pyarrow.Table or pyarrow.RecordBatch, got {type(data)}"
            )
        return SplitPayload(
            data=table,
            split_id=split_id or self.split_id,
        )

    def column(self, name: str) -> pa.ChunkedArray:
        return self.data.column(name)

    def is_empty(self) -> bool:
        return len(self) == 0

    @classmethod
    def from_arrow(
        cls,
        data: Union[pa.Table, pa.RecordBatch],
        split_id: str,
    ) -> "SplitPayload":
        """Construct a batch from Arrow data."""
        if isinstance(data, pa.Table):
            table = data
        elif isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        else:
            raise TypeError(
                f"data must be a pyarrow.Table or pyarrow.RecordBatch, got {type(data)}"
            )
        return cls(data=table, split_id=split_id)

    @classmethod
    def from_records(
        cls,
        records: Sequence[Union[Record, Dict[str, Any]]],
        split_id: str,
        schema: Optional[pa.Schema] = None,
    ) -> "SplitPayload":
        """Materialize an Arrow batch from Python ``Record`` objects or dictionaries."""
        rows: List[Dict[str, Any]] = []
        for record in records:
            if isinstance(record, Record):
                rows.append(cls._record_to_row(record))
            else:
                rows.append(dict(record))

        return cls(data=pa.Table.from_pylist(rows, schema=schema), split_id=split_id)

    @classmethod
    def empty(
        cls,
        split_id: str,
        schema: Optional[pa.Schema] = None,
    ) -> "SplitPayload":
        """Create an empty batch with an optional schema."""
        if schema:
            arrays = [pa.array([], type=field.type) for field in schema]
            table = pa.Table.from_arrays(arrays, schema=schema)
        else:
            table = pa.table({})
        return cls(data=table, split_id=split_id)

    @classmethod
    def _record_to_row(cls, record: Record) -> Dict[str, Any]:
        row: Dict[str, Any] = {}
        if isinstance(record.value, dict):
            row.update(record.value)
        else:
            row["value"] = record.value
        row[cls.SOLSTICE_KEY_COLUMN] = record.key
        row[cls.SOLSTICE_TS_COLUMN] = record.timestamp
        return row

    @classmethod
    def _rows_from_records(cls, records: Sequence[Record]) -> List[Dict[str, Any]]:
        return [cls._record_to_row(record) for record in records]
