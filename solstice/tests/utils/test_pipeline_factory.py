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

"""Test pipeline factory for distributed correctness tests.

Provides utilities to create standardized test pipelines with
configurable source data, transforms, and sinks.
"""

import hashlib
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from solstice.core.job import Job, JobConfig
from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig
from solstice.core.stage import Stage
from solstice.operators.sources.source import SourceMaster
from solstice.queue import QueueType

from .collecting_sink import CollectingSinkConfig


# ============================================================================
# Test Source Operator
# ============================================================================


@dataclass
class TestSourceConfig(OperatorConfig):
    """Configuration for test source operator."""

    num_records: int = 1000
    batch_size: int = 100
    with_checksum: bool = False
    # Pre-generated data (optional, for custom test data)
    source_data: Optional[List[Dict]] = None


TestSourceConfig.operator_class = None  # Will be set below
TestSourceConfig.master_class = None  # Will be set below


class TestSourceOperator(Operator):
    """Test source operator that generates test data."""

    def __init__(self, config: TestSourceConfig, worker_id: str = None):
        super().__init__(config, worker_id)
        self._generated = 0

    def generate_splits(self) -> List[Split]:
        """Generate splits for the source."""
        splits = []
        num_batches = (self.config.num_records + self.config.batch_size - 1) // self.config.batch_size
        for i in range(num_batches):
            start = i * self.config.batch_size
            end = min((i + 1) * self.config.batch_size, self.config.num_records)
            splits.append(
                Split(
                    split_id=f"source_split_{i}",
                    stage_id="source",
                    data_range={
                        "start": start,
                        "end": end,
                    },
                )
            )
        return splits

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Generate data for a split."""
        start = split.data_range["start"]
        end = split.data_range["end"]

        # Use pre-generated data if provided
        if self.config.source_data is not None:
            records = self.config.source_data[start:end]
            data = pa.table({
                col: [r[col] for r in records]
                for col in records[0].keys()
            }) if records else pa.table({})
        else:
            # Generate test data
            ids = list(range(start, end))
            values = [f"record_{i}" for i in range(start, end)]

            if self.config.with_checksum:
                checksums = [
                    hashlib.md5(f"record_{i}".encode()).hexdigest()
                    for i in range(start, end)
                ]
                data = pa.table({
                    "id": ids,
                    "value": values,
                    "checksum": checksums,
                })
            else:
                data = pa.table({
                    "id": ids,
                    "value": values,
                })

        self._generated += end - start
        return SplitPayload(data=data, split_id=split.split_id)

    def close(self) -> None:
        pass


TestSourceConfig.operator_class = TestSourceOperator


class TestSourceMaster(SourceMaster):
    """Test source master that generates splits from config."""

    def plan_splits(self):
        """Generate splits based on operator config."""
        config = self.stage.operator_config
        num_batches = (config.num_records + config.batch_size - 1) // config.batch_size

        for i in range(num_batches):
            start = i * config.batch_size
            end = min((i + 1) * config.batch_size, config.num_records)
            yield Split(
                split_id=f"source_split_{i}",
                stage_id=self.stage_id,
                data_range={
                    "start": start,
                    "end": end,
                },
            )


TestSourceConfig.master_class = TestSourceMaster


# ============================================================================
# Passthrough Transform Operator
# ============================================================================


@dataclass
class PassthroughConfig(OperatorConfig):
    """Configuration for passthrough transform operator."""

    # Optional delay per record (seconds) for simulating slow processing
    delay_per_record: float = 0.0


PassthroughConfig.operator_class = None  # Will be set below


class PassthroughOperator(Operator):
    """Passthrough operator that forwards data without modification."""

    def __init__(self, config: PassthroughConfig, worker_id: str = None):
        super().__init__(config, worker_id)
        self._processed = 0

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Forward data without modification."""
        if payload is None:
            return None

        # Simulate slow processing if configured
        if self.config.delay_per_record > 0:
            import time
            time.sleep(self.config.delay_per_record * len(payload))

        self._processed += len(payload)
        return payload

    def close(self) -> None:
        pass


PassthroughConfig.operator_class = PassthroughOperator


# ============================================================================
# Slow Transform Operator (for backpressure testing)
# ============================================================================


@dataclass
class SlowTransformConfig(OperatorConfig):
    """Configuration for slow transform operator."""

    delay_seconds: float = 0.5  # Delay per split


SlowTransformConfig.operator_class = None  # Will be set below


class SlowTransformOperator(Operator):
    """Slow transform operator for testing backpressure."""

    def __init__(self, config: SlowTransformConfig, worker_id: str = None):
        super().__init__(config, worker_id)

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Process with artificial delay."""
        import time
        time.sleep(self.config.delay_seconds)
        return payload

    def close(self) -> None:
        pass


SlowTransformConfig.operator_class = SlowTransformOperator


# ============================================================================
# Filter Transform Operator (reduces row count)
# ============================================================================


@dataclass
class FilterConfig(OperatorConfig):
    """Configuration for filter operator.

    Filters rows based on id % modulo == remainder.
    E.g., modulo=2, remainder=0 keeps even IDs (50% of data).
    """

    modulo: int = 2  # Keep rows where id % modulo == remainder
    remainder: int = 0
    id_field: str = "id"


FilterConfig.operator_class = None  # Will be set below


class FilterOperator(Operator):
    """Filter operator that reduces row count based on ID modulo.

    Used for testing data consistency when row count changes.
    The filter is deterministic based on ID, so results are reproducible.
    """

    def __init__(self, config: FilterConfig, worker_id: str = None):
        super().__init__(config, worker_id)
        self._input_count = 0
        self._output_count = 0

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Filter rows based on ID modulo condition."""
        if payload is None:
            return None

        table = payload.to_table()
        self._input_count += len(table)

        # Filter: keep rows where id % modulo == remainder
        id_col = table.column(self.config.id_field).to_pylist()
        mask = [
            i % self.config.modulo == self.config.remainder
            for i in id_col
        ]

        # Apply filter
        filtered_table = table.filter(pa.array(mask))
        self._output_count += len(filtered_table)

        if len(filtered_table) == 0:
            return None

        return SplitPayload(data=filtered_table, split_id=split.split_id)

    def close(self) -> None:
        pass


FilterConfig.operator_class = FilterOperator


# ============================================================================
# Explode Transform Operator (increases row count)
# ============================================================================


@dataclass
class ExplodeConfig(OperatorConfig):
    """Configuration for explode operator.

    Duplicates each row 'factor' times, adding a 'copy_idx' column.
    E.g., factor=3 turns 100 rows into 300 rows.
    """

    factor: int = 2  # Number of copies per row
    add_copy_index: bool = True  # Add copy_idx column


ExplodeConfig.operator_class = None  # Will be set below


class ExplodeOperator(Operator):
    """Explode operator that increases row count by duplicating rows.

    Each input row is duplicated 'factor' times. A 'copy_idx' column
    is added to distinguish copies (0, 1, 2, ..., factor-1).
    """

    def __init__(self, config: ExplodeConfig, worker_id: str = None):
        super().__init__(config, worker_id)
        self._input_count = 0
        self._output_count = 0

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Explode rows by duplicating each row 'factor' times."""
        if payload is None:
            return None

        table = payload.to_table()
        self._input_count += len(table)

        # Build exploded data
        exploded_data = {}
        for col_name in table.column_names:
            col_values = table.column(col_name).to_pylist()
            # Repeat each value 'factor' times
            exploded_values = []
            for val in col_values:
                exploded_values.extend([val] * self.config.factor)
            exploded_data[col_name] = exploded_values

        # Add copy index column
        if self.config.add_copy_index:
            copy_indices = []
            for _ in range(len(table)):
                copy_indices.extend(list(range(self.config.factor)))
            exploded_data["copy_idx"] = copy_indices

        exploded_table = pa.table(exploded_data)
        self._output_count += len(exploded_table)

        return SplitPayload(data=exploded_table, split_id=split.split_id)

    def close(self) -> None:
        pass


ExplodeConfig.operator_class = ExplodeOperator


# ============================================================================
# Filter + Explode Combined Pipeline Factory
# ============================================================================


@dataclass
class FilterExplodeConfig(OperatorConfig):
    """Configuration for combined filter-then-explode operator.

    First filters rows (id % filter_modulo == filter_remainder),
    then explodes remaining rows by explode_factor.

    Example:
        - Input: 10000 rows (ids 0-9999)
        - filter_modulo=5, filter_remainder=0: keeps 2000 rows (ids 0,5,10,...)
        - explode_factor=3: produces 6000 rows

    This tests both row reduction and expansion in a single operator.
    """

    filter_modulo: int = 5
    filter_remainder: int = 0
    explode_factor: int = 3
    id_field: str = "id"


FilterExplodeConfig.operator_class = None  # Will be set below


class FilterExplodeOperator(Operator):
    """Combined filter-then-explode operator for complex row count changes."""

    def __init__(self, config: FilterExplodeConfig, worker_id: str = None):
        super().__init__(config, worker_id)
        self._input_count = 0
        self._after_filter_count = 0
        self._output_count = 0

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Filter then explode rows."""
        if payload is None:
            return None

        table = payload.to_table()
        self._input_count += len(table)

        # Step 1: Filter
        id_col = table.column(self.config.id_field).to_pylist()
        mask = [
            i % self.config.filter_modulo == self.config.filter_remainder
            for i in id_col
        ]
        filtered_table = table.filter(pa.array(mask))
        self._after_filter_count += len(filtered_table)

        if len(filtered_table) == 0:
            return None

        # Step 2: Explode
        exploded_data = {}
        for col_name in filtered_table.column_names:
            col_values = filtered_table.column(col_name).to_pylist()
            exploded_values = []
            for val in col_values:
                exploded_values.extend([val] * self.config.explode_factor)
            exploded_data[col_name] = exploded_values

        # Add copy index
        copy_indices = []
        for _ in range(len(filtered_table)):
            copy_indices.extend(list(range(self.config.explode_factor)))
        exploded_data["copy_idx"] = copy_indices

        exploded_table = pa.table(exploded_data)
        self._output_count += len(exploded_table)

        return SplitPayload(data=exploded_table, split_id=split.split_id)

    def close(self) -> None:
        pass


FilterExplodeConfig.operator_class = FilterExplodeOperator


# ============================================================================
# Pipeline Factory
# ============================================================================


def create_test_pipeline(
    num_records: int = 1000,
    batch_size: int = 100,
    min_workers: int = 1,
    max_workers: int = 4,
    collector_name: str = "test_collector",
    with_checksum: bool = False,
    source_data: Optional[List[Dict]] = None,
    job_id: Optional[str] = None,
    queue_type: QueueType = QueueType.TANSU,
    transform_config: Optional[OperatorConfig] = None,
) -> Job:
    """Create a standard test pipeline for distributed correctness tests.

    Pipeline structure: source -> transform -> sink

    Args:
        num_records: Number of records to generate
        batch_size: Records per batch/split
        min_workers: Minimum workers for transform stage
        max_workers: Maximum workers for transform stage
        collector_name: Name of the RecordCollector actor
        with_checksum: Include checksum field in records
        source_data: Pre-generated source data (overrides num_records)
        job_id: Optional job ID (auto-generated if not provided)
        queue_type: Queue type to use (TANSU or MEMORY)
        transform_config: Optional custom transform config

    Returns:
        Configured Job instance
    """
    if job_id is None:
        job_id = f"test_{uuid.uuid4().hex[:8]}"

    # Use source_data length if provided
    if source_data is not None:
        num_records = len(source_data)

    job = Job(
        job_id=job_id,
        config=JobConfig(queue_type=queue_type),
    )

    # Source stage
    source_config = TestSourceConfig(
        num_records=num_records,
        batch_size=batch_size,
        with_checksum=with_checksum,
        source_data=source_data,
    )
    source_stage = Stage(
        stage_id="source",
        operator_config=source_config,
        parallelism=(1, 1),  # Source is single-threaded
    )
    job.add_stage(source_stage)

    # Transform stage
    if transform_config is None:
        transform_config = PassthroughConfig()
    transform_stage = Stage(
        stage_id="transform",
        operator_config=transform_config,
        parallelism=(min_workers, max_workers),
    )
    job.add_stage(transform_stage, upstream_stages=["source"])

    # Sink stage
    sink_config = CollectingSinkConfig(collector_name=collector_name)
    sink_stage = Stage(
        stage_id="sink",
        operator_config=sink_config,
        parallelism=(1, 2),
    )
    job.add_stage(sink_stage, upstream_stages=["transform"])

    return job


def create_multi_stage_pipeline(
    num_records: int = 1000,
    batch_size: int = 100,
    num_transform_stages: int = 3,
    min_workers: int = 1,
    max_workers: int = 4,
    collector_name: str = "test_collector",
    with_checksum: bool = False,
    job_id: Optional[str] = None,
) -> Job:
    """Create a multi-stage test pipeline.

    Pipeline structure: source -> transform_1 -> transform_2 -> ... -> sink

    Args:
        num_records: Number of records to generate
        batch_size: Records per batch/split
        num_transform_stages: Number of transform stages
        min_workers: Minimum workers per transform stage
        max_workers: Maximum workers per transform stage
        collector_name: Name of the RecordCollector actor
        with_checksum: Include checksum field in records
        job_id: Optional job ID

    Returns:
        Configured Job instance
    """
    if job_id is None:
        job_id = f"test_multi_{uuid.uuid4().hex[:8]}"

    job = Job(
        job_id=job_id,
        config=JobConfig(queue_type=QueueType.TANSU),
    )

    # Source stage
    source_config = TestSourceConfig(
        num_records=num_records,
        batch_size=batch_size,
        with_checksum=with_checksum,
    )
    source_stage = Stage(
        stage_id="source",
        operator_config=source_config,
        parallelism=(1, 1),
    )
    job.add_stage(source_stage)

    # Transform stages
    prev_stage = "source"
    for i in range(num_transform_stages):
        stage_id = f"transform_{i}"
        transform_stage = Stage(
            stage_id=stage_id,
            operator_config=PassthroughConfig(),
            parallelism=(min_workers, max_workers),
        )
        job.add_stage(transform_stage, upstream_stages=[prev_stage])
        prev_stage = stage_id

    # Sink stage
    sink_config = CollectingSinkConfig(collector_name=collector_name)
    sink_stage = Stage(
        stage_id="sink",
        operator_config=sink_config,
        parallelism=(1, 2),
    )
    job.add_stage(sink_stage, upstream_stages=[prev_stage])

    return job


def generate_test_data_with_checksum(num_records: int) -> List[Dict]:
    """Generate test data with checksums for verification.

    Args:
        num_records: Number of records to generate

    Returns:
        List of records with id, value, and checksum fields
    """
    records = []
    for i in range(num_records):
        value = f"record_{i}"
        checksum = hashlib.md5(value.encode()).hexdigest()
        records.append({
            "id": i,
            "value": value,
            "checksum": checksum,
        })
    return records
