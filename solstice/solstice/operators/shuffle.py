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

"""Shuffle operator base classes for partition-aware data processing.

This module provides base classes for operators that need to shuffle data
across partitions, such as:

- GroupBy: Aggregate by key
- Repartition: Redistribute data by key
- HashDedupe: Deduplicate by key
- Join: Join tables by key

Key concepts:

1. **Partition Keys**: Columns used to determine which partition a row belongs to
2. **Partition Function**: Hash function to map keys to partition IDs
3. **Shuffle Output**: Output is partitioned by key, not randomly distributed

Architecture:
    Input -> ShuffleOperator -> Partitioned Output
                |
                v
    Uses DuckDB to compute partition IDs for each row

The worker handles the actual routing:
1. Operator processes data and adds __target_partition column
2. Worker splits output by partition
3. Worker produces each partition's data to the correct queue partition
"""

from abc import abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, List, Optional, Type

import pyarrow as pa

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig
from solstice.compute import DuckDBEngine


@dataclass
class ShuffleOperatorConfig(OperatorConfig):
    """Base configuration for shuffle operators.

    Shuffle operators partition their output by key columns, enabling
    downstream stages to process data with the same key together.

    Attributes:
        partition_keys: Columns to partition by (hash of these determines partition)
        num_partitions: Number of output partitions (None = use downstream partition count)
    """

    partition_keys: List[str] = field(default_factory=list)
    num_partitions: Optional[int] = None

    # Subclasses must set these
    operator_class: ClassVar[Type["ShuffleOperator"]]


class ShuffleOperator(Operator):
    """Base class for operators that shuffle data by partition key.

    Shuffle operators:
    1. Process input data (transform, aggregate, etc.)
    2. Add __target_partition column to output
    3. Worker handles splitting and routing to correct partitions

    Subclasses implement:
    - `process_data()`: Transform the input data
    - Optionally override `compute_partitions()` for custom partitioning

    Example:
        class MyShuffleOperator(ShuffleOperator):
            def process_data(self, table: pa.Table) -> pa.Table:
                # Transform the data
                return transformed_table

    The base class handles:
    - DuckDB engine lifecycle
    - Partition ID computation
    - Adding __target_partition column
    """

    # Column name for target partition (added to output)
    PARTITION_COLUMN = "__target_partition"

    def __init__(
        self,
        config: ShuffleOperatorConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.shuffle_config = config

        # DuckDB engine for partition computation (created lazily)
        self._engine: Optional[DuckDBEngine] = None

        # Cache partition count (set by worker)
        self._num_partitions: Optional[int] = None

    @property
    def engine(self) -> DuckDBEngine:
        """Get or create the DuckDB engine."""
        if self._engine is None:
            self._engine = DuckDBEngine()
        return self._engine

    def set_num_partitions(self, num_partitions: int) -> None:
        """Set the number of output partitions.

        Called by the worker with the actual downstream partition count.
        """
        self._num_partitions = num_partitions

    @property
    def num_partitions(self) -> int:
        """Get the number of output partitions."""
        if self.shuffle_config.num_partitions is not None:
            return self.shuffle_config.num_partitions
        if self._num_partitions is not None:
            return self._num_partitions
        raise ValueError("num_partitions not set - call set_num_partitions() first")

    @property
    def partition_keys(self) -> List[str]:
        """Get the partition key columns."""
        return self.shuffle_config.partition_keys

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split and add partition information.

        This method:
        1. Calls process_data() to transform the input
        2. Computes partition IDs for each row
        3. Adds __target_partition column to output

        The worker will use __target_partition to route data.
        """
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        # Process the data (subclass implementation)
        result_table = self.process_data(table)
        if result_table is None or result_table.num_rows == 0:
            return None

        # Add partition column if we have partition keys
        if self.partition_keys:
            result_table = self._add_partition_column(result_table)

        return SplitPayload(data=result_table, split_id=split.split_id)

    @abstractmethod
    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Process the input data.

        Subclasses implement this to perform their specific transformation.

        Args:
            table: Input Arrow table

        Returns:
            Transformed Arrow table, or None if no output
        """
        pass

    def _add_partition_column(self, table: pa.Table) -> pa.Table:
        """Add __target_partition column to the table.

        If the column already exists (from a previous shuffle stage),
        it's removed first to avoid duplicate columns.
        """
        # Remove existing partition column if present (from upstream shuffle)
        if self.PARTITION_COLUMN in table.column_names:
            table = table.drop([self.PARTITION_COLUMN])

        partition_ids = self.engine.compute_partition_ids(
            table,
            partition_keys=self.partition_keys,
            num_partitions=self.num_partitions,
        )

        # Add the partition column
        return table.append_column(self.PARTITION_COLUMN, partition_ids)

    def close(self) -> None:
        """Clean up resources."""
        if self._engine is not None:
            self._engine.close()
            self._engine = None


@dataclass
class RepartitionConfig(ShuffleOperatorConfig):
    """Configuration for repartition operator.

    Repartition redistributes data by partition keys without any transformation.
    This is useful for:
    - Co-locating data with the same key for downstream processing
    - Changing the partition count
    - Preparing for a join operation
    """

    operator_class: ClassVar[Type["RepartitionOperator"]] = None  # type: ignore[assignment]  # Set below


class RepartitionOperator(ShuffleOperator):
    """Operator that repartitions data by key without transformation.

    This is the simplest shuffle operator - it just redistributes data
    so that rows with the same key end up in the same partition.

    Example:
        config = RepartitionConfig(partition_keys=["user_id"])
        stage = Stage("repartition", config, parallelism=8)
    """

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Pass through data unchanged."""
        return table


# Set the operator class reference
RepartitionConfig.operator_class = RepartitionOperator


def split_by_partition(table: pa.Table) -> dict[int, pa.Table]:
    """Split a table by the __target_partition column.

    This is a utility function used by workers to split shuffle output
    before producing to the queue.

    Args:
        table: Table with __target_partition column

    Returns:
        Dictionary mapping partition ID to table for that partition
    """
    if ShuffleOperator.PARTITION_COLUMN not in table.column_names:
        raise ValueError(f"Table missing {ShuffleOperator.PARTITION_COLUMN} column")

    partition_col = table.column(ShuffleOperator.PARTITION_COLUMN)
    unique_partitions = pa.compute.unique(partition_col).to_pylist()

    result = {}
    for partition_id in unique_partitions:
        mask = pa.compute.equal(partition_col, partition_id)
        partition_table = table.filter(mask)
        # Remove the partition column from output
        partition_table = partition_table.drop([ShuffleOperator.PARTITION_COLUMN])
        result[partition_id] = partition_table

    return result


def is_shuffle_operator(config: OperatorConfig) -> bool:
    """Check if an operator config is for a shuffle operator.

    Args:
        config: Operator configuration

    Returns:
        True if this is a shuffle operator
    """
    return isinstance(config, ShuffleOperatorConfig)
