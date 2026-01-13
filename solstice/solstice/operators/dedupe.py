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

"""Deduplication operators for removing duplicate records.

This module provides operators for deduplicating data:

1. **HashDedupeOperator**: Exact deduplication by key columns
   - Shuffles data by dedup key
   - Uses SlateDB to track seen keys (partition-scoped)
   - Outputs only first occurrence of each key
   - Stateless operator - all state is in SlateDB

Architecture for HashDedupe:
    Input -> Shuffle by dedup_keys -> HashDedupeOperator -> Deduplicated Output
                                           |
                                           v
                                    SlateDB (seen keys)

The dedup operator is stateless - it reads/writes state directly to SlateDB
without maintaining in-memory caches. This ensures:
- Fault tolerance: any worker can resume processing
- Exactly-once deduplication across restarts
- Partition-scoped state for scalability
"""

from dataclasses import dataclass, field
from typing import ClassVar, List, Optional, Type

import pyarrow as pa

from solstice.operators.shuffle import ShuffleOperator, ShuffleOperatorConfig
from solstice.state import SlateDBPartitionStateStore


@dataclass
class HashDedupeConfig(ShuffleOperatorConfig):
    """Configuration for exact hash-based deduplication.

    Deduplicates records by computing a hash of the specified key columns.
    Records with the same key hash are considered duplicates.

    Attributes:
        dedup_keys: Columns that define uniqueness (same as partition_keys)
        keep: Which duplicate to keep ("first" or "last")
        state_store_path: Path for SlateDB state storage
    """

    dedup_keys: List[str] = field(default_factory=list)
    keep: str = "first"  # "first" or "last"
    state_store_path: Optional[str] = None

    operator_class: ClassVar[Type["HashDedupeOperator"]] = None  # type: ignore[assignment]  # Set below

    def __post_init__(self):
        # dedup_keys are also partition_keys for shuffle
        if self.dedup_keys and not self.partition_keys:
            self.partition_keys = self.dedup_keys


class HashDedupeOperator(ShuffleOperator):
    """Stateless operator for exact hash-based deduplication.

    This operator:
    1. Shuffles data by dedup keys (handled by ShuffleOperator base)
    2. Checks seen keys in SlateDB for each record
    3. Outputs only records with keys not seen before
    4. Marks new keys as seen in SlateDB

    The operator is STATELESS - it does not maintain any in-memory state.
    All seen-key tracking is done via the external SlateDB state store.
    This enables:
    - Any worker can process any partition (after acquiring it)
    - Fault tolerance via SlateDB checkpoints
    - Elastic scaling without state migration

    Example:
        config = HashDedupeConfig(dedup_keys=["user_id", "event_id"])
        stage = Stage("dedupe", config, parallelism=8)

    State Management:
        - Keys are stored as: hash(dedup_key_values) -> "1"
        - State is checkpointed with the partition via SlateDB
        - On recovery, SlateDB state is restored automatically
    """

    def __init__(
        self,
        config: HashDedupeConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.dedupe_config = config

        # State store reference (set by worker, not owned by operator)
        self._state_store: Optional[SlateDBPartitionStateStore] = None
        self._partition_id: Optional[int] = None

    def set_state_store(
        self,
        state_store: SlateDBPartitionStateStore,
        partition_id: int,
    ) -> None:
        """Set the state store for tracking seen keys.

        Called by the worker with the partition's state store.
        The operator does not own or manage the state store lifecycle.
        """
        self._state_store = state_store
        self._partition_id = partition_id

    @property
    def dedup_keys(self) -> List[str]:
        """Get the deduplication key columns."""
        return self.dedupe_config.dedup_keys

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Deduplicate the input data.

        For each row:
        1. Use DuckDB to dedupe within the batch
        2. For each unique row, check SlateDB if key was seen
        3. If not seen, output the row and mark as seen in SlateDB

        This is stateless - all state operations go directly to SlateDB.
        """
        if not self.dedup_keys:
            # No dedup keys specified, pass through
            return table

        if table.num_rows == 0:
            return None

        # Use DuckDB for efficient deduplication within the batch
        deduped_table = self.engine.dedupe(
            table,
            key_columns=self.dedup_keys,
            keep=self.dedupe_config.keep,
        )

        if deduped_table.num_rows == 0:
            return None

        # If no state store, only do batch-level dedup
        if self._state_store is None:
            self.logger.warning("No state store set - only performing batch-level deduplication")
            return deduped_table

        # Cross-batch dedup via state store (synchronous)
        output_rows = []
        keys_to_mark = []

        for i in range(deduped_table.num_rows):
            key_hash = self._compute_key_hash(deduped_table, i)

            # Check if key exists in state store (synchronous)
            assert self._partition_id is not None, "partition_id not set"
            existing = self._state_store.get(self._partition_id, key_hash)

            if existing is None:
                # Key not seen before - output it
                output_rows.append(i)
                keys_to_mark.append(key_hash)

        # Mark new keys as seen (synchronous)
        # _partition_id assertion already done above
        partition_id = self._partition_id
        assert partition_id is not None
        for key_hash in keys_to_mark:
            self._state_store.put(partition_id, key_hash, b"1")

        if not output_rows:
            return None

        # Select only the non-duplicate rows
        return deduped_table.take(output_rows)

    def _compute_key_hash(self, table: pa.Table, row_idx: int) -> bytes:
        """Compute a hash of the dedup key values for a row."""
        import hashlib

        key_parts = []
        for col_name in self.dedup_keys:
            value = table.column(col_name)[row_idx].as_py()
            key_parts.append(str(value))

        key_str = "|".join(key_parts)
        return hashlib.sha256(key_str.encode()).digest()[:16]

    def close(self) -> None:
        """Clean up resources.

        Note: The operator does not own the state store, so we don't close it.
        """
        super().close()


# Set the operator class reference
HashDedupeConfig.operator_class = HashDedupeOperator
