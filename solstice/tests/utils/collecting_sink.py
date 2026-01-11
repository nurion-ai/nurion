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

"""Collecting sink for distributed test data verification.

Provides a Ray Actor-based collector that aggregates records from
distributed workers for validation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

import ray

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig


@ray.remote
class RecordCollector:
    """Ray Actor that collects records from distributed workers.

    This actor serves as a centralized collection point for test data,
    allowing verification of data integrity across distributed processing.

    Implements exactly-once semantics by deduplicating records based on
    their unique ID. This is critical for fault tolerance tests where
    workers may be killed and restarted, potentially producing duplicates.
    """

    def __init__(self, deduplicate: bool = True):
        """Initialize the collector.

        Args:
            deduplicate: If True, deduplicates records based on ID.
                        Required for exactly-once semantics in chaos tests.
        """
        self._records: List[Dict] = []
        self._seen_ids: set = set()  # For deduplication
        self._deduplicate = deduplicate
        self._duplicate_count = 0

    def add_records(self, records: List[Dict]) -> None:
        """Add records to the collection, deduplicating if enabled."""
        for record in records:
            self._add_single(record)

    def add_record(self, record: Dict) -> None:
        """Add a single record to the collection."""
        self._add_single(record)

    def _add_single(self, record: Dict) -> None:
        """Add a single record, with optional deduplication.

        Deduplication uses a composite key of (id, copy_idx) if copy_idx exists,
        otherwise just id. This supports explode operations where the same id
        is legitimately duplicated with different copy_idx values.
        """
        if self._deduplicate:
            record_id = record.get("id")
            copy_idx = record.get("copy_idx")

            # Build composite key for deduplication
            if record_id is not None:
                if copy_idx is not None:
                    # Exploded records: use (id, copy_idx) as unique key
                    dedup_key = (record_id, copy_idx)
                else:
                    # Normal records: use just id
                    dedup_key = (record_id,)

                if dedup_key in self._seen_ids:
                    self._duplicate_count += 1
                    return  # Skip duplicate
                self._seen_ids.add(dedup_key)

        self._records.append(record)

    def get_all(self) -> List[Dict]:
        """Get all collected records."""
        return self._records.copy()

    def count(self) -> int:
        """Get the count of collected records."""
        return len(self._records)

    def get_duplicate_count(self) -> int:
        """Get the count of duplicates that were filtered out."""
        return self._duplicate_count

    def clear(self) -> None:
        """Clear all collected records."""
        self._records.clear()
        self._seen_ids.clear()
        self._duplicate_count = 0

    def get_by_id(self, record_id: int) -> Optional[Dict]:
        """Get a record by its ID."""
        for record in self._records:
            if record.get("id") == record_id:
                return record
        return None


@dataclass
class CollectingSinkConfig(OperatorConfig):
    """Configuration for CollectingSink operator."""

    collector_name: str = "test_collector"


# Set operator_class after defining the class
CollectingSinkConfig.operator_class = None  # Will be set below


class CollectingSink(Operator):
    """Test sink operator that collects all records to a Ray Actor.

    Used for distributed test verification - all workers send their
    output to a centralized collector for validation.
    """

    def __init__(self, config: CollectingSinkConfig, worker_id: str = None):
        super().__init__(config, worker_id)
        self._collector_name = config.collector_name
        self._collector = None

    def _get_collector(self):
        """Lazily get the collector actor.

        Note: The collector is created with lifetime="detached" in the main
        test process. Workers in different processes can find it by name
        in the default namespace.
        """
        if self._collector is None:
            try:
                self._collector = ray.get_actor(self._collector_name)
            except ValueError as e:
                # More descriptive error message
                raise ValueError(
                    f"Could not find collector actor '{self._collector_name}'. "
                    f"Make sure create_collector() was called before starting the pipeline. "
                    f"Original error: {e}"
                ) from e
        return self._collector

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Process a split by collecting records to the Ray Actor."""
        if payload is not None:
            records = payload.to_pylist()
            collector = self._get_collector()
            ray.get(collector.add_records.remote(records))
        # Sink returns None - no downstream output
        return None

    def close(self) -> None:
        """Close the operator."""
        pass


# Set operator_class
CollectingSinkConfig.operator_class = CollectingSink


def create_collector(name: str = "test_collector") -> "ray.actor.ActorHandle":
    """Create a new RecordCollector actor with the given name.

    The actor is created with lifetime="detached" so it can be accessed
    from other Ray processes (e.g., StageWorker actors).

    Args:
        name: Name for the actor (used to retrieve it later)

    Returns:
        Ray actor handle for the collector

    Raises:
        ValueError: If an actor with this name already exists
    """
    return RecordCollector.options(
        name=name,
        lifetime="detached",
    ).remote()


def get_collector(name: str = "test_collector") -> "ray.actor.ActorHandle":
    """Get an existing RecordCollector actor by name.

    Args:
        name: Name of the actor to retrieve

    Returns:
        Ray actor handle for the collector
    """
    return ray.get_actor(name)


def get_sink_records(collector_name: str = "test_collector") -> List[Dict]:
    """Get all records from a collector.

    Args:
        collector_name: Name of the collector actor

    Returns:
        List of all collected records
    """
    collector = ray.get_actor(collector_name)
    return ray.get(collector.get_all.remote())


def count_sink_records(collector_name: str = "test_collector") -> int:
    """Get the count of records in a collector.

    Args:
        collector_name: Name of the collector actor

    Returns:
        Count of collected records
    """
    collector = ray.get_actor(collector_name)
    return ray.get(collector.count.remote())


def clear_collector(collector_name: str = "test_collector") -> None:
    """Clear all records from a collector.

    Args:
        collector_name: Name of the collector actor
    """
    collector = ray.get_actor(collector_name)
    ray.get(collector.clear.remote())
