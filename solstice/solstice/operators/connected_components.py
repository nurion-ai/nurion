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

"""Distributed Connected Components via iterative label propagation.

This module implements distributed Connected Components (CC) for clustering
similar documents in MinHash deduplication. The algorithm uses iterative
label propagation:

Algorithm (per iteration):
1. **Map**: For each edge (A, B), emit (A, label[B]) and (B, label[A])
2. **Shuffle**: Route by doc_id to correct partition
3. **Reduce**: new_label[X] = min(current_label[X], received_labels)
4. **Converge**: If no label changed across all partitions, done

This is a distributed version that works across partitions:
- Labels are stored in SlateDB (external state)
- Each partition maintains labels for its assigned documents
- Messages are shuffled between partitions
- Convergence is detected globally by the runner

ALL OPERATORS ARE STATELESS:
- No in-memory caches or state
- All state is managed via SlateDB
- Enables fault tolerance and elastic scaling

Stages:
1. **CCInitOperator**: Initialize labels (label = doc_id) from candidate pairs
2. **CCIterateOperator**: One round of label propagation (reduce step)
3. **CCMessageOperator**: Generate messages for next iteration (map step)

The runner orchestrates iterations until convergence.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Type

import pyarrow as pa

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig
from solstice.operators.shuffle import ShuffleOperator, ShuffleOperatorConfig
from solstice.state import SlateDBPartitionStateStore

if TYPE_CHECKING:
    from solstice.operators.cc_master import CCIterateMaster


@dataclass
class CCInitConfig(OperatorConfig):
    """Configuration for CC initialization.

    Takes candidate pairs and initializes labels for all documents.

    Attributes:
        doc_id_1_column: Column for first document ID
        doc_id_2_column: Column for second document ID
    """

    doc_id_1_column: str = "doc_id_1"
    doc_id_2_column: str = "doc_id_2"

    operator_class: ClassVar[Type["CCInitOperator"]] = None  # type: ignore[assignment]  # Set below


class CCInitOperator(Operator):
    """Initialize labels and generate initial messages from candidate pairs.

    Input: Candidate pairs (doc_id_1, doc_id_2, similarity)
    Output: Initial messages (doc_id, neighbor_label) for label propagation

    Each document starts with its own ID as its label.

    This operator is STATELESS - it generates messages without storing state.
    """

    def __init__(
        self,
        config: CCInitConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.init_config = config

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Initialize labels and generate messages."""
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        config = self.init_config

        doc_ids_1 = table.column(config.doc_id_1_column).to_pylist()
        doc_ids_2 = table.column(config.doc_id_2_column).to_pylist()

        # Generate bidirectional messages
        # For edge (A, B): emit (A, B) and (B, A)
        # This means "A should consider B's label" and vice versa
        messages = []
        for doc1, doc2 in zip(doc_ids_1, doc_ids_2):
            # Message to doc1: consider doc2's label
            messages.append(
                {
                    "doc_id": doc1,
                    "neighbor_label": doc2,  # Initially, label = doc_id
                }
            )
            # Message to doc2: consider doc1's label
            messages.append(
                {
                    "doc_id": doc2,
                    "neighbor_label": doc1,
                }
            )

        if not messages:
            return None

        result = pa.table(
            {
                "doc_id": [m["doc_id"] for m in messages],
                "neighbor_label": [m["neighbor_label"] for m in messages],
            }
        )

        return SplitPayload(data=result, split_id=split.split_id)


CCInitConfig.operator_class = CCInitOperator


@dataclass
class CCIterateConfig(ShuffleOperatorConfig):
    """Configuration for CC iteration (reduce step).

    Takes messages and updates labels. Uses CCIterateMaster for
    self-contained iteration - no special logic needed in RayJobRunner.

    Attributes:
        doc_id_column: Column for document ID
        neighbor_label_column: Column for neighbor's label
        current_label_column: Column for current label (in input)
        state_store_path: Path for SlateDB state storage
        max_iterations: Maximum iterations before forced stop
        convergence_threshold: Number of changes below which to stop (0 = require full convergence)
    """

    doc_id_column: str = "doc_id"
    neighbor_label_column: str = "neighbor_label"
    current_label_column: str = "current_label"
    state_store_path: Optional[str] = None
    max_iterations: int = 100
    convergence_threshold: int = 0

    operator_class: ClassVar[Type["CCIterateOperator"]] = None  # type: ignore[assignment]  # Set below
    master_class: ClassVar[Optional[Type["CCIterateMaster"]]] = None  # Set below

    def __post_init__(self):
        # Partition by doc_id for label aggregation
        if not self.partition_keys:
            self.partition_keys = [self.doc_id_column]


class CCIterateOperator(ShuffleOperator):
    """Stateless operator for one iteration of label propagation (reduce step).

    Input: Messages (doc_id, neighbor_label) + current labels
    Output: Updated labels (doc_id, label, changed)

    For each document, the new label is the minimum of:
    - Current label (from input or SlateDB)
    - All received neighbor labels

    This operator is STATELESS - it reads/writes labels directly to SlateDB
    without maintaining in-memory state.

    The input should include current labels. On the first iteration, current
    labels equal doc_id. On subsequent iterations, the runner passes the
    labels from the previous iteration or they are read from SlateDB.
    """

    def __init__(
        self,
        config: CCIterateConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.iterate_config = config

        # State store reference (set by worker, not owned by operator)
        self._state_store: Optional[SlateDBPartitionStateStore] = None
        self._partition_id: Optional[int] = None

    def set_state_store(
        self,
        state_store: SlateDBPartitionStateStore,
        partition_id: int,
    ) -> None:
        """Set the state store for label persistence.

        Called by the worker with the partition's state store.
        """
        self._state_store = state_store
        self._partition_id = partition_id

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Update labels based on received messages.

        This is stateless - all label lookups go to SlateDB.
        """
        config = self.iterate_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        neighbor_labels = table.column(config.neighbor_label_column).to_pylist()

        # Get current labels from table if available, else from state store
        current_labels_from_table: Dict[str, str] = {}
        if config.current_label_column in table.column_names:
            current_label_values = table.column(config.current_label_column).to_pylist()
            for doc_id, current_label in zip(doc_ids, current_label_values):
                if doc_id not in current_labels_from_table:
                    current_labels_from_table[doc_id] = current_label

        # Group messages by doc_id
        messages_by_doc: Dict[str, List[str]] = {}
        for doc_id, neighbor_label in zip(doc_ids, neighbor_labels):
            if doc_id not in messages_by_doc:
                messages_by_doc[doc_id] = []
            messages_by_doc[doc_id].append(neighbor_label)

        # Update labels
        results = []
        changes = 0

        for doc_id, neighbor_labels_list in messages_by_doc.items():
            # Get current label: from table, from state store, or default to doc_id
            current_label = current_labels_from_table.get(doc_id)
            if current_label is None and self._state_store is not None:
                key = f"label:{doc_id}".encode()
                assert self._partition_id is not None, "partition_id not set"
                stored = self._state_store.get(self._partition_id, key)
                if stored is not None:
                    current_label = stored.decode()
            if current_label is None:
                current_label = doc_id  # Default: label = doc_id

            # New label is minimum of current and all neighbors
            all_labels = [current_label] + neighbor_labels_list
            new_label = min(all_labels, key=str)

            # Check if changed
            changed = new_label != current_label
            if changed:
                changes += 1

            # Store updated label in state store (synchronous)
            if self._state_store is not None:
                key = f"label:{doc_id}".encode()
                assert self._partition_id is not None, "partition_id not set"
                self._state_store.put(self._partition_id, key, new_label.encode())

            results.append(
                {
                    "doc_id": doc_id,
                    "label": new_label,
                    "changed": changed,
                }
            )

        if not results:
            return None

        # Log changes for convergence detection
        self.logger.debug(f"CC iteration: {changes} label changes")

        return pa.table(
            {
                "doc_id": [r["doc_id"] for r in results],
                "label": [r["label"] for r in results],
                "changed": [r["changed"] for r in results],
            }
        )


CCIterateConfig.operator_class = CCIterateOperator

# Set master_class after imports to avoid circular imports
from solstice.operators.cc_master import CCIterateMaster  # noqa: E402

CCIterateConfig.master_class = CCIterateMaster


@dataclass
class CCMessageConfig(OperatorConfig):
    """Configuration for CC message generation (map step).

    Takes current labels and edges, generates messages for next iteration.

    Attributes:
        doc_id_column: Column for document ID
        label_column: Column for current label
        neighbor_column: Column for neighbor document ID (for edges)
    """

    doc_id_column: str = "doc_id"
    label_column: str = "label"
    neighbor_column: str = "neighbor_id"

    operator_class: ClassVar[Type["CCMessageOperator"]] = None  # type: ignore[assignment]  # Set below


class CCMessageOperator(Operator):
    """Stateless operator for generating messages (map step).

    Input: Current labels with edges (doc_id, label, neighbor_id)
    Output: Messages (doc_id, neighbor_label, current_label) for next round

    For each row with (doc_id, label, neighbor_id):
    - Emit message to neighbor with current label

    This operator is STATELESS - edges must come from the input data.
    The pipeline should include edge information in the data flow.
    """

    def __init__(
        self,
        config: CCMessageConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.message_config = config

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Generate messages from current labels and edges."""
        if payload is None:
            return None

        table = payload.to_table()
        if table.num_rows == 0:
            return None

        result = self.process_data(table)
        if result is None:
            return None

        return SplitPayload(data=result, split_id=split.split_id)

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Generate messages from labels and edges."""
        config = self.message_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        labels = table.column(config.label_column).to_pylist()
        neighbors = table.column(config.neighbor_column).to_pylist()

        # Build label lookup
        label_map: Dict[str, str] = {}
        for doc_id, label in zip(doc_ids, labels):
            label_map[doc_id] = label

        # Generate messages: for each (doc, neighbor), send doc's label to neighbor
        messages = []
        for doc_id, label, neighbor in zip(doc_ids, labels, neighbors):
            if neighbor is not None:
                messages.append(
                    {
                        "doc_id": neighbor,
                        "neighbor_label": label,
                        "current_label": label_map.get(neighbor, neighbor),
                    }
                )

        if not messages:
            return None

        return pa.table(
            {
                "doc_id": [m["doc_id"] for m in messages],
                "neighbor_label": [m["neighbor_label"] for m in messages],
                "current_label": [m["current_label"] for m in messages],
            }
        )


CCMessageConfig.operator_class = CCMessageOperator


@dataclass
class DedupeByClusterConfig(ShuffleOperatorConfig):
    """Configuration for deduplication by cluster.

    Takes clustered documents and keeps one representative per cluster.

    Attributes:
        doc_id_column: Column for document ID
        cluster_id_column: Column for cluster ID (label)
    """

    doc_id_column: str = "doc_id"
    cluster_id_column: str = "label"

    operator_class: ClassVar[Type["DedupeByClusterOperator"]] = None  # type: ignore[assignment]  # Set below

    def __post_init__(self):
        # Partition by cluster_id for grouping
        self.partition_keys = [self.cluster_id_column]


class DedupeByClusterOperator(ShuffleOperator):
    """Stateless operator to keep one representative document per cluster.

    Input: Documents with cluster labels (doc_id, label, ...)
    Output: One document per cluster (the one with smallest doc_id)

    This operator is STATELESS - it deduplicates within the batch only.
    Since data is shuffled by cluster_id, all documents in a cluster
    end up in the same partition, enabling within-batch deduplication.

    This is the final stage of MinHash deduplication.
    """

    def __init__(
        self,
        config: DedupeByClusterConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        self.cluster_config = config

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Keep one document per cluster (within batch)."""
        config = self.cluster_config

        doc_ids = table.column(config.doc_id_column).to_pylist()
        cluster_ids = table.column(config.cluster_id_column).to_pylist()

        # Group by cluster
        clusters: Dict[str, List[int]] = {}
        for i, (doc_id, cluster_id) in enumerate(zip(doc_ids, cluster_ids)):
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(i)

        # Keep first document per cluster (smallest doc_id)
        keep_rows = []
        for cluster_id, row_indices in clusters.items():
            # Find row with smallest doc_id
            min_idx = min(row_indices, key=lambda i: str(doc_ids[i]))
            keep_rows.append(min_idx)

        if not keep_rows:
            return None

        # Return selected rows (without the partition column)
        return table.take(keep_rows)


DedupeByClusterConfig.operator_class = DedupeByClusterOperator
