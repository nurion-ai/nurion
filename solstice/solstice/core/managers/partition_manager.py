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

"""Partition Manager - handles partition assignment and rebalancing.

Responsibilities:
- Compute partition count based on config
- Query upstream partition count
- Assign partitions to workers (round-robin)
- Rebalance partitions on worker changes
- Track orphaned partitions during recovery
"""

from __future__ import annotations

from typing import Dict, List, Optional

from solstice.queue import QueueType, TansuQueueClient
from solstice.core.stage_config import StageConfig, QueueEndpoint
from solstice.utils.logging import create_ray_logger


class PartitionManager:
    """Manages partition assignment and rebalancing for a stage.

    Uses round-robin distribution to ensure all partitions are covered:
    - 4 partitions, 2 workers: worker0 -> [0,2], worker1 -> [1,3]
    - 4 partitions, 3 workers: worker0 -> [0,3], worker1 -> [1], worker2 -> [2]

    Thread-safe: all state modifications happen in the main asyncio loop.
    """

    def __init__(
        self,
        stage_id: str,
        config: StageConfig,
        upstream_endpoint: Optional[QueueEndpoint],
        upstream_topic: Optional[str],
    ):
        self._stage_id = stage_id
        self._config = config
        self._upstream_endpoint = upstream_endpoint
        self._upstream_topic = upstream_topic
        self._logger = create_ray_logger(f"PartitionMgr-{stage_id}")

        # Partition state
        self._partition_count: Optional[int] = None
        self._upstream_partition_count: Optional[int] = None

        # Worker -> Partitions mapping
        self._assignments: Dict[str, List[int]] = {}

        # Cached upstream queue client for partition queries
        self._upstream_queue: Optional[TansuQueueClient] = None

    @property
    def partition_count(self) -> int:
        """Get the output partition count (cached after first computation)."""
        if self._partition_count is None:
            self._partition_count = self._compute_partition_count()
        return self._partition_count

    @property
    def assignments(self) -> Dict[str, List[int]]:
        """Get current partition assignments (read-only view)."""
        return self._assignments.copy()

    def _compute_partition_count(self) -> int:
        """Compute the number of partitions based on worker configuration.

        Returns:
            Number of partitions to use. If partition_count is explicitly set,
            use that. Otherwise, auto-compute based on max_workers.
        """
        if self._config.partition_count is not None:
            return max(1, self._config.partition_count)

        # Auto-compute: use max_workers as partition count
        if self._config.max_workers <= 1:
            return 1
        return self._config.max_workers

    async def get_upstream_partition_count(self) -> int:
        """Get the partition count of the upstream topic.

        For non-source stages, workers need to be assigned partitions based on
        the upstream topic's partition count, not this stage's output partition count.

        Returns:
            Number of partitions in upstream topic, or 1 if no upstream.
        """
        if self._upstream_partition_count is not None:
            return self._upstream_partition_count

        # Source stages have no upstream
        if not self._upstream_endpoint or not self._upstream_topic:
            self._upstream_partition_count = 1
            return 1

        # Query upstream topic partition count
        queue = await self._get_upstream_queue()
        if queue is None:
            self._upstream_partition_count = 1
            return 1

        try:
            offsets = queue.get_all_partition_offsets(self._upstream_topic)
            self._upstream_partition_count = max(1, len(offsets))
            self._logger.debug(
                f"Upstream topic {self._upstream_topic} has "
                f"{self._upstream_partition_count} partition(s)"
            )
        except Exception as e:
            self._logger.warning(f"Failed to get upstream partition count: {e}")
            self._upstream_partition_count = 1

        return self._upstream_partition_count

    async def _get_upstream_queue(self) -> Optional[TansuQueueClient]:
        """Get or create a client-only queue for upstream partition queries."""
        if not self._upstream_endpoint:
            return None
        if self._upstream_endpoint.queue_type != QueueType.TANSU:
            return None

        if self._upstream_queue is None:
            broker_url = f"{self._upstream_endpoint.host}:{self._upstream_endpoint.port}"
            self._upstream_queue = TansuQueueClient(broker_url)
            self._upstream_queue.start()

        return self._upstream_queue

    def get_assignment(self, worker_id: str) -> List[int]:
        """Get the current partition assignment for a worker.

        Returns empty list if worker has no assigned partitions (idle worker).
        """
        return self._assignments.get(worker_id, [])

    def compute_initial_assignment(
        self,
        worker_index: int,
        target_worker_count: int,
        partition_count: int,
    ) -> List[int]:
        """Compute partition assignment for a new worker during startup.

        This is used during startup when spawning workers one at a time,
        but we want correct distribution from the beginning.

        Args:
            worker_index: Index of the new worker (0-based)
            target_worker_count: Total number of workers expected
            partition_count: Total number of partitions

        Returns:
            List of partition IDs assigned to this worker
        """
        return [p for p in range(partition_count) if p % target_worker_count == worker_index]

    def assign_worker(
        self,
        worker_id: str,
        worker_index: int,
        target_worker_count: int,
        partition_count: int,
    ) -> List[int]:
        """Assign partitions to a new worker.

        Args:
            worker_id: ID of the new worker
            worker_index: Index of this worker (0-based)
            target_worker_count: Expected total workers
            partition_count: Total partitions

        Returns:
            List of assigned partition IDs
        """
        partitions = self.compute_initial_assignment(
            worker_index, target_worker_count, partition_count
        )
        self._assignments[worker_id] = partitions
        return partitions

    def remove_worker(self, worker_id: str) -> List[int]:
        """Remove a worker and return its orphaned partitions.

        Args:
            worker_id: ID of the worker to remove

        Returns:
            List of partitions that were assigned to this worker (now orphaned)
        """
        return self._assignments.pop(worker_id, [])

    def collect_orphaned_partitions(self, worker_ids: List[str]) -> List[int]:
        """Collect orphaned partitions from multiple failed workers.

        Args:
            worker_ids: IDs of failed workers

        Returns:
            Sorted list of unique orphaned partition IDs
        """
        orphaned: List[int] = []
        for worker_id in worker_ids:
            partitions = self._assignments.pop(worker_id, [])
            orphaned.extend(partitions)
            if partitions:
                self._logger.debug(f"Collected orphaned partitions {partitions} from {worker_id}")
        return sorted(set(orphaned))

    def assign_orphaned_partition(self, worker_id: str, partition: int) -> bool:
        """Assign a single orphaned partition to a worker.

        A partition can only be assigned to ONE worker. If the partition
        is already assigned to another worker, this method returns False.

        Args:
            worker_id: ID of the worker to receive the partition
            partition: Partition ID to assign

        Returns:
            True if assigned successfully, False if partition already assigned
        """
        # Check if partition is already assigned to another worker
        for wid, partitions in self._assignments.items():
            if wid != worker_id and partition in partitions:
                self._logger.warning(
                    f"Partition {partition} already assigned to {wid}, cannot assign to {worker_id}"
                )
                return False

        current = self._assignments.get(worker_id, [])
        if partition not in current:
            current.append(partition)
            self._assignments[worker_id] = sorted(current)
        return True

    def rebalance(self, worker_ids: List[str], partition_count: int) -> None:
        """Recompute partition assignments for all workers.

        Uses round-robin distribution to ensure all partitions are covered.

        Args:
            worker_ids: List of current worker IDs
            partition_count: Total number of partitions
        """
        self._assignments.clear()

        if not worker_ids:
            return

        num_workers = len(worker_ids)

        # Round-robin assignment
        for i, worker_id in enumerate(worker_ids):
            partitions = [p for p in range(partition_count) if p % num_workers == i]
            self._assignments[worker_id] = partitions

        idle_workers = [wid for wid, parts in self._assignments.items() if not parts]
        if idle_workers:
            self._logger.warning(
                f"Partition rebalance: {len(idle_workers)} workers have no partitions "
                f"(partition_count={partition_count} < num_workers={num_workers}). "
                f"Consider increasing partition_count or reducing workers."
            )
        self._logger.debug(f"Partition rebalance: {self._assignments}")

    def validate_no_duplicate_assignments(self) -> bool:
        """Validate that no partition is assigned to multiple workers.

        Returns:
            True if valid (no duplicates), False if duplicates found
        """
        seen: Dict[int, str] = {}
        for worker_id, partitions in self._assignments.items():
            for p in partitions:
                if p in seen:
                    self._logger.error(f"Partition {p} assigned to both {seen[p]} and {worker_id}")
                    return False
                seen[p] = worker_id
        return True

    def stop(self) -> None:
        """Clean up resources."""
        if self._upstream_queue:
            try:
                self._upstream_queue.stop()
            except Exception as e:
                self._logger.warning(f"Error stopping upstream queue: {e}")
            self._upstream_queue = None
