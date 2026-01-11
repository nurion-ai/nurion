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

"""Recovery Manager - handles failure tracking and worker recovery.

Responsibilities:
- Track worker failures with sliding window
- Calculate failure rates and determine recovery strategy
- Exponential backoff for recovery attempts
- Orchestrate worker recovery (spawn + partition assignment)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List, Optional, Tuple

from solstice.core.stage_config import FailurePolicy, FailureTracker
from solstice.core.managers.partition_manager import PartitionManager
from solstice.core.managers.worker_manager import WorkerManager
from solstice.utils.logging import create_ray_logger


@dataclass
class RecoveryResult:
    """Result of a recovery attempt."""

    spawned_count: int
    failed_to_spawn: int
    orphaned_partitions_remaining: List[int]
    should_give_up: bool
    give_up_reason: Optional[str] = None


class RecoveryManager:
    """Manages failure tracking and worker recovery.

    Uses a sliding window approach for failure rate calculation:
    - Tracks failures within a configurable time window
    - Calculates failure rate per worker
    - Applies exponential backoff for recovery attempts
    - Decides when to give up based on failure rate threshold

    Thread-safe: all state modifications happen in the main asyncio loop.
    """

    def __init__(
        self,
        stage_id: str,
        partition_manager: PartitionManager,
        worker_manager: WorkerManager,
        policy: Optional[FailurePolicy] = None,
    ):
        self._stage_id = stage_id
        self._partition_manager = partition_manager
        self._worker_manager = worker_manager
        self._policy = policy or FailurePolicy()
        self._logger = create_ray_logger(f"RecoveryMgr-{stage_id}")
        self._tracker = FailureTracker(self._policy, self._logger)

    @property
    def failure_count(self) -> int:
        """Get total failure count in current window."""
        return len(self._tracker._failure_timestamps)

    @property
    def is_in_recovery(self) -> bool:
        """Check if currently in recovery mode (backoff active)."""
        return self._tracker._recovery_attempt > 0

    def record_failures(self, count: int, current_worker_count: int) -> None:
        """Record worker failures.

        Args:
            count: Number of workers that failed
            current_worker_count: Current number of active workers
        """
        self._tracker.record_failures(count, current_worker_count)

    def record_success(self) -> None:
        """Record successful worker completions (resets backoff)."""
        self._tracker.record_success()

    def should_give_up(self, current_worker_count: int) -> Tuple[bool, Optional[str]]:
        """Check if we should give up recovery attempts.

        Args:
            current_worker_count: Current number of active workers

        Returns:
            (should_give_up, reason)
        """
        return self._tracker.should_give_up(current_worker_count)

    def get_recovery_delay(self) -> float:
        """Get the current recovery delay (exponential backoff)."""
        return self._tracker.get_recovery_delay()

    async def recover_failed_workers(
        self,
        failed_worker_ids: List[str],
        partition_count: int,
    ) -> RecoveryResult:
        """Attempt to recover failed workers.

        This method:
        1. Collects orphaned partitions from failed workers
        2. Spawns replacement workers
        3. Assigns orphaned partitions to new workers
        4. Notifies new workers of upstream completion if applicable

        Args:
            failed_worker_ids: IDs of workers that failed
            partition_count: Total partition count for assignment

        Returns:
            RecoveryResult with spawn stats and remaining orphaned partitions
        """
        delay = self.get_recovery_delay()
        failure_count = len(failed_worker_ids)

        # Collect orphaned partitions
        orphaned_partitions = self._partition_manager.collect_orphaned_partitions(failed_worker_ids)

        self._logger.info(
            f"Recovering {failure_count} failed workers (backoff: {delay:.1f}s), "
            f"orphaned partitions: {orphaned_partitions}"
        )

        # Also remove from worker manager tracking
        self._worker_manager.cleanup_workers(failed_worker_ids)

        # Spawn replacement workers
        spawned = 0
        failed_to_spawn = 0

        for _ in range(failure_count):
            try:
                # If we have orphaned partitions, pass them directly to spawn_worker
                # This ensures the replacement worker gets the exact partitions the failed worker had
                partitions_for_worker = None
                if orphaned_partitions:
                    partitions_for_worker = list(orphaned_partitions)
                    orphaned_partitions.clear()

                worker_id = await self._worker_manager.spawn_worker(
                    partition_count=partition_count,
                    is_min_worker=False,
                    assigned_partitions=partitions_for_worker,
                )
                if worker_id is None:
                    # Restore orphaned partitions if spawn failed
                    if partitions_for_worker:
                        orphaned_partitions.extend(partitions_for_worker)
                    failed_to_spawn += 1
                    continue

                spawned += 1

                if partitions_for_worker:
                    self._logger.info(
                        f"Assigned orphaned partitions {partitions_for_worker} to {worker_id}"
                    )

                # Notify of upstream completion if applicable
                await self._worker_manager.notify_worker_upstream_finished(worker_id)

            except Exception as e:
                self._logger.warning(f"Failed to spawn replacement worker: {e}")
                failed_to_spawn += 1

        if spawned > 0:
            self._logger.info(f"Spawned {spawned}/{failure_count} replacement workers")
            await asyncio.sleep(delay)

        # Check if we should give up
        should_give_up, reason = self.should_give_up(self._worker_manager.worker_count)

        return RecoveryResult(
            spawned_count=spawned,
            failed_to_spawn=failed_to_spawn,
            orphaned_partitions_remaining=orphaned_partitions,
            should_give_up=should_give_up,
            give_up_reason=reason,
        )

    def reset(self) -> None:
        """Reset failure tracking state."""
        self._tracker = FailureTracker(self._policy, self._logger)
