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

"""Self-contained Connected Components Master.

CCIterateMaster handles iteration internally - no special logic needed
in RayJobRunner. This allows multiple iterative stages in a pipeline.

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    CCIterateMaster                          │
    │                    (self-contained)                         │
    ├─────────────────────────────────────────────────────────────┤
    │  run():                                                     │
    │    1. Read input from upstream (candidate pairs/messages)   │
    │    2. Process and update labels in state store              │
    │    3. Check if any labels changed                           │
    │    4. If changed and iteration < max:                       │
    │       - Generate new messages from updated labels           │
    │       - Loop back to step 2                                 │
    │    5. Output final labels to downstream                     │
    └─────────────────────────────────────────────────────────────┘

Key design points:
- Iteration happens INSIDE the stage, not in the runner
- State (labels) is stored in SlateDB per partition
- Each worker processes its assigned partitions
- Master coordinates iterations and checks convergence
- Multiple CCIterateMaster stages can exist in one pipeline
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Set


from solstice.core.stage_master import StageMaster
from solstice.core.stage_config import StageConfig

if TYPE_CHECKING:
    from solstice.core.stage import Stage
    from solstice.core.split_payload_store import SplitPayloadStore


@dataclass
class IterationStats:
    """Statistics for one iteration."""

    iteration: int
    changes: int = 0
    duration: float = 0.0
    partition_changes: Dict[int, int] = field(default_factory=dict)


class CCIterateMaster(StageMaster):
    """Self-contained iterative stage master for Connected Components.

    Handles iteration internally:
    1. Workers process input and report changes
    2. Master collects changes and checks convergence
    3. If not converged, triggers next iteration
    4. Workers re-process from their state
    5. When converged, outputs final results

    No special handling needed in RayJobRunner.

    Configuration is read from stage.operator_config (CCIterateConfig):
    - max_iterations: Maximum iterations before forced stop
    - convergence_threshold: Number of changes below which to stop
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        config: StageConfig,
        payload_store: "SplitPayloadStore",
    ):
        super().__init__(job_id, stage, config, payload_store)

        # Read iteration config from operator config
        op_config = stage.operator_config
        self._max_iterations = getattr(op_config, "max_iterations", 100)
        self._convergence_threshold = getattr(op_config, "convergence_threshold", 0)
        self._iteration_stats: List[IterationStats] = []

        # Iteration state
        self._current_iteration = 0
        self._converged = False
        self._partition_changes: Dict[int, int] = {}
        self._reported_partitions: Set[int] = set()

        # Event for iteration completion
        self._iteration_complete = asyncio.Event()

    async def run(self) -> bool:
        """Run the stage with internal iteration loop.

        TODO: Full iteration logic is not yet implemented.
        Currently delegates to base StageMaster.run() which just does
        one pass. The iteration logic requires:
        1. StageWorker to have start_iteration() and output_final_labels() methods
        2. Workers to call report_partition_changes() back to master
        3. Master to re-trigger workers for each iteration

        For now, cc_iterate just processes the input once and outputs results.
        This still provides label propagation - just not iterative convergence.
        """
        self.logger.info(
            f"CCIterateMaster running (max_iterations={self._max_iterations}, "
            f"NOTE: full iteration not yet implemented, running single pass)"
        )

        # Run standard stage logic for now
        return await super().run()

    async def _notify_workers_iteration(self, iteration: int) -> None:
        """Notify all workers of new iteration."""
        if not self._worker_manager:
            return

        for worker_id, worker in self._worker_manager.workers.items():
            try:
                worker.start_iteration.remote(iteration, self._get_iteration_config())
            except Exception as e:
                self.logger.warning(f"Failed to notify worker {worker_id}: {e}")

    def _get_iteration_config(self) -> Dict[str, Any]:
        """Get configuration for workers in current iteration."""
        return {
            "iteration": self._current_iteration,
            "max_iterations": self._max_iterations,
            "is_first_iteration": self._current_iteration == 1,
        }

    async def _wait_for_iteration_complete(self, timeout: float = 300.0) -> bool:
        """Wait for all partitions to report for current iteration."""
        try:
            await asyncio.wait_for(
                self._iteration_complete.wait(),
                timeout=timeout,
            )
            return True
        except asyncio.TimeoutError:
            self.logger.warning(
                f"Timeout waiting for iteration {self._current_iteration} "
                f"({len(self._reported_partitions)}/{self._partition_manager.partition_count} reported)"
            )
            return False

    def report_partition_changes(
        self,
        partition_id: int,
        change_count: int,
        iteration: int,
    ) -> None:
        """Called by workers to report changes for a partition.

        This is called via Ray remote method.
        """
        if iteration != self._current_iteration:
            self.logger.warning(
                f"Iteration mismatch: got {iteration}, expected {self._current_iteration}"
            )
            return

        self._partition_changes[partition_id] = change_count
        self._reported_partitions.add(partition_id)

        # Check if all partitions reported
        if len(self._reported_partitions) >= self._partition_manager.partition_count:
            self._iteration_complete.set()

    async def _output_final_results(self) -> None:
        """Output final labels to downstream queue.

        Workers read their final labels and output to the stage's output queue.
        """
        if not self._worker_manager:
            return

        # Tell workers to output final results
        for worker_id, worker in self._worker_manager.workers.items():
            try:
                worker.output_final_labels.remote()
            except Exception as e:
                self.logger.warning(f"Failed to trigger final output for {worker_id}: {e}")

        # Wait for workers to finish outputting
        await asyncio.sleep(1.0)  # Give workers time to output

    def get_iteration_summary(self) -> Dict[str, Any]:
        """Get summary of iteration execution."""
        return {
            "converged": self._converged,
            "total_iterations": self._current_iteration,
            "max_iterations": self._max_iterations,
            "iteration_stats": [
                {
                    "iteration": s.iteration,
                    "changes": s.changes,
                    "duration": s.duration,
                }
                for s in self._iteration_stats
            ],
        }
