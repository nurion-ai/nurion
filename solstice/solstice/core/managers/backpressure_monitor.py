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

"""Backpressure Monitor - handles backpressure detection and scaling.

Responsibilities:
- Monitor input queue lag
- Monitor output queue size
- Detect and signal backpressure conditions
- Calculate partition skew
- Scale up/down workers based on load
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Mapping, Optional, Protocol

from solstice.queue import QueueType, QueueClient, TansuQueueClient
from solstice.core.stage_config import StageConfig, QueueEndpoint
from solstice.core.managers.partition_manager import PartitionManager
from solstice.core.managers.worker_manager import WorkerManager

if TYPE_CHECKING:
    from solstice.core.stage_master import StageStatus


class StageStatusProvider(Protocol):
    """Protocol for objects that can provide stage status."""

    def get_status(self) -> StageStatus: ...


@dataclass
class BackpressureSignal:
    """Signal for backpressure propagation."""

    from_stage: str
    to_stage: str
    slow_down_factor: float  # 0.0 = pause, 1.0 = normal
    reason: str


@dataclass
class PartitionMetrics:
    """Metrics for a single partition."""

    partition_id: int
    latest_offset: int
    committed_offset: int
    lag: int


@dataclass
class SkewInfo:
    """Information about partition skew."""

    is_skewed: bool
    skew_ratio: float  # max_lag / avg_lag
    partition_lags: Dict[int, int]


class BackpressureMonitor:
    """Monitors backpressure and handles scaling decisions.

    Tracks:
    - Input queue lag (messages pending processing)
    - Output queue size (messages produced)
    - Partition-level skew

    Provides:
    - Backpressure signals for upstream stages
    - Scaling recommendations based on load

    Thread-safe: all state modifications happen in the main asyncio loop.
    """

    def __init__(
        self,
        stage_id: str,
        config: StageConfig,
        partition_manager: PartitionManager,
        worker_manager: WorkerManager,
        upstream_endpoint: Optional[QueueEndpoint],
        upstream_topic: Optional[str],
        consumer_group: str,
        logger: logging.Logger,
    ):
        self._stage_id = stage_id
        self._config = config
        self._partition_manager = partition_manager
        self._worker_manager = worker_manager
        self._upstream_endpoint = upstream_endpoint
        self._upstream_topic = upstream_topic
        self._consumer_group = consumer_group
        self._logger = logger

        # State
        self._backpressure_active = False
        self._downstream_refs: Dict[str, StageStatusProvider] = {}

        # Cached upstream queue client for metrics
        self._metrics_queue: Optional[TansuQueueClient] = None

    @property
    def is_backpressure_active(self) -> bool:
        """Check if backpressure is currently active."""
        return self._backpressure_active

    def set_downstream_refs(self, refs: Mapping[str, StageStatusProvider]) -> None:
        """Set references to downstream stages for backpressure propagation."""
        self._downstream_refs = dict(refs)

    def _get_metrics_queue(self) -> Optional[TansuQueueClient]:
        """Get or create a client for upstream metrics."""
        if not self._upstream_endpoint:
            return None
        if self._upstream_endpoint.queue_type != QueueType.TANSU:
            return None

        if self._metrics_queue is None:
            broker_url = f"{self._upstream_endpoint.host}:{self._upstream_endpoint.port}"
            self._metrics_queue = TansuQueueClient(broker_url)
            self._metrics_queue.start()

        return self._metrics_queue

    def get_input_lag(self) -> int:
        """Get total input queue lag (messages pending processing).

        Returns:
            Sum of (latest_offset - committed_offset) across all partitions.
        """
        if not self._upstream_endpoint or not self._upstream_topic:
            return 0

        queue = self._get_metrics_queue()
        if queue is None:
            return 0

        try:
            partition_offsets = queue.get_all_partition_offsets(self._upstream_topic)
            committed_offsets = queue.get_all_committed_offsets(
                self._consumer_group, self._upstream_topic
            )
            total_lag = 0
            for partition_id, latest_offset in partition_offsets.items():
                committed = committed_offsets.get(partition_id, 0)
                total_lag += max(0, latest_offset - committed)
            return total_lag
        except Exception as e:
            self._logger.debug(f"Error getting input lag: {e}")
            return 0

    def get_partition_metrics(self) -> Dict[int, PartitionMetrics]:
        """Get metrics for all input partitions.

        Returns:
            Dictionary mapping partition_id to PartitionMetrics
        """
        if not self._upstream_endpoint or not self._upstream_topic:
            return {}

        queue = self._get_metrics_queue()
        if queue is None:
            return {}

        try:
            partition_offsets = queue.get_all_partition_offsets(self._upstream_topic)
            committed_offsets = queue.get_all_committed_offsets(
                self._consumer_group, self._upstream_topic
            )
            metrics: Dict[int, PartitionMetrics] = {}

            for partition_id, latest_offset in partition_offsets.items():
                committed = committed_offsets.get(partition_id, 0)
                lag = max(0, latest_offset - committed)

                metrics[partition_id] = PartitionMetrics(
                    partition_id=partition_id,
                    latest_offset=latest_offset,
                    committed_offset=committed,
                    lag=lag,
                )
            return metrics
        except Exception as e:
            self._logger.debug(f"Error getting partition metrics: {e}")
            return {}

    def detect_skew(self, threshold: float = 2.0) -> SkewInfo:
        """Detect partition-level skew in input queue.

        Args:
            threshold: Skew threshold (max_lag / avg_lag)

        Returns:
            SkewInfo with detection result and partition lags
        """
        if not self._upstream_endpoint or not self._upstream_topic:
            return SkewInfo(is_skewed=False, skew_ratio=0.0, partition_lags={})

        try:
            queue = self._get_metrics_queue()
            if queue is None:
                return SkewInfo(is_skewed=False, skew_ratio=0.0, partition_lags={})

            partition_offsets = queue.get_all_partition_offsets(self._upstream_topic)
            committed_offsets = queue.get_all_committed_offsets(
                self._consumer_group, self._upstream_topic
            )
            partition_lags: Dict[int, int] = {}

            for partition_id, latest_offset in partition_offsets.items():
                committed = committed_offsets.get(partition_id, 0)
                partition_lags[partition_id] = max(0, latest_offset - committed)

            if not partition_lags:
                return SkewInfo(is_skewed=False, skew_ratio=0.0, partition_lags={})

            lags = list(partition_lags.values())
            avg_lag = sum(lags) / len(lags)
            max_lag = max(lags)

            if avg_lag == 0:
                return SkewInfo(is_skewed=False, skew_ratio=0.0, partition_lags=partition_lags)

            skew_ratio = max_lag / avg_lag
            is_skewed = skew_ratio > threshold

            if is_skewed:
                self._logger.warning(
                    f"Partition skew detected in {self._stage_id}: "
                    f"max_lag={max_lag}, avg_lag={avg_lag:.1f}, "
                    f"skew_ratio={skew_ratio:.2f}, threshold={threshold}"
                )

            return SkewInfo(
                is_skewed=is_skewed,
                skew_ratio=skew_ratio,
                partition_lags=partition_lags,
            )
        except Exception as e:
            self._logger.debug(f"Error detecting skew: {e}")
            return SkewInfo(is_skewed=False, skew_ratio=0.0, partition_lags={})

    def check_backpressure(self, output_queue: Optional[QueueClient], output_topic: str) -> bool:
        """Check if backpressure should be activated.

        Args:
            output_queue: Output queue client (if available)
            output_topic: Output topic name

        Returns:
            True if backpressure should be active
        """
        # Check input queue lag
        input_lag = self.get_input_lag()
        if input_lag > self._config.backpressure_threshold_lag:
            if not self._backpressure_active:
                self._logger.warning(
                    f"Backpressure activated for {self._stage_id}: "
                    f"input_lag={input_lag} > threshold={self._config.backpressure_threshold_lag}"
                )
            self._backpressure_active = True
            return True

        # Check output queue size
        if output_queue:
            try:
                output_size = output_queue.get_latest_offset(output_topic)
                if output_size > self._config.backpressure_threshold_queue_size:
                    if not self._backpressure_active:
                        self._logger.warning(
                            f"Backpressure activated for {self._stage_id}: "
                            f"output_queue_size={output_size} > "
                            f"threshold={self._config.backpressure_threshold_queue_size}"
                        )
                    self._backpressure_active = True
                    return True
            except Exception:
                pass

        # Deactivate with hysteresis (only when well below threshold)
        if self._backpressure_active:
            if input_lag < self._config.backpressure_threshold_lag * 0.7:
                self._logger.info(f"Backpressure deactivated for {self._stage_id}: lag={input_lag}")
                self._backpressure_active = False

        return self._backpressure_active

    def get_backpressure_signal(self) -> Optional[BackpressureSignal]:
        """Get backpressure signal for propagation to upstream stages.

        Returns:
            BackpressureSignal if backpressure is active, None otherwise
        """
        if not self._backpressure_active:
            return None

        return BackpressureSignal(
            from_stage=self._stage_id,
            to_stage="",  # Set by caller
            slow_down_factor=0.5,  # Default: slow down by 50%
            reason="queue_lag_exceeded",
        )

    async def check_downstream_backpressure(self) -> bool:
        """Check if any downstream stage has backpressure.

        Returns:
            True if production should be paused
        """
        if not self._downstream_refs:
            return False

        for stage_id, stage_ref in self._downstream_refs.items():
            try:
                status = stage_ref.get_status()
                if status.backpressure_active:
                    self._logger.debug(f"Backpressure detected from downstream stage {stage_id}")
                    return True

                if status.output_queue_size > self._config.backpressure_threshold_queue_size * 0.8:
                    self._logger.debug(
                        f"Downstream queue size {status.output_queue_size} approaching threshold"
                    )
                    return True
            except Exception as e:
                self._logger.debug(f"Error checking backpressure from {stage_id}: {e}")

        return False

    async def scale_down(self, count: int) -> int:
        """Scale down workers by removing the specified count.

        Args:
            count: Number of workers to remove

        Returns:
            Number of workers actually removed
        """
        if count <= 0:
            return 0

        current = self._worker_manager.worker_count
        min_workers = self._config.min_workers
        safe_to_remove = max(0, current - min_workers)
        actual_remove = min(count, safe_to_remove)

        if actual_remove == 0:
            self._logger.debug(f"Cannot scale down: current={current}, min={min_workers}")
            return 0

        # Select workers to remove (last N workers)
        worker_ids = self._worker_manager.worker_ids[-actual_remove:]

        removed = 0
        for worker_id in worker_ids:
            if await self._worker_manager.stop_worker(worker_id):
                removed += 1
                self._logger.debug(f"Removed worker {worker_id}")

        # Rebalance partitions among remaining workers
        if removed > 0:
            partition_count = await self._partition_manager.get_upstream_partition_count()
            self._partition_manager.rebalance(self._worker_manager.worker_ids, partition_count)
            await self._worker_manager.notify_all_partition_update()

        self._logger.info(
            f"Scaled down {self._stage_id}: removed {removed}/{count} workers "
            f"(now {self._worker_manager.worker_count} workers)"
        )
        return removed

    def stop(self) -> None:
        """Clean up resources."""
        if self._metrics_queue:
            try:
                self._metrics_queue.stop()
            except Exception as e:
                self._logger.warning(f"Error stopping metrics queue: {e}")
            self._metrics_queue = None
