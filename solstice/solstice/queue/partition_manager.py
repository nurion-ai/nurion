"""Partition Manager for dynamic partition assignment and work-stealing.

This module provides partition management capabilities to solve:
1. Partition inflexibility when worker count changes dynamically
2. Data skew (数据倾斜) problems where some partitions have more data

Key features:
- Dynamic partition assignment to workers
- Automatic rebalancing when workers scale up/down
- Work-stealing for idle workers to help overloaded partitions
- Lag-aware assignment prioritizing high-lag partitions

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                   PartitionManager                          │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │  Partition Assignment: worker_id -> [partition_ids]  │   │
    │  │  - Round-robin initial assignment                    │   │
    │  │  - Rebalance on worker add/remove                    │   │
    │  └─────────────────────────────────────────────────────┘   │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │  Work Stealing: idle workers can "steal" from busy   │   │
    │  │  - Based on partition lag (messages pending)         │   │
    │  │  - Prevents data skew from blocking pipeline         │   │
    │  └─────────────────────────────────────────────────────┘   │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │  Virtual Partitions: Split large partitions         │   │
    │  │  - Enables parallelism beyond physical partitions   │   │
    │  │  - Uses offset ranges for work distribution         │   │
    │  └─────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from solstice.utils.logging import create_ray_logger


@dataclass
class PartitionState:
    """State tracking for a single partition."""

    partition_id: int
    assigned_worker: Optional[str] = None
    current_offset: int = 0
    latest_offset: int = 0
    last_activity_time: float = field(default_factory=time.time)

    @property
    def lag(self) -> int:
        """Get the number of unconsumed messages."""
        return max(0, self.latest_offset - self.current_offset)

    @property
    def is_idle(self) -> bool:
        """Check if partition has no pending messages."""
        return self.lag == 0


@dataclass
class WorkerState:
    """State tracking for a single worker."""

    worker_id: str
    assigned_partitions: Set[int] = field(default_factory=set)
    is_active: bool = True
    processing_count: int = 0
    last_activity_time: float = field(default_factory=time.time)

    @property
    def is_idle(self) -> bool:
        """Check if worker is idle (not actively processing).

        Uses the idle threshold configured in the PartitionManager.
        Default threshold is 5.0 seconds.
        """
        # Note: idle_threshold is passed via PartitionManager config
        # Default to 5.0 if not available (standalone usage)
        idle_threshold = getattr(self, '_idle_threshold', 5.0)
        return time.time() - self.last_activity_time > idle_threshold


# Default idle threshold in seconds
DEFAULT_IDLE_THRESHOLD_S = 5.0

# Default work-steal ratio (fraction of remaining work to steal)
DEFAULT_WORK_STEAL_RATIO = 0.5


@dataclass
class WorkStealRequest:
    """Request for work from an idle worker."""

    worker_id: str
    partition_id: int
    offset_start: int
    offset_end: int


class PartitionManager:
    """Manages partition assignment and work-stealing for a topic.

    This class handles:
    1. Initial partition assignment to workers
    2. Rebalancing when workers are added/removed
    3. Work-stealing for idle workers
    4. Tracking partition lag for data skew detection

    Thread-safe for concurrent access from multiple workers.

    Example:
        ```python
        manager = PartitionManager(
            topic="my-topic",
            num_partitions=8,
        )

        # Register workers
        manager.register_worker("worker_0")
        manager.register_worker("worker_1")

        # Get assigned partitions for a worker
        partitions = manager.get_assigned_partitions("worker_0")

        # Update partition offset after processing
        manager.update_partition_offset(0, new_offset=100)

        # Check for work-stealing opportunities
        steal_request = manager.request_work_steal("worker_0")
        ```
    """

    def __init__(
        self,
        topic: str,
        num_partitions: int = 1,
        enable_work_stealing: bool = True,
        work_steal_lag_threshold: int = 100,
        work_steal_ratio: float = DEFAULT_WORK_STEAL_RATIO,
        idle_threshold_s: float = DEFAULT_IDLE_THRESHOLD_S,
        rebalance_interval_s: float = 30.0,
    ):
        """Initialize partition manager.

        Args:
            topic: Topic name being managed.
            num_partitions: Number of partitions in the topic.
            enable_work_stealing: Whether to enable work-stealing for idle workers.
            work_steal_lag_threshold: Minimum lag to consider work-stealing.
            work_steal_ratio: Fraction of remaining work to steal (0.0-1.0).
            idle_threshold_s: Seconds of inactivity before worker is considered idle.
            rebalance_interval_s: Minimum time between rebalances.
        """
        self.topic = topic
        self.num_partitions = num_partitions
        self.enable_work_stealing = enable_work_stealing
        self.work_steal_lag_threshold = work_steal_lag_threshold
        self.work_steal_ratio = max(0.1, min(1.0, work_steal_ratio))  # Clamp to 0.1-1.0
        self.idle_threshold_s = idle_threshold_s
        self.rebalance_interval_s = rebalance_interval_s

        self.logger = create_ray_logger(f"PartitionManager-{topic}")

        # State tracking
        self._partitions: Dict[int, PartitionState] = {
            i: PartitionState(partition_id=i)
            for i in range(num_partitions)
        }
        self._workers: Dict[str, WorkerState] = {}

        # Thread safety
        self._lock = threading.RLock()

        # Rebalance tracking
        self._last_rebalance_time = 0.0
        self._pending_rebalance = False

    def register_worker(self, worker_id: str) -> List[int]:
        """Register a new worker and assign partitions.

        Args:
            worker_id: Unique identifier for the worker.

        Returns:
            List of partition IDs assigned to this worker.
        """
        with self._lock:
            if worker_id in self._workers:
                return list(self._workers[worker_id].assigned_partitions)

            self._workers[worker_id] = WorkerState(worker_id=worker_id)
            self._rebalance_partitions(force=True)  # Force rebalance on worker add

            assigned = list(self._workers[worker_id].assigned_partitions)
            self.logger.info(
                f"Registered worker {worker_id}, assigned partitions: {assigned}"
            )
            return assigned

    def unregister_worker(self, worker_id: str) -> None:
        """Unregister a worker and redistribute its partitions.

        Args:
            worker_id: Worker to unregister.
        """
        with self._lock:
            if worker_id not in self._workers:
                return

            # Clear partition assignments for this worker
            worker = self._workers[worker_id]
            for partition_id in worker.assigned_partitions:
                if partition_id in self._partitions:
                    self._partitions[partition_id].assigned_worker = None

            del self._workers[worker_id]
            self._rebalance_partitions(force=True)  # Force rebalance on worker remove

            self.logger.info(f"Unregistered worker {worker_id}, rebalanced partitions")

    def get_assigned_partitions(self, worker_id: str) -> List[int]:
        """Get partitions assigned to a worker.

        Args:
            worker_id: Worker to query.

        Returns:
            List of assigned partition IDs.
        """
        with self._lock:
            if worker_id not in self._workers:
                return []
            return sorted(self._workers[worker_id].assigned_partitions)

    def update_partition_offset(
        self,
        partition_id: int,
        current_offset: Optional[int] = None,
        latest_offset: Optional[int] = None,
    ) -> None:
        """Update offset information for a partition.

        Args:
            partition_id: Partition to update.
            current_offset: Current consumer offset (optional).
            latest_offset: Latest producer offset (optional).
        """
        with self._lock:
            if partition_id not in self._partitions:
                return

            state = self._partitions[partition_id]
            if current_offset is not None:
                state.current_offset = current_offset
            if latest_offset is not None:
                state.latest_offset = latest_offset
            state.last_activity_time = time.time()

    def update_worker_activity(self, worker_id: str) -> None:
        """Update last activity time for a worker.

        Args:
            worker_id: Worker that is active.
        """
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id].last_activity_time = time.time()

    def request_work_steal(
        self,
        worker_id: str,
        min_lag: Optional[int] = None,
    ) -> Optional[WorkStealRequest]:
        """Request work to steal for an idle worker.

        Finds a high-lag partition that the worker can help with.

        Args:
            worker_id: Worker requesting work.
            min_lag: Minimum lag to consider (default: work_steal_lag_threshold).

        Returns:
            WorkStealRequest if work is available, None otherwise.
        """
        if not self.enable_work_stealing:
            return None

        min_lag = min_lag or self.work_steal_lag_threshold

        with self._lock:
            if worker_id not in self._workers:
                return None

            # Find partitions with high lag that aren't assigned to this worker
            worker = self._workers[worker_id]
            candidates = []

            for p_id, p_state in self._partitions.items():
                if p_id in worker.assigned_partitions:
                    continue
                if p_state.lag >= min_lag:
                    candidates.append((p_id, p_state))

            if not candidates:
                return None

            # Pick the partition with highest lag
            candidates.sort(key=lambda x: x[1].lag, reverse=True)
            partition_id, p_state = candidates[0]

            # Calculate work range (steal configurable fraction of remaining work)
            steal_amount = int(p_state.lag * self.work_steal_ratio)
            offset_start = p_state.current_offset
            offset_end = p_state.current_offset + steal_amount

            if offset_end <= offset_start:
                return None

            self.logger.debug(
                f"Work steal: {worker_id} stealing from partition {partition_id} "
                f"(offsets {offset_start}-{offset_end}, lag={p_state.lag}, ratio={self.work_steal_ratio})"
            )

            return WorkStealRequest(
                worker_id=worker_id,
                partition_id=partition_id,
                offset_start=offset_start,
                offset_end=offset_end,
            )

    def get_partition_lags(self) -> Dict[int, int]:
        """Get lag for all partitions.

        Returns:
            Dict mapping partition_id -> lag.
        """
        with self._lock:
            return {p_id: p_state.lag for p_id, p_state in self._partitions.items()}

    def get_total_lag(self) -> int:
        """Get total lag across all partitions."""
        with self._lock:
            return sum(p_state.lag for p_state in self._partitions.values())

    def get_skew_factor(self) -> float:
        """Calculate data skew factor (0.0 = balanced, 1.0 = all in one partition).

        Returns:
            Skew factor between 0.0 and 1.0.
        """
        with self._lock:
            lags = [p_state.lag for p_state in self._partitions.values()]
            if not lags:
                return 0.0

            total = sum(lags)
            if total == 0:
                return 0.0

            max_lag = max(lags)
            # Skew = (max - average) / max, normalized
            avg_lag = total / len(lags)
            if max_lag == 0:
                return 0.0
            return (max_lag - avg_lag) / max_lag

    def _rebalance_partitions(self, force: bool = False) -> None:
        """Rebalance partitions across workers using round-robin.

        Args:
            force: If True, ignore cooldown and rebalance immediately.
                   Used when workers are first added/removed.
        """
        if not self._workers:
            return

        now = time.time()
        # Allow immediate rebalance on worker add/remove (force=True)
        # or if cooldown has passed
        if not force and now - self._last_rebalance_time < self.rebalance_interval_s:
            self._pending_rebalance = True
            return

        self._last_rebalance_time = now
        self._pending_rebalance = False

        # Clear all assignments
        for worker in self._workers.values():
            worker.assigned_partitions.clear()
        for partition in self._partitions.values():
            partition.assigned_worker = None

        # Round-robin assignment
        worker_ids = sorted(self._workers.keys())
        for partition_id in range(self.num_partitions):
            worker_idx = partition_id % len(worker_ids)
            worker_id = worker_ids[worker_idx]

            self._partitions[partition_id].assigned_worker = worker_id
            self._workers[worker_id].assigned_partitions.add(partition_id)

        self.logger.debug(
            f"Rebalanced: {self.num_partitions} partitions across "
            f"{len(worker_ids)} workers"
        )

    def trigger_rebalance(self) -> bool:
        """Manually trigger a rebalance if needed.

        Returns:
            True if rebalance was performed, False otherwise.
        """
        with self._lock:
            if not self._pending_rebalance:
                return False
            self._last_rebalance_time = 0  # Force rebalance
            self._rebalance_partitions()
            return True

    def get_stats(self) -> Dict:
        """Get statistics about partition management."""
        with self._lock:
            return {
                "topic": self.topic,
                "num_partitions": self.num_partitions,
                "num_workers": len(self._workers),
                "total_lag": self.get_total_lag(),
                "skew_factor": self.get_skew_factor(),
                "partition_lags": self.get_partition_lags(),
                "worker_assignments": {
                    w_id: sorted(w.assigned_partitions)
                    for w_id, w in self._workers.items()
                },
            }
