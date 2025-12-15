"""Stage Master v2 - Simplified queue-based architecture.

Key differences from v1:
- Master only manages its output queue
- Workers pull directly from upstream queue (not master-to-master)
- Uses QueueBackend abstraction for flexibility
- Cleaner separation of concerns

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                     Stage Master                            │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │              Output Queue (QueueBackend)            │   │
    │  │  - Persistent (Tansu) or in-memory                  │   │
    │  │  - Offset tracking for exactly-once                 │   │
    │  └─────────────────────────────────────────────────────┘   │
    │                           ▲                                 │
    │                           │ produce                         │
    │  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
    │  │  Worker 1  │  │  Worker 2  │  │  Worker N  │            │
    │  │            │  │            │  │            │            │
    │  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘            │
    │        │               │               │                    │
    │        │ fetch         │ fetch         │ fetch              │
    │        ▼               ▼               ▼                    │
    └────────────────────────────────────────────────────────────┘
                             │
                             │ fetch from upstream queue
                             ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                  Upstream Stage Master                      │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │              Output Queue (QueueBackend)            │   │
    │  └─────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional

import ray

from solstice.queue import QueueBackend
from solstice.queue.factory import create_queue_backend
from solstice.utils.logging import create_ray_logger
from solstice.core.split_payload_store import SplitPayloadStore

if TYPE_CHECKING:
    from solstice.core.stage import Stage


class QueueType(str, Enum):
    """Type of queue backend to use."""

    MEMORY = "memory"  # In-process only (for single-worker testing)
    TANSU = "tansu"  # Persistent broker (for production)


@dataclass
class StageConfig:
    """Configuration for Stage Master v2.

    Attributes:
        queue_type: Type of queue backend:
            - MEMORY: In-process only (single-worker testing)
            - RAY: Shared via Ray actor (distributed testing)
            - TANSU: Persistent broker (production)
        tansu_storage_url: Storage URL for Tansu backend (memory://, s3://)
        tansu_port: Port for Tansu broker
        max_workers: Maximum number of workers
        min_workers: Minimum number of workers
        batch_size: Number of messages to fetch per batch
        commit_interval_ms: Interval between offset commits (ms)
        processing_timeout_s: Timeout for processing a single message
        partition_count: Number of partitions for the output queue.
            If None, automatically set based on max_workers.
            For single worker, uses 1 partition. For multiple workers,
            uses min(max_workers, actual_worker_count) partitions.
    """

    queue_type: QueueType = QueueType.TANSU  # Default to Tansu for persistence
    tansu_storage_url: str = "memory://"
    tansu_port: int = 9092

    max_workers: int = 4
    min_workers: int = 1

    batch_size: int = 100
    commit_interval_ms: int = 5000
    processing_timeout_s: float = 300.0

    # Partition configuration
    partition_count: Optional[int] = None  # None = auto based on workers

    # Backpressure thresholds
    backpressure_threshold_lag: int = 5000
    backpressure_threshold_queue_size: int = 1000

    # Worker resources
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "tansu_storage_url": self.tansu_storage_url,
            "tansu_port": self.tansu_port,
            "max_workers": self.max_workers,
            "min_workers": self.min_workers,
            "batch_size": self.batch_size,
            "commit_interval_ms": self.commit_interval_ms,
            "processing_timeout_s": self.processing_timeout_s,
            "partition_count": self.partition_count,
            "backpressure_threshold_lag": self.backpressure_threshold_lag,
            "backpressure_threshold_queue_size": self.backpressure_threshold_queue_size,
        }


@dataclass
class QueueMessage:
    """Message format for inter-stage communication.

    The actual data payload is stored in SplitPayloadStore,
    only the reference key is passed through the queue.
    """

    message_id: str
    split_id: str
    payload_key: str  # Key to lookup SplitPayload in SplitPayloadStore
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "message_id": self.message_id,
                "split_id": self.split_id,
                "payload_key": self.payload_key,
                "metadata": self.metadata,
                "timestamp": self.timestamp,
            }
        ).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueueMessage":
        d = json.loads(data.decode())
        return cls(**d)


@dataclass
class StageStatus:
    """Status of a stage."""

    stage_id: str
    worker_count: int
    output_queue_size: int
    is_running: bool
    is_finished: bool
    failed: bool = False
    failure_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    backpressure_active: bool = False  # Backpressure status


@dataclass
class QueueEndpoint:
    """Queue connection info that can be serialized to workers.

    Workers use this to create their own queue connections.
    """

    queue_type: QueueType
    host: str = "localhost"
    port: int = 9092
    storage_url: str = "memory://"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "host": self.host,
            "port": self.port,
            "storage_url": self.storage_url,
        }


def create_queue_endpoint(
    queue_type: QueueType,
    host: str | None = None,
    port: int | None = None,
    storage_url: str | None = None,
) -> QueueEndpoint:
    """Factory to build a queue endpoint without scattering conditionals."""
    return QueueEndpoint(
        queue_type=queue_type,
        host=host or "localhost",
        port=port if port is not None else 9092,
        storage_url=storage_url or "memory://",
    )


class StageMaster:
    """Simplified stage master that only manages output queue.

    Responsibilities:
    1. Manage output queue (create, provide access)
    2. Spawn and monitor workers
    3. Track stage completion

    NOT responsible for:
    - Pulling from upstream (workers do this)
    - Scheduling splits to workers (workers self-schedule)
    - Complex backpressure (queue handles this)
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        config: StageConfig,
        payload_store: SplitPayloadStore,
        upstream_endpoint: Optional[QueueEndpoint] = None,
        upstream_topic: Optional[str] = None,
    ):
        self.job_id = job_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.config = config
        self.upstream_endpoint = upstream_endpoint
        self.upstream_topic = upstream_topic

        self.logger = create_ray_logger(f"Master-{self.stage_id}")

        # SplitPayloadStore - shared across all stages
        self.payload_store = payload_store

        # Output queue (managed by master)
        self._output_queue: Optional[QueueBackend] = None
        self._output_topic = f"{job_id}_{self.stage_id}_output"

        # Output endpoint info for workers/downstream
        self._output_endpoint: Optional[QueueEndpoint] = None

        # Workers
        self._workers: Dict[str, ray.actor.ActorHandle] = {}
        self._worker_tasks: Dict[str, ray.ObjectRef] = {}

        # State
        self._running = False
        self._finished = False
        self._failed = False
        self._failure_message: Optional[str] = None
        self._start_time: Optional[float] = None

        # Upstream completion tracking
        self._upstream_finished = False

        # Consumer group for offset tracking
        self._consumer_group = f"{job_id}_{self.stage_id}"

        # Cached upstream queue backend for metrics collection (client-only, reused)
        self._upstream_metrics_queue: Optional[QueueBackend] = None

        # Backpressure state
        self._backpressure_active = False
        self._downstream_stage_refs: Dict[str, StageMaster] = {}  # For backpressure propagation

    async def _get_upstream_metrics_queue(self) -> Optional[QueueBackend]:
        """Get or create a client-only queue backend for upstream metrics/lag/skew."""
        if not self.upstream_endpoint or self.upstream_endpoint.queue_type != QueueType.TANSU:
            return None

        if self._upstream_metrics_queue is None:
            self._upstream_metrics_queue = create_queue_backend(
                queue_type=self.upstream_endpoint.queue_type,
                storage_url=self.upstream_endpoint.storage_url,
                port=self.upstream_endpoint.port,
                client_only=True,
            )
            await self._upstream_metrics_queue.start()

        return self._upstream_metrics_queue

    def _compute_partition_count(self) -> int:
        """Compute the number of partitions based on worker configuration.

        Returns:
            Number of partitions to use. If partition_count is explicitly set,
            use that. Otherwise, auto-compute based on max_workers:
            - Single worker: 1 partition
            - Multiple workers: min(max_workers, current_worker_count)
        """
        if self.config.partition_count is not None:
            return max(1, self.config.partition_count)

        # Auto-compute based on workers
        # Use max_workers as a proxy for expected parallelism
        # For single worker, use 1 partition; for multiple, use max_workers
        if self.config.max_workers <= 1:
            return 1

        # For multiple workers, use max_workers as partition count
        # This allows each worker to potentially consume from a different partition
        return self.config.max_workers

    async def _create_queue(self) -> QueueBackend:
        """Create the appropriate queue backend with dynamic partition count."""
        # Compute partition count
        partition_count = self._compute_partition_count()

        # MEMORY - only for single-process testing; clamp partition count
        if self.config.queue_type != QueueType.TANSU and partition_count > 1:
            self.logger.warning(
                f"Memory backend doesn't support multiple partitions. "
                f"Using 1 partition instead of {partition_count}"
            )
            partition_count = 1

        queue = create_queue_backend(
            queue_type=self.config.queue_type,
            storage_url=self.config.tansu_storage_url,
            port=None,  # allow backend to choose a free port if applicable
            client_only=False,
        )
        await queue.start()

        self._output_endpoint = QueueEndpoint(
            queue_type=self.config.queue_type,
            host=queue.host,
            port=queue.port,
            storage_url=self.config.tansu_storage_url,
        )
        self.logger.info(
            f"Created {self.config.queue_type} backend on {self._output_endpoint.host}:{self._output_endpoint.port} "
            f"with {partition_count} partition(s)"
        )

        await queue.create_topic(self._output_topic, partitions=partition_count)
        self.logger.info(f"Created topic {self._output_topic} with {partition_count} partition(s)")
        return queue

    async def start(self) -> None:
        """Start the stage master."""
        if self._running:
            return

        self.logger.info(f"Starting stage {self.stage_id}")
        self._start_time = time.time()
        self._running = True

        # Create output queue
        self._output_queue = await self._create_queue()

        # Spawn workers
        for i in range(self.config.min_workers):
            await self._spawn_worker()

        self.logger.info(f"Stage {self.stage_id} started with {len(self._workers)} workers")

    async def _spawn_worker(self) -> str:
        """Spawn a new worker."""
        worker_id = f"{self.stage_id}_w{len(self._workers)}_{uuid.uuid4().hex[:6]}"

        # Create worker actor
        resources = {}
        if self.config.num_cpus > 0:
            resources["num_cpus"] = self.config.num_cpus
        if self.config.num_gpus > 0:
            resources["num_gpus"] = self.config.num_gpus
        if self.config.memory_mb > 0:
            resources["memory"] = self.config.memory_mb * 1024 * 1024

        worker = StageWorker.options(
            name=f"{self.stage_id}:{worker_id}",
            **resources,
        ).remote(
            worker_id=worker_id,
            stage=self.stage,
            upstream_endpoint=self.upstream_endpoint,
            upstream_topic=self.upstream_topic,
            output_endpoint=self._output_endpoint,
            output_topic=self._output_topic,
            consumer_group=self._consumer_group,
            config=self.config,
            payload_store=self.payload_store,
        )

        self._workers[worker_id] = worker

        # Start worker run loop
        task = worker.run.remote()
        self._worker_tasks[worker_id] = task

        self.logger.info(f"Spawned worker {worker_id}")
        return worker_id

    async def run(self) -> bool:
        """Run the stage until completion."""
        if not self._running:
            await self.start()

        try:
            # Wait for all workers to complete
            while self._running and not self._finished:
                # Check worker status
                done_tasks = []
                for worker_id, task in list(self._worker_tasks.items()):
                    try:
                        ready, _ = ray.wait([task], timeout=0.1)
                        if ready:
                            try:
                                result = ray.get(ready[0])
                                self.logger.info(f"Worker {worker_id} completed: {result}")
                            except Exception as e:
                                self.logger.error(f"Worker {worker_id} failed: {e}")
                                self._failed = True
                                self._failure_message = str(e)
                            done_tasks.append(worker_id)
                    except Exception as e:
                        self.logger.error(f"Error checking worker {worker_id}: {e}")

                # Remove completed workers
                for worker_id in done_tasks:
                    self._workers.pop(worker_id, None)
                    self._worker_tasks.pop(worker_id, None)

                # Check if all workers done
                if not self._workers:
                    self._finished = True
                    break

                await asyncio.sleep(0.1)

            if self._failed:
                raise RuntimeError(self._failure_message)

            return True

        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the stage master."""
        self._running = False

        # Stop all workers
        for worker_id, worker in list(self._workers.items()):
            try:
                ray.get(worker.stop.remote(), timeout=5)
            except Exception as e:
                self.logger.warning(f"Error stopping worker {worker_id}: {e}")

        self._workers.clear()
        self._worker_tasks.clear()

        # Clean up metrics queue backend if it was created
        if self._upstream_metrics_queue:
            try:
                await self._upstream_metrics_queue.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping metrics queue backend: {e}")
            self._upstream_metrics_queue = None

        # Note: Don't stop output queue here - downstream stages may still need it
        # The queue will be cleaned up by the runner after all stages are done

        self.logger.info(f"Stage {self.stage_id} stopped")

    async def cleanup_queue(self) -> None:
        """Clean up the output queue. Called by runner after all consumers are done."""
        if self._output_queue:
            await self._output_queue.stop()
            self._output_queue = None

    def notify_upstream_finished(self) -> None:
        """Notify this stage that all upstream stages have finished.

        This allows workers to stop waiting for more data once
        they've consumed everything from the upstream queue.
        """
        self._upstream_finished = True
        self.logger.info(f"Stage {self.stage_id} notified: upstream finished")

        # Notify all workers that upstream is done
        for worker_id, worker in self._workers.items():
            try:
                ray.get(worker.notify_upstream_finished.remote(), timeout=5)
            except Exception as e:
                self.logger.warning(f"Failed to notify worker {worker_id}: {e}")

    def get_output_queue(self) -> Optional[QueueBackend]:
        """Get the output queue for downstream stages."""
        return self._output_queue

    def get_output_topic(self) -> str:
        """Get the output topic name."""
        return self._output_topic

    def get_status(self) -> StageStatus:
        """Get current stage status."""
        return StageStatus(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            output_queue_size=0,  # Use async get_status_async for queue size
            is_running=self._running,
            is_finished=self._finished,
            failed=self._failed,
            failure_message=self._failure_message,
            backpressure_active=self._backpressure_active,
        )

    async def get_status_async(self) -> StageStatus:
        """Get current stage status with queue metrics."""
        output_size = 0
        if self._output_queue:
            try:
                output_size = await self._output_queue.get_latest_offset(self._output_topic)
            except Exception:
                pass

        return StageStatus(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            output_queue_size=output_size,
            is_running=self._running,
            is_finished=self._finished,
            failed=self._failed,
            failure_message=self._failure_message,
            backpressure_active=self._backpressure_active,
        )

    async def collect_metrics(self):
        """Collect comprehensive stage metrics including partition-level information."""
        from solstice.core.models import StageMetrics

        # Check backpressure status
        await self._check_backpressure()

        # Get partition metrics and detect skew
        partition_metrics = await self.get_partition_metrics()
        skew_detected, skew_ratio, partition_lags = await self._detect_partition_skew()

        # Calculate total input lag

        return StageMetrics(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            input_records=0,  # TODO: Aggregate from workers
            output_records=0,  # TODO: Aggregate from workers
            total_processing_time=0.0,  # TODO: Aggregate from workers
            pending_splits=0,  # Not applicable in queue-based model
            inflight_results=0,  # Not applicable in queue-based model
            output_buffer_size=0,  # Not applicable in queue-based model
            backpressure_active=self._backpressure_active,
            uptime_secs=time.time() - (self._start_time or time.time()),
            partition_metrics=partition_metrics,
            skew_detected=skew_detected,
            skew_ratio=skew_ratio,
        )

    async def get_input_queue_lag(self) -> int:
        """Get the input queue lag (messages pending to be processed).

        This is calculated as: sum of (latest_offset - committed_offset) across all partitions
        Returns 0 if upstream info is not available.
        """
        if not self.upstream_endpoint or not self.upstream_topic:
            return 0

        queue = await self._get_upstream_metrics_queue()
        if queue is None:
            return 0

        partition_offsets = await queue.get_all_partition_offsets(self.upstream_topic)
        total_lag = 0
        for partition_id, latest_offset in partition_offsets.items():
            committed = await queue.get_committed_offset(
                self._consumer_group, self.upstream_topic, partition=partition_id
            )
            committed = committed or 0
            total_lag += max(0, latest_offset - committed)

        return total_lag

    async def _detect_partition_skew(
        self, skew_threshold: float = 2.0
    ) -> tuple[bool, float, Dict[int, int]]:
        """Detect partition-level skew in the input queue.

        Args:
            skew_threshold: Threshold for skew detection. If max_lag / avg_lag > threshold,
                skew is detected. Default is 2.0 (max lag is 2x average).

        Returns:
            Tuple of (skew_detected, skew_ratio, partition_lags)
            - skew_detected: True if skew is detected
            - skew_ratio: max_lag / avg_lag (1.0 means no skew)
            - partition_lags: Dict mapping partition_id to lag
        """
        if not self.upstream_endpoint or not self.upstream_topic:
            return False, 0.0, {}

        try:
            queue = await self._get_upstream_metrics_queue()
            if queue is None:
                return False, 0.0, {}

            partition_offsets = await queue.get_all_partition_offsets(self.upstream_topic)
            partition_lags: Dict[int, int] = {}

            for partition_id, latest_offset in partition_offsets.items():
                committed = await queue.get_committed_offset(
                    self._consumer_group, self.upstream_topic, partition=partition_id
                )
                committed = committed or 0
                lag = max(0, latest_offset - committed)
                partition_lags[partition_id] = lag

            if not partition_lags:
                return False, 0.0, {}

            # Calculate skew
            lags = list(partition_lags.values())
            avg_lag = sum(lags) / len(lags)
            max_lag = max(lags)

            if avg_lag == 0:
                return False, 0.0, partition_lags

            skew_ratio = max_lag / avg_lag
            skew_detected = skew_ratio > skew_threshold

            if skew_detected:
                self.logger.warning(
                    f"Partition skew detected in {self.stage_id}: "
                    f"max_lag={max_lag}, avg_lag={avg_lag:.1f}, "
                    f"skew_ratio={skew_ratio:.2f}, threshold={skew_threshold}"
                )

            return skew_detected, skew_ratio, partition_lags
        except Exception as e:
            self.logger.debug(f"Error detecting partition skew: {e}")

        return False, 0.0, {}

    async def get_partition_metrics(self) -> Dict[int, Any]:
        """Get metrics for all partitions in the input queue.

        Returns:
            Dictionary mapping partition_id to PartitionMetrics
        """
        from solstice.core.models import PartitionMetrics

        if not self.upstream_endpoint or not self.upstream_topic:
            return {}

        try:
            queue = await self._get_upstream_metrics_queue()
            if queue is None:
                return {}

            partition_offsets = await queue.get_all_partition_offsets(self.upstream_topic)
            partition_metrics: Dict[int, PartitionMetrics] = {}

            for partition_id, latest_offset in partition_offsets.items():
                committed = await queue.get_committed_offset(
                    self._consumer_group, self.upstream_topic, partition=partition_id
                )
                committed = committed or 0
                lag = max(0, latest_offset - committed)

                partition_metrics[partition_id] = PartitionMetrics(
                    partition_id=partition_id,
                    latest_offset=latest_offset,
                    committed_offset=committed,
                    lag=lag,
                )

            return partition_metrics
        except Exception as e:
            self.logger.debug(f"Error getting partition metrics: {e}")

        return {}

    async def _check_backpressure(self) -> bool:
        """Check if backpressure should be activated based on queue lag and size.

        Returns:
            True if backpressure should be active, False otherwise
        """
        # Check input queue lag
        input_lag = await self.get_input_queue_lag()
        if input_lag > self.config.backpressure_threshold_lag:
            if not self._backpressure_active:
                self.logger.warning(
                    f"Backpressure activated for {self.stage_id}: "
                    f"input_lag={input_lag} > threshold={self.config.backpressure_threshold_lag}"
                )
            self._backpressure_active = True
            return True

        # Check output queue size (if we have output queue)
        if self._output_queue:
            try:
                output_size = await self._output_queue.get_latest_offset(self._output_topic)
                if output_size > self.config.backpressure_threshold_queue_size:
                    if not self._backpressure_active:
                        self.logger.warning(
                            f"Backpressure activated for {self.stage_id}: "
                            f"output_queue_size={output_size} > threshold={self.config.backpressure_threshold_queue_size}"
                        )
                    self._backpressure_active = True
                    return True
            except Exception:
                pass

        # Deactivate backpressure if conditions are met
        if self._backpressure_active:
            # Use hysteresis: deactivate only when well below threshold
            if input_lag < self.config.backpressure_threshold_lag * 0.7:
                self.logger.info(f"Backpressure deactivated for {self.stage_id}: lag={input_lag}")
                self._backpressure_active = False

        return self._backpressure_active

    def get_backpressure_signal(self):
        """Get backpressure signal for propagation to upstream stages.

        Returns:
            BackpressureSignal if backpressure is active, None otherwise
        """
        from solstice.core.models import BackpressureSignal

        if not self._backpressure_active:
            return None

        # Calculate slow-down factor based on queue lag
        # Factor ranges from 0.0 (pause) to 1.0 (normal speed)
        # For now, use a default factor - actual lag will be checked by the caller
        slow_down_factor = 0.5  # Default: slow down by 50%

        return BackpressureSignal(
            from_stage=self.stage_id,
            to_stage="",  # Will be set by propagation logic
            slow_down_factor=slow_down_factor,
            reason="queue_lag_exceeded",
        )

    def set_downstream_stage_refs(self, downstream_refs: Dict[str, Any]) -> None:
        self._downstream_stage_refs = downstream_refs

    async def propagate_backpressure_to_upstream(self) -> None:
        """Propagate backpressure signal to upstream stages.

        This method should be called periodically to check backpressure
        and propagate signals to upstream stages.
        """
        if not self._backpressure_active:
            return

        signal = self.get_backpressure_signal()
        if not signal:
            return

        # TODO: Implement upstream stage reference tracking and propagation
        # For now, this is a placeholder
        self.logger.debug(f"Would propagate backpressure from {self.stage_id} to upstream stages")

    async def _check_backpressure_before_produce(self) -> bool:
        """Check if we should pause production due to downstream backpressure.

        This method can be used by source stages to check downstream backpressure
        before producing data.

        Returns:
            True if production should be paused, False otherwise
        """
        # Check if we have downstream stages configured
        if not self._downstream_stage_refs:
            return False

        # Check all downstream stages for backpressure
        for stage_id, stage_ref in self._downstream_stage_refs.items():
            try:
                status = await stage_ref.get_status_async()

                # Check if backpressure is active
                if status.backpressure_active:
                    self.logger.debug(
                        f"Backpressure detected from downstream stage {stage_id}, "
                        f"pausing production"
                    )
                    return True

                # Also check queue size if available
                # Use a threshold (e.g., 80% of max queue size)
                queue_size = status.output_queue_size
                if queue_size > self.config.backpressure_threshold_queue_size * 0.8:
                    self.logger.debug(
                        f"Downstream queue size {queue_size} approaching threshold, "
                        f"slowing down production"
                    )
                    return True

            except Exception as e:
                self.logger.debug(f"Error checking backpressure from {stage_id}: {e}")
                # Continue checking other downstream stages

        return False

    async def scale_down(self, count: int) -> int:
        """Gracefully remove workers.

        Args:
            count: Number of workers to remove

        Returns:
            Number of workers actually removed
        """
        if count <= 0:
            return 0

        # Don't go below min_workers
        current = len(self._workers)
        min_workers = self.config.min_workers
        safe_to_remove = max(0, current - min_workers)
        actual_remove = min(count, safe_to_remove)

        if actual_remove == 0:
            self.logger.debug(f"Cannot scale down: current={current}, min={min_workers}")
            return 0

        # Select workers to remove (prefer idle workers, but we don't track that yet)
        # For now, just remove the last N workers
        workers_to_remove = list(self._workers.items())[-actual_remove:]

        removed = 0
        for worker_id, worker in workers_to_remove:
            try:
                # Stop the worker gracefully
                ray.get(worker.stop.remote(), timeout=10)
                self._workers.pop(worker_id, None)
                self._worker_tasks.pop(worker_id, None)
                removed += 1
                self.logger.debug(f"Removed worker {worker_id}")
            except Exception as e:
                self.logger.warning(f"Error removing worker {worker_id}: {e}")

        self.logger.info(
            f"Scaled down {self.stage_id}: removed {removed}/{count} workers "
            f"(now {len(self._workers)} workers)"
        )
        return removed


@ray.remote
class StageWorker:
    """Worker that pulls from upstream queue and produces to output queue.

    This worker is self-scheduling: it pulls messages from upstream,
    processes them, and produces results to the output queue.

    Exactly-once semantics:
    1. Fetch batch from upstream
    2. Process each message
    3. Produce output to output queue
    4. Commit upstream offset (only after output is durably stored)

    Note: Workers create their own queue connections from endpoints,
    since QueueBackend instances contain locks and cannot be serialized.
    """

    def __init__(
        self,
        worker_id: str,
        stage: "Stage",
        upstream_endpoint: Optional[QueueEndpoint],
        upstream_topic: Optional[str],
        output_endpoint: QueueEndpoint,
        output_topic: str,
        consumer_group: str,
        config: StageConfig,
        payload_store: SplitPayloadStore,
    ):
        self.worker_id = worker_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.config = config

        # SplitPayloadStore for storing SplitPayload data across workers
        self.payload_store = payload_store

        # Store endpoints (will create connections in run())
        self.upstream_endpoint = upstream_endpoint
        self.upstream_topic = upstream_topic
        self.output_endpoint = output_endpoint
        self.output_topic = output_topic
        self.consumer_group = consumer_group

        # Queue connections (created lazily)
        self.upstream_queue: Optional[QueueBackend] = None
        self.output_queue: Optional[QueueBackend] = None

        self.logger = create_ray_logger(f"Worker-{self.stage_id}-{worker_id}")

        # Initialize operator using OperatorConfig.setup()
        self.operator = stage.operator_config.setup(worker_id=worker_id)

        # State
        self._running = False
        self._processed_count = 0
        self._error_count = 0
        self._last_commit_time = time.time()
        self._upstream_finished = False

    async def _create_queue_from_endpoint(self, endpoint: QueueEndpoint) -> QueueBackend:
        """Create a queue connection from endpoint info."""
        queue = create_queue_backend(
            queue_type=endpoint.queue_type,
            storage_url=endpoint.storage_url,
            port=endpoint.port,
            client_only=True,  # Worker should only connect to existing queue
        )
        await queue.start()
        return queue

    async def run(self) -> Dict[str, Any]:
        """Main processing loop.

        Workers always consume from upstream queue. Source stages use
        SourceMaster which writes splits to a queue before workers consume.
        """
        self._running = True
        self.logger.info(f"Worker {self.worker_id} starting")

        if not self.upstream_endpoint or not self.upstream_topic:
            raise RuntimeError(
                f"Worker {self.worker_id} requires upstream_endpoint and upstream_topic. "
                "Source stages should use SourceMaster to generate splits into a queue."
            )

        try:
            # Create queue connections
            self.logger.info(f"Output endpoint received: {self.output_endpoint}")
            self.output_queue = await self._create_queue_from_endpoint(self.output_endpoint)

            self.logger.info(f"Connecting to upstream queue: {self.upstream_endpoint}")
            self.upstream_queue = await self._create_queue_from_endpoint(self.upstream_endpoint)

            # Process from upstream queue
            await self._process_from_upstream()

            return {
                "worker_id": self.worker_id,
                "processed_count": self._processed_count,
                "error_count": self._error_count,
            }

        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} failed: {e}")
            raise
        finally:
            self._running = False

            # Close operator (allows sink to flush buffers, etc.)
            try:
                self.operator.close()
            except Exception as e:
                self.logger.warning(f"Error during operator close: {e}")

            # Cleanup queue connections
            if self.upstream_queue:
                await self.upstream_queue.stop()
            if self.output_queue:
                await self.output_queue.stop()

    def notify_upstream_finished(self) -> None:
        """Called by master when upstream stage(s) have finished."""
        self._upstream_finished = True
        self.logger.info(f"Worker {self.worker_id} notified: upstream finished")

    async def _process_from_upstream(self) -> None:
        """Process messages from upstream queue using consumer group for partition assignment.

        Completion criteria:
        - When upstream is finished AND we've consumed all messages from all assigned partitions
        - Exit immediately when both conditions are met
        """
        # Use consumer group for automatic partition assignment
        # This allows multiple workers to consume from different partitions in parallel
        consecutive_empty = 0
        last_committed_offsets: Dict[int, int] = {}  # Track offsets per partition

        self.logger.info(
            f"Worker {self.worker_id} starting to consume from {self.upstream_topic} "
            f"with consumer group {self.consumer_group}"
        )

        while self._running:
            # Fetch batch from upstream using consumer group
            # The queue backend will automatically assign partitions based on consumer group
            records = await self.upstream_queue.fetch(
                self.upstream_topic,
                offset=0,  # Offset is managed by consumer group
                max_records=self.config.batch_size,
                timeout_ms=1000,  # Shorter timeout for faster completion detection
                group_id=self.consumer_group,  # Use consumer group for partition assignment
            )

            # Debug: Check queue status periodically
            if consecutive_empty == 0 or consecutive_empty % 10 == 0:
                # For multi-partition, we need to check all partitions
                # For now, log the record count
                self.logger.debug(
                    f"Fetch got {len(records)} records, empty polls: {consecutive_empty}, "
                    f"upstream_finished: {self._upstream_finished}"
                )

            if not records:
                consecutive_empty += 1

                # Check if we should stop: upstream finished AND queue exhausted
                # For consumer group, we check if all partitions are consumed
                if self._upstream_finished:
                    # Check if there's more data in any partition
                    # This is a simplified check - in production, we'd check all partitions
                    if consecutive_empty >= 50:
                        self.logger.info(
                            f"Worker {self.worker_id} finished: upstream done, "
                            f"no new data for {consecutive_empty} polls"
                        )
                        break

                # Don't wait too long if upstream is finished
                if self._upstream_finished:
                    await asyncio.sleep(0.05)  # Quick check
                else:
                    await asyncio.sleep(0.1)
                continue

            consecutive_empty = 0

            # Process each record and track offsets per partition
            for record in records:
                try:
                    message = QueueMessage.from_bytes(record.value)
                    await self._process_message(message)
                    self._processed_count += 1

                    # Track the highest offset for each partition
                    # Note: record doesn't directly contain partition info in our current Record model
                    # For now, we'll commit based on the highest offset seen
                    # In a full implementation, we'd track partition-specific offsets
                except Exception as e:
                    import traceback

                    self.logger.error(
                        f"Error processing message at offset {record.offset}: {type(e).__name__}: {e}"
                    )
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    self._error_count += 1
                    # Continue processing - don't block on single errors

                # Track the highest offset seen across all partitions
                # In consumer group mode, Kafka manages partition assignment automatically
                # We commit the highest offset for all assigned partitions
                # Note: This is a simplification - ideally we'd track per-partition offsets
                # but that requires Record to include partition information
                current_offset = record.offset + 1
                if not last_committed_offsets or current_offset > max(
                    last_committed_offsets.values()
                ):
                    # Update the highest offset seen
                    # Since we don't have partition info in Record, we use a single entry
                    # representing the highest offset across all partitions
                    last_committed_offsets[0] = current_offset

            # Commit offset periodically using consumer group
            # Commit the highest offset for all assigned partitions
            if time.time() - self._last_commit_time > self.config.commit_interval_ms / 1000:
                if last_committed_offsets:
                    # Commit the highest offset seen for all assigned partitions
                    highest_offset = max(last_committed_offsets.values())
                    await self.upstream_queue.commit_offset(
                        self.consumer_group, self.upstream_topic, highest_offset
                    )
                self._last_commit_time = time.time()

        # Final commit for all partitions
        if self.upstream_queue and last_committed_offsets:
            highest_offset = max(last_committed_offsets.values())
            await self.upstream_queue.commit_offset(
                self.consumer_group, self.upstream_topic, highest_offset
            )

    async def _process_message(self, message: QueueMessage) -> None:
        """Process a single message.

        Handles two types of messages:
        1. Source messages: payload_key is empty, data_range is in metadata
           - Create split from metadata and call operator.process_split(split, None)
        2. Regular messages: payload_key points to SplitPayloadStore
           - Get payload from store and call operator.process_split(split, payload)
        """
        from solstice.core.models import Split, SplitPayload

        payload: Optional[SplitPayload] = None
        is_source_message = not message.payload_key

        if is_source_message:
            # Source message: data_range is in metadata
            data_range = message.metadata.get("data_range", {})
            split = Split(
                split_id=message.split_id,
                stage_id=self.stage_id,
                data_range=data_range,
                parent_split_ids=[],
            )
            # payload is None for source operators
        else:
            # Regular message: get payload from store
            payload = self.payload_store.get(message.payload_key)
            if payload is None:
                raise RuntimeError(f"Payload not found for key: {message.payload_key}")

            split = Split(
                split_id=message.split_id,
                stage_id=self.stage_id,
                data_range={"message_id": message.message_id},
                parent_split_ids=[message.split_id],
            )

        # Process with operator
        output_payload = self.operator.process_split(split, payload)

        if output_payload:
            # Generate unique key for this payload
            payload_key = f"{self.worker_id}_{self._processed_count}_{split.split_id}"

            # Store in SplitPayloadStore
            self.payload_store.store(payload_key, output_payload)

            output_message = QueueMessage(
                message_id=f"{self.worker_id}_{self._processed_count}",
                split_id=f"{self.stage_id}_{message.split_id}",
                payload_key=payload_key,
                metadata={
                    "source_stage": self.stage_id,
                    "parent_message_id": message.message_id,
                },
            )

            # Produce to output queue
            offset = await self.output_queue.produce(self.output_topic, output_message.to_bytes())
            self.logger.debug(f"Produced output for {message.split_id} at offset {offset}")
        else:
            self.logger.debug(f"Operator returned None for {message.split_id}, no output produced")

        # Delete input payload if it was from store (not source message)
        if not is_source_message and message.payload_key:
            self.payload_store.delete(message.payload_key)

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.logger.info(f"Worker {self.worker_id} stopping")

        try:
            self.operator.close()
        except Exception as e:
            self.logger.error(f"Error closing operator: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
        }
