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

"""Stage Master v2 - Simplified queue-based architecture.

Key differences from v1:
- Master only manages its output queue
- Workers pull directly from upstream queue (not master-to-master)
- Uses QueueClient abstraction for flexibility
- Cleaner separation of concerns

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                     Stage Master                            │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │              Output Queue (QueueClient)            │   │
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
    │  │              Output Queue (QueueClient)            │   │
    │  └─────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray

from solstice.queue import (
    QueueType,
    QueueClient,
    MemoryBroker,
    MemoryClient,
    TansuBrokerManager,
    TansuQueueClient,
)
from solstice.utils.logging import create_ray_logger
from solstice.core.split_payload_store import SplitPayloadStore

if TYPE_CHECKING:
    from solstice.core.stage import Stage


@dataclass
class StageConfig:
    """Configuration for Stage Master v2.

    Attributes:
        queue_type: Type of queue backend:
            - MEMORY: In-process only (single-worker testing)
            - RAY: Shared via Ray actor (distributed testing)
            - TANSU: Persistent broker (production)
        tansu_storage_url: Storage URL for Tansu backend (memory://, s3://)
        max_workers: Maximum number of workers
        min_workers: Minimum number of workers
        batch_size: Number of messages to fetch per batch
        commit_interval_ms: Interval between offset commits (ms)
        partition_count: Number of partitions for the output queue.
            If None, automatically set based on max_workers.
            For single worker, uses 1 partition. For multiple workers,
            uses min(max_workers, actual_worker_count) partitions.
    """

    queue_type: QueueType = QueueType.TANSU  # Default to Tansu for persistence
    tansu_storage_url: str = "memory://"

    max_workers: int = 4
    min_workers: int = 1

    batch_size: int = 100
    commit_interval_ms: int = 5000

    # Partition configuration
    partition_count: Optional[int] = None  # None = auto based on workers

    # Backpressure thresholds
    backpressure_threshold_lag: int = 5000
    backpressure_threshold_queue_size: int = 1000

    # Worker resources
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0

    # Resource backoff configuration
    worker_ready_timeout_seconds: float = 30.0  # Max time to wait for worker to be ready
    worker_spawn_retry_delay_seconds: float = 2.0  # Delay between spawn retries

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "tansu_storage_url": self.tansu_storage_url,
            "max_workers": self.max_workers,
            "min_workers": self.min_workers,
            "batch_size": self.batch_size,
            "commit_interval_ms": self.commit_interval_ms,
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
        self._output_broker: Optional[TansuBrokerManager | MemoryBroker] = None
        self._output_queue = None  # MemoryClient or TansuQueueClient
        self._output_topic = f"{job_id}_{self.stage_id}_output"

        # Output endpoint info for workers/downstream
        self._output_endpoint: Optional[QueueEndpoint] = None

        # Workers
        self._workers: Dict[str, ray.actor.ActorHandle] = {}
        self._worker_tasks: Dict[str, ray.ObjectRef] = {}

        # Partition assignment: worker_id -> List[partition_ids]
        # Managed centrally, recomputed on worker add/remove
        self._partition_assignments: Dict[str, List[int]] = {}
        self._partition_count: Optional[int] = None  # Cached after queue creation

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
        self._upstream_metrics_queue: Optional[QueueClient] = None

        # Backpressure state
        self._backpressure_active = False
        self._downstream_stage_refs: Dict[str, StageMaster] = {}  # For backpressure propagation

    async def _get_upstream_metrics_queue(self) -> Optional[TansuQueueClient]:
        """Get or create a client-only queue for upstream metrics/lag/skew."""
        if not self.upstream_endpoint or self.upstream_endpoint.queue_type != QueueType.TANSU:
            return None

        if self._upstream_metrics_queue is None:
            broker_url = f"{self.upstream_endpoint.host}:{self.upstream_endpoint.port}"
            self._upstream_metrics_queue = TansuQueueClient(broker_url)
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

    async def _create_queue(self) -> QueueClient:
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

        if self.config.queue_type == QueueType.TANSU:
            # Tansu: Start broker + create client
            self._output_broker = TansuBrokerManager(
                storage_url=self.config.tansu_storage_url or "memory://tansu/",
            )
            await self._output_broker.start()

            broker_url = self._output_broker.get_broker_url()
            host, port_str = broker_url.split(":")
            queue = TansuQueueClient(broker_url)
            await queue.start()

            self._output_endpoint = QueueEndpoint(
                queue_type=self.config.queue_type,
                host=host,
                port=int(port_str),
                storage_url=self.config.tansu_storage_url or "memory://tansu/",
            )
        else:
            # Memory: Start broker + create client
            self._output_broker = MemoryBroker()
            await self._output_broker.start()

            queue = MemoryClient(self._output_broker)
            await queue.start()

            self._output_endpoint = QueueEndpoint(
                queue_type=self.config.queue_type,
                host="memory",
                port=0,
                storage_url=self._output_broker.get_broker_url(),
            )

        self.logger.info(
            f"Created {self.config.queue_type} backend on {self._output_endpoint.host}:{self._output_endpoint.port} "
            f"with {partition_count} partition(s)"
        )

        await queue.create_topic(self._output_topic, partitions=partition_count)
        self.logger.info(f"Created topic {self._output_topic} with {partition_count} partition(s)")
        return queue

    async def start(self) -> None:
        """Start the stage master.

        Spawns workers with resource backoff strategy:
        1. Tries to spawn min_workers first - these are required
        2. If any min_worker fails to start due to resources, raises RuntimeError
        3. Additional workers beyond min are optional and will be skipped if resources unavailable
        """
        if self._running:
            return

        self.logger.info(f"Starting stage {self.stage_id}")
        self._start_time = time.time()
        self._running = True

        # Create output queue
        self._output_queue = await self._create_queue()

        # Spawn minimum required workers first (these must succeed)
        for i in range(self.config.min_workers):
            # is_min_worker=True means failure will raise RuntimeError
            await self._spawn_worker_with_resource_check(is_min_worker=True)

        # Rebalance partitions after all workers are spawned
        # This ensures all workers have consistent, non-overlapping assignments
        if self._workers:
            self._rebalance_partitions()
            await self._notify_workers_partition_update()

        self.logger.info(f"Stage {self.stage_id} started with {len(self._workers)} workers")

    def _rebalance_partitions(self) -> None:
        """Recompute partition assignments for all workers.

        Uses round-robin distribution to ensure all partitions are covered:
        - 4 partitions, 2 workers: worker0 -> [0,2], worker1 -> [1,3]
        - 4 partitions, 3 workers: worker0 -> [0,3], worker1 -> [1], worker2 -> [2]
        """
        if self._partition_count is None:
            self._partition_count = self._compute_partition_count()

        partition_count = self._partition_count
        worker_ids = list(self._workers.keys())
        num_workers = len(worker_ids)

        # Clear existing assignments
        self._partition_assignments.clear()

        if num_workers == 0:
            return

        # Round-robin assignment
        # When partition_count < num_workers, some workers will have empty assignments
        # and remain idle. This is intentional to avoid duplicate message processing.
        for i, worker_id in enumerate(worker_ids):
            partitions = [p for p in range(partition_count) if p % num_workers == i]
            self._partition_assignments[worker_id] = partitions

        idle_workers = [wid for wid, parts in self._partition_assignments.items() if not parts]
        if idle_workers:
            self.logger.warning(
                f"Partition rebalance: {len(idle_workers)} workers have no partitions "
                f"(partition_count={partition_count} < num_workers={num_workers}). "
                f"Consider increasing partition_count or reducing workers."
            )
        self.logger.debug(f"Partition rebalance: {self._partition_assignments}")

    def get_partition_assignment(self, worker_id: str) -> List[int]:
        """Get the current partition assignment for a worker.

        Returns empty list if worker has no assigned partitions (idle worker).
        """
        return self._partition_assignments.get(worker_id, [])

    async def _spawn_worker(self) -> str:
        """Spawn a new worker without resource checking.

        Returns the worker_id of the spawned worker.
        """
        worker_index = len(self._workers)
        worker_id = f"{self.stage_id}_w{worker_index}_{uuid.uuid4().hex[:6]}"

        # Pre-compute partition count if not set
        if self._partition_count is None:
            self._partition_count = self._compute_partition_count()

        # Compute initial partition assignment for this new worker
        # This will be updated by rebalance after worker is added
        # When partition_count < num_workers, some workers will have empty assignments
        num_workers = len(self._workers) + 1
        partition_count = self._partition_count
        assigned_partitions = [p for p in range(partition_count) if p % num_workers == worker_index]

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
            assigned_partitions=assigned_partitions,
            config=self.config,
            payload_store=self.payload_store,
        )

        self._workers[worker_id] = worker
        self._partition_assignments[worker_id] = assigned_partitions

        # Start worker run loop
        task = worker.run.remote()
        self._worker_tasks[worker_id] = task

        self.logger.info(f"Spawned worker {worker_id} with partitions {assigned_partitions}")
        return worker_id

    async def _check_worker_ready(self, worker_id: str, timeout: float) -> bool:
        """Check if a worker is ready (actor has started and is responsive).

        Args:
            worker_id: The ID of the worker to check
            timeout: Maximum time to wait in seconds

        Returns:
            True if worker is ready, False if timeout or error
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            return False

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Try to call a lightweight method on the worker
                # get_status is a method that should return quickly if worker is ready
                ready_refs, _ = ray.wait(
                    [worker.get_status.remote()],
                    timeout=min(1.0, timeout - (time.time() - start_time)),
                )
                if ready_refs:
                    # Worker responded, it's ready
                    return True
            except ray.exceptions.GetTimeoutError:
                # Worker not ready yet, continue waiting
                pass
            except Exception as e:
                self.logger.debug(f"Worker {worker_id} not ready yet: {e}")

            await asyncio.sleep(self.config.worker_spawn_retry_delay_seconds)

        return False

    async def _cancel_worker(self, worker_id: str) -> None:
        """Cancel a pending worker that couldn't start due to resource constraints."""
        worker = self._workers.pop(worker_id, None)
        task = self._worker_tasks.pop(worker_id, None)
        self._partition_assignments.pop(worker_id, None)

        if worker is not None:
            try:
                ray.kill(worker)
                self.logger.info(f"Cancelled worker {worker_id} due to resource constraints")
            except Exception as e:
                self.logger.debug(f"Error killing worker {worker_id}: {e}")

        if task is not None:
            try:
                ray.cancel(task, force=True)
            except Exception:
                pass

    async def _spawn_worker_with_resource_check(self, is_min_worker: bool = False) -> Optional[str]:
        """Spawn a worker with resource availability checking.

        Args:
            is_min_worker: If True, this worker is required for min_workers.
                          If False, it's an optional worker that can be skipped.

        Returns:
            worker_id if worker started successfully, None if cancelled due to resources

        Raises:
            RuntimeError: If is_min_worker=True and worker cannot start
        """
        worker_id = await self._spawn_worker()

        # Wait for worker to be ready
        is_ready = await self._check_worker_ready(
            worker_id, self.config.worker_ready_timeout_seconds
        )

        if is_ready:
            return worker_id

        # Worker didn't start in time - resource constraints
        if is_min_worker:
            # This is a required worker for min_workers
            # Don't cancel, raise error immediately
            current_count = len(self._workers)
            min_required = self.config.min_workers
            await self._cancel_worker(worker_id)
            raise RuntimeError(
                f"Stage {self.stage_id}: Cannot satisfy minimum worker requirement. "
                f"Started {current_count - 1}/{min_required} workers. "
                f"Worker {worker_id} failed to start within {self.config.worker_ready_timeout_seconds}s "
                f"due to insufficient resources (CPU: {self.config.num_cpus}, "
                f"GPU: {self.config.num_gpus}, Memory: {self.config.memory_mb}MB). "
                f"Consider reducing min_workers or adding more cluster resources."
            )
        else:
            # This is an optional worker beyond min_workers
            # Cancel it and continue
            self.logger.warning(
                f"Worker {worker_id} could not start due to resource constraints. "
                f"Cancelling worker and continuing with {len(self._workers) - 1} workers."
            )
            await self._cancel_worker(worker_id)
            return None

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
        if self._output_broker:
            await self._output_broker.stop()
            self._output_broker = None

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

    def get_output_queue(self) -> Optional[QueueClient]:
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
                self._partition_assignments.pop(worker_id, None)
                removed += 1
                self.logger.debug(f"Removed worker {worker_id}")
            except Exception as e:
                self.logger.warning(f"Error removing worker {worker_id}: {e}")

        # Rebalance partitions among remaining workers
        if removed > 0:
            self._rebalance_partitions()
            # Notify remaining workers of new partition assignments
            await self._notify_workers_partition_update()

        self.logger.info(
            f"Scaled down {self.stage_id}: removed {removed}/{count} workers "
            f"(now {len(self._workers)} workers)"
        )
        return removed

    async def _notify_workers_partition_update(self) -> None:
        """Notify all workers of their updated partition assignments."""
        for worker_id, worker in self._workers.items():
            partitions = self._partition_assignments.get(worker_id, [])
            try:
                # Ray ObjectRef can be awaited directly in async context
                obj_ref = worker.update_partitions.remote(partitions)
                await asyncio.wait_for(
                    asyncio.to_thread(ray.get, obj_ref),
                    timeout=5.0,
                )
            except Exception as e:
                self.logger.warning(f"Failed to notify worker {worker_id} of partition update: {e}")


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
    since QueueClient instances contain locks and cannot be serialized.
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
        assigned_partitions: List[int],
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
        self.assigned_partitions = assigned_partitions

        # Queue connections (created lazily)
        self.upstream_queue: Optional[QueueClient] = None
        self.output_queue: Optional[QueueClient] = None

        self.logger = create_ray_logger(f"Worker-{self.stage_id}-{worker_id}")

        # Initialize operator using OperatorConfig.setup()
        self.operator = stage.operator_config.setup(worker_id=worker_id)

        # State
        self._running = False
        self._processed_count = 0
        self._error_count = 0
        self._last_commit_time = time.time()
        self._upstream_finished = False
        self._partitions_updated = False  # Flag to signal partition rebalance

    async def _create_queue_from_endpoint(self, endpoint: QueueEndpoint):
        """Create a queue connection from endpoint info."""
        if endpoint.queue_type == QueueType.TANSU:
            broker_url = f"{endpoint.host}:{endpoint.port}"
            queue = TansuQueueClient(broker_url)
        else:
            # Memory: Use broker URL to look up the broker instance
            queue = MemoryClient(endpoint.storage_url)
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

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status. Used for health checks and monitoring."""
        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "upstream_finished": self._upstream_finished,
            "assigned_partitions": self.assigned_partitions,
        }

    async def _process_from_upstream(self) -> None:
        """Process messages from upstream queue from all assigned partitions.

        Completion criteria:
        - When upstream is finished AND we've consumed all messages from all assigned partitions
        - Exit immediately when both conditions are met
        """
        consecutive_empty = 0
        last_committed_offsets: Dict[int, int] = {}  # Track offsets per partition
        current_partition_idx = 0  # Round-robin index for partition polling
        active_partitions = list(self.assigned_partitions)  # Local copy

        self.logger.info(
            f"Worker {self.worker_id} starting to consume from {self.upstream_topic} "
            f"partitions {active_partitions} with consumer group {self.consumer_group}"
        )

        while self._running:
            # Check if partitions were updated by master
            if self._partitions_updated:
                self._partitions_updated = False
                old_partitions = set(active_partitions)
                new_partitions = set(self.assigned_partitions)
                active_partitions = list(self.assigned_partitions)

                # Reset index to avoid out-of-bounds
                current_partition_idx = 0

                # Clean up offset tracking for removed partitions
                removed = old_partitions - new_partitions
                for p in removed:
                    if p in last_committed_offsets:
                        # Commit final offset before removing
                        try:
                            await self.upstream_queue.commit_offset(
                                self.consumer_group,
                                self.upstream_topic,
                                last_committed_offsets[p],
                                partition=p,
                            )
                        except Exception as e:
                            self.logger.warning(
                                f"Failed to commit offset for removed partition {p}: {e}"
                            )
                        del last_committed_offsets[p]

                self.logger.info(
                    f"Worker {self.worker_id} switched to partitions {active_partitions}"
                )

                # Reset empty poll counter since we have new partitions
                consecutive_empty = 0

            # Safety check: ensure we have partitions
            if not active_partitions:
                await asyncio.sleep(0.5)
                continue

            # Round-robin across assigned partitions
            partition = active_partitions[current_partition_idx]
            current_partition_idx = (current_partition_idx + 1) % len(active_partitions)

            # Fetch batch from current partition
            records = await self.upstream_queue.fetch(
                self.upstream_topic,
                # offset=None to use consumer's current position (auto-managed)
                max_records=self.config.batch_size,
                timeout_ms=1000,  # Shorter timeout for faster completion detection
                partition=partition,
            )

            # Debug: Check queue status periodically
            if consecutive_empty == 0 or consecutive_empty % 10 == 0:
                self.logger.debug(
                    f"Fetch from partition {partition} got {len(records)} records, "
                    f"empty polls: {consecutive_empty}, upstream_finished: {self._upstream_finished}"
                )

            if not records:
                consecutive_empty += 1

                # Check if we should stop: upstream finished AND queue exhausted
                # Need consecutive empty polls across ALL partitions
                if self._upstream_finished:
                    # Require more empty polls when handling multiple partitions
                    min_empty_polls = 50 * len(active_partitions)
                    if consecutive_empty >= min_empty_polls:
                        self.logger.info(
                            f"Worker {self.worker_id} finished: upstream done, "
                            f"no new data for {consecutive_empty} polls across {len(active_partitions)} partitions"
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
            # Note: 'partition' variable is from the round-robin loop above
            for record in records:
                try:
                    message = QueueMessage.from_bytes(record.value)
                    await self._process_message(message)
                    self._processed_count += 1
                except Exception as e:
                    import traceback

                    self.logger.error(
                        f"Error processing message at offset {record.offset}: {type(e).__name__}: {e}"
                    )
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    self._error_count += 1
                    # Continue processing - don't block on single errors

                # Track the highest offset for this partition
                current_offset = record.offset + 1
                last_committed_offsets[partition] = max(
                    last_committed_offsets.get(partition, 0),
                    current_offset,
                )

            # Commit offset periodically for all assigned partitions
            if time.time() - self._last_commit_time > self.config.commit_interval_ms / 1000:
                for p, offset in last_committed_offsets.items():
                    await self.upstream_queue.commit_offset(
                        self.consumer_group,
                        self.upstream_topic,
                        offset,
                        partition=p,
                    )
                self._last_commit_time = time.time()

        # Final commit for all assigned partitions
        if self.upstream_queue and last_committed_offsets:
            for p, offset in last_committed_offsets.items():
                await self.upstream_queue.commit_offset(
                    self.consumer_group,
                    self.upstream_topic,
                    offset,
                    partition=p,
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
        # FIXME: Disable payload deletion to prevent race conditions in distributed execution
        # Rely on Ray's object store eviction or end-of-job cleanup
        # if not is_source_message and message.payload_key:
        #     self.payload_store.delete(message.payload_key)

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.logger.info(f"Worker {self.worker_id} stopping")

        try:
            self.operator.close()
        except Exception as e:
            self.logger.error(f"Error closing operator: {e}")

    def update_partitions(self, partitions: List[int]) -> None:
        """Update the partition assignment for this worker.

        Called by master when partition rebalance occurs (e.g., scale up/down).
        Sets a flag that the processing loop will detect and handle.
        """
        old_partitions = set(self.assigned_partitions)
        new_partitions = set(partitions)

        added = new_partitions - old_partitions
        removed = old_partitions - new_partitions

        self.assigned_partitions = partitions
        self._partitions_updated = True  # Signal to processing loop

        self.logger.info(
            f"Worker {self.worker_id} partition update: "
            f"added={list(added)}, removed={list(removed)}, "
            f"now handling {partitions}"
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
        }
