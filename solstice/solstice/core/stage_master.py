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

from solstice.queue import QueueBackend, MemoryBackend
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
        num_partitions: Number of partitions for output queue (default: max_workers for parallelism)
        enable_work_stealing: Enable work-stealing for idle workers (helps with data skew)
        work_steal_lag_threshold: Minimum lag to consider work-stealing
        batch_size: Number of messages to fetch per batch
        commit_interval_ms: Interval between offset commits (ms)
        processing_timeout_s: Timeout for processing a single message
    """

    queue_type: QueueType = QueueType.TANSU  # Default to Tansu for persistence
    tansu_storage_url: str = "memory://"
    tansu_port: int = 9092

    max_workers: int = 4
    min_workers: int = 1

    # Multi-partition support for parallel consumption
    num_partitions: Optional[int] = None  # None means auto (= max_workers)
    enable_work_stealing: bool = True
    work_steal_lag_threshold: int = 100

    batch_size: int = 100
    commit_interval_ms: int = 5000
    processing_timeout_s: float = 300.0

    # Worker resources
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0

    @property
    def effective_partitions(self) -> int:
        """Get the effective number of partitions (auto = max_workers)."""
        if self.num_partitions is not None:
            return self.num_partitions
        return self.max_workers

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "tansu_storage_url": self.tansu_storage_url,
            "tansu_port": self.tansu_port,
            "max_workers": self.max_workers,
            "min_workers": self.min_workers,
            "num_partitions": self.effective_partitions,
            "enable_work_stealing": self.enable_work_stealing,
            "batch_size": self.batch_size,
            "commit_interval_ms": self.commit_interval_ms,
            "processing_timeout_s": self.processing_timeout_s,
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


@dataclass
class QueueEndpoint:
    """Queue connection info that can be serialized to workers.

    Workers use this to create their own queue connections.
    """

    queue_type: QueueType
    host: str = "localhost"
    port: int = 9092
    storage_url: str = "memory://"
    num_partitions: int = 1  # Number of partitions in the topic

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "host": self.host,
            "port": self.port,
            "storage_url": self.storage_url,
            "num_partitions": self.num_partitions,
        }


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

        self.logger = create_ray_logger(f"MasterV2-{self.stage_id}")

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

    async def _create_queue(self) -> QueueBackend:
        """Create the appropriate queue backend."""
        num_partitions = self.config.effective_partitions

        if self.config.queue_type == QueueType.TANSU:
            from solstice.queue import TansuBackend

            # Auto-select port (TansuBackend handles this when port=None)
            queue = TansuBackend(
                storage_url=self.config.tansu_storage_url,
                port=None,  # Auto-select free port
            )
            await queue.start()
            # Now we can get the actual port and host that was selected
            self._output_endpoint = QueueEndpoint(
                queue_type=QueueType.TANSU,
                host=queue.host,
                port=queue.port,
                storage_url=self.config.tansu_storage_url,
                num_partitions=num_partitions,
            )
            self.logger.info(f"Created Tansu backend on {queue.host}:{queue.port}")
        else:
            # MEMORY - only for single-process testing
            queue = MemoryBackend()
            await queue.start()
            self._output_endpoint = QueueEndpoint(
                queue_type=QueueType.MEMORY,
                num_partitions=num_partitions,
            )

        # Create topic with configured number of partitions for parallel consumption
        await queue.create_topic(self._output_topic, partitions=num_partitions)
        self.logger.info(
            f"Created output topic {self._output_topic} with {num_partitions} partitions"
        )
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

        # Spawn workers with partition assignments
        for i in range(self.config.min_workers):
            await self._spawn_worker()

        self.logger.info(f"Stage {self.stage_id} started with {len(self._workers)} workers")

    def _compute_partition_assignment(self, worker_index: int, total_workers: int) -> list:
        """Compute partition assignment for a worker using round-robin.

        Args:
            worker_index: Index of this worker (0-based).
            total_workers: Total number of workers.

        Returns:
            List of partition IDs assigned to this worker.
        """
        num_partitions = self.config.effective_partitions
        if num_partitions <= 0:
            return [0]

        # Round-robin assignment
        assigned = []
        for p in range(num_partitions):
            if p % total_workers == worker_index:
                assigned.append(p)

        # Ensure each worker gets at least one partition if possible
        if not assigned and num_partitions > 0:
            assigned = [worker_index % num_partitions]

        return assigned

    async def _spawn_worker(self) -> str:
        """Spawn a new worker with partition assignment."""
        worker_index = len(self._workers)
        worker_id = f"{self.stage_id}_w{worker_index}_{uuid.uuid4().hex[:6]}"

        # Compute partition assignment for this worker
        total_workers = max(worker_index + 1, self.config.min_workers)
        assigned_partitions = self._compute_partition_assignment(worker_index, total_workers)

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
            assigned_partitions=assigned_partitions,
        )

        self._workers[worker_id] = worker

        # Start worker run loop
        task = worker.run.remote()
        self._worker_tasks[worker_id] = task

        self.logger.info(f"Spawned worker {worker_id} with partitions {assigned_partitions}")
        return worker_id

    async def rebalance_partitions(self) -> None:
        """Rebalance partition assignments across all workers.

        Called when workers are added or removed to redistribute partitions evenly.
        """
        if not self._workers:
            return

        total_workers = len(self._workers)
        worker_ids = sorted(self._workers.keys())

        for idx, worker_id in enumerate(worker_ids):
            new_assignment = self._compute_partition_assignment(idx, total_workers)
            try:
                ray.get(
                    self._workers[worker_id].update_partition_assignment.remote(new_assignment),
                    timeout=5,
                )
                self.logger.debug(f"Updated {worker_id} partition assignment: {new_assignment}")
            except Exception as e:
                self.logger.warning(f"Failed to update partition assignment for {worker_id}: {e}")

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
        )

    async def get_input_queue_lag(self) -> int:
        """Get the input queue lag (messages pending to be processed).

        This is calculated as: latest_offset - committed_offset
        Returns 0 if upstream info is not available.
        """
        if not self.upstream_endpoint or not self.upstream_topic:
            return 0

        try:
            # Create a temporary connection to check offsets
            from solstice.queue import TansuBackend

            if self.upstream_endpoint.queue_type.value == "tansu":
                queue = TansuBackend(
                    storage_url=self.upstream_endpoint.storage_url,
                    port=self.upstream_endpoint.port,
                    client_only=True,
                )
                await queue.start()

                try:
                    latest = await queue.get_latest_offset(self.upstream_topic)
                    committed = await queue.get_committed_offset(
                        self._consumer_group, self.upstream_topic
                    )
                    committed = committed or 0
                    return max(0, latest - committed)
                finally:
                    await queue.stop()
        except Exception:
            pass

        return 0

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

    Multi-partition support:
    - Workers are assigned specific partitions to consume from
    - Each worker processes messages only from its assigned partitions
    - Work-stealing allows idle workers to help with high-lag partitions

    Exactly-once semantics:
    1. Fetch batch from assigned partitions
    2. Process each message
    3. Produce output to output queue (round-robin to partitions)
    4. Commit upstream offset per partition (only after output is durably stored)

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
        assigned_partitions: Optional[list] = None,
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

        # Partition assignment for this worker
        # If None, will be computed based on worker index and total partitions
        self._assigned_partitions = assigned_partitions or []

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

        # Per-partition offsets for multi-partition consumption
        self._partition_offsets: Dict[int, int] = {}

    async def _create_queue_from_endpoint(self, endpoint: QueueEndpoint) -> QueueBackend:
        """Create a queue connection from endpoint info."""
        if endpoint.queue_type == QueueType.TANSU:
            from solstice.queue import TansuBackend

            # Client-only mode: connect to existing Tansu server
            queue = TansuBackend(
                storage_url=endpoint.storage_url,
                port=endpoint.port,
                client_only=True,  # Don't start a new Tansu process
            )
        else:
            queue = MemoryBackend()

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

    def update_partition_assignment(self, partitions: list) -> None:
        """Update the partitions assigned to this worker.

        Called by the master when workers scale up/down and partitions need rebalancing.
        """
        old_partitions = self._assigned_partitions
        self._assigned_partitions = partitions
        self.logger.info(
            f"Worker {self.worker_id} partition assignment updated: {old_partitions} -> {partitions}"
        )

    async def _process_from_upstream(self) -> None:
        """Process messages from upstream queue.

        Multi-partition support:
        - Processes messages from all assigned partitions
        - Maintains per-partition offsets for proper checkpointing
        - Supports work-stealing for data skew mitigation

        Completion criteria:
        - When upstream is finished AND all partitions are exhausted
        - Exit immediately when both conditions are met
        """
        # Determine partitions to process
        # QueueEndpoint has num_partitions field (default 1 for backward compatibility)
        num_partitions = self.upstream_endpoint.num_partitions if self.upstream_endpoint else 1

        if not self._assigned_partitions:
            # Fallback: single partition mode for backward compatibility
            self._assigned_partitions = [0]

        # Initialize per-partition offsets from committed offsets
        for partition in self._assigned_partitions:
            committed = await self.upstream_queue.get_committed_partition_offset(
                self.consumer_group, self.upstream_topic, partition
            )
            self._partition_offsets[partition] = committed or 0

        self.logger.info(
            f"Worker {self.worker_id} starting with partitions {self._assigned_partitions}, "
            f"offsets: {self._partition_offsets}"
        )

        consecutive_empty = 0
        partition_idx = 0  # Round-robin across partitions

        while self._running:
            # Round-robin through assigned partitions
            if not self._assigned_partitions:
                await asyncio.sleep(0.1)
                continue

            partition = self._assigned_partitions[partition_idx % len(self._assigned_partitions)]
            partition_idx += 1

            offset = self._partition_offsets.get(partition, 0)

            # Fetch batch from the current partition
            records = await self.upstream_queue.fetch_from_partition(
                self.upstream_topic,
                partition=partition,
                offset=offset,
                max_records=self.config.batch_size,
                timeout_ms=500,  # Shorter timeout for multi-partition
            )

            if not records:
                consecutive_empty += 1

                # Check if all partitions are exhausted
                all_exhausted = await self._check_all_partitions_exhausted()
                if all_exhausted and self._upstream_finished:
                    self.logger.info(
                        f"Worker {self.worker_id} finished: all partitions exhausted"
                    )
                    break

                # Work-stealing: if idle and work-stealing is enabled, try to help other partitions
                if (
                    consecutive_empty >= 10
                    and self.config.enable_work_stealing
                    and not self._upstream_finished
                ):
                    stolen = await self._try_work_steal()
                    if stolen:
                        consecutive_empty = 0
                        continue

                if consecutive_empty >= len(self._assigned_partitions) * 10:
                    # Waited long enough across all partitions
                    if self._upstream_finished:
                        await asyncio.sleep(0.05)
                    else:
                        await asyncio.sleep(0.1)
                continue

            consecutive_empty = 0

            # Process each record
            for record in records:
                try:
                    message = QueueMessage.from_bytes(record.value)
                    await self._process_message(message)
                    self._processed_count += 1
                except Exception as e:
                    import traceback

                    self.logger.error(
                        f"Error processing message at p{partition}:o{record.offset}: {type(e).__name__}: {e}"
                    )
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    self._error_count += 1
                    # Continue processing - don't block on single errors

                self._partition_offsets[partition] = record.offset + 1

            # Commit offset periodically
            if time.time() - self._last_commit_time > self.config.commit_interval_ms / 1000:
                await self._commit_all_partition_offsets()
                self._last_commit_time = time.time()

        # Final commit for all partitions
        await self._commit_all_partition_offsets()

    async def _check_all_partitions_exhausted(self) -> bool:
        """Check if all assigned partitions have been fully consumed."""
        for partition in self._assigned_partitions:
            offset = self._partition_offsets.get(partition, 0)
            latest = await self.upstream_queue.get_partition_latest_offset(
                self.upstream_topic, partition
            )
            if offset < latest:
                return False
        return True

    async def _commit_all_partition_offsets(self) -> None:
        """Commit offsets for all assigned partitions."""
        if not self.upstream_queue:
            return

        for partition, offset in self._partition_offsets.items():
            await self.upstream_queue.commit_partition_offset(
                self.consumer_group, self.upstream_topic, partition, offset
            )

    async def _try_work_steal(self) -> bool:
        """Try to steal work from high-lag partitions not assigned to this worker.

        Returns True if work was found and processed.
        """
        num_partitions = self.upstream_endpoint.num_partitions if self.upstream_endpoint else 1

        # Find partitions with high lag that aren't assigned to us
        for partition in range(num_partitions):
            if partition in self._assigned_partitions:
                continue

            # Check lag for this partition
            lag = await self.upstream_queue.get_partition_lag(
                self.consumer_group, self.upstream_topic, partition
            )

            if lag >= self.config.work_steal_lag_threshold:
                # Found a partition with work to steal
                offset = await self.upstream_queue.get_committed_partition_offset(
                    self.consumer_group, self.upstream_topic, partition
                )
                offset = offset or 0

                # Steal a small batch (don't take too much)
                steal_size = min(self.config.batch_size // 2, lag // 2)
                if steal_size < 1:
                    continue

                records = await self.upstream_queue.fetch_from_partition(
                    self.upstream_topic,
                    partition=partition,
                    offset=offset,
                    max_records=steal_size,
                    timeout_ms=500,
                )

                if records:
                    self.logger.debug(
                        f"Work steal: processing {len(records)} messages from partition {partition}"
                    )

                    for record in records:
                        try:
                            message = QueueMessage.from_bytes(record.value)
                            await self._process_message(message)
                            self._processed_count += 1
                        except Exception as e:
                            self._error_count += 1

                        # Commit immediately for stolen work
                        await self.upstream_queue.commit_partition_offset(
                            self.consumer_group,
                            self.upstream_topic,
                            partition,
                            record.offset + 1,
                        )

                    return True

        return False

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
