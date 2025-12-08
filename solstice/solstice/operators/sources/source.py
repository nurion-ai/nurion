"""Source Master for source stages that generate splits.

SourceMaster is responsible for:
1. Generating splits via the abstract plan_splits() method
2. Writing split metadata to a persistent TansuBackend queue
3. Spawning workers that consume from this queue and process data

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                      SourceMaster                               │
    │                                                                 │
    │  ┌─────────────────────────────────────────────────────────┐   │
    │  │              Source Queue (Tansu, persistent)           │   │
    │  │  - Split metadata written by plan_splits()              │   │
    │  │  - Enables exactly-once via offset tracking             │   │
    │  └─────────────────────────────────────────────────────────┘   │
    │                           ▲                                     │
    │                           │ produce splits                      │
    │  plan_splits() ───────────┘                                    │
    │                                                                 │
    │                           │                                     │
    │                           ▼ workers consume                     │
    │  ┌────────────┐  ┌────────────┐  ┌────────────┐               │
    │  │  Worker 1  │  │  Worker 2  │  │  Worker N  │               │
    │  │ (process)  │  │ (process)  │  │ (process)  │               │
    │  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘               │
    │        │               │               │                        │
    │        └───────────────┼───────────────┘                        │
    │                        │ produce to output                      │
    │                        ▼                                        │
    │  ┌─────────────────────────────────────────────────────────┐   │
    │  │              Output Queue (for downstream)              │   │
    │  └─────────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────────┘

Key design decisions:
- SourceMaster uses TansuBackend for source queue (persistence)
- Split metadata is written to source queue, workers read actual data
- Workers consume from source queue, produce to output queue
- This enables crash recovery and exactly-once semantics
"""

from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator, Optional


from solstice.core.models import Split
from solstice.core.stage_master import (
    QueueEndpoint,
    QueueMessage,
    QueueType,
    StageConfig,
    StageStatus,
    StageMaster,
)
from solstice.queue import TansuBackend, QueueBackend, MemoryBackend
from solstice.utils.logging import create_ray_logger
from solstice.core.split_payload_store import SplitPayloadStore

if TYPE_CHECKING:
    from solstice.core.stage import Stage


@dataclass
class SourceConfig(StageConfig):
    """Configuration for SourceMaster.

    Source stages always use TansuBackend for the source queue (persistence).
    """

    # Override queue_type to always be TANSU for source queue
    queue_type: QueueType = QueueType.TANSU

    # Tansu storage URL (memory://, s3://)
    tansu_storage_url: str = "memory://"
    tansu_port: int = 9092


class SourceMaster(StageMaster):
    """Master for source stages that generates splits and spawns workers.

    SourceMaster extends StageMaster with split generation capability:
    1. Generate splits via plan_splits()
    2. Write split metadata to a persistent source queue
    3. Spawn workers that consume from source queue
    4. Workers produce output to output queue (for downstream stages)

    This design ensures:
    - Split planning is deterministic and persistent
    - Crash recovery can resume from last committed offset
    - Workers only need to consume from queue (no special source logic)

    Subclasses must implement:
    - plan_splits() -> Iterator[Split]: Generate splits for this source
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        config: Optional[SourceConfig] = None,
        payload_store: Optional[SplitPayloadStore] = None,
        **kwargs,
    ):
        # Source stages use their own source queue as "upstream"
        # We don't pass upstream_endpoint/topic to parent - we'll create our own
        config = config or SourceConfig()
        super().__init__(
            job_id=job_id,
            stage=stage,
            config=config,
            payload_store=payload_store,
            upstream_endpoint=None,  # Will set after creating source queue
            upstream_topic=None,
        )

        # Source queue (for split metadata, distinct from output queue)
        self._source_queue: Optional[QueueBackend] = None
        self._source_topic = f"{job_id}_{self.stage_id}_source"
        self._source_endpoint: Optional[QueueEndpoint] = None

        # Metrics
        self._splits_produced = 0

        # Override logger
        self.logger = create_ray_logger(f"SourceMaster-{self.stage_id}")

    async def _create_source_queue(self) -> QueueBackend:
        """Create queue backend for source queue.

        For production (TANSU): Uses persistent TansuBackend.
        For testing (MEMORY): Uses in-memory MemoryBackend.
        """
        if self.config.queue_type == QueueType.MEMORY:
            # Use Memory for testing
            queue = MemoryBackend()
            await queue.start()
            self._source_endpoint = QueueEndpoint(
                queue_type=QueueType.MEMORY,
                port=0,
                storage_url="memory://",
            )
            await queue.create_topic(self._source_topic)
            self.logger.info(f"Created Memory source queue for {self.stage_id}")
            return queue
        else:
            # Use Tansu for production (persistent)
            queue = TansuBackend(
                storage_url=self.config.tansu_storage_url,
                port=None,  # Auto-select free port
            )
            await queue.start()

            # Now we can get the actual port that was selected
            self._source_endpoint = QueueEndpoint(
                queue_type=QueueType.TANSU,
                port=queue.port,
                storage_url=self.config.tansu_storage_url,
            )

            await queue.create_topic(self._source_topic)

            self.logger.info(f"Created Tansu source queue on port {queue.port} for {self.stage_id}")
            return queue

    async def start(self) -> None:
        """Start the source master.

        1. Create source queue for split metadata
        2. Generate splits and write to source queue
        3. Create output queue (via parent StageMaster)
        4. Spawn workers that consume from source queue
        """
        if self._running:
            return

        self.logger.info(f"Starting source {self.stage_id}")
        self._start_time = time.time()
        self._running = True

        # Create source queue (for split metadata)
        self._source_queue = await self._create_source_queue()

        # Generate splits and write to source queue
        await self._produce_splits()

        # Create output queue (for downstream stages)
        self._output_queue = await self._create_queue()

        # Set upstream to our source queue (workers will consume from here)
        self.upstream_endpoint = self._source_endpoint
        self.upstream_topic = self._source_topic

        # Spawn workers
        for i in range(self.config.min_workers):
            await self._spawn_worker()

        self.logger.info(
            f"Source {self.stage_id} started: {self._splits_produced} splits, "
            f"{len(self._workers)} workers"
        )

        # Notify workers that all splits have been produced (source queue is complete)
        # Workers can exit once they've consumed all splits from the source queue
        self._notify_splits_complete()

    async def _produce_splits(self) -> None:
        """Generate splits and write to source queue."""
        self.logger.info(f"Generating splits for source {self.stage_id}")

        split_iterator = self.plan_splits()

        for split in split_iterator:
            if not self._running:
                break

            try:
                await self._produce_split(split)
                self._splits_produced += 1

                if self._splits_produced % 100 == 0:
                    self.logger.info(f"Produced {self._splits_produced} splits")

            except Exception as e:
                self.logger.error(f"Error producing split {split.split_id}: {e}")
                self._failed = True
                self._failure_message = str(e)
                raise

        self.logger.info(f"Source {self.stage_id} produced {self._splits_produced} splits to queue")

    def _notify_splits_complete(self) -> None:
        """Notify workers that all splits have been produced.
        
        This allows workers to exit once they've consumed all splits.
        """
        import ray
        self.logger.info(f"Notifying {len(self._workers)} workers: all splits produced")
        for worker_id, worker in self._workers.items():
            try:
                ray.get(worker.notify_upstream_finished.remote(), timeout=5)
            except Exception as e:
                self.logger.warning(f"Failed to notify worker {worker_id}: {e}")

    async def _produce_split(self, split: Split) -> None:
        """Produce a split to the source queue.

        The split metadata is serialized and written to the queue.
        Workers will consume this and use the SourceOperator to read actual data.
        """
        # Create message with split metadata
        message = QueueMessage(
            message_id=f"{self.stage_id}_{self._splits_produced}",
            split_id=split.split_id,
            payload_key="",  # No payload for source splits - data will be read by operator
            metadata={
                "source_stage": self.stage_id,
                "data_range": split.data_range,
                "split_index": self._splits_produced,
            },
        )

        # Produce to source queue
        offset = await self._source_queue.produce(self._source_topic, message.to_bytes())
        self.logger.debug(f"Produced split {split.split_id} at offset {offset}")

    @abstractmethod
    def plan_splits(self) -> Iterator[Split]:
        """Plan and generate splits for this source.

        Subclasses must implement this to define how data is split.

        Returns:
            Iterator of Split objects, each containing metadata for one split
        """
        raise NotImplementedError("plan_splits must be implemented by subclasses")

    async def cleanup_queue(self) -> None:
        """Clean up queues. Called by runner after all consumers are done."""
        # Clean up source queue
        if self._source_queue:
            await self._source_queue.stop()
            self._source_queue = None

        # Clean up output queue (parent)
        await super().cleanup_queue()

    def get_source_queue(self) -> Optional[QueueBackend]:
        """Get the source queue (for debugging/testing)."""
        return self._source_queue

    def get_source_topic(self) -> str:
        """Get the source topic name."""
        return self._source_topic

    def get_source_endpoint(self) -> Optional[QueueEndpoint]:
        """Get the source endpoint (for debugging/testing)."""
        return self._source_endpoint

    def get_status(self) -> StageStatus:
        """Get current source status."""
        status = super().get_status()
        status.metrics["splits_produced"] = self._splits_produced
        return status

    async def get_status_async(self) -> StageStatus:
        """Get current source status with queue metrics."""
        status = await super().get_status_async()

        # Add source queue size
        if self._source_queue:
            try:
                source_size = await self._source_queue.get_latest_offset(self._source_topic)
                status.metrics["source_queue_size"] = source_size
            except Exception:
                pass

        status.metrics["splits_produced"] = self._splits_produced
        return status
