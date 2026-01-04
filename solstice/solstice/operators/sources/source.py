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

"""Source Master for source stages that generate splits.

SourceMaster is responsible for:
1. Generating splits via the abstract plan_splits() method
2. Writing split metadata to a persistent queue (Tansu broker)
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
- SourceMaster uses TansuBrokerManager + TansuQueueClient for source queue
- Split metadata is written to source queue, workers read actual data
- Workers consume from source queue, produce to output queue
- This enables crash recovery and exactly-once semantics
"""

from __future__ import annotations

import asyncio
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
from solstice.queue import (
    QueueBroker,
    QueueClient,
    TansuBrokerManager,
    TansuQueueClient,
    MemoryBroker,
    MemoryClient,
)
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.core.stage import Stage
    from solstice.core.split_payload_store import SplitPayloadStore


@dataclass
class SourceConfig(StageConfig):
    """Configuration for SourceMaster.

    Source stages always use Tansu broker for the source queue (persistence).
    """

    # Override queue_type to always be TANSU for source queue
    queue_type: QueueType = QueueType.TANSU

    # Tansu storage URL (memory://, s3://)
    tansu_storage_url: str = "memory://"


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
        payload_store: "SplitPayloadStore",
        config: Optional[SourceConfig] = None,
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
        # Broker manages lifecycle, client handles produce/consume
        self._source_broker: Optional[QueueBroker] = None
        self._source_client: Optional[QueueClient] = None
        self._source_topic = f"{job_id}_{self.stage_id}_source"
        self._source_endpoint: Optional[QueueEndpoint] = None

        # Metrics
        self._splits_produced = 0

        # Backpressure configuration (inherited from parent, but can be overridden)
        self._backpressure_threshold_queue_size = config.backpressure_threshold_queue_size

        # Override logger
        self.logger = create_ray_logger(f"SourceMaster-{self.stage_id}")

    async def _create_source_queue(self) -> QueueClient:
        """Create queue broker and client for source queue.

        For production (TANSU): Uses persistent TansuBrokerManager + TansuQueueClient.
        For testing (MEMORY): Uses in-memory MemoryBroker + MemoryClient.

        Returns:
            QueueClient for producing/consuming messages.
        """
        if self.config.queue_type == QueueType.MEMORY:
            # Use Memory for testing
            broker = MemoryBroker()
            await broker.start()
            self._source_broker = broker

            client = MemoryClient(broker)
            await client.start()
            self._source_client = client

            self._source_endpoint = QueueEndpoint(
                queue_type=QueueType.MEMORY,
                port=0,
                storage_url="memory://",
            )
            await client.create_topic(self._source_topic)
            self.logger.info(f"Created Memory source queue for {self.stage_id}")
            return client
        else:
            # Use Tansu for production (persistent)
            broker = TansuBrokerManager(
                storage_url=self.config.tansu_storage_url,
                port=None,  # Auto-select free port
            )
            await broker.start()
            self._source_broker = broker

            # Parse broker URL to get host:port
            broker_url = broker.get_broker_url()
            host, port_str = broker_url.split(":")
            port = int(port_str)

            client = TansuQueueClient(broker_url)
            await client.start()
            self._source_client = client

            self._source_endpoint = QueueEndpoint(
                queue_type=QueueType.TANSU,
                host=host,
                port=port,
                storage_url=self.config.tansu_storage_url,
            )

            await client.create_topic(self._source_topic)

            self.logger.info(f"Created Tansu source queue on port {port} for {self.stage_id}")
            return client

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

        # Create source queue (broker + client for split metadata)
        self._source_client = await self._create_source_queue()

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
        """Generate splits and write to source queue with backpressure awareness."""
        self.logger.info(f"Generating splits for source {self.stage_id}")

        split_iterator = self.plan_splits()
        backpressure_check_interval = 10  # Check backpressure every N splits
        consecutive_backpressure_pauses = 0
        max_consecutive_pauses = 100  # Max pauses before logging warning

        for split in split_iterator:
            if not self._running:
                break

            # Check backpressure periodically
            if self._splits_produced % backpressure_check_interval == 0:
                should_pause = await self._check_backpressure_before_produce()
                if should_pause:
                    consecutive_backpressure_pauses += 1
                    if consecutive_backpressure_pauses >= max_consecutive_pauses:
                        self.logger.warning(
                            f"Source {self.stage_id} paused for {consecutive_backpressure_pauses} "
                            f"consecutive checks due to backpressure"
                        )
                    # Wait a bit before checking again
                    await asyncio.sleep(0.1)
                    continue
                else:
                    consecutive_backpressure_pauses = 0

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

    async def _check_backpressure_before_produce(self) -> bool:
        """Check if we should pause production due to downstream backpressure.

        Returns:
            True if production should be paused, False otherwise
        """
        # Check if we have downstream stages configured
        if not self._downstream_stage_refs:
            return False

        # Check all downstream stages for backpressure
        for stage_id, stage_ref in self._downstream_stage_refs.items():
            try:
                # Get status from downstream stage
                status = await stage_ref.get_status_async()

                # Check if backpressure is active
                if status.backpressure_active:
                    self.logger.debug(
                        f"Backpressure detected from downstream stage {stage_id}, "
                        f"pausing split production"
                    )
                    return True

                # Also check queue size if available
                # Use a threshold (e.g., 80% of max queue size)
                queue_size = status.output_queue_size
                if queue_size > self._backpressure_threshold_queue_size * 0.8:
                    self.logger.debug(
                        f"Downstream queue size {queue_size} approaching threshold, "
                        f"slowing down production"
                    )
                    return True

            except Exception as e:
                self.logger.debug(f"Error checking backpressure from {stage_id}: {e}")
                # Continue checking other downstream stages

        return False

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
        offset = await self._source_client.produce(self._source_topic, message.to_bytes())
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
        # Clean up source client first
        if self._source_client:
            await self._source_client.stop()
            self._source_client = None

        # Clean up source broker
        if self._source_broker:
            await self._source_broker.stop()
            self._source_broker = None

        # Clean up output queue (parent)
        await super().cleanup_queue()

    def get_source_client(self) -> Optional[QueueClient]:
        """Get the source queue client (for debugging/testing)."""
        return self._source_client

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
        if self._source_client:
            try:
                source_size = await self._source_client.get_latest_offset(self._source_topic)
                status.metrics["source_queue_size"] = source_size
            except Exception:
                pass

        status.metrics["splits_produced"] = self._splits_produced
        return status
