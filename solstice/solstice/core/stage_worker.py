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

"""StageWorker - Pull-based streaming worker.

This worker pulls from an upstream queue, processes messages, and produces
to an output queue. It's designed for streaming-style execution with:
- Exactly-once semantics via offset tracking
- EOF-based completion detection
- Partition-aware processing
- WebUI metrics push
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray

from solstice.queue import QueueType, QueueClient, MemoryClient, TansuQueueClient
from solstice.utils.logging import create_ray_logger
from solstice.core.stage_config import (
    StageConfig,
    QueueEndpoint,
    QueueMessage,
)
from solstice.core.split_payload_store import SplitPayloadStore

if TYPE_CHECKING:
    from solstice.core.stage import Stage


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

    Metrics Push:
    Workers push metrics to a state topic for WebUI monitoring.
    This replaces the pull-based ray.get() polling approach.
    """

    def __init__(
        self,
        worker_id: str,
        job_id: str,
        stage: "Stage",
        upstream_endpoint: Optional[QueueEndpoint],
        upstream_topic: Optional[str],
        output_endpoint: QueueEndpoint,
        output_topic: str,
        consumer_group: str,
        assigned_partitions: List[int],
        config: StageConfig,
        payload_store: SplitPayloadStore,
        state_endpoint: Optional[QueueEndpoint] = None,
        state_topic: Optional[str] = None,
        lineage_sample_rate: float = 0.0,
    ):
        self.worker_id = worker_id
        self.job_id = job_id
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

        # State push configuration (optional, for WebUI)
        self.state_endpoint = state_endpoint
        self.state_topic = state_topic
        self._state_producer = None  # Created in run() if configured

        # Lineage tracking configuration (from WebUIConfig via runner)
        self._lineage_sample_rate = lineage_sample_rate

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
        self._total_input_records = 0
        self._total_output_records = 0
        self._total_processing_time = 0.0
        self._last_commit_time = time.time()
        self._last_metrics_emit_time = 0.0
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

            # Initialize state producer if configured (for WebUI metrics push)
            await self._init_state_producer()

            # Emit worker started event
            await self._emit_worker_started()

            # Process from upstream queue
            await self._process_from_upstream()

            # Emit worker stopped event
            await self._emit_worker_stopped(reason="completed")

            return {
                "worker_id": self.worker_id,
                "processed_count": self._processed_count,
                "error_count": self._error_count,
            }

        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} failed: {e}")
            # Emit exception and worker stopped
            await self._emit_exception(e)
            await self._emit_worker_stopped(reason="failed")
            raise
        finally:
            self._running = False

            # Close operator (allows sink to flush buffers, etc.)
            try:
                self.operator.close()
            except Exception as e:
                self.logger.warning(f"Error during operator close: {e}")

            # Stop state producer
            if self._state_producer:
                try:
                    await self._state_producer.stop()
                except Exception as e:
                    self.logger.warning(f"Error stopping state producer: {e}")

            # Cleanup queue connections
            if self.upstream_queue:
                await self.upstream_queue.stop()
            if self.output_queue:
                await self.output_queue.stop()

    async def _init_state_producer(self) -> None:
        """Initialize state producer for metrics push."""
        if not self.state_endpoint or not self.state_topic:
            return

        try:
            from solstice.webui.state.producer import StateProducer

            state_queue = await self._create_queue_from_endpoint(self.state_endpoint)
            self._state_producer = StateProducer(
                job_id=self.job_id,
                queue_client=state_queue,
                state_topic=self.state_topic,
            )
            await self._state_producer.start()
            self.logger.debug("State producer initialized")
        except Exception as e:
            self.logger.warning(f"Failed to init state producer: {e}")
            self._state_producer = None

    async def _emit_worker_started(self) -> None:
        """Emit WORKER_STARTED event."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import worker_started_message

            msg = worker_started_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                assigned_partitions=self.assigned_partitions,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker started: {e}")

    async def _emit_worker_stopped(self, reason: str = "completed") -> None:
        """Emit WORKER_STOPPED event."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import worker_stopped_message

            msg = worker_stopped_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                reason=reason,
                processed_count=self._processed_count,
                error_count=self._error_count,
                input_records=self._total_input_records,
                output_records=self._total_output_records,
                processing_time=self._total_processing_time,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker stopped: {e}")

    async def _emit_worker_metrics(self) -> None:
        """Emit WORKER_METRICS event (rate limited)."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import worker_metrics_message

            msg = worker_metrics_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                input_records=self._total_input_records,
                output_records=self._total_output_records,
                processing_time=self._total_processing_time,
                processed_count=self._processed_count,
                assigned_partitions=self.assigned_partitions,
                is_running=self._running,
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit worker metrics: {e}")

    async def _emit_exception(self, exception: Exception) -> None:
        """Emit EXCEPTION event."""
        if not self._state_producer:
            return

        try:
            import traceback
            from solstice.webui.state.messages import exception_message

            msg = exception_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                exception_type=type(exception).__name__,
                message=str(exception),
                stacktrace=traceback.format_exc(),
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit exception: {e}")

    def notify_upstream_finished(self) -> None:
        """Called by master when upstream stage(s) have finished."""
        self._upstream_finished = True
        self.logger.info(f"Worker {self.worker_id} notified: upstream finished")

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status. Used for health checks and monitoring."""
        import os

        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "upstream_finished": self._upstream_finished,
            "assigned_partitions": self.assigned_partitions,
            "pid": os.getpid(),
        }

    async def _process_from_upstream(self) -> None:
        """Process messages from upstream queue from all assigned partitions.

        Completion criteria:
        - When EOF markers have been received for ALL assigned partitions
        - EOF markers are sent by upstream stage when it completes

        This is more reliable than polling-based completion detection because:
        1. No race conditions - EOF is guaranteed to come after all data
        2. No need for offset queries - just track EOF receipt
        3. Faster completion - no need for multiple empty polls
        """
        last_committed_offsets: Dict[int, int] = {}  # Track offsets per partition
        eof_received: set = set()  # Track which partitions have received EOF
        current_partition_idx = 0  # Round-robin index for partition polling
        active_partitions = list(self.assigned_partitions)  # Local copy

        # Track consecutive empty fetches per partition
        # Used to detect end-of-partition after recovery when EOF was already consumed
        empty_fetch_count: Dict[int, int] = {p: 0 for p in active_partitions}
        MAX_EMPTY_FETCHES_WHEN_UPSTREAM_DONE = 10

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
                    eof_received.discard(p)

                self.logger.info(
                    f"Worker {self.worker_id} switched to partitions {active_partitions}"
                )

            # Safety check: ensure we have partitions
            if not active_partitions:
                if self._upstream_finished:
                    self.logger.info(
                        f"Worker {self.worker_id} finished: no partitions and upstream done"
                    )
                    break
                await asyncio.sleep(0.5)
                continue

            # Check if all partitions have received EOF
            if eof_received >= set(active_partitions):
                self.logger.info(
                    f"Worker {self.worker_id} finished: received EOF from all "
                    f"{len(active_partitions)} partitions"
                )
                break

            # Round-robin across assigned partitions (skip EOF'd partitions)
            partitions_to_poll = [p for p in active_partitions if p not in eof_received]
            if not partitions_to_poll:
                # All partitions have EOF, exit
                break

            partition = partitions_to_poll[current_partition_idx % len(partitions_to_poll)]
            current_partition_idx = (current_partition_idx + 1) % len(partitions_to_poll)

            # Fetch batch from current partition
            # IMPORTANT: Must use consumer_group to share offset state with commit_offset
            records = await self.upstream_queue.fetch(
                self.upstream_topic,
                max_records=self.config.batch_size,
                timeout_ms=1000,
                partition=partition,
                group_id=self.consumer_group,
            )

            if not records:
                # Track empty fetches to detect end-of-partition after recovery
                # When worker recovers from a crash, it may resume at an offset past the EOF
                # (because EOF was processed but worker crashed before completion)
                if partition not in empty_fetch_count:
                    empty_fetch_count[partition] = 0
                empty_fetch_count[partition] += 1

                # If upstream is finished and we've had many consecutive empty fetches,
                # assume this partition is done (EOF was already consumed before recovery)
                if (
                    self._upstream_finished
                    and empty_fetch_count[partition] >= MAX_EMPTY_FETCHES_WHEN_UPSTREAM_DONE
                ):
                    eof_received.add(partition)
                    self.logger.info(
                        f"Worker {self.worker_id} marking partition {partition} as done "
                        f"(upstream finished, {empty_fetch_count[partition]} empty fetches, "
                        f"likely resumed past EOF)"
                    )
                else:
                    await asyncio.sleep(0.05)
                continue

            # Reset empty fetch count on successful fetch
            empty_fetch_count[partition] = 0

            # Debug: Log first batch fetched
            if self._processed_count == 0 and records:
                self.logger.info(
                    f"Worker {self.worker_id} first fetch: {len(records)} records, "
                    f"offset range [{records[0].offset}-{records[-1].offset}]"
                )

            # Process each record with frequent commits for exactly-once semantics.
            # Commit every N messages (config.commit_batch_size) to balance performance vs duplicate risk.
            commit_batch_size = self.config.commit_batch_size
            messages_since_commit = 0

            for record in records:
                try:
                    message = QueueMessage.from_bytes(record.value)

                    # Check for EOF marker
                    if message.is_eof():
                        eof_received.add(partition)
                        self.logger.info(
                            f"Worker {self.worker_id} received EOF for partition {partition} "
                            f"({len(eof_received)}/{len(active_partitions)} complete)"
                        )
                        # Commit offset for EOF marker immediately
                        eof_offset = record.offset + 1
                        await self.upstream_queue.commit_offset(
                            self.consumer_group,
                            self.upstream_topic,
                            eof_offset,
                            partition=partition,
                        )
                        # Update last_committed_offsets to prevent final commit from rolling back
                        last_committed_offsets[partition] = eof_offset
                        continue

                    await self._process_message(message, partition_id=partition)
                    self._processed_count += 1

                except Exception as e:
                    import traceback

                    self.logger.error(
                        f"Error processing message at offset {record.offset}: {type(e).__name__}: {e}"
                    )
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    self._error_count += 1

                # Track offset for this partition
                current_offset = record.offset + 1
                last_committed_offsets[partition] = current_offset
                messages_since_commit += 1

                # Commit frequently to minimize duplicate window
                if messages_since_commit >= commit_batch_size:
                    await self.upstream_queue.commit_offset(
                        self.consumer_group,
                        self.upstream_topic,
                        current_offset,
                        partition=partition,
                    )
                    messages_since_commit = 0

            # Final commit for any remaining messages in this batch
            if messages_since_commit > 0:
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

    async def _process_message(self, message: QueueMessage, partition_id: int = -1) -> None:
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
        dequeue_time = time.time()
        output_payload = self.operator.process_split(split, payload)
        complete_time = time.time()
        processing_time = complete_time - dequeue_time

        # Update metrics
        input_records = len(payload) if payload else 0
        output_records = len(output_payload) if output_payload else 0
        self._total_input_records += input_records
        self._total_output_records += output_records
        self._total_processing_time += processing_time

        # Calculate payload sizes for lineage
        input_bytes = 0
        output_bytes = 0
        if payload:
            # Estimate size from Arrow table
            input_bytes = payload.data.nbytes if hasattr(payload.data, "nbytes") else 0
        if output_payload:
            output_bytes = (
                output_payload.data.nbytes if hasattr(output_payload.data, "nbytes") else 0
            )

        payload_key = ""
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

        # Emit lineage tracking (gated by sample rate: 0=off, 1=full, 0.x=sampling)
        if self._should_track_lineage():
            # The output split ID that downstream stages will use as parent
            # This must match the split_id in output_message
            output_split_id = f"{self.stage_id}_{message.split_id}"

            # For source operators: no parents
            # For other operators: input message's split_id is the parent
            if is_source_message:
                parent_ids: list[str] = []  # Source has no parents
            else:
                parent_ids = [message.split_id]  # Input split is the parent

            await self._emit_split_lineage(
                output_split_id=output_split_id,
                parent_split_ids=parent_ids,
                partition_id=partition_id,
                enqueue_time=message.timestamp,
                dequeue_time=dequeue_time,
                complete_time=complete_time,
                input_records=input_records,
                output_records=output_records,
                input_bytes=input_bytes,
                output_bytes=output_bytes,
                payload_key=payload_key,
            )

        # Emit metrics periodically (rate limited by StateProducer)
        await self._emit_worker_metrics()

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

    def get_metrics(self):
        """Get worker metrics for WebUI.

        Returns:
            WorkerMetrics dataclass with current metrics
        """
        from solstice.core.models import WorkerMetrics

        return WorkerMetrics(
            worker_id=self.worker_id,
            stage_id=self.stage_id,
            input_records=self._total_input_records,
            output_records=self._total_output_records,
            processing_time=self._total_processing_time,
        )

    def _should_track_lineage(self) -> bool:
        """Check if this split should be tracked based on sample rate.

        - rate=0.0: never track (disabled)
        - rate=1.0: always track (full)
        - rate=0.x: probabilistic sampling
        """
        rate = self._lineage_sample_rate
        if rate <= 0.0:
            return False
        if rate >= 1.0:
            return True
        import random

        return random.random() < rate

    async def _emit_split_lineage(
        self,
        output_split_id: str,
        parent_split_ids: list[str],
        partition_id: int,
        enqueue_time: float,
        dequeue_time: float,
        complete_time: float,
        input_records: int,
        output_records: int,
        input_bytes: int,
        output_bytes: int,
        payload_key: str,
    ) -> None:
        """Emit SPLIT_PROCESSED event for lineage tracking."""
        if not self._state_producer:
            return

        try:
            from solstice.webui.state.messages import split_processed_message

            msg = split_processed_message(
                job_id=self.job_id,
                stage_id=self.stage_id,
                worker_id=self.worker_id,
                split_id=output_split_id,
                parent_split_ids=parent_split_ids,
                partition_id=partition_id,
                enqueue_time=enqueue_time,
                dequeue_time=dequeue_time,
                complete_time=complete_time,
                input_records=input_records,
                output_records=output_records,
                input_bytes=input_bytes,
                output_bytes=output_bytes,
                payload_store_key=payload_key,
                payload_storage_path=None,  # TODO: Add external storage path if needed
            )
            await self._state_producer.produce(msg)
        except Exception as e:
            self.logger.debug(f"Failed to emit split lineage: {e}")
