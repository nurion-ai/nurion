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

"""
Tansu Queue Implementation.

This module provides Tansu-based queue components:
- TansuBrokerManager: Manages embedded Tansu broker lifecycle (QueueBroker)
- TansuQueueClient: Kafka client for produce/consume (QueueClient)

Architecture:
    StageMaster uses TansuBrokerManager to start broker, then creates
    TansuQueueClient for local operations. Workers only use TansuQueueClient
    connecting to the master's broker.

Example:
    # On Master
    broker = TansuBrokerManager(storage_url="memory://tansu/")
    await broker.start()

    client = TansuQueueClient(broker.get_broker_url())
    await client.start()
    await client.create_topic("my-topic")
    await client.produce("my-topic", b"hello")

    # On Worker (only needs broker_url)
    client = TansuQueueClient("master-host:9092")
    await client.start()
    await client.produce("my-topic", b"from worker")
    records = await client.fetch("my-topic", offset=0)
"""

from __future__ import annotations

import asyncio
import socket
import time
from typing import Dict, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import OffsetAndMetadata

from tansu_py import BrokerConfig, BrokerError, BrokerEventHandler, TansuBroker

from solstice.queue.backend import Record
from solstice.utils.logging import create_ray_logger


def _find_free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# =============================================================================
# TansuBrokerManager - Implements QueueBroker
# =============================================================================


class _BrokerEventHandler(BrokerEventHandler):
    """Internal event handler for broker lifecycle events."""

    def __init__(self, manager: "TansuBrokerManager"):
        self.manager = manager
        self.logger = manager.logger

    def on_started(self, port: int) -> None:
        self.logger.info(f"Tansu broker started on port {port}")
        self.manager._actual_port = port
        self.manager._running = True
        try:
            loop = asyncio.get_event_loop()
            loop.call_soon_threadsafe(self.manager._ready_event.set)
        except RuntimeError:
            self.manager._ready_event.set()

    def on_stopped(self) -> None:
        self.logger.info("Tansu broker stopped")
        self.manager._running = False

    def on_error(self, error: BrokerError) -> None:
        self.logger.warning(f"Tansu broker error: {error.message}")

    def on_fatal(self, error: BrokerError) -> None:
        self.logger.error(f"Tansu broker fatal error: {error.message}")
        self.manager._running = False
        try:
            loop = asyncio.get_event_loop()
            loop.call_soon_threadsafe(self.manager._ready_event.set)
        except RuntimeError:
            pass


class TansuBrokerManager:
    """
    Manages the embedded Tansu broker lifecycle.

    Implements QueueBroker protocol. Should only run on StageMaster.
    Workers connect to the broker using TansuQueueClient.

    Example:
        broker = TansuBrokerManager(storage_url="memory://tansu/")
        await broker.start()
        broker_url = broker.get_broker_url()  # "localhost:9092"
        # ... workers connect using broker_url ...
        await broker.stop()
    """

    def __init__(
        self,
        storage_url: str = "memory://tansu/",
        port: Optional[int] = None,
        host: str = "localhost",
        startup_timeout: float = 30.0,
    ):
        """
        Initialize broker manager.

        Args:
            storage_url: Storage backend URL (memory://tansu/, s3://bucket/)
            port: Port for Kafka protocol. None = auto-select free port.
            host: Host to advertise to clients.
            startup_timeout: Timeout for broker startup in seconds.
        """
        self.storage_url = storage_url
        self.port = port or _find_free_port()
        self.host = host
        self.startup_timeout = startup_timeout

        self._broker: Optional[TansuBroker] = None
        self._running = False
        self._actual_port: Optional[int] = None
        self._ready_event = asyncio.Event()

        self.logger = create_ray_logger(f"TansuBroker:{self.port}")

    async def start(self) -> None:
        """Start the embedded Tansu broker."""
        if self._running:
            return

        config = BrokerConfig(
            storage_url=self.storage_url,
            listener_port=self.port,
            advertised_host=self.host,
        )

        handler = _BrokerEventHandler(self)
        self._broker = TansuBroker(config, event_handler=handler)
        self._broker.start()

        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=self.startup_timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Tansu broker failed to start within {self.startup_timeout}s")

        if not self._running:
            raise RuntimeError("Tansu broker failed to start (fatal error)")

        self.logger.info(f"Broker ready at {self.get_broker_url()}")

    async def stop(self) -> None:
        """Stop the embedded Tansu broker."""
        if self._broker:
            try:
                self._broker.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping broker: {e}")
            self._broker = None
        self._running = False

    def get_broker_url(self) -> str:
        """Get the broker URL for clients to connect."""
        port = self._actual_port or self.port
        return f"{self.host}:{port}"

    def is_running(self) -> bool:
        """Check if broker is running."""
        return self._running


# =============================================================================
# TansuQueueClient - Implements QueueClient (Producer + Consumer + Admin)
# =============================================================================


class TansuQueueClient:
    """
    Kafka client for Tansu broker.

    Implements QueueClient protocol (Producer + Consumer + Admin).
    Can run on any node - only needs broker_url to connect.

    Example:
        client = TansuQueueClient(broker_url="master-host:9092")
        await client.start()

        await client.create_topic("my-topic")
        offset = await client.produce("my-topic", b"hello")
        records = await client.fetch("my-topic", offset=0)

        await client.stop()
    """

    def __init__(self, broker_url: str):
        """
        Initialize queue client.

        Args:
            broker_url: Broker address in format "host:port".
        """
        self.broker_url = broker_url

        self._producer: Optional[AIOKafkaProducer] = None
        self._admin_client: Optional[AIOKafkaAdminClient] = None
        self._consumers: Dict[tuple, AIOKafkaConsumer] = {}
        self._committed_offsets: Dict[tuple, int] = {}  # (group, topic, partition) -> offset
        self._running = False

        self.logger = create_ray_logger(f"TansuClient:{broker_url}")

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the client and connect to broker."""
        if self._running:
            return

        # Initialize producer
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.broker_url,
            acks="all",
            request_timeout_ms=10000,
        )
        await asyncio.wait_for(self._producer.start(), timeout=10.0)

        # Initialize admin client
        self._admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.broker_url,
        )
        await asyncio.wait_for(self._admin_client.start(), timeout=10.0)

        self._running = True
        self.logger.info(f"Client connected to {self.broker_url}")

    async def stop(self) -> None:
        """Stop the client and disconnect from broker."""
        self._running = False

        # Stop consumers
        for consumer in self._consumers.values():
            try:
                await asyncio.wait_for(consumer.stop(), timeout=5.0)
            except Exception:
                pass
        self._consumers.clear()

        # Stop producer
        if self._producer:
            try:
                await asyncio.wait_for(self._producer.stop(), timeout=5.0)
            except Exception:
                pass
            self._producer = None

        # Stop admin client
        if self._admin_client:
            try:
                await asyncio.wait_for(self._admin_client.close(), timeout=5.0)
            except Exception:
                pass
            self._admin_client = None

        self.logger.info("Client disconnected")

    def is_running(self) -> bool:
        """Check if client is running."""
        return self._running

    # -------------------------------------------------------------------------
    # QueueAdmin Implementation
    # -------------------------------------------------------------------------

    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic."""
        if not self._admin_client:
            raise RuntimeError("Client not started")

        try:
            await self._admin_client.create_topics(
                [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
            )
            self.logger.info(f"Created topic: {topic}")
        except Exception as e:
            if "TopicAlreadyExistsError" in str(type(e).__name__):
                pass  # Topic already exists, that's fine
            else:
                raise

    async def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        if not self._admin_client:
            raise RuntimeError("Client not started")

        try:
            await self._admin_client.delete_topics([topic])
            self.logger.info(f"Deleted topic: {topic}")
        except Exception:
            pass  # Topic may not exist

    async def health_check(self) -> bool:
        """Check if client is healthy."""
        return self._running and self._producer is not None

    # -------------------------------------------------------------------------
    # QueueProducer Implementation
    # -------------------------------------------------------------------------

    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
    ) -> int:
        """Produce a message to a topic."""
        if not self._producer:
            raise RuntimeError("Client not started")

        result = await self._producer.send_and_wait(
            topic, value=value, key=key, partition=partition
        )
        return result.offset

    # -------------------------------------------------------------------------
    # QueueConsumer Implementation
    # -------------------------------------------------------------------------

    async def fetch(
        self,
        topic: str,
        offset: Optional[int] = None,
        max_records: int = 100,
        timeout_ms: int = 5000,
        partition: int = 0,
    ) -> List[Record]:
        """Fetch records from a topic.

        Args:
            topic: Topic name
            offset: If specified, seek to this offset before fetching.
                    If None, continue from current consumer position.
            max_records: Maximum records to fetch
            timeout_ms: Fetch timeout in milliseconds
            partition: Partition to fetch from
        """
        consumer = await self._get_consumer(topic, partition=partition)
        tp = TopicPartition(topic, partition)
        # Only seek if offset is explicitly specified
        if offset is not None:
            consumer.seek(tp, offset)
        records = []
        try:
            fetch_timeout = (timeout_ms / 1000) + 2.0  # Reduced buffer from 5s to 2s
            batch = await asyncio.wait_for(
                consumer.getmany(timeout_ms=timeout_ms, max_records=max_records),
                timeout=fetch_timeout,
            )

            for tp_key, tp_records in batch.items():
                for record in tp_records:
                    records.append(
                        Record(
                            offset=record.offset,
                            value=record.value,
                            key=record.key,
                            timestamp=record.timestamp or int(time.time() * 1000),
                        )
                    )
        except asyncio.TimeoutError:
            self.logger.warning(f"Fetch timed out after {timeout_ms}ms")
        except Exception as e:
            self.logger.warning(f"Fetch error: {e}")

        return records

    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
        partition: int = 0,
    ) -> None:
        """Commit the consumer offset for a consumer group."""
        # Get or create a consumer for this group
        consumer = await self._get_consumer(topic, partition=partition, group_id=group)

        tp = TopicPartition(topic, partition)
        offsets = {tp: OffsetAndMetadata(offset, "")}

        try:
            await asyncio.wait_for(consumer.commit(offsets), timeout=10.0)
            self._committed_offsets[(group, topic, partition)] = offset
            self.logger.debug(f"Committed offset {offset} for {group}/{topic}/{partition}")
        except asyncio.TimeoutError:
            self.logger.warning("Timeout committing offset")
            raise
        except Exception as e:
            self.logger.warning(f"Failed to commit offset: {e}")
            raise

    async def get_committed_offset(
        self,
        group: str,
        topic: str,
        partition: int = 0,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group."""
        # Try local cache first
        cached = self._committed_offsets.get((group, topic, partition))
        if cached is not None:
            return cached

        # Query from broker
        try:
            consumer = await self._get_consumer(topic, partition=partition, group_id=group)
            tp = TopicPartition(topic, partition)
            result = await asyncio.wait_for(consumer.committed(tp), timeout=10.0)
            if result is not None:
                self._committed_offsets[(group, topic, partition)] = result
            return result
        except asyncio.TimeoutError:
            self.logger.warning("Timeout getting committed offset")
            return None
        except Exception as e:
            self.logger.warning(f"Failed to get committed offset: {e}")
            return None

    async def get_latest_offset(
        self,
        topic: str,
        partition: int = 0,
    ) -> int:
        """Get the latest offset in the topic."""
        consumer = await self._get_consumer(topic, partition=partition)
        tp = TopicPartition(topic, partition)

        try:
            # Get end offsets with timeout
            end_offsets = await asyncio.wait_for(consumer.end_offsets([tp]), timeout=10.0)
            return end_offsets.get(tp, 0)
        except asyncio.TimeoutError:
            self.logger.warning("Timeout getting latest offset")
            return 0
        except Exception as e:
            self.logger.warning(f"Failed to get latest offset: {e}")
            return 0

    async def get_all_partition_offsets(self, topic: str) -> Dict[int, int]:
        """Get latest offsets for all partitions of a topic.

        Returns:
            Dict mapping partition id to latest offset.
        """
        result: Dict[int, int] = {}

        try:
            # Get a consumer to query partition info
            consumer = await self._get_consumer(topic, partition=0)

            # Get partitions for the topic
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                # Topic might not exist or no partitions yet
                return {0: 0}

            # Get end offsets for all partitions
            tps = [TopicPartition(topic, p) for p in partitions]
            end_offsets = await asyncio.wait_for(consumer.end_offsets(tps), timeout=10.0)

            for tp, offset in end_offsets.items():
                result[tp.partition] = offset

        except asyncio.TimeoutError:
            self.logger.warning("Timeout getting partition offsets")
            return {0: 0}
        except Exception as e:
            self.logger.warning(f"Failed to get partition offsets: {e}")
            return {0: 0}

        return result if result else {0: 0}

    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------

    async def _get_consumer(
        self,
        topic: str,
        partition: int = 0,
        group_id: Optional[str] = None,
    ) -> AIOKafkaConsumer:
        """Get or create a consumer for the topic/partition."""
        consumer_key = (topic, partition, group_id)

        if consumer_key not in self._consumers:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.broker_url,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=5000,
                fetch_max_wait_ms=500,
                group_id=group_id,
            )
            await asyncio.wait_for(consumer.start(), timeout=10.0)

            # Always use manual partition assignment for predictability
            # (group_id is still set for offset commit tracking)
            tp = TopicPartition(topic, partition)
            consumer.assign([tp])
            self.logger.debug(f"Created consumer for {topic}:{partition} (group={group_id})")

            self._consumers[consumer_key] = consumer

        return self._consumers[consumer_key]
