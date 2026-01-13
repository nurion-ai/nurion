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

All methods are synchronous for simplicity (confluent-kafka is inherently sync).

Architecture:
    StageMaster uses TansuBrokerManager to start broker, then creates
    TansuQueueClient for local operations. Workers only use TansuQueueClient
    connecting to the master's broker.

Example:
    # On Master
    broker = TansuBrokerManager(storage_url="memory://tansu/")
    broker.start()

    client = TansuQueueClient(broker.get_broker_url())
    client.start()
    client.create_topic("my-topic")
    client.produce("my-topic", b"hello")

    # On Worker (only needs broker_url)
    client = TansuQueueClient("master-host:9092")
    client.start()
    client.produce("my-topic", b"from worker")
    records = client.fetch("my-topic", offset=0)
"""

from __future__ import annotations

import socket
import threading
import time
from typing import Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka._model import ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient, NewTopic

from tansu_py import BrokerConfig, BrokerError, BrokerEventHandler, TansuBroker

from solstice.queue.backend import Record
from solstice.utils.logging import create_ray_logger


def _find_free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port: int = s.getsockname()[1]
        return port


# =============================================================================
# TansuBrokerManager - Implements QueueBroker
# =============================================================================


class _BrokerEventHandler(BrokerEventHandler):
    """Internal event handler for broker lifecycle events."""

    def __init__(
        self,
        manager: "TansuBrokerManager",
        ready_event: threading.Event,
    ):
        self.manager = manager
        self.logger = manager.logger
        self._ready_event = ready_event

    def on_started(self, port: int) -> None:
        self.logger.info(f"Tansu broker started on port {port}")
        self.manager._actual_port = port
        self.manager._running = True
        self._ready_event.set()

    def on_stopped(self) -> None:
        self.logger.info("Tansu broker stopped")
        self.manager._running = False

    def on_error(self, error: BrokerError) -> None:
        self.logger.warning(f"Tansu broker error: {error.message}")

    def on_fatal(self, error: BrokerError) -> None:
        self.logger.error(f"Tansu broker fatal error: {error.message}")
        self.manager._running = False
        self._ready_event.set()


class TansuBrokerManager:
    """
    Manages the embedded Tansu broker lifecycle.

    Implements QueueBroker protocol. Should only run on StageMaster.
    Workers connect to the broker using TansuQueueClient.

    Example:
        broker = TansuBrokerManager(storage_url="memory://tansu/")
        broker.start()
        broker_url = broker.get_broker_url()  # "127.0.0.1:9092"
        # ... workers connect using broker_url ...
        broker.stop()
    """

    def __init__(
        self,
        storage_url: str = "memory://tansu/",
        port: Optional[int] = None,
        host: str = "127.0.0.1",
        startup_timeout: float = 30.0,
    ):
        """
        Initialize broker manager.

        Args:
            storage_url: Storage backend URL (memory://tansu/, s3://bucket/)
            port: Port for Kafka protocol. None = auto-select free port.
            host: Host to advertise to clients. Use 127.0.0.1 to avoid IPv6 issues.
            startup_timeout: Timeout for broker startup in seconds.
        """
        self.storage_url = storage_url
        self.port = port or _find_free_port()
        self.host = host
        self.startup_timeout = startup_timeout

        self._broker: Optional[TansuBroker] = None
        self._running = False
        self._actual_port: Optional[int] = None

        self.logger = create_ray_logger(f"TansuBroker:{self.port}")

    def start(self) -> None:
        """Start the embedded Tansu broker."""
        if self._running:
            return

        config = BrokerConfig(
            storage_url=self.storage_url,
            listener_port=self.port,
            advertised_host=self.host,
        )

        # Create event for cross-thread signaling
        ready_event = threading.Event()
        handler = _BrokerEventHandler(self, ready_event)
        self._broker = TansuBroker(config, event_handler=handler)
        self._broker.start()

        # Wait for broker to be ready
        if not ready_event.wait(timeout=self.startup_timeout):
            raise RuntimeError(f"Tansu broker failed to start within {self.startup_timeout}s")

        if not self._running:
            raise RuntimeError("Tansu broker failed to start (fatal error)")

        self.logger.info(f"Broker ready at {self.get_broker_url()}")

    def stop(self) -> None:
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
    Kafka client for Tansu broker using confluent-kafka.

    Implements QueueClient protocol (Producer + Consumer + Admin).
    Can run on any node - only needs broker_url to connect.
    All methods are synchronous (confluent-kafka is inherently sync).

    Example:
        client = TansuQueueClient(broker_url="master-host:9092")
        client.start()

        client.create_topic("my-topic")
        offset = client.produce("my-topic", b"hello")
        records = client.fetch("my-topic", offset=0)

        client.stop()
    """

    def __init__(self, broker_url: str):
        """
        Initialize queue client.

        Args:
            broker_url: Broker address in format "host:port".
        """
        self.broker_url = broker_url

        self._producer: Optional[Producer] = None
        self._admin_client: Optional[AdminClient] = None
        self._consumers: Dict[tuple, Consumer] = {}
        self._running = False

        self.logger = create_ray_logger(f"TansuClient:{broker_url}")

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start the client and connect to broker."""
        if self._running:
            return

        self._producer = Producer(
            {
                "bootstrap.servers": self.broker_url,
                "acks": "all",
                "request.timeout.ms": 10000,
                "socket.timeout.ms": 10000,
                "message.timeout.ms": 10000,
            }
        )

        self._admin_client = AdminClient(
            {
                "bootstrap.servers": self.broker_url,
            }
        )

        self._running = True
        self.logger.info(f"Client connected to {self.broker_url}")

    def stop(self) -> None:
        """Stop the client and disconnect from broker."""
        self._running = False

        # Stop consumers
        for consumer in self._consumers.values():
            try:
                consumer.close()
            except Exception:
                pass
        self._consumers.clear()

        # Flush and cleanup producer
        if self._producer:
            try:
                self._producer.flush(timeout=5.0)
            except Exception:
                pass
            self._producer = None

        # Admin client doesn't need explicit cleanup in confluent-kafka
        self._admin_client = None

        self.logger.info("Client disconnected")

    def is_running(self) -> bool:
        """Check if client is running."""
        return self._running

    # -------------------------------------------------------------------------
    # QueueAdmin Implementation
    # -------------------------------------------------------------------------

    def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic."""
        if self._admin_client is None:
            raise RuntimeError("Client not started")

        new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=1)
        futures = self._admin_client.create_topics([new_topic])
        for topic_name, future in futures.items():
            try:
                future.result(timeout=10.0)
                self.logger.info(f"Created topic: {topic}")
            except KafkaException as e:
                # Topic already exists is OK
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    pass
                else:
                    raise

    def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        if self._admin_client is None:
            raise RuntimeError("Client not started")

        futures = self._admin_client.delete_topics([topic])
        for topic_name, future in futures.items():
            try:
                future.result(timeout=10.0)
                self.logger.info(f"Deleted topic: {topic}")
            except Exception:
                pass  # Topic may not exist

    def health_check(self) -> bool:
        """Check if client is healthy."""
        return self._running and self._producer is not None and self._admin_client is not None

    # -------------------------------------------------------------------------
    # QueueProducer Implementation
    # -------------------------------------------------------------------------

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
    ) -> int:
        """Produce a message to a topic."""
        if self._producer is None:
            raise RuntimeError("Client not started")

        # Use a holder to capture callback result
        result_holder: dict[str, int | KafkaError | None] = {"offset": -1, "error": None}

        def delivery_callback(err: KafkaError | None, msg: "Message") -> None:  # type: ignore[name-defined]  # noqa: F821
            if err:
                result_holder["error"] = err
            else:
                result_holder["offset"] = msg.offset()

        # Build produce arguments
        produce_key = key
        produce_partition = partition if partition is not None else -1  # -1 means auto-assign

        self._producer.produce(
            topic,
            value=value,
            key=produce_key,
            partition=produce_partition,
            callback=delivery_callback,
        )
        # Flush to ensure message is sent and callback is called
        remaining = self._producer.flush(timeout=10.0)

        # Check if flush timed out (messages still in queue)
        if remaining > 0:
            raise KafkaException(
                KafkaError(
                    -192,  # ERR__MSG_TIMED_OUT
                    f"Produce timed out: {remaining} message(s) still in queue after flush",
                )
            )

        error = result_holder["error"]
        if error is not None and isinstance(error, KafkaError):
            raise KafkaException(error)
        offset = result_holder["offset"]
        return int(offset) if isinstance(offset, int) else -1

    # -------------------------------------------------------------------------
    # QueueConsumer Implementation
    # -------------------------------------------------------------------------

    def fetch(
        self,
        topic: str,
        offset: Optional[int] = None,
        max_records: int = 100,
        timeout_ms: int = 5000,
        partition: int = 0,
        group_id: Optional[str] = None,
    ) -> List[Record]:
        """Fetch records from a topic.

        Args:
            topic: Topic name
            offset: If specified, seek to this offset before fetching.
                    If None, continue from current consumer position.
            max_records: Maximum records to fetch
            timeout_ms: Fetch timeout in milliseconds
            partition: Partition to fetch from
            group_id: Consumer group ID (should match commit_offset calls)
        """
        consumer = self._get_consumer(topic, partition=partition, group_id=group_id)

        # Only seek if offset is explicitly specified
        if offset is not None:
            consumer.seek(TopicPartition(topic, partition, offset))

        records: List[Record] = []
        remaining_timeout = timeout_ms / 1000.0
        start_time = time.time()

        while len(records) < max_records and remaining_timeout > 0:
            msg = consumer.poll(timeout=min(remaining_timeout, 1.0))
            if msg is None:
                break
            if msg.error():
                self.logger.warning(f"Consumer error: {msg.error()}")
                break
            msg_offset = msg.offset()
            msg_value = msg.value()
            records.append(
                Record(
                    offset=msg_offset if msg_offset is not None else -1,
                    value=msg_value if msg_value is not None else b"",
                    key=msg.key(),
                    timestamp=msg.timestamp()[1] if msg.timestamp()[0] else int(time.time() * 1000),
                )
            )
            remaining_timeout = (timeout_ms / 1000.0) - (time.time() - start_time)

        return records

    def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
        partition: int = 0,
    ) -> None:
        """Commit the consumer offset for a consumer group."""
        consumer = self._get_consumer(topic, partition=partition, group_id=group)
        tp = TopicPartition(topic, partition, offset)

        try:
            consumer.commit(offsets=[tp], asynchronous=False)
            self.logger.debug(f"Committed offset {offset} for {group}/{topic}/{partition}")
        except Exception as e:
            self.logger.warning(f"Failed to commit offset: {e}")
            raise

    def get_committed_offset(
        self,
        group: str,
        topic: str,
        partition: int = 0,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group using AdminClient."""
        offsets = self.get_all_committed_offsets(group, topic)
        return offsets.get(partition)

    def get_all_committed_offsets(
        self,
        group: str,
        topic: str,
    ) -> Dict[int, int]:
        """Get committed offsets for all partitions using AdminClient.

        More efficient than calling get_committed_offset for each partition.
        """
        if self._admin_client is None:
            raise RuntimeError("Client not started")

        try:
            # First get all partitions for the topic
            consumer = self._get_consumer(topic, partition=0)
            metadata = consumer.list_topics(topic, timeout=10.0)
            if topic not in metadata.topics:
                return {}

            partition_ids = list(metadata.topics[topic].partitions.keys())
            topic_partitions = [TopicPartition(topic, p) for p in partition_ids]

            # Query committed offsets for all partitions at once
            cgtp = ConsumerGroupTopicPartitions(group, topic_partitions)
            futures = self._admin_client.list_consumer_group_offsets([cgtp])
            result: Dict[int, int] = {}

            for group_name, future in futures.items():
                group_result = future.result(timeout=10.0)
                for part in group_result.topic_partitions:
                    if part.topic == topic and part.offset >= 0:
                        result[part.partition] = part.offset

            return result
        except Exception as e:
            self.logger.warning(f"Failed to get committed offsets: {e}")
            return {}

    def get_latest_offset(
        self,
        topic: str,
        partition: int = 0,
    ) -> int:
        """Get the latest offset in the topic."""
        consumer = self._get_consumer(topic, partition=partition)
        tp = TopicPartition(topic, partition)

        try:
            low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
            return high
        except Exception as e:
            self.logger.warning(f"Failed to get watermarks: {e}")
            return 0

    def get_all_partition_offsets(self, topic: str) -> Dict[int, int]:
        """Get latest offsets for all partitions of a topic.

        Returns:
            Dict mapping partition id to latest offset.
        """
        result: Dict[int, int] = {}

        try:
            consumer = self._get_consumer(topic, partition=0)

            # Get cluster metadata to find partitions
            metadata = consumer.list_topics(topic, timeout=10.0)
            if topic not in metadata.topics:
                return {0: 0}

            topic_metadata = metadata.topics[topic]
            partition_ids = list(topic_metadata.partitions.keys())

            for p in partition_ids:
                tp = TopicPartition(topic, p)
                try:
                    low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
                    result[p] = high
                except Exception:
                    result[p] = 0

        except Exception as e:
            self.logger.warning(f"Failed to get partition offsets: {e}")
            return {0: 0}

        return result if result else {0: 0}

    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------

    def _get_consumer(
        self,
        topic: str,
        partition: int = 0,
        group_id: Optional[str] = None,
    ) -> Consumer:
        """Get or create a consumer for the topic/partition.

        For consumers with a group_id, automatically seeks to the committed offset
        to support resumption after crashes (exactly-once semantics).
        """
        consumer_key = (topic, partition, group_id)

        if consumer_key not in self._consumers:
            config: dict[str, str | int | float | bool | None] = {
                "bootstrap.servers": self.broker_url,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "fetch.wait.max.ms": 500,
                "group.id": group_id or f"_temp_{topic}_{partition}_{id(self)}",
            }

            consumer = Consumer(config)
            consumer.assign([TopicPartition(topic, partition)])
            consumer.poll(timeout=0.1)  # Required for initialization before seek

            # For consumers with a group_id, seek to committed offset for crash recovery
            if group_id:
                tp = TopicPartition(topic, partition)
                committed = consumer.committed([tp], timeout=10.0)
                if committed and committed[0] and committed[0].offset >= 0:
                    consumer.seek(TopicPartition(topic, partition, committed[0].offset))
                    self.logger.debug(
                        f"Consumer for {topic}:{partition} (group={group_id}) "
                        f"resuming from committed offset {committed[0].offset}"
                    )
                else:
                    # No committed offset, start from beginning
                    consumer.seek(TopicPartition(topic, partition, 0))
                    self.logger.debug(
                        f"Consumer for {topic}:{partition} (group={group_id}) "
                        f"starting from offset 0 (no committed offset)"
                    )

            self.logger.debug(f"Created consumer for {topic}:{partition} (group={group_id})")
            self._consumers[consumer_key] = consumer

        return self._consumers[consumer_key]
