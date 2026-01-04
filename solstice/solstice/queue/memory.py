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

"""In-memory queue implementation.

This module provides a fast, non-persistent queue suitable for testing
and lightweight stages where re-processing on failure is acceptable.

Components:
- MemoryBroker: Manages in-memory topic storage (implements QueueBroker)
- MemoryClient: Producer/consumer operations (implements QueueClient)

Example:
    ```python
    # Create broker (on master)
    broker = MemoryBroker()
    await broker.start()

    # Create client
    client = MemoryClient(broker)
    await client.start()

    await client.create_topic("my-topic")
    offset = await client.produce("my-topic", b"hello")
    records = await client.fetch("my-topic", offset=0)

    await client.stop()
    await broker.stop()
    ```
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from solstice.queue.backend import Record


# =============================================================================
# Internal Data Structures
# =============================================================================


@dataclass
class _TopicData:
    """Internal data structure for a topic."""

    records: List[Tuple[int, bytes, Optional[bytes], int]] = field(default_factory=list)
    next_offset: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


# =============================================================================
# MemoryBroker - Implements QueueBroker
# =============================================================================


class MemoryBroker:
    """In-memory message broker.

    Manages topic storage in memory. Data is NOT persisted across restarts.

    Implements QueueBroker protocol:
    - start() / stop() for lifecycle
    - get_broker_url() returns a reference ID
    - is_running() for status check

    Example:
        broker = MemoryBroker()
        await broker.start()
        client = MemoryClient(broker)
        await client.start()
        # ...
        await broker.stop()
    """

    # Class-level registry for broker instances (for URL-based lookup)
    _instances: Dict[str, "MemoryBroker"] = {}
    _instance_counter = 0
    _registry_lock = threading.Lock()

    def __init__(self, gc_interval_seconds: float = 60.0):
        """Initialize the memory broker.

        Args:
            gc_interval_seconds: Interval for automatic garbage collection.
        """
        self._topics: Dict[str, _TopicData] = {}
        self._committed_offsets: Dict[
            Tuple[str, str, int], int
        ] = {}  # (group, topic, partition) -> offset
        self._global_lock = threading.Lock()
        self._gc_interval = gc_interval_seconds
        self._gc_task: Optional[asyncio.Task] = None
        self._running = False
        self._broker_id: Optional[str] = None

    async def start(self) -> None:
        """Start the memory broker."""
        if self._running:
            return

        self._running = True

        # Register this instance
        with MemoryBroker._registry_lock:
            MemoryBroker._instance_counter += 1
            self._broker_id = f"memory://{MemoryBroker._instance_counter}"
            MemoryBroker._instances[self._broker_id] = self

        # Start background GC task
        self._gc_task = asyncio.create_task(self._gc_loop())

    async def stop(self) -> None:
        """Stop the memory broker."""
        self._running = False

        if self._gc_task:
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass
            self._gc_task = None

        # Unregister this instance
        if self._broker_id:
            with MemoryBroker._registry_lock:
                MemoryBroker._instances.pop(self._broker_id, None)

        # Clear all data
        with self._global_lock:
            self._topics.clear()
            self._committed_offsets.clear()

    def get_broker_url(self) -> str:
        """Get the broker URL (reference ID for clients)."""
        return self._broker_id or "memory://0"

    def is_running(self) -> bool:
        """Check if broker is running."""
        return self._running

    @classmethod
    def get_instance(cls, broker_url: str) -> Optional["MemoryBroker"]:
        """Get a broker instance by URL."""
        with cls._registry_lock:
            return cls._instances.get(broker_url)

    # -------------------------------------------------------------------------
    # Internal: Topic Management
    # -------------------------------------------------------------------------

    def _get_or_create_topic(self, topic: str) -> _TopicData:
        """Get or create a topic (thread-safe)."""
        with self._global_lock:
            if topic not in self._topics:
                self._topics[topic] = _TopicData()
            return self._topics[topic]

    def _delete_topic(self, topic: str) -> None:
        """Delete a topic (thread-safe)."""
        with self._global_lock:
            self._topics.pop(topic, None)
            keys_to_remove = [k for k in self._committed_offsets if k[1] == topic]
            for key in keys_to_remove:
                del self._committed_offsets[key]

    def _get_topic(self, topic: str) -> Optional[_TopicData]:
        """Get a topic if it exists."""
        with self._global_lock:
            return self._topics.get(topic)

    # -------------------------------------------------------------------------
    # Internal: GC
    # -------------------------------------------------------------------------

    async def _gc_loop(self) -> None:
        """Background task for garbage collection."""
        while self._running:
            await asyncio.sleep(self._gc_interval)
            self._gc_all_topics()

    def _gc_all_topics(self) -> None:
        """Garbage collect consumed records from all topics."""
        with self._global_lock:
            for topic_name, topic_data in list(self._topics.items()):
                self._gc_topic(topic_name, topic_data)

    def _gc_topic(self, topic_name: str, topic_data: _TopicData) -> None:
        """Garbage collect consumed records from a single topic."""
        min_offset = None
        for (group, topic, partition), offset in self._committed_offsets.items():
            if topic == topic_name:
                if min_offset is None or offset < min_offset:
                    min_offset = offset

        if min_offset is None:
            return

        with topic_data.lock:
            topic_data.records = [r for r in topic_data.records if r[0] >= min_offset]


# =============================================================================
# MemoryClient - Implements QueueClient
# =============================================================================


class MemoryClient:
    """In-memory queue client.

    Provides producer, consumer, and admin operations against a MemoryBroker.

    Implements QueueClient protocol (QueueProducer + QueueConsumer + QueueAdmin).

    Example:
        broker = MemoryBroker()
        await broker.start()

        client = MemoryClient(broker)
        await client.start()

        await client.create_topic("my-topic")
        offset = await client.produce("my-topic", b"hello")
        records = await client.fetch("my-topic", offset=0)

        await client.stop()
    """

    def __init__(self, broker: MemoryBroker | str):
        """Initialize the memory client.

        Args:
            broker: Either a MemoryBroker instance or a broker URL string.
        """
        if isinstance(broker, str):
            # Look up broker by URL
            resolved = MemoryBroker.get_instance(broker)
            if resolved is None:
                raise ValueError(f"No MemoryBroker found for URL: {broker}")
            self._broker = resolved
            self._broker_url = broker
        else:
            self._broker = broker
            self._broker_url = broker.get_broker_url()

        self._running = False
        # Track consumer positions per (topic, partition) for auto-position fetch
        self._consumer_positions: dict[tuple[str, int], int] = {}

    async def start(self) -> None:
        """Start the client."""
        self._running = True

    async def stop(self) -> None:
        """Stop the client."""
        self._running = False

    def is_running(self) -> bool:
        """Check if client is running."""
        return self._running

    # -------------------------------------------------------------------------
    # QueueAdmin Implementation
    # -------------------------------------------------------------------------

    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic."""
        self._broker._get_or_create_topic(topic)

    async def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        self._broker._delete_topic(topic)

    async def health_check(self) -> bool:
        """Check if the client is healthy."""
        return self._running and self._broker.is_running()

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
        """Produce a message to the topic."""
        topic_data = self._broker._get_or_create_topic(topic)
        timestamp = int(time.time() * 1000)

        with topic_data.lock:
            offset = topic_data.next_offset
            topic_data.records.append((offset, value, key, timestamp))
            topic_data.next_offset += 1
            return offset

    # -------------------------------------------------------------------------
    # QueueConsumer Implementation
    # -------------------------------------------------------------------------

    async def fetch(
        self,
        topic: str,
        offset: Optional[int] = None,
        max_records: int = 100,
        timeout_ms: int = 1000,
        partition: int = 0,
    ) -> List[Record]:
        """Fetch records from the topic.

        Args:
            topic: Topic name.
            offset: Starting offset. If None, uses tracked position for this client.
            max_records: Maximum records to fetch.
            timeout_ms: Fetch timeout (not used in memory implementation).
            partition: Partition to read from.
        """
        topic_data = self._broker._get_topic(topic)
        if topic_data is None:
            return []

        # Use tracked position if offset not specified
        position_key = (topic, partition)
        if offset is None:
            offset = self._consumer_positions.get(position_key, 0)

        result = []
        with topic_data.lock:
            for rec_offset, value, key, timestamp in topic_data.records:
                if rec_offset < offset:
                    continue
                if len(result) >= max_records:
                    break
                result.append(
                    Record(
                        offset=rec_offset,
                        value=value,
                        key=key,
                        timestamp=timestamp,
                    )
                )

        # Update position for next fetch
        if result:
            self._consumer_positions[position_key] = result[-1].offset + 1

        return result

    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
        partition: int = 0,
    ) -> None:
        """Commit the consumer offset for a consumer group."""
        with self._broker._global_lock:
            self._broker._committed_offsets[(group, topic, partition)] = offset

    async def get_committed_offset(
        self,
        group: str,
        topic: str,
        partition: int = 0,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group."""
        with self._broker._global_lock:
            return self._broker._committed_offsets.get((group, topic, partition))

    async def get_latest_offset(
        self,
        topic: str,
        partition: int = 0,
    ) -> int:
        """Get the latest offset in the topic."""
        topic_data = self._broker._get_topic(topic)
        if topic_data is None:
            return 0

        with topic_data.lock:
            return topic_data.next_offset

    async def get_all_partition_offsets(self, topic: str) -> Dict[int, int]:
        """Get latest offsets for all partitions (memory only has partition 0)."""
        latest = await self.get_latest_offset(topic, partition=0)
        return {0: latest}

    async def truncate_before(self, topic: str, offset: int) -> int:
        """Truncate (garbage collect) records before the given offset.

        Returns:
            Number of records deleted.
        """
        topic_data = self._broker._get_topic(topic)
        if topic_data is None:
            return 0

        with topic_data.lock:
            original_count = len(topic_data.records)
            topic_data.records = [r for r in topic_data.records if r[0] >= offset]
            deleted = original_count - len(topic_data.records)
            return deleted

    async def get_min_committed_offset(self, topic: str) -> Optional[int]:
        """Get the minimum committed offset across all consumer groups.

        Returns:
            The minimum committed offset, or None if no offsets are committed.
        """
        min_offset = None
        with self._broker._global_lock:
            for (group, t, partition), offset in self._broker._committed_offsets.items():
                if t == topic:
                    if min_offset is None or offset < min_offset:
                        min_offset = offset
        return min_offset

    @property
    def is_persistent(self) -> bool:
        """Memory backend is not persistent."""
        return False

    # Convenience properties for backward compatibility
    @property
    def host(self) -> str:
        return "localhost"

    @property
    def port(self) -> int:
        return 0
