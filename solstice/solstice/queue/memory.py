"""In-memory queue backend for lightweight stages.

This backend provides a fast, non-persistent queue suitable for stages
where re-processing on failure is acceptable (e.g., simple filtering,
format conversion).

Features:
- O(1) produce operations
- Thread-safe for concurrent access
- Offset tracking for consumer groups
- Automatic garbage collection of consumed messages

Limitations:
- Data is lost on process restart
- Not suitable for expensive operations (GPU, API calls)
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from solstice.queue.backend import QueueBackend, Record


@dataclass
class PartitionData:
    """Internal data structure for a single partition."""

    records: List[Tuple[int, bytes, Optional[bytes], int]] = field(default_factory=list)
    next_offset: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class TopicData:
    """Internal data structure for a topic with multiple partitions."""

    partitions: List[PartitionData] = field(default_factory=list)
    num_partitions: int = 1
    global_lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        """Initialize partitions if not already set."""
        if not self.partitions:
            self.partitions = [PartitionData() for _ in range(self.num_partitions)]

    def get_partition(self, partition: int) -> PartitionData:
        """Get partition data, creating if necessary."""
        with self.global_lock:
            while len(self.partitions) <= partition:
                self.partitions.append(PartitionData())
            return self.partitions[partition]


class MemoryBackend(QueueBackend):
    """Fast in-memory queue backend.

    This backend stores all data in memory and is designed for high throughput
    with low latency. Data is NOT persisted across restarts.

    Use this backend for:
    - Lightweight stages (filter, format, simple transforms)
    - Testing and development
    - Scenarios where re-processing is acceptable

    Do NOT use this backend for:
    - Expensive operations (GPU inference, API calls)
    - Stages that require exactly-once guarantees

    Thread Safety:
        All operations are thread-safe and can be called concurrently
        from multiple workers.

    Example:
        ```python
        backend = MemoryBackend()
        await backend.start()

        await backend.create_topic("my-topic")

        # Produce
        offset = await backend.produce("my-topic", b"hello")

        # Fetch
        records = await backend.fetch("my-topic", offset=0)
        print(records[0].value)  # b"hello"

        await backend.stop()
        ```
    """

    def __init__(self, gc_interval_seconds: float = 60.0):
        """Initialize the memory backend.

        Args:
            gc_interval_seconds: Interval for automatic garbage collection.
        """
        self._topics: Dict[str, TopicData] = {}
        # Key format: (group, topic, partition) for per-partition offsets
        self._committed_offsets: Dict[Tuple[str, str, int], int] = {}
        self._global_lock = threading.Lock()
        self._gc_interval = gc_interval_seconds
        self._gc_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the memory backend."""
        self._running = True
        # Start background GC task
        self._gc_task = asyncio.create_task(self._gc_loop())

    async def stop(self) -> None:
        """Stop the memory backend."""
        self._running = False
        if self._gc_task:
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass

        # Clear all data
        with self._global_lock:
            self._topics.clear()
            self._committed_offsets.clear()

    async def _gc_loop(self) -> None:
        """Background task for garbage collection."""
        while self._running:
            await asyncio.sleep(self._gc_interval)
            self._gc_all_topics()

    def _gc_all_topics(self) -> None:
        """Garbage collect consumed records from all topics."""
        with self._global_lock:
            for topic_name, topic_data in self._topics.items():
                self._gc_topic(topic_name, topic_data)

    def _gc_topic(self, topic_name: str, topic_data: TopicData) -> None:
        """Garbage collect consumed records from a single topic."""
        # Process each partition separately
        for partition_idx, partition_data in enumerate(topic_data.partitions):
            # Find minimum committed offset for this partition across all consumer groups
            min_offset = None
            for (group, topic, partition), offset in self._committed_offsets.items():
                if topic == topic_name and partition == partition_idx:
                    if min_offset is None or offset < min_offset:
                        min_offset = offset

            if min_offset is None:
                continue  # No consumers have committed for this partition

            # Remove records with offset < min_offset
            with partition_data.lock:
                partition_data.records = [r for r in partition_data.records if r[0] >= min_offset]

    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic with the specified number of partitions."""
        with self._global_lock:
            if topic not in self._topics:
                self._topics[topic] = TopicData(
                    num_partitions=partitions,
                    partitions=[PartitionData() for _ in range(partitions)]
                )

    async def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        with self._global_lock:
            self._topics.pop(topic, None)
            # Remove committed offsets for this topic
            keys_to_remove = [k for k in self._committed_offsets if k[1] == topic]
            for key in keys_to_remove:
                del self._committed_offsets[key]

    def _compute_partition(self, topic: str, key: Optional[bytes]) -> int:
        """Compute the partition for a message based on key hash."""
        with self._global_lock:
            if topic not in self._topics:
                return 0
            topic_data = self._topics[topic]
            num_partitions = topic_data.num_partitions

        if key is None or num_partitions <= 1:
            return 0

        # Use hash of key to determine partition
        return hash(key) % num_partitions

    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Produce a message to the topic (round-robin to partitions)."""
        # Auto-create topic if needed
        with self._global_lock:
            if topic not in self._topics:
                self._topics[topic] = TopicData(num_partitions=1, partitions=[PartitionData()])
            topic_data = self._topics[topic]

        # Compute partition based on key
        partition = self._compute_partition(topic, key)
        partition_data = topic_data.get_partition(partition)

        timestamp = int(time.time() * 1000)

        with partition_data.lock:
            offset = partition_data.next_offset
            partition_data.records.append((offset, value, key, timestamp))
            partition_data.next_offset += 1
            return offset

    async def produce_batch(
        self,
        topic: str,
        values: List[bytes],
        keys: Optional[List[Optional[bytes]]] = None,
    ) -> List[int]:
        """Produce multiple messages to the topic."""
        if keys is not None and len(keys) != len(values):
            raise ValueError(f"keys length ({len(keys)}) must match values length ({len(values)})")

        if not values:
            return []

        # Auto-create topic if needed
        with self._global_lock:
            if topic not in self._topics:
                self._topics[topic] = TopicData(num_partitions=1, partitions=[PartitionData()])
            topic_data = self._topics[topic]

        timestamp = int(time.time() * 1000)
        offsets = []

        for i, value in enumerate(values):
            key = keys[i] if keys else None
            partition = self._compute_partition(topic, key)
            partition_data = topic_data.get_partition(partition)

            with partition_data.lock:
                offset = partition_data.next_offset
                partition_data.records.append((offset, value, key, timestamp))
                partition_data.next_offset += 1
                offsets.append(offset)

        return offsets

    async def fetch(
        self,
        topic: str,
        offset: int = 0,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[Record]:
        """Fetch records from partition 0 (for backward compatibility)."""
        return await self.fetch_from_partition(topic, 0, offset, max_records, timeout_ms)

    async def fetch_from_partition(
        self,
        topic: str,
        partition: int,
        offset: int = 0,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[Record]:
        """Fetch records from a specific partition."""
        with self._global_lock:
            if topic not in self._topics:
                return []
            topic_data = self._topics[topic]

        if partition >= len(topic_data.partitions):
            return []

        partition_data = topic_data.partitions[partition]
        result = []

        with partition_data.lock:
            for rec_offset, value, key, timestamp in partition_data.records:
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
                        partition=partition,
                    )
                )

        return result

    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
    ) -> None:
        """Commit the consumer offset for a consumer group (partition 0)."""
        await self.commit_partition_offset(group, topic, 0, offset)

    async def commit_partition_offset(
        self,
        group: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Commit the consumer offset for a specific partition."""
        with self._global_lock:
            self._committed_offsets[(group, topic, partition)] = offset

    async def get_committed_offset(
        self,
        group: str,
        topic: str,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group (partition 0)."""
        return await self.get_committed_partition_offset(group, topic, 0)

    async def get_committed_partition_offset(
        self,
        group: str,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """Get the committed offset for a specific partition."""
        with self._global_lock:
            return self._committed_offsets.get((group, topic, partition))

    async def get_latest_offset(self, topic: str) -> int:
        """Get the latest offset in partition 0."""
        return await self.get_partition_latest_offset(topic, 0)

    async def get_partition_latest_offset(self, topic: str, partition: int) -> int:
        """Get the latest offset for a specific partition."""
        with self._global_lock:
            if topic not in self._topics:
                return 0
            topic_data = self._topics[topic]

        if partition >= len(topic_data.partitions):
            return 0

        partition_data = topic_data.partitions[partition]
        with partition_data.lock:
            return partition_data.next_offset

    async def get_partition_count(self, topic: str) -> int:
        """Get the number of partitions for a topic."""
        with self._global_lock:
            if topic not in self._topics:
                return 1
            return self._topics[topic].num_partitions

    async def produce_to_partition(
        self,
        topic: str,
        partition: int,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Produce a message to a specific partition."""
        # Auto-create topic if needed
        with self._global_lock:
            if topic not in self._topics:
                self._topics[topic] = TopicData(num_partitions=max(1, partition + 1))
            topic_data = self._topics[topic]

        partition_data = topic_data.get_partition(partition)
        timestamp = int(time.time() * 1000)

        with partition_data.lock:
            offset = partition_data.next_offset
            partition_data.records.append((offset, value, key, timestamp))
            partition_data.next_offset += 1
            return offset

    @property
    def is_persistent(self) -> bool:
        """Memory backend does not persist data."""
        return False

    async def health_check(self) -> bool:
        """Check if the backend is healthy."""
        return self._running

    def get_stats(self) -> Dict:
        """Get statistics about the backend (for debugging)."""
        with self._global_lock:
            stats = {
                "topics": {},
                "committed_offsets": {
                    f"{g}:{t}:p{p}": o
                    for (g, t, p), o in self._committed_offsets.items()
                },
            }
            for topic_name, topic_data in self._topics.items():
                partitions_info = []
                total_records = 0
                for p_idx, p_data in enumerate(topic_data.partitions):
                    with p_data.lock:
                        record_count = len(p_data.records)
                        total_records += record_count
                        partitions_info.append({
                            "partition": p_idx,
                            "record_count": record_count,
                            "next_offset": p_data.next_offset,
                        })
                stats["topics"][topic_name] = {
                    "num_partitions": topic_data.num_partitions,
                    "total_records": total_records,
                    "partitions": partitions_info,
                }
            return stats

    async def truncate_before(self, topic: str, offset: int) -> int:
        """Truncate (garbage collect) records before the given offset in all partitions.

        Args:
            topic: Name of the topic.
            offset: Delete all records with offset < this value.

        Returns:
            Number of records deleted across all partitions.
        """
        with self._global_lock:
            if topic not in self._topics:
                return 0
            topic_data = self._topics[topic]

        total_deleted = 0
        for partition_data in topic_data.partitions:
            with partition_data.lock:
                original_count = len(partition_data.records)
                partition_data.records = [r for r in partition_data.records if r[0] >= offset]
                total_deleted += original_count - len(partition_data.records)
        return total_deleted

    async def get_min_committed_offset(self, topic: str) -> Optional[int]:
        """Get the minimum committed offset across all consumer groups and partitions.

        Args:
            topic: Name of the topic.

        Returns:
            The minimum committed offset, or None if no offsets are committed.
        """
        with self._global_lock:
            min_offset = None
            for (group, t, partition), offset in self._committed_offsets.items():
                if t == topic:
                    if min_offset is None or offset < min_offset:
                        min_offset = offset
            return min_offset
