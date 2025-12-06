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
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from solstice.queue.backend import QueueBackend, Record


@dataclass
class TopicData:
    """Internal data structure for a topic."""
    records: List[Tuple[int, bytes, Optional[bytes], int]] = field(default_factory=list)
    next_offset: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


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
        self._committed_offsets: Dict[Tuple[str, str], int] = {}  # (group, topic) -> offset
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
        # Find minimum committed offset across all consumer groups
        min_offset = None
        for (group, topic), offset in self._committed_offsets.items():
            if topic == topic_name:
                if min_offset is None or offset < min_offset:
                    min_offset = offset
        
        if min_offset is None:
            return  # No consumers have committed
        
        # Remove records with offset < min_offset
        with topic_data.lock:
            topic_data.records = [
                r for r in topic_data.records if r[0] >= min_offset
            ]
    
    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic (no-op if exists)."""
        with self._global_lock:
            if topic not in self._topics:
                self._topics[topic] = TopicData()
    
    async def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        with self._global_lock:
            self._topics.pop(topic, None)
            # Remove committed offsets for this topic
            keys_to_remove = [k for k in self._committed_offsets if k[1] == topic]
            for key in keys_to_remove:
                del self._committed_offsets[key]
    
    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Produce a message to the topic."""
        # Auto-create topic if needed
        with self._global_lock:
            if topic not in self._topics:
                self._topics[topic] = TopicData()
            topic_data = self._topics[topic]
        
        timestamp = int(time.time() * 1000)
        
        with topic_data.lock:
            offset = topic_data.next_offset
            topic_data.records.append((offset, value, key, timestamp))
            topic_data.next_offset += 1
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
                self._topics[topic] = TopicData()
            topic_data = self._topics[topic]
        
        timestamp = int(time.time() * 1000)
        offsets = []
        
        with topic_data.lock:
            for i, value in enumerate(values):
                key = keys[i] if keys else None
                offset = topic_data.next_offset
                topic_data.records.append((offset, value, key, timestamp))
                topic_data.next_offset += 1
                offsets.append(offset)
        
        return offsets
    
    async def fetch(
        self,
        topic: str,
        offset: int = 0,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[Record]:
        """Fetch records from the topic starting at the given offset."""
        with self._global_lock:
            if topic not in self._topics:
                return []
            topic_data = self._topics[topic]
        
        result = []
        
        with topic_data.lock:
            for rec_offset, value, key, timestamp in topic_data.records:
                if rec_offset < offset:
                    continue
                if len(result) >= max_records:
                    break
                result.append(Record(
                    offset=rec_offset,
                    value=value,
                    key=key,
                    timestamp=timestamp,
                ))
        
        return result
    
    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
    ) -> None:
        """Commit the consumer offset for a consumer group."""
        with self._global_lock:
            self._committed_offsets[(group, topic)] = offset
    
    async def get_committed_offset(
        self,
        group: str,
        topic: str,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group."""
        with self._global_lock:
            return self._committed_offsets.get((group, topic))
    
    async def get_latest_offset(self, topic: str) -> int:
        """Get the latest offset in the topic."""
        with self._global_lock:
            if topic not in self._topics:
                return 0
            topic_data = self._topics[topic]
        
        with topic_data.lock:
            return topic_data.next_offset
    
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
                "committed_offsets": dict(self._committed_offsets),
            }
            for topic_name, topic_data in self._topics.items():
                with topic_data.lock:
                    stats["topics"][topic_name] = {
                        "record_count": len(topic_data.records),
                        "next_offset": topic_data.next_offset,
                    }
            return stats
