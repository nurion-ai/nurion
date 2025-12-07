"""Abstract interface for queue backends.

This module defines the contract that all queue backends must implement.
The interface is designed to support:
- Exactly-once semantics via offset tracking
- Batch operations for performance
- Multiple backend implementations (memory, Tansu, etc.)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
import time


@dataclass
class Record:
    """A record fetched from the queue.
    
    Attributes:
        offset: Monotonically increasing sequence number assigned by the queue.
                This is the primary identifier for exactly-once semantics.
        key: Optional key for partitioning (not used in single-partition mode).
        value: The message payload as bytes.
        timestamp: Unix timestamp in milliseconds when the record was produced.
    """
    offset: int
    value: bytes
    key: Optional[bytes] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    
    def __repr__(self) -> str:
        value_preview = self.value[:50] if len(self.value) <= 50 else self.value[:50] + b"..."
        return f"Record(offset={self.offset}, value={value_preview!r})"


@dataclass
class QueueConfig:
    """Configuration for queue backends.
    
    Attributes:
        backend_type: Type of backend ("memory", "tansu").
        storage_url: Storage URL for persistent backends (e.g., "s3://bucket/").
        port: Port for Tansu broker (default: 9092).
        batch_size: Default batch size for fetch operations.
        fetch_timeout_ms: Timeout for fetch operations in milliseconds.
    """
    backend_type: str = "memory"
    storage_url: str = "memory://"
    port: int = 9092
    batch_size: int = 100
    fetch_timeout_ms: int = 1000


class QueueBackend(ABC):
    """Abstract base class for queue backends.
    
    All queue backends must implement this interface. The interface is designed
    to support exactly-once semantics through offset tracking.
    
    Lifecycle:
        1. Create backend instance
        2. Call start() to initialize
        3. Use produce/fetch/commit operations
        4. Call stop() to cleanup
    
    Thread Safety:
        Implementations should be thread-safe for concurrent produce/fetch
        operations from multiple workers.
    
    Example:
        ```python
        backend = SomeBackend(config)
        await backend.start()
        try:
            # Create topic
            await backend.create_topic("my-topic")
            
            # Produce
            offset = await backend.produce("my-topic", b"data")
            
            # Fetch
            records = await backend.fetch("my-topic", offset=0)
            
            # Commit
            await backend.commit_offset("group", "my-topic", records[-1].offset + 1)
        finally:
            await backend.stop()
        ```
    """
    
    @abstractmethod
    async def start(self) -> None:
        """Start the queue backend.
        
        This method should initialize any resources needed by the backend,
        such as starting subprocess, establishing connections, etc.
        
        Raises:
            RuntimeError: If the backend fails to start.
        """
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the queue backend.
        
        This method should clean up all resources, close connections,
        and terminate any subprocesses.
        """
        pass
    
    @abstractmethod
    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic.
        
        Args:
            topic: Name of the topic to create.
            partitions: Number of partitions (default: 1).
        
        Raises:
            RuntimeError: If topic creation fails.
        
        Note:
            If the topic already exists, this should be a no-op.
        """
        pass
    
    @abstractmethod
    async def delete_topic(self, topic: str) -> None:
        """Delete a topic.
        
        Args:
            topic: Name of the topic to delete.
        
        Raises:
            RuntimeError: If topic deletion fails.
        
        Note:
            If the topic doesn't exist, this should be a no-op.
        """
        pass
    
    @abstractmethod
    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Produce a message to the topic.
        
        Args:
            topic: Name of the topic.
            value: Message payload as bytes.
            key: Optional key for partitioning.
        
        Returns:
            The offset of the produced message.
        
        Raises:
            RuntimeError: If produce fails.
        
        Note:
            The returned offset is monotonically increasing and can be used
            to track progress for exactly-once semantics.
        """
        pass
    
    @abstractmethod
    async def produce_batch(
        self,
        topic: str,
        values: List[bytes],
        keys: Optional[List[Optional[bytes]]] = None,
    ) -> List[int]:
        """Produce multiple messages to the topic.
        
        Args:
            topic: Name of the topic.
            values: List of message payloads.
            keys: Optional list of keys (must match length of values if provided).
        
        Returns:
            List of offsets for the produced messages.
        
        Raises:
            RuntimeError: If produce fails.
            ValueError: If keys length doesn't match values length.
        """
        pass
    
    @abstractmethod
    async def fetch(
        self,
        topic: str,
        offset: int = 0,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[Record]:
        """Fetch records from the topic starting at the given offset.
        
        Args:
            topic: Name of the topic.
            offset: Starting offset (inclusive).
            max_records: Maximum number of records to fetch.
            timeout_ms: Timeout in milliseconds.
        
        Returns:
            List of records. Empty list if no records available.
        
        Raises:
            RuntimeError: If fetch fails.
        
        Note:
            Records are returned in offset order. The next offset to fetch
            is `records[-1].offset + 1`.
        """
        pass
    
    @abstractmethod
    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
    ) -> None:
        """Commit the consumer offset for a consumer group.
        
        Args:
            group: Consumer group ID.
            topic: Name of the topic.
            offset: Offset to commit (next offset to consume).
        
        Raises:
            RuntimeError: If commit fails.
        
        Note:
            The committed offset represents the NEXT offset to consume,
            not the last consumed offset. So after processing record with
            offset N, commit N+1.
        """
        pass
    
    @abstractmethod
    async def get_committed_offset(
        self,
        group: str,
        topic: str,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group.
        
        Args:
            group: Consumer group ID.
            topic: Name of the topic.
        
        Returns:
            The committed offset, or None if no offset has been committed.
        
        Raises:
            RuntimeError: If the operation fails.
        """
        pass
    
    @abstractmethod
    async def get_latest_offset(self, topic: str) -> int:
        """Get the latest offset in the topic.
        
        Args:
            topic: Name of the topic.
        
        Returns:
            The next offset that will be assigned to a new message.
            This is one greater than the offset of the last message.
        
        Raises:
            RuntimeError: If the operation fails.
        """
        pass
    
    @property
    @abstractmethod
    def is_persistent(self) -> bool:
        """Whether this backend persists data across restarts.
        
        Returns:
            True if data survives backend restart, False otherwise.
        """
        pass
    
    async def health_check(self) -> bool:
        """Check if the backend is healthy.
        
        Returns:
            True if the backend is operational, False otherwise.
        
        Note:
            Default implementation returns True. Backends can override
            for more sophisticated health checks.
        """
        return True
    
    async def truncate_before(self, topic: str, offset: int) -> int:
        """Truncate (garbage collect) records before the given offset.
        
        This is useful for cleaning up old messages that have been processed
        by all consumers. The offset should typically be the minimum committed
        offset across all consumer groups.
        
        Args:
            topic: Name of the topic.
            offset: Delete all records with offset < this value.
        
        Returns:
            Number of records deleted.
        
        Note:
            Default implementation is a no-op. Backends that support GC
            should override this method.
        """
        return 0
    
    async def get_min_committed_offset(self, topic: str) -> Optional[int]:
        """Get the minimum committed offset across all consumer groups.
        
        This is useful for determining which messages can be safely garbage
        collected (all messages before this offset have been processed).
        
        Args:
            topic: Name of the topic.
        
        Returns:
            The minimum committed offset, or None if no offsets are committed.
        
        Note:
            Default implementation returns None. Backends that track multiple
            consumer groups should override this method.
        """
        return None