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
Queue protocols based on Interface Segregation Principle.

This module defines small, focused protocols for queue operations:
- QueueProducer: For producing messages
- QueueConsumer: For consuming messages
- QueueAdmin: For topic management
- QueueBroker: For broker lifecycle management

Classes can implement only the protocols they need.
"""

from typing import List, Optional, Protocol, runtime_checkable

from solstice.queue.backend import Record


# =============================================================================
# Producer Protocol
# =============================================================================


@runtime_checkable
class QueueProducer(Protocol):
    """Protocol for message production."""

    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
    ) -> int:
        """Produce a message to the topic.

        Args:
            topic: Name of the topic.
            value: Message payload as bytes.
            key: Optional key for partitioning.
            partition: Optional specific partition.

        Returns:
            The offset of the produced message.
        """
        ...


# =============================================================================
# Consumer Protocol
# =============================================================================


@runtime_checkable
class QueueConsumer(Protocol):
    """Protocol for message consumption."""

    async def fetch(
        self,
        topic: str,
        offset: Optional[int] = None,
        max_records: int = 100,
        timeout_ms: int = 5000,
        partition: int = 0,
    ) -> List[Record]:
        """Fetch records from the topic.

        Args:
            topic: Name of the topic.
            offset: Starting offset (inclusive). If None, use current consumer position.
            max_records: Maximum number of records to fetch.
            timeout_ms: Timeout in milliseconds.
            partition: Partition to read from.

        Returns:
            List of records.
        """
        ...

    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
        partition: int = 0,
    ) -> None:
        """Commit the consumer offset for a consumer group.

        Args:
            group: Consumer group ID.
            topic: Name of the topic.
            offset: Offset to commit (next offset to consume).
            partition: Partition to commit.
        """
        ...

    async def get_committed_offset(
        self,
        group: str,
        topic: str,
        partition: int = 0,
    ) -> Optional[int]:
        """Get the committed offset for a consumer group.

        Args:
            group: Consumer group ID.
            topic: Name of the topic.
            partition: Partition.

        Returns:
            The committed offset, or None if not committed.
        """
        ...

    async def get_latest_offset(
        self,
        topic: str,
        partition: int = 0,
    ) -> int:
        """Get the latest offset in the topic.

        Args:
            topic: Name of the topic.
            partition: Partition.

        Returns:
            The next offset that will be assigned.
        """
        ...


# =============================================================================
# Admin Protocol
# =============================================================================


@runtime_checkable
class QueueAdmin(Protocol):
    """Protocol for topic administration."""

    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic.

        Args:
            topic: Name of the topic.
            partitions: Number of partitions.
        """
        ...

    async def delete_topic(self, topic: str) -> None:
        """Delete a topic.

        Args:
            topic: Name of the topic.
        """
        ...

    async def health_check(self) -> bool:
        """Check if the backend is healthy.

        Returns:
            True if healthy.
        """
        ...


# =============================================================================
# Broker Protocol
# =============================================================================


@runtime_checkable
class QueueBroker(Protocol):
    """Protocol for broker lifecycle management."""

    async def start(self) -> None:
        """Start the broker."""
        ...

    async def stop(self) -> None:
        """Stop the broker."""
        ...

    def get_broker_url(self) -> str:
        """Get the broker URL for clients to connect.

        Returns:
            Broker URL in format "host:port".
        """
        ...

    def is_running(self) -> bool:
        """Check if broker is running.

        Returns:
            True if running.
        """
        ...


# =============================================================================
# Combined Protocol for convenience
# =============================================================================


@runtime_checkable
class QueueClient(QueueProducer, QueueConsumer, QueueAdmin, Protocol):
    """Combined protocol for a full-featured queue client.

    Implements Producer + Consumer + Admin capabilities.
    """

    async def start(self) -> None:
        """Start the client."""
        ...

    async def stop(self) -> None:
        """Stop the client."""
        ...
