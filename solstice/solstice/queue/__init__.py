"""Queue backends for inter-stage communication.

This module provides abstractions for message queue backends used for
communication between pipeline stages.

Types:
- QueueType: Enum for queue backend types (MEMORY, TANSU)

Protocols (Interface Segregation):
- QueueProducer: For producing messages
- QueueConsumer: For consuming messages
- QueueAdmin: For topic management
- QueueBroker: For broker lifecycle management
- QueueClient: Combined Producer + Consumer + Admin

Implementations:
- MemoryBroker + MemoryClient: Fast in-memory queue
- TansuBrokerManager + TansuQueueClient: Kafka-compatible broker

Example:
    ```python
    from solstice.queue import QueueType, TansuBrokerManager, TansuQueueClient

    # On StageMaster - start broker
    broker = TansuBrokerManager(storage_url="memory://tansu/")
    await broker.start()

    # Create client
    client = TansuQueueClient(broker.get_broker_url())
    await client.start()

    await client.create_topic("my-topic")
    offset = await client.produce("my-topic", b"message data")
    records = await client.fetch("my-topic", offset=0)

    await client.stop()
    await broker.stop()
    ```
"""

from enum import Enum

from solstice.queue.backend import Record
from solstice.queue.protocols import (
    QueueProducer,
    QueueConsumer,
    QueueAdmin,
    QueueBroker,
    QueueClient,
)
from solstice.queue.memory import MemoryBroker, MemoryClient
from solstice.queue.tansu import TansuBrokerManager, TansuQueueClient


class QueueType(str, Enum):
    """Type of queue backend to use."""

    MEMORY = "memory"  # In-process only (for single-worker testing)
    TANSU = "tansu"  # Persistent broker (for production)


__all__ = [
    # Types
    "QueueType",
    "Record",
    # Protocols
    "QueueProducer",
    "QueueConsumer",
    "QueueAdmin",
    "QueueBroker",
    "QueueClient",
    # Memory implementations
    "MemoryBroker",
    "MemoryClient",
    # Tansu implementations
    "TansuBrokerManager",
    "TansuQueueClient",
]
