"""Queue backends for inter-stage communication.

This module provides abstractions for message queue backends used for
communication between pipeline stages. The key abstraction is `QueueBackend`
which defines the interface for producing and consuming messages.

Available backends:
- MemoryBackend: Fast in-memory queue for lightweight stages
- TansuBackend: Persistent queue using Tansu broker subprocess

Example:
    ```python
    from solstice.queue import MemoryBackend, TansuBackend
    
    # For lightweight stages (no persistence)
    backend = MemoryBackend()
    await backend.start()
    
    # For expensive stages (with persistence)
    backend = TansuBackend(storage_url="s3://bucket/")
    await backend.start()
    
    # Produce messages
    offset = await backend.produce("my-topic", b"message data")
    
    # Consume messages
    records = await backend.fetch("my-topic", offset=0, max_records=100)
    
    # Commit offset (for exactly-once semantics)
    await backend.commit_offset("my-group", "my-topic", records[-1].offset + 1)
    ```
"""

from solstice.queue.backend import QueueBackend, Record, QueueConfig
from solstice.queue.memory import MemoryBackend
from solstice.queue.tansu import TansuBackend
from solstice.queue.ray_backend import RayBackend, QueueActor

__all__ = [
    "QueueBackend",
    "Record",
    "QueueConfig",
    "MemoryBackend",
    "TansuBackend",
    "RayBackend",
    "QueueActor",
]
