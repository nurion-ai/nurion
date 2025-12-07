"""Ray-based shared queue backend for testing and single-node deployments.

This backend uses a Ray actor to provide a shared queue that can be accessed
from multiple Ray workers. Unlike MemoryBackend, the state is shared across
all workers through the Ray actor.

Use cases:
- Testing without external dependencies (no Tansu needed)
- Single-node deployments
- Development and debugging

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                      Ray Cluster                            │
    │                                                             │
    │  ┌─────────────────────────────────────────────────────┐   │
    │  │           QueueActor (Ray Actor)                     │   │
    │  │  - Holds all queue data in memory                    │   │
    │  │  - Serializable reference can be passed to workers   │   │
    │  │  - Thread-safe (Ray handles concurrency)             │   │
    │  └─────────────────────────────────────────────────────┘   │
    │                           ▲                                 │
    │                           │ Ray remote calls                │
    │  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
    │  │  Master    │  │  Worker 1  │  │  Worker 2  │            │
    │  │ (creates)  │  │ (uses ref) │  │ (uses ref) │            │
    │  └────────────┘  └────────────┘  └────────────┘            │
    └─────────────────────────────────────────────────────────────┘
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import ray

from solstice.queue.backend import QueueBackend, Record


@ray.remote
class QueueActor:
    """Ray actor that holds queue state.
    
    This actor is the single source of truth for queue data.
    All operations are serialized through Ray's actor model.
    """
    
    def __init__(self):
        # topic -> list of (offset, value, key, timestamp)
        self._topics: Dict[str, List[Tuple[int, bytes, Optional[bytes], int]]] = {}
        self._next_offsets: Dict[str, int] = {}
        # (group, topic) -> committed offset
        self._committed_offsets: Dict[Tuple[str, str], int] = {}
    
    def create_topic(self, topic: str) -> None:
        """Create a topic."""
        if topic not in self._topics:
            self._topics[topic] = []
            self._next_offsets[topic] = 0
    
    def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        self._topics.pop(topic, None)
        self._next_offsets.pop(topic, None)
        # Remove committed offsets for this topic
        keys_to_remove = [k for k in self._committed_offsets if k[1] == topic]
        for key in keys_to_remove:
            del self._committed_offsets[key]
    
    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Produce a message."""
        if topic not in self._topics:
            self.create_topic(topic)
        
        offset = self._next_offsets[topic]
        timestamp = int(time.time() * 1000)
        self._topics[topic].append((offset, value, key, timestamp))
        self._next_offsets[topic] = offset + 1
        return offset
    
    def produce_batch(
        self,
        topic: str,
        values: List[bytes],
        keys: Optional[List[Optional[bytes]]] = None,
    ) -> List[int]:
        """Produce multiple messages."""
        if topic not in self._topics:
            self.create_topic(topic)
        
        offsets = []
        timestamp = int(time.time() * 1000)
        
        for i, value in enumerate(values):
            key = keys[i] if keys else None
            offset = self._next_offsets[topic]
            self._topics[topic].append((offset, value, key, timestamp))
            self._next_offsets[topic] = offset + 1
            offsets.append(offset)
        
        return offsets
    
    def fetch(
        self,
        topic: str,
        offset: int = 0,
        max_records: int = 100,
    ) -> List[Tuple[int, bytes, Optional[bytes], int]]:
        """Fetch records from topic."""
        if topic not in self._topics:
            return []
        
        result = []
        for rec_offset, value, key, timestamp in self._topics[topic]:
            if rec_offset < offset:
                continue
            if len(result) >= max_records:
                break
            result.append((rec_offset, value, key, timestamp))
        
        return result
    
    def commit_offset(self, group: str, topic: str, offset: int) -> None:
        """Commit consumer offset."""
        self._committed_offsets[(group, topic)] = offset
    
    def get_committed_offset(self, group: str, topic: str) -> Optional[int]:
        """Get committed offset."""
        return self._committed_offsets.get((group, topic))
    
    def get_latest_offset(self, topic: str) -> int:
        """Get latest offset."""
        return self._next_offsets.get(topic, 0)
    
    def get_stats(self) -> Dict:
        """Get statistics."""
        return {
            "topics": {
                topic: {
                    "record_count": len(records),
                    "next_offset": self._next_offsets.get(topic, 0),
                }
                for topic, records in self._topics.items()
            },
            "committed_offsets": dict(self._committed_offsets),
        }


class RayBackend(QueueBackend):
    """Queue backend using a Ray actor for shared state.
    
    This backend creates a Ray actor to hold queue state, allowing
    multiple workers to share the same queue through Ray remote calls.
    
    The actor reference can be serialized and passed to workers,
    making this suitable for distributed testing without Tansu.
    
    Example:
        ```python
        # Master creates backend
        backend = RayBackend()
        await backend.start()
        
        # Get actor reference (can be serialized)
        actor_ref = backend.get_actor_ref()
        
        # Worker creates backend from actor reference
        worker_backend = RayBackend.from_actor_ref(actor_ref)
        await worker_backend.start()
        
        # Both can now access the same queue
        ```
    """
    
    def __init__(self, actor_ref: Optional[ray.actor.ActorHandle] = None):
        """Initialize the backend.
        
        Args:
            actor_ref: Optional existing actor reference. If None, a new
                      actor will be created when start() is called.
        """
        self._actor: Optional[ray.actor.ActorHandle] = actor_ref
        self._owns_actor = actor_ref is None
        self._running = False
    
    @classmethod
    def from_actor_ref(cls, actor_ref: ray.actor.ActorHandle) -> "RayBackend":
        """Create a backend from an existing actor reference.
        
        Use this in workers to connect to a shared queue created by master.
        """
        return cls(actor_ref=actor_ref)
    
    def get_actor_ref(self) -> ray.actor.ActorHandle:
        """Get the actor reference (can be passed to workers)."""
        return self._actor
    
    async def start(self) -> None:
        """Start the backend."""
        if self._running:
            return
        
        if self._actor is None:
            # Create a new actor
            self._actor = QueueActor.options(
                name=f"queue_actor_{id(self)}",
                lifetime="detached",
            ).remote()
            self._owns_actor = True
        
        self._running = True
    
    async def stop(self) -> None:
        """Stop the backend."""
        self._running = False
        
        # Only kill actor if we created it
        if self._owns_actor and self._actor:
            try:
                ray.kill(self._actor)
            except Exception:
                pass
            self._actor = None
    
    async def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a topic."""
        ray.get(self._actor.create_topic.remote(topic))
    
    async def delete_topic(self, topic: str) -> None:
        """Delete a topic."""
        ray.get(self._actor.delete_topic.remote(topic))
    
    async def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Produce a message."""
        return ray.get(self._actor.produce.remote(topic, value, key))
    
    async def produce_batch(
        self,
        topic: str,
        values: List[bytes],
        keys: Optional[List[Optional[bytes]]] = None,
    ) -> List[int]:
        """Produce multiple messages."""
        if keys is not None and len(keys) != len(values):
            raise ValueError(f"keys length ({len(keys)}) must match values length ({len(values)})")
        return ray.get(self._actor.produce_batch.remote(topic, values, keys))
    
    async def fetch(
        self,
        topic: str,
        offset: int = 0,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> List[Record]:
        """Fetch records from topic."""
        raw_records = ray.get(self._actor.fetch.remote(topic, offset, max_records))
        return [
            Record(offset=r[0], value=r[1], key=r[2], timestamp=r[3])
            for r in raw_records
        ]
    
    async def commit_offset(
        self,
        group: str,
        topic: str,
        offset: int,
    ) -> None:
        """Commit consumer offset."""
        ray.get(self._actor.commit_offset.remote(group, topic, offset))
    
    async def get_committed_offset(
        self,
        group: str,
        topic: str,
    ) -> Optional[int]:
        """Get committed offset."""
        return ray.get(self._actor.get_committed_offset.remote(group, topic))
    
    async def get_latest_offset(self, topic: str) -> int:
        """Get latest offset."""
        return ray.get(self._actor.get_latest_offset.remote(topic))
    
    @property
    def is_persistent(self) -> bool:
        """Ray backend is not persistent (in-memory)."""
        return False
    
    async def health_check(self) -> bool:
        """Check if backend is healthy."""
        if not self._running or not self._actor:
            return False
        try:
            ray.get(self._actor.get_stats.remote(), timeout=5)
            return True
        except Exception:
            return False
    
    def get_stats(self) -> Dict:
        """Get backend statistics."""
        if not self._actor:
            return {}
        return ray.get(self._actor.get_stats.remote())



