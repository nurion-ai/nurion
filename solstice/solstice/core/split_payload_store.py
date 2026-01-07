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

"""SplitPayloadStore - Abstract interface for storing SplitPayload data.

This module provides a flexible storage abstraction for SplitPayload objects.
Different implementations can use various backends:
- Ray Object Store (default, for distributed in-memory storage)
- S3/GCS (for persistent storage)
- Redis (for shared caching)
- etc.

Usage:
    # Create a Ray-backed store
    store = RaySplitPayloadStore(name="my_store")

    # Store payload (synchronous API - same across all implementations)
    store.store("key1", payload)

    # Retrieve payload
    payload = store.get("key1")

    # Delete when done
    store.delete("key1")

    # Clear all
    store.clear()
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import ray

from solstice.core.models import SplitPayload
from solstice.utils.logging import create_ray_logger


class SplitPayloadStore(ABC):
    """Abstract base class for SplitPayload storage backends.

    All implementations provide a synchronous interface for simplicity.
    The underlying implementation may use async/actors internally.
    """

    @abstractmethod
    def store(self, key: str, payload: SplitPayload) -> str:
        """Store a SplitPayload with the given key.

        Args:
            key: Unique identifier for this payload
            payload: The SplitPayload to store

        Returns:
            The key (for confirmation/chaining)
        """
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[SplitPayload]:
        """Retrieve a SplitPayload by key.

        Args:
            key: The key used when storing

        Returns:
            The SplitPayload, or None if not found
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a stored payload.

        Args:
            key: The key to delete

        Returns:
            True if deleted, False if key not found
        """
        pass

    @abstractmethod
    def clear(self) -> int:
        """Clear all stored payloads.

        Returns:
            Number of payloads cleared
        """
        pass


# =============================================================================
# Ray Object Store Implementation
# =============================================================================


@ray.remote
class _RaySplitPayloadStoreActor:
    """Internal Ray actor that manages ObjectRef mappings.

    This actor stores key -> ObjectRef mappings. The actual objects are put
    by callers with _owner=actor to prevent GC when original workers exit.
    """

    def __init__(self):
        self._refs: dict[str, ray.ObjectRef] = {}
        self._logger = create_ray_logger("RaySplitPayloadStoreActor")

        # Metrics tracking
        self._total_stored = 0
        self._total_deleted = 0
        self._estimated_bytes = 0

    def register(self, key: str, ref_wrapper: dict) -> str:
        """Register an ObjectRef (wrapped in dict to prevent auto-deref) with a key."""
        self._refs[key] = ref_wrapper["ref"]
        self._total_stored += 1
        self._logger.debug(f"Registered payload for key {key}")
        return key

    def get_ref(self, key: str) -> Optional[dict]:
        """Get the ObjectRef (wrapped in dict) for a key."""
        ref = self._refs.get(key)
        if ref is None:
            return None
        return {"ref": ref}

    def delete(self, key: str) -> bool:
        if key in self._refs:
            del self._refs[key]
            self._total_deleted += 1
            return True
        return False

    def clear(self) -> int:
        count = len(self._refs)
        self._refs.clear()
        self._logger.info(f"Cleared {count} payloads")
        return count

    def get_metrics(self) -> dict:
        """Get storage metrics.

        Returns:
            Dictionary with storage statistics
        """
        return {
            "total_objects": len(self._refs),
            "total_stored": self._total_stored,
            "total_deleted": self._total_deleted,
            "estimated_bytes": self._estimated_bytes,
        }


class RaySplitPayloadStore(SplitPayloadStore):
    """Ray Object Store backed implementation of SplitPayloadStore.

    This class wraps an internal Ray actor that stores SplitPayload objects
    in Ray's distributed object store.

    The interface is synchronous - all Ray actor calls are wrapped with ray.get()
    to provide a consistent API across different storage backends.

    Usage:
        store = RaySplitPayloadStore(name="my_store")

        store.store("key", payload)
        payload = store.get("key")
        store.delete("key")
        store.clear()
    """

    def __init__(self, name: str):
        """Initialize the store.

        Args:
            name: Name for the Ray actor (required for discovery and debugging)
        """
        if not name:
            raise ValueError("RaySplitPayloadStore requires a non-empty name")
        self._actor_name = name
        self._actor = _RaySplitPayloadStoreActor.options(name=name).remote()

    @property
    def actor_name(self) -> str:
        """Get the actor name."""
        return self._actor_name

    def store(self, key: str, payload: SplitPayload) -> str:
        # Put directly to object store with actor as owner
        # This avoids serializing payload twice (once to actor, once to object store)
        ref = ray.put(payload, _owner=self._actor)
        # Wrap ObjectRef in dict to prevent Ray from auto-dereferencing it
        return ray.get(self._actor.register.remote(key, {"ref": ref}))

    # Prefix for JVM-written Arrow data keys (embedded directly in payload_key)
    JVM_ARROW_PREFIX = "_jvm_arrow:"

    def get(self, key: str) -> Optional[SplitPayload]:
        # Check for JVM direct Arrow data key
        # Format: _jvm_arrow:{base64_encoded_arrow_ipc}
        if key.startswith(self.JVM_ARROW_PREFIX):
            return self._get_from_arrow_data(key)

        # Standard path: lookup from actor's registered refs
        ref_wrapper = ray.get(self._actor.get_ref.remote(key))
        if ref_wrapper is None:
            return None

        data = ray.get(ref_wrapper["ref"])
        return self._convert_to_payload(data, split_id=key)

    def _get_from_arrow_data(self, key: str) -> Optional[SplitPayload]:
        """Extract Arrow data directly from key.

        This is used when JVM writes directly to queue with payload_key
        containing the base64-encoded Arrow IPC bytes.

        This approach embeds data directly in the message, avoiding
        ObjectRef serialization issues between JVM and Python.
        """
        import base64

        # Extract base64-encoded Arrow IPC data
        arrow_b64 = key[len(self.JVM_ARROW_PREFIX) :]
        arrow_bytes = base64.b64decode(arrow_b64)

        return self._convert_to_payload(arrow_bytes, split_id=key)

    def _convert_to_payload(self, data, split_id: str) -> SplitPayload:
        """Convert various data types to SplitPayload."""
        # Already a SplitPayload (from Python writers)
        if isinstance(data, SplitPayload):
            return data

        # Arrow IPC bytes (from JVM writers)
        if isinstance(data, bytes):
            import pyarrow.ipc as ipc
            import io

            table = ipc.open_stream(io.BytesIO(data)).read_all()
            return SplitPayload.from_arrow(table, split_id=split_id)

        # Arrow Table (direct)
        import pyarrow as pa

        if isinstance(data, pa.Table):
            return SplitPayload.from_arrow(data, split_id=split_id)

        raise ValueError(f"Unsupported data type in store: {type(data)}")

    def delete(self, key: str) -> bool:
        return ray.get(self._actor.delete.remote(key))

    def clear(self) -> int:
        return ray.get(self._actor.clear.remote())

    def get_metrics(self) -> dict:
        """Get storage metrics for monitoring.

        Returns:
            Dictionary with:
            - total_objects: Current number of stored objects
            - total_stored: Lifetime count of stored objects
            - total_deleted: Lifetime count of deleted objects
            - estimated_bytes: Estimated storage size (placeholder)
        """
        return ray.get(self._actor.get_metrics.remote())
