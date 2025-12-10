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

    def register(self, key: str, ref_wrapper: dict) -> str:
        """Register an ObjectRef (wrapped in dict to prevent auto-deref) with a key."""
        self._refs[key] = ref_wrapper["ref"]
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
            return True
        return False

    def clear(self) -> int:
        count = len(self._refs)
        self._refs.clear()
        self._logger.info(f"Cleared {count} payloads")
        return count


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

    def __init__(self, name: Optional[str] = None):
        """Initialize the store.

        Args:
            name: Optional name for the Ray actor (for debugging/discovery)
        """
        actor_options = {"name": name} if name else {}
        self._actor = _RaySplitPayloadStoreActor.options(**actor_options).remote()

    def store(self, key: str, payload: SplitPayload) -> str:
        # Put directly to object store with actor as owner
        # This avoids serializing payload twice (once to actor, once to object store)
        ref = ray.put(payload, _owner=self._actor)
        # Wrap ObjectRef in dict to prevent Ray from auto-dereferencing it
        return ray.get(self._actor.register.remote(key, {"ref": ref}))

    def get(self, key: str) -> Optional[SplitPayload]:
        ref_wrapper = ray.get(self._actor.get_ref.remote(key))
        if ref_wrapper is None:
            return None
        return ray.get(ref_wrapper["ref"])

    def delete(self, key: str) -> bool:
        return ray.get(self._actor.delete.remote(key))

    def clear(self) -> int:
        return ray.get(self._actor.clear.remote())
