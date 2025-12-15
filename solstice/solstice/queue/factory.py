"""Factory helpers to create queue backends without leaking concrete types."""

from typing import Any

from solstice.queue.memory import MemoryBackend
from solstice.queue.tansu import TansuBackend
from solstice.queue.backend import QueueBackend


def _queue_type_value(queue_type: Any) -> str:
    """Normalize queue_type which may be Enum or str."""
    if hasattr(queue_type, "value"):
        return str(queue_type.value)
    return str(queue_type)


def create_queue_backend(
    queue_type: Any,
    storage_url: str | None = None,
    port: int | None = None,
    client_only: bool = False,
) -> QueueBackend:
    """Create a queue backend based on queue_type.

    Args:
        queue_type: Enum or string indicating backend type ("tansu" or "memory").
        storage_url: Storage url (used by persistent backends).
        port: Port for network backends (None lets backend auto-select).
        client_only: For network backends, do not start server, only connect.

    Returns:
        QueueBackend instance (not started).
    """
    qt = _queue_type_value(queue_type).lower()
    if qt == "tansu":
        return TansuBackend(storage_url=storage_url or "memory://", port=port, client_only=client_only)
    # default to memory
    return MemoryBackend()
