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

"""Protocols for partition-scoped state storage.

This module defines the interfaces for state storage used by stateful operators.
The key design principle is that state is scoped to partitions, not workers,
enabling elastic scaling without state migration.

Note: Interface is synchronous because SlateDB is an embedded database with
synchronous API. No need for async wrappers.
"""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class PartitionStateStore(Protocol):
    """Protocol for partition-scoped state storage.

    This interface defines how stateful operators interact with
    persistent state. Key design principles:

    1. **Partition-scoped**: State is keyed by (partition_id, key)
    2. **Worker-agnostic**: Any worker can access any partition's state
    3. **Fencing**: Only one writer per partition at a time

    Note: All methods are synchronous (SlateDB is an embedded DB).
    """

    def acquire_partition(self, partition_id: int) -> bool:
        """Acquire write access to a partition.

        Must be called before writing to a partition.

        Args:
            partition_id: The partition to acquire

        Returns:
            True if acquisition succeeded
        """
        ...

    def release_partition(self, partition_id: int) -> None:
        """Release write access to a partition.

        Args:
            partition_id: The partition to release
        """
        ...

    def get(self, partition_id: int, key: bytes) -> Optional[bytes]:
        """Get a value from partition state.

        Args:
            partition_id: The partition containing the key
            key: The key to look up

        Returns:
            The value if found, None otherwise
        """
        ...

    def put(self, partition_id: int, key: bytes, value: bytes) -> None:
        """Put a value into partition state.

        Args:
            partition_id: The partition to write to
            key: The key to write
            value: The value to write
        """
        ...

    def close(self) -> None:
        """Close the state store and release all resources."""
        ...
