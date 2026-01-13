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

"""SlateDB-backed partition state store.

This module implements partition-scoped state storage using SlateDB,
an S3-native embedded key-value store. Key features:

- **Per-partition isolation**: Each partition has its own SlateDB instance
- **Built-in fencing**: SlateDB detects and rejects stale writers

Storage layout:
    {base_path}/{job_id}/{stage_id}/partition_{id}/

Fencing mechanism:
    SlateDB uses manifest versioning for fencing. When a new writer opens
    and flushes, it updates the manifest. If an old writer tries to write
    after this, it gets a ClosedError with "detected newer DB client".

Note: All methods are synchronous - SlateDB is an embedded database.
"""

from pathlib import Path
from typing import Dict, Optional

from slatedb import ClosedError, SlateDB

from solstice.state.protocols import PartitionStateStore
from solstice.utils.logging import create_ray_logger


class SlateDBPartitionStateStore(PartitionStateStore):
    """SlateDB-backed partition state store.

    Simple implementation that directly reads/writes to SlateDB.
    All methods are synchronous.

    Usage:
        store = SlateDBPartitionStateStore(
            base_path="s3://bucket/state/",
            job_id="my_job",
            stage_id="groupby",
        )

        # Acquire partition before writing
        store.acquire_partition(0)

        # Read/write state
        store.put(0, b"user_123", b"state_data")
        value = store.get(0, b"user_123")

        # Release when done
        store.release_partition(0)
    """

    def __init__(
        self,
        base_path: str,
        job_id: str,
        stage_id: str,
    ):
        """Initialize the state store.

        Args:
            base_path: Base storage path (local or S3)
            job_id: Job identifier
            stage_id: Stage identifier
        """
        self.base_path = base_path.rstrip("/")
        self.job_id = job_id
        self.stage_id = stage_id

        self.logger = create_ray_logger(f"StateStore-{stage_id}")

        # Per-partition SlateDB instances
        self._dbs: Dict[int, SlateDB] = {}

    def _get_partition_path(self, partition_id: int) -> str:
        """Get the storage path for a partition."""
        path = f"{self.base_path}/{self.job_id}/{self.stage_id}/partition_{partition_id}"
        if path.startswith("s3://"):
            return path + "/"
        else:
            # Ensure local directory exists
            Path(path).mkdir(parents=True, exist_ok=True)
            return f"file://{path}/"

    def acquire_partition(self, partition_id: int) -> bool:
        """Acquire write access to a partition."""
        if partition_id in self._dbs:
            return True

        path = self._get_partition_path(partition_id)
        self.logger.debug(f"Acquiring partition {partition_id} at {path}")

        try:
            db = SlateDB("db", url=path)
            self._dbs[partition_id] = db
            return True

        except Exception as e:
            self.logger.error(f"Failed to acquire partition {partition_id}: {e}")
            raise

    def release_partition(self, partition_id: int) -> None:
        """Release write access to a partition."""
        db = self._dbs.pop(partition_id, None)
        if db:
            try:
                db.close()
            except Exception as e:
                self.logger.warning(f"Error closing partition {partition_id}: {e}")

    def _check_partition(self, partition_id: int) -> SlateDB:
        """Check that partition is acquired and return its DB."""
        if partition_id not in self._dbs:
            raise ValueError(f"Partition {partition_id} not acquired")
        return self._dbs[partition_id]

    def get(self, partition_id: int, key: bytes) -> Optional[bytes]:
        """Get a value from partition state."""
        db = self._check_partition(partition_id)
        try:
            return db.get(key)
        except ClosedError:
            self.logger.error(f"Partition {partition_id} fenced out during get")
            raise

    def put(self, partition_id: int, key: bytes, value: bytes) -> None:
        """Put a value into partition state."""
        db = self._check_partition(partition_id)
        try:
            db.put(key, value)
            db.flush()
        except ClosedError as e:
            self.logger.error(f"Partition {partition_id} fenced out: {e}")
            del self._dbs[partition_id]
            raise

    def close(self) -> None:
        """Close the state store and release all resources."""
        for partition_id in list(self._dbs.keys()):
            self.release_partition(partition_id)
