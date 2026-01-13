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

"""Checkpoint storage using fsspec.

Simple single-file checkpoint storage with atomic writes.

Storage layout:
    {base_path}/{job_id}/checkpoint.json

Uses atomic write (write to temp, then rename) to prevent corruption.
"""

import uuid
from typing import Optional, Protocol, runtime_checkable

import fsspec

from solstice.checkpoint.models import JobCheckpointData
from solstice.utils.logging import create_ray_logger


@runtime_checkable
class CheckpointStorage(Protocol):
    """Protocol for checkpoint storage backends."""

    async def save(self, checkpoint: JobCheckpointData) -> None:
        """Save a checkpoint (overwrites existing)."""
        ...

    async def load(self) -> Optional[JobCheckpointData]:
        """Load the checkpoint."""
        ...


class FsspecCheckpointStorage:
    """Checkpoint storage using fsspec for unified storage access.

    Simple implementation: one checkpoint file per job, atomic writes.
    No history, no cleanup needed.
    """

    def __init__(self, base_path: str, job_id: str):
        """Initialize checkpoint storage.

        Args:
            base_path: Base storage path (local or cloud URL)
            job_id: Job identifier
        """
        self.base_path = base_path.rstrip("/")
        self.job_id = job_id
        self.logger = create_ray_logger(f"CheckpointStorage-{job_id}")

        # Initialize filesystem from path protocol
        self.fs, self._root = fsspec.url_to_fs(self.base_path)

        # Checkpoint file path
        self._checkpoint_dir = f"{self._root}/{job_id}"
        self._checkpoint_path = f"{self._checkpoint_dir}/checkpoint.json"

        # Ensure directory exists
        try:
            self.fs.makedirs(self._checkpoint_dir, exist_ok=True)
        except Exception:
            pass  # Some backends don't support makedirs

    async def save(self, checkpoint: JobCheckpointData) -> None:
        """Save a checkpoint with atomic write."""
        # Write to temp file first
        tmp_path = f"{self._checkpoint_path}.{uuid.uuid4().hex[:8]}.tmp"

        try:
            with self.fs.open(tmp_path, "w") as f:
                f.write(checkpoint.to_json())

            # Atomic rename (overwrites existing)
            self.fs.rename(tmp_path, self._checkpoint_path)
            self.logger.debug(f"Saved checkpoint {checkpoint.checkpoint_id}")

        except Exception as e:
            # Clean up temp file on failure
            try:
                if self.fs.exists(tmp_path):
                    self.fs.rm(tmp_path)
            except Exception:
                pass
            raise e

    async def load(self) -> Optional[JobCheckpointData]:
        """Load the checkpoint."""
        try:
            if not self.fs.exists(self._checkpoint_path):
                return None

            with self.fs.open(self._checkpoint_path, "r") as f:
                json_str = f.read()

            checkpoint = JobCheckpointData.from_json(json_str)

            # Only return completed checkpoints
            if not checkpoint.is_complete():
                self.logger.warning(
                    f"Checkpoint {checkpoint.checkpoint_id} is incomplete, ignoring"
                )
                return None

            return checkpoint

        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return None
