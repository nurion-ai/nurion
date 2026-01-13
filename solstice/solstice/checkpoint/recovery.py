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

"""Job-level recovery from checkpoints.

Recovery is simple:
1. Load the checkpoint
2. Reset consumer offsets to checkpoint values
3. SlateDB state is automatically restored (it's S3-backed)
"""

from dataclasses import dataclass
from typing import Optional

from solstice.checkpoint.models import JobCheckpointData
from solstice.checkpoint.storage import CheckpointStorage
from solstice.utils.logging import create_ray_logger


@dataclass
class RecoveryResult:
    """Result of a recovery attempt."""

    recovered: bool
    checkpoint_id: Optional[str] = None
    error: Optional[str] = None


async def recover_from_checkpoint(
    storage: CheckpointStorage,
    job_id: str,
) -> tuple[Optional[JobCheckpointData], RecoveryResult]:
    """Load the checkpoint for recovery.

    Args:
        storage: Checkpoint storage backend
        job_id: Job identifier

    Returns:
        Tuple of (checkpoint data if found, recovery result)
    """
    logger = create_ray_logger(f"Recovery-{job_id}")

    try:
        checkpoint = await storage.load()

        if checkpoint is None:
            logger.info("No checkpoint found, starting fresh")
            return None, RecoveryResult(recovered=False)

        if checkpoint.job_id != job_id:
            error = f"Checkpoint job_id mismatch: {checkpoint.job_id} != {job_id}"
            logger.error(error)
            return None, RecoveryResult(recovered=False, error=error)

        logger.info(f"Loaded checkpoint {checkpoint.checkpoint_id} for recovery")
        return checkpoint, RecoveryResult(
            recovered=True,
            checkpoint_id=checkpoint.checkpoint_id,
        )

    except Exception as e:
        error = f"Failed to load checkpoint: {e}"
        logger.error(error)
        return None, RecoveryResult(recovered=False, error=error)


def get_partition_offset(
    checkpoint: Optional[JobCheckpointData],
    stage_id: str,
    partition_id: int,
) -> Optional[int]:
    """Get the offset to resume from for a partition.

    Args:
        checkpoint: Checkpoint data (can be None)
        stage_id: Stage identifier
        partition_id: Partition identifier

    Returns:
        Offset to resume from, or None if no checkpoint
    """
    if checkpoint is None:
        return None

    data = checkpoint.get_partition_data(stage_id, partition_id)
    if data is None:
        return None

    return data.input_offset
