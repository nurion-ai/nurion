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

"""Checkpoint management for fault tolerance.

Key components:
- Models: Data structures for checkpoints (partition, stage, job level)
- Storage: Persistence using fsspec (local, S3, etc.)
- Recovery: Loading checkpoints for job restart

Usage:
    from solstice.checkpoint import (
        FsspecCheckpointStorage,
        JobCheckpointData,
        recover_from_checkpoint,
    )

    # Save checkpoint
    storage = FsspecCheckpointStorage("/tmp/checkpoints", "my_job")
    await storage.save(checkpoint_data)

    # Recover on restart
    checkpoint, result = await recover_from_checkpoint(storage, "my_job")
    if result.recovered:
        # Use checkpoint.stages[stage_id].partitions[p].input_offset
        # to seek consumers
        pass
"""

from solstice.checkpoint.models import (
    CheckpointStatus,
    JobCheckpointData,
    PartitionCheckpointData,
    StageCheckpointData,
)
from solstice.checkpoint.storage import (
    CheckpointStorage,
    FsspecCheckpointStorage,
)
from solstice.checkpoint.recovery import (
    RecoveryResult,
    recover_from_checkpoint,
    get_partition_offset,
)

__all__ = [
    # Models
    "CheckpointStatus",
    "JobCheckpointData",
    "PartitionCheckpointData",
    "StageCheckpointData",
    # Storage
    "CheckpointStorage",
    "FsspecCheckpointStorage",
    # Recovery
    "RecoveryResult",
    "recover_from_checkpoint",
    "get_partition_offset",
]
