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

"""Data models for checkpoint management.

These models represent checkpoint state at different levels:
- PartitionCheckpointData: State for a single partition
- StageCheckpointData: State for a stage (all partitions)
- JobCheckpointData: State for an entire job (all stages)

Key design principle: No worker_id in checkpoint data.
State is tied to partitions, enabling elastic scaling.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional
import json
import time


class CheckpointStatus(str, Enum):
    """Status of a checkpoint."""

    IN_PROGRESS = "IN_PROGRESS"  # Checkpoint started but not complete
    COMPLETED = "COMPLETED"  # Checkpoint successfully completed
    FAILED = "FAILED"  # Checkpoint failed


@dataclass
class PartitionCheckpointData:
    """Checkpoint data for a single partition.

    This captures everything needed to restore a partition's state:
    - Input offset: Where to resume consuming from Tansu
    - State snapshot: SlateDB checkpoint ID for state restoration

    Note: No worker_id - any worker can restore this partition.
    """

    partition_id: int
    input_offset: int  # Tansu committed offset
    state_snapshot_id: Optional[str] = None  # SlateDB checkpoint ID
    state_snapshot_path: Optional[str] = None  # Full path to snapshot
    output_offset: Optional[int] = None  # Output queue offset (if applicable)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "partition_id": self.partition_id,
            "input_offset": self.input_offset,
            "state_snapshot_id": self.state_snapshot_id,
            "state_snapshot_path": self.state_snapshot_path,
            "output_offset": self.output_offset,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PartitionCheckpointData":
        """Create from dictionary."""
        return cls(
            partition_id=data["partition_id"],
            input_offset=data["input_offset"],
            state_snapshot_id=data.get("state_snapshot_id"),
            state_snapshot_path=data.get("state_snapshot_path"),
            output_offset=data.get("output_offset"),
            timestamp=data.get("timestamp", time.time()),
        )


@dataclass
class StageCheckpointData:
    """Checkpoint data for a stage.

    Contains checkpoint data for all partitions in the stage.
    """

    stage_id: str
    partitions: Dict[int, PartitionCheckpointData] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "stage_id": self.stage_id,
            "partitions": {str(k): v.to_dict() for k, v in self.partitions.items()},
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StageCheckpointData":
        """Create from dictionary."""
        partitions = {}
        for k, v in data.get("partitions", {}).items():
            partitions[int(k)] = PartitionCheckpointData.from_dict(v)
        return cls(
            stage_id=data["stage_id"],
            partitions=partitions,
            timestamp=data.get("timestamp", time.time()),
        )


@dataclass
class JobCheckpointData:
    """Checkpoint data for an entire job.

    This is the top-level checkpoint structure containing:
    - Checkpoint metadata (ID, status, timestamps)
    - Stage checkpoint data for all stages
    - Optional metadata for recovery

    The checkpoint follows an intent-based protocol:
    1. Create with status=IN_PROGRESS
    2. Populate stage data
    3. Update status to COMPLETED

    If status is IN_PROGRESS on recovery, the checkpoint is incomplete
    and should be discarded.
    """

    checkpoint_id: str
    job_id: str
    status: CheckpointStatus = CheckpointStatus.IN_PROGRESS
    stages: Dict[str, StageCheckpointData] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    iteration: Optional[int] = None  # For iterative algorithms (CC)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "job_id": self.job_id,
            "status": self.status.value,
            "stages": {k: v.to_dict() for k, v in self.stages.items()},
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "iteration": self.iteration,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobCheckpointData":
        """Create from dictionary."""
        stages = {}
        for k, v in data.get("stages", {}).items():
            stages[k] = StageCheckpointData.from_dict(v)
        return cls(
            checkpoint_id=data["checkpoint_id"],
            job_id=data["job_id"],
            status=CheckpointStatus(data.get("status", "IN_PROGRESS")),
            stages=stages,
            created_at=data.get("created_at", time.time()),
            completed_at=data.get("completed_at"),
            iteration=data.get("iteration"),
            metadata=data.get("metadata", {}),
        )

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> "JobCheckpointData":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def mark_completed(self) -> None:
        """Mark the checkpoint as completed."""
        self.status = CheckpointStatus.COMPLETED
        self.completed_at = time.time()

    def mark_failed(self) -> None:
        """Mark the checkpoint as failed."""
        self.status = CheckpointStatus.FAILED
        self.completed_at = time.time()

    def is_complete(self) -> bool:
        """Check if checkpoint is complete."""
        return self.status == CheckpointStatus.COMPLETED

    def get_partition_data(
        self, stage_id: str, partition_id: int
    ) -> Optional[PartitionCheckpointData]:
        """Get checkpoint data for a specific partition."""
        stage = self.stages.get(stage_id)
        if stage is None:
            return None
        return stage.partitions.get(partition_id)
