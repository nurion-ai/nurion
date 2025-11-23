"""Checkpoint coordination and management"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import uuid

from solstice.core.models import CheckpointHandle, CheckpointStatus, Barrier
from solstice.state.backend import StateBackend


@dataclass
class Checkpoint:
    """Represents a complete checkpoint across all stages"""

    checkpoint_id: str
    job_id: str
    timestamp: float = field(default_factory=time.time)
    status: CheckpointStatus = CheckpointStatus.PENDING
    handles: Dict[str, List[CheckpointHandle]] = field(default_factory=dict)  # stage_id -> handles
    manifest_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CheckpointManifest:
    """Manifest describing a checkpoint"""

    checkpoint_id: str
    job_id: str
    timestamp: float
    stage_handles: Dict[str, List[Dict[str, Any]]]  # stage_id -> list of handle dicts
    global_metadata: Dict[str, Any] = field(default_factory=dict)


class CheckpointCoordinator:
    """Coordinates checkpointing across all stages"""

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        checkpoint_interval_secs: int = 300,
        checkpoint_interval_records: Optional[int] = None,
    ):
        self.job_id = job_id
        self.state_backend = state_backend
        self.checkpoint_interval_secs = checkpoint_interval_secs
        self.checkpoint_interval_records = checkpoint_interval_records

        self.logger = logging.getLogger(self.__class__.__name__)

        # Checkpoint tracking
        self.checkpoints: Dict[str, Checkpoint] = {}
        self.latest_completed_checkpoint: Optional[str] = None
        self.last_checkpoint_time = time.time()
        self.records_since_checkpoint = 0

        # Barrier tracking
        self.active_barriers: Dict[str, Barrier] = {}

    def should_trigger_checkpoint(self) -> bool:
        """Check if a new checkpoint should be triggered"""
        time_based = (time.time() - self.last_checkpoint_time) >= self.checkpoint_interval_secs

        if self.checkpoint_interval_records:
            count_based = self.records_since_checkpoint >= self.checkpoint_interval_records
            return time_based or count_based

        return time_based

    def trigger_checkpoint(self) -> str:
        """Trigger a new checkpoint"""
        checkpoint_id = f"checkpoint_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            job_id=self.job_id,
            status=CheckpointStatus.PENDING,
        )

        self.checkpoints[checkpoint_id] = checkpoint
        self.last_checkpoint_time = time.time()
        self.records_since_checkpoint = 0

        self.logger.info(f"Triggered checkpoint: {checkpoint_id}")
        return checkpoint_id

    def create_barrier(
        self,
        checkpoint_id: str,
        stage_id: str,
        upstream_stages: List[str],
        downstream_stages: List[str],
    ) -> Barrier:
        """Create a barrier for a stage"""
        barrier_id = f"{checkpoint_id}_{stage_id}"

        barrier = Barrier(
            barrier_id=barrier_id,
            checkpoint_id=checkpoint_id,
            stage_id=stage_id,
            upstream_stages=upstream_stages,
            downstream_stages=downstream_stages,
        )

        self.active_barriers[barrier_id] = barrier
        return barrier

    def add_checkpoint_handle(
        self,
        checkpoint_id: str,
        stage_id: str,
        handle: CheckpointHandle,
    ) -> None:
        """Add a checkpoint handle from a worker"""
        if checkpoint_id not in self.checkpoints:
            self.logger.warning(f"Unknown checkpoint: {checkpoint_id}")
            return

        checkpoint = self.checkpoints[checkpoint_id]

        if stage_id not in checkpoint.handles:
            checkpoint.handles[stage_id] = []

        checkpoint.handles[stage_id].append(handle)
        self.logger.debug(
            f"Added checkpoint handle for {checkpoint_id}/{stage_id}/{handle.split_id}"
        )

    def finalize_checkpoint(
        self,
        checkpoint_id: str,
        expected_stages: List[str],
    ) -> bool:
        """Finalize a checkpoint once all stages have reported"""
        if checkpoint_id not in self.checkpoints:
            self.logger.warning(f"Unknown checkpoint: {checkpoint_id}")
            return False

        checkpoint = self.checkpoints[checkpoint_id]

        # Check if all stages have reported
        if set(checkpoint.handles.keys()) != set(expected_stages):
            missing = set(expected_stages) - set(checkpoint.handles.keys())
            self.logger.warning(f"Checkpoint {checkpoint_id} missing stages: {missing}")
            return False

        # Save checkpoint manifest
        manifest = CheckpointManifest(
            checkpoint_id=checkpoint.checkpoint_id,
            job_id=self.job_id,
            timestamp=checkpoint.timestamp,
            stage_handles={
                stage_id: [
                    {
                        "checkpoint_id": h.checkpoint_id,
                        "split_id": h.split_id,
                        "split_attempt": h.split_attempt,
                        "state_path": h.state_path,
                        "offset": h.offset,
                        "size_bytes": h.size_bytes,
                        "timestamp": h.timestamp,
                    }
                    for h in handles
                ]
                for stage_id, handles in checkpoint.handles.items()
            },
            global_metadata=checkpoint.metadata,
        )

        manifest_path = f"{self.job_id}/checkpoints/{checkpoint_id}/manifest.json"

        from dataclasses import asdict

        self.state_backend.save_state(manifest_path, {"manifest": asdict(manifest)})

        checkpoint.manifest_path = manifest_path
        checkpoint.status = CheckpointStatus.COMPLETED
        self.latest_completed_checkpoint = checkpoint_id

        self.logger.info(f"Finalized checkpoint: {checkpoint_id}")
        return True

    def get_latest_checkpoint(self) -> Optional[Checkpoint]:
        """Get the latest completed checkpoint"""
        if self.latest_completed_checkpoint:
            return self.checkpoints.get(self.latest_completed_checkpoint)
        return None

    def load_checkpoint(self, checkpoint_id: str) -> Optional[CheckpointManifest]:
        """Load a checkpoint manifest"""
        manifest_path = f"{self.job_id}/checkpoints/{checkpoint_id}/manifest.json"

        if not self.state_backend.exists(manifest_path):
            self.logger.warning(f"Checkpoint manifest not found: {manifest_path}")
            return None

        manifest_data = self.state_backend.load_state(manifest_path)
        manifest_dict = manifest_data["manifest"]

        manifest = CheckpointManifest(**manifest_dict)
        self.logger.info(f"Loaded checkpoint: {checkpoint_id}")
        return manifest

    def list_checkpoints(self) -> List[str]:
        """List all available checkpoints"""
        prefix = f"{self.job_id}/checkpoints"
        paths = self.state_backend.list_checkpoints(prefix)

        # Extract checkpoint IDs from paths
        checkpoint_ids = set()
        for path in paths:
            parts = path.split("/")
            if len(parts) >= 3 and parts[-1] == "manifest.json":
                checkpoint_ids.add(parts[-2])

        return sorted(checkpoint_ids)

    def cleanup_old_checkpoints(self, keep_last_n: int = 5) -> None:
        """Clean up old checkpoints, keeping only the last N"""
        all_checkpoints = self.list_checkpoints()

        if len(all_checkpoints) <= keep_last_n:
            return

        to_delete = all_checkpoints[:-keep_last_n]

        for checkpoint_id in to_delete:
            prefix = f"{self.job_id}/checkpoints/{checkpoint_id}"
            paths = self.state_backend.list_checkpoints(prefix)

            for path in paths:
                try:
                    self.state_backend.delete_state(path)
                except Exception as e:
                    self.logger.error(f"Failed to delete {path}: {e}")

            if checkpoint_id in self.checkpoints:
                del self.checkpoints[checkpoint_id]

            self.logger.info(f"Cleaned up checkpoint: {checkpoint_id}")

    def increment_record_count(self, count: int = 1) -> None:
        """Increment the count of records processed since last checkpoint"""
        self.records_since_checkpoint += count
