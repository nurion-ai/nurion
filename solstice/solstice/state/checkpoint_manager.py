"""Checkpoint Manager - handles checkpoint lifecycle and split tracking.

Core responsibilities:
1. Track completed/inflight splits per stage
2. Trigger and coordinate checkpoints
3. Restore job state from checkpoints
"""

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from solstice.core.models import JobCheckpointConfig, CheckpointStatus
from solstice.state.store import (
    CheckpointStore,
    CheckpointManifest,
    StageCheckpointData,
)


@dataclass
class CheckpointState:
    """Runtime state of an in-progress checkpoint."""

    checkpoint_id: str
    status: CheckpointStatus = CheckpointStatus.PENDING
    started_at: float = field(default_factory=time.time)
    stages_reported: Set[str] = field(default_factory=set)
    stages_expected: Set[str] = field(default_factory=set)  # All registered stages
    error: Optional[str] = None


class StageCheckpointTracker:
    """Tracks split completion status for a single stage.

    Used by StageMaster to track which splits have been completed,
    enabling checkpoint and recovery.
    """

    def __init__(self, stage_id: str):
        self.stage_id = stage_id
        self.logger = logging.getLogger(f"CheckpointTracker-{stage_id}")

        # Split tracking
        self._completed_splits: Set[str] = set()
        self._inflight_splits: Set[str] = set()

        # Stage-level offset (for sources)
        self._offset: Dict[str, Any] = {}

        # Pending checkpoint
        self._pending_checkpoint_id: Optional[str] = None

    def mark_split_started(self, split_id: str) -> None:
        """Mark a split as started processing."""
        self._inflight_splits.add(split_id)

    def mark_split_completed(self, split_id: str) -> None:
        """Mark a split as completed."""
        self._inflight_splits.discard(split_id)
        self._completed_splits.add(split_id)

    def mark_split_failed(self, split_id: str) -> None:
        """Mark a split as failed (will be retried)."""
        self._inflight_splits.discard(split_id)
        # Don't add to completed - will be reprocessed

    def is_split_completed(self, split_id: str) -> bool:
        """Check if a split has been completed."""
        return split_id in self._completed_splits

    def update_offset(self, offset: Dict[str, Any]) -> None:
        """Update stage-level offset."""
        self._offset.update(offset)

    def get_checkpoint_data(self) -> StageCheckpointData:
        """Get checkpoint data for this stage."""
        return StageCheckpointData(
            stage_id=self.stage_id,
            completed_splits=set(self._completed_splits),
            inflight_splits=set(self._inflight_splits),
            offset=dict(self._offset),
            last_checkpoint_id=self._pending_checkpoint_id,
        )

    def restore_from_checkpoint(self, data: StageCheckpointData) -> None:
        """Restore state from checkpoint data."""
        self._completed_splits = set(data.completed_splits)
        # Inflight splits from checkpoint should be reprocessed
        self._inflight_splits = set()
        self._offset = dict(data.offset)
        self._pending_checkpoint_id = data.last_checkpoint_id

        self.logger.info(
            f"Restored stage {self.stage_id}: "
            f"{len(self._completed_splits)} completed splits, "
            f"offset={self._offset}"
        )

    def prepare_checkpoint(self, checkpoint_id: str) -> StageCheckpointData:
        """Prepare checkpoint data (called during checkpoint trigger)."""
        self._pending_checkpoint_id = checkpoint_id
        return self.get_checkpoint_data()

    def clear(self) -> None:
        """Clear all tracking state."""
        self._completed_splits.clear()
        self._inflight_splits.clear()
        self._offset.clear()
        self._pending_checkpoint_id = None


class CheckpointManager:
    """Coordinates checkpointing across all stages.

    Manages the checkpoint lifecycle:
    1. Trigger checkpoint based on config
    2. Collect checkpoint data from stages
    3. Persist checkpoint manifest
    4. Restore from checkpoint
    """

    def __init__(
        self,
        job_id: str,
        store: CheckpointStore,
        config: Optional[JobCheckpointConfig] = None,
    ):
        self.job_id = job_id
        self.store = store
        self.config = config or JobCheckpointConfig()
        self.logger = logging.getLogger(f"CheckpointManager-{job_id}")

        # Tracking
        self._last_checkpoint_time = time.time()
        self._records_since_checkpoint = 0
        self._current_checkpoint: Optional[CheckpointState] = None
        self._completed_checkpoints: List[str] = []

        # Stage trackers (populated by register_stage)
        self._stage_trackers: Dict[str, StageCheckpointTracker] = {}

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def register_stage(self, stage_id: str) -> StageCheckpointTracker:
        """Register a stage for checkpointing.

        Args:
            stage_id: The stage ID

        Returns:
            StageCheckpointTracker for the stage
        """
        tracker = StageCheckpointTracker(stage_id)
        self._stage_trackers[stage_id] = tracker
        return tracker

    def get_tracker(self, stage_id: str) -> Optional[StageCheckpointTracker]:
        """Get the checkpoint tracker for a stage."""
        return self._stage_trackers.get(stage_id)

    def get_registered_stages(self) -> List[str]:
        """Get list of registered stage IDs."""
        return list(self._stage_trackers.keys())

    def should_trigger_checkpoint(self) -> bool:
        """Check if a checkpoint should be triggered."""
        if not self.enabled:
            return False

        if self._current_checkpoint is not None:
            # Already have a checkpoint in progress
            return False

        time_elapsed = time.time() - self._last_checkpoint_time

        # Check minimum pause
        if time_elapsed < self.config.min_pause_between_secs:
            return False

        # Check interval
        return time_elapsed >= self.config.interval_secs

    def increment_record_count(self, count: int = 1) -> None:
        """Increment processed record count."""
        self._records_since_checkpoint += count

    def trigger_checkpoint(self) -> Optional[str]:
        """Trigger a new checkpoint.

        Returns:
            Checkpoint ID, or None if trigger failed
        """
        if not self.enabled:
            return None

        if self._current_checkpoint is not None:
            self.logger.warning("Checkpoint already in progress")
            return None

        checkpoint_id = f"ckpt_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        self._current_checkpoint = CheckpointState(
            checkpoint_id=checkpoint_id,
            status=CheckpointStatus.IN_PROGRESS,
            stages_expected=set(self._stage_trackers.keys()),
        )

        self.logger.info(f"Triggered checkpoint {checkpoint_id}")
        return checkpoint_id

    def collect_stage_checkpoint(
        self,
        stage_id: str,
        data: StageCheckpointData,
    ) -> None:
        """Collect checkpoint data from a stage.

        Called by each stage after preparing their checkpoint.
        """
        if self._current_checkpoint is None:
            self.logger.warning(f"No checkpoint in progress for stage {stage_id}")
            return

        self._current_checkpoint.stages_reported.add(stage_id)

        # Store stage checkpoint data
        key = (
            f"{self.job_id}/checkpoints/{self._current_checkpoint.checkpoint_id}/stages/{stage_id}"
        )
        self.store.put_json(key, data.to_dict())

    def finalize_checkpoint(self) -> bool:
        """Finalize the current checkpoint.

        Returns:
            True if successful, False otherwise
        """
        if self._current_checkpoint is None:
            return False

        checkpoint = self._current_checkpoint

        # Check if all expected stages reported
        missing = checkpoint.stages_expected - checkpoint.stages_reported
        if missing:
            self.logger.warning(f"Checkpoint {checkpoint.checkpoint_id} missing stages: {missing}")
            # Still proceed - partial checkpoint is better than none

        # Build manifest
        stages_data = {}
        for stage_id in checkpoint.stages_reported:
            key = f"{self.job_id}/checkpoints/{checkpoint.checkpoint_id}/stages/{stage_id}"
            data = self.store.get_json(key)
            if data:
                stages_data[stage_id] = StageCheckpointData.from_dict(data)

        manifest = CheckpointManifest(
            checkpoint_id=checkpoint.checkpoint_id,
            job_id=self.job_id,
            stages=stages_data,
        )

        # Save manifest
        manifest_key = f"{self.job_id}/checkpoints/{checkpoint.checkpoint_id}/manifest"
        self.store.put_json(manifest_key, manifest.to_dict())

        # Update state
        self._completed_checkpoints.append(checkpoint.checkpoint_id)
        self._last_checkpoint_time = time.time()
        self._records_since_checkpoint = 0
        self._current_checkpoint = None

        self.logger.info(
            f"Finalized checkpoint {checkpoint.checkpoint_id} with {len(stages_data)} stages"
        )
        return True

    def get_latest_checkpoint_id(self) -> Optional[str]:
        """Get the most recent completed checkpoint ID."""
        if self._completed_checkpoints:
            return self._completed_checkpoints[-1]

        # Check store for existing checkpoints
        checkpoints = self.list_checkpoints()
        return checkpoints[-1] if checkpoints else None

    def list_checkpoints(self) -> List[str]:
        """List all completed checkpoint IDs."""
        prefix = f"{self.job_id}/checkpoints/"
        keys = self.store.list_keys(prefix)

        # Extract checkpoint IDs from manifest keys
        checkpoint_ids = set()
        for key in keys:
            if "manifest" in key:
                # Extract checkpoint ID from path
                parts = key.replace(prefix, "").split("/")
                if parts:
                    checkpoint_ids.add(parts[0])

        return sorted(checkpoint_ids)

    def load_checkpoint(self, checkpoint_id: str) -> Optional[CheckpointManifest]:
        """Load a checkpoint manifest."""
        manifest_key = f"{self.job_id}/checkpoints/{checkpoint_id}/manifest"
        data = self.store.get_json(manifest_key)
        if data is None:
            return None
        return CheckpointManifest.from_dict(data)

    def restore_from_checkpoint(
        self,
        checkpoint_id: Optional[str] = None,
    ) -> bool:
        """Restore all stages from a checkpoint.

        Args:
            checkpoint_id: Specific checkpoint to restore, or latest if None

        Returns:
            True if successful
        """
        if checkpoint_id is None:
            checkpoint_id = self.get_latest_checkpoint_id()

        if checkpoint_id is None:
            self.logger.warning("No checkpoint available to restore")
            return False

        manifest = self.load_checkpoint(checkpoint_id)
        if manifest is None:
            self.logger.error(f"Failed to load checkpoint {checkpoint_id}")
            return False

        self.logger.info(f"Restoring from checkpoint {checkpoint_id}")

        # Restore each stage
        for stage_id, stage_data in manifest.stages.items():
            tracker = self._stage_trackers.get(stage_id)
            if tracker:
                tracker.restore_from_checkpoint(stage_data)
            else:
                self.logger.warning(f"Stage {stage_id} in checkpoint but not registered")

        self.logger.info(f"Restored {len(manifest.stages)} stages from checkpoint {checkpoint_id}")
        return True

    def cleanup_old_checkpoints(self, keep_last_n: int = 5) -> None:
        """Delete old checkpoints, keeping only the last N."""
        checkpoints = self.list_checkpoints()

        if len(checkpoints) <= keep_last_n:
            return

        to_delete = checkpoints[:-keep_last_n]

        for checkpoint_id in to_delete:
            prefix = f"{self.job_id}/checkpoints/{checkpoint_id}"
            keys = self.store.list_keys(prefix)
            for key in keys:
                self.store.delete(key)
            self.logger.info(f"Deleted checkpoint {checkpoint_id}")
