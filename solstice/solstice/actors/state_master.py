"""Global State Master for coordinating checkpoints"""

import logging
import time
from typing import Any, Dict, List, Optional
import ray

from solstice.core.models import CheckpointHandle
from solstice.state.checkpoint import CheckpointCoordinator
from solstice.state.backend import StateBackend


@ray.remote
class GlobalStateMaster:
    """Global actor for coordinating state and checkpoints across all stages"""

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        checkpoint_interval_secs: int = 300,
        checkpoint_interval_records: Optional[int] = None,
    ):
        self.job_id = job_id
        self.state_backend = state_backend

        self.logger = logging.getLogger("GlobalStateMaster")

        # Checkpoint coordination
        self.checkpoint_coordinator = CheckpointCoordinator(
            job_id=job_id,
            state_backend=state_backend,
            checkpoint_interval_secs=checkpoint_interval_secs,
            checkpoint_interval_records=checkpoint_interval_records,
        )

        # Stage tracking
        self.stages: List[str] = []
        self.stage_masters: Dict[str, ray.ObjectRef] = {}

        self.logger.info(f"Global State Master initialized for job {job_id}")

    def register_stage(self, stage_id: str, stage_master_ref: ray.ObjectRef) -> None:
        """Register a stage with the global state master"""
        self.stages.append(stage_id)
        self.stage_masters[stage_id] = stage_master_ref

        self.logger.info(f"Registered stage {stage_id}")

    def should_trigger_checkpoint(self) -> bool:
        """Check if a new checkpoint should be triggered"""
        return self.checkpoint_coordinator.should_trigger_checkpoint()

    def trigger_global_checkpoint(self) -> str:
        """Trigger a checkpoint across all stages"""
        checkpoint_id = self.checkpoint_coordinator.trigger_checkpoint()

        self.logger.info(
            f"Triggered global checkpoint {checkpoint_id} across {len(self.stages)} stages"
        )

        # Trigger checkpoint in each stage
        trigger_refs = []
        for stage_id, stage_master in self.stage_masters.items():
            ref = stage_master.trigger_checkpoint.remote(checkpoint_id)
            trigger_refs.append((stage_id, ref))

        # Wait for all stages to trigger
        for stage_id, ref in trigger_refs:
            try:
                ray.get(ref, timeout=30)
            except Exception as e:
                self.logger.error(f"Error triggering checkpoint in stage {stage_id}: {e}")

        return checkpoint_id

    def collect_checkpoint_handles(self, checkpoint_id: str) -> bool:
        """Collect checkpoint handles from all stages"""
        self.logger.info(f"Collecting checkpoint handles for {checkpoint_id}")

        # Collect from each stage
        collect_refs = []
        for stage_id, stage_master in self.stage_masters.items():
            ref = stage_master.collect_checkpoints.remote()
            collect_refs.append((stage_id, ref))

        # Gather handles
        all_handles = {}
        for stage_id, ref in collect_refs:
            try:
                handles_payload = ray.get(ref, timeout=120)
                if handles_payload:
                    stage_handles = []
                    for handle in handles_payload:
                        checkpoint_handle = CheckpointHandle(
                            checkpoint_id=handle.get("checkpoint_id", checkpoint_id),
                            stage_id=handle["stage_id"],
                            split_id=handle["split_id"],
                            split_attempt=handle.get("split_attempt", 0),
                            state_path=handle["state_path"],
                            offset=handle.get("offset", {}),
                            size_bytes=handle.get("size_bytes", 0),
                            timestamp=handle.get("timestamp", time.time()),
                            metadata=handle.get("metadata", {}),
                        )
                        self.checkpoint_coordinator.add_checkpoint_handle(
                            checkpoint_id=checkpoint_id,
                            stage_id=stage_id,
                            handle=checkpoint_handle,
                        )
                        stage_handles.append(checkpoint_handle)
                    all_handles[stage_id] = stage_handles
            except Exception as e:
                self.logger.error(f"Error collecting handles from stage {stage_id}: {e}")
                return False

        # Finalize checkpoint
        success = self.checkpoint_coordinator.finalize_checkpoint(
            checkpoint_id=checkpoint_id,
            expected_stages=self.stages,
        )

        if success:
            self.logger.info(
                f"Successfully finalized checkpoint {checkpoint_id} "
                f"with {sum(len(h) for h in all_handles.values())} handles"
            )
        else:
            self.logger.error(f"Failed to finalize checkpoint {checkpoint_id}")

        return success

    def get_latest_checkpoint(self) -> Optional[str]:
        """Get the ID of the latest completed checkpoint"""
        checkpoint = self.checkpoint_coordinator.get_latest_checkpoint()
        return checkpoint.checkpoint_id if checkpoint else None

    def list_checkpoints(self) -> List[str]:
        """List all available checkpoints"""
        return self.checkpoint_coordinator.list_checkpoints()

    def restore_from_checkpoint(self, checkpoint_id: str) -> bool:
        """Restore all stages from a checkpoint"""
        self.logger.info(f"Restoring job {self.job_id} from checkpoint {checkpoint_id}")

        # Load checkpoint manifest
        manifest = self.checkpoint_coordinator.load_checkpoint(checkpoint_id)
        if not manifest:
            self.logger.error(f"Failed to load checkpoint {checkpoint_id}")
            return False

        # Restore each stage
        restore_refs = []
        for stage_id, stage_master in self.stage_masters.items():
            stage_handles = manifest.stage_handles.get(stage_id, [])
            ref = stage_master.restore_from_checkpoint.remote(checkpoint_id, stage_handles)
            restore_refs.append((stage_id, ref))

        # Wait for all restorations
        for stage_id, ref in restore_refs:
            try:
                ray.get(ref, timeout=120)
                self.logger.info(f"Restored stage {stage_id}")
            except Exception as e:
                self.logger.error(f"Error restoring stage {stage_id}: {e}")
                return False

        self.logger.info(f"Successfully restored from checkpoint {checkpoint_id}")
        return True

    def cleanup_old_checkpoints(self, keep_last_n: int = 5) -> None:
        """Clean up old checkpoints"""
        self.checkpoint_coordinator.cleanup_old_checkpoints(keep_last_n)

    def increment_record_count(self, count: int = 1) -> None:
        """Increment processed record count"""
        self.checkpoint_coordinator.increment_record_count(count)

    def get_checkpoint_status(self) -> Dict[str, Any]:
        """Get checkpoint status"""
        latest = self.checkpoint_coordinator.get_latest_checkpoint()

        return {
            "latest_checkpoint": latest.checkpoint_id if latest else None,
            "latest_checkpoint_time": latest.timestamp if latest else None,
            "total_checkpoints": len(self.checkpoint_coordinator.checkpoints),
            "records_since_checkpoint": self.checkpoint_coordinator.records_since_checkpoint,
        }

    def health_check(self) -> bool:
        """Health check"""
        return True
