"""Job definition and execution"""

import time
import logging
from typing import Any, Dict, List, Optional
import ray

from solstice.core.stage import Stage, StageMaster
from solstice.state.backend import StateBackend, LocalStateBackend
from solstice.actors.meta_service import MetaService
from solstice.actors.state_master import GlobalStateMaster


class Job:
    """Represents a complete streaming job with DAG of stages"""

    def __init__(
        self,
        job_id: str,
        state_backend: Optional[StateBackend] = None,
        checkpoint_interval_secs: int = 300,
        checkpoint_interval_records: Optional[int] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a streaming job.

        Args:
            job_id: Unique identifier for the job
            state_backend: Backend for storing state (defaults to local)
            checkpoint_interval_secs: Checkpoint interval in seconds
            checkpoint_interval_records: Checkpoint interval in records processed
            config: Additional job configuration
        """
        self.job_id = job_id
        self.state_backend = state_backend or LocalStateBackend(f"/tmp/solstice/{job_id}")
        self.checkpoint_interval_secs = checkpoint_interval_secs
        self.checkpoint_interval_records = checkpoint_interval_records
        self.config = config or {}

        self.logger = logging.getLogger(f"Job-{job_id}")

        # DAG components
        self.stages: Dict[str, Stage] = {}
        self.stage_masters: Dict[str, StageMaster] = {}
        self.dag_edges: Dict[str, List[str]] = {}  # stage_id -> downstream stages

        # Ray actors
        self.meta_service: Optional[ray.ObjectRef] = None
        self.global_state_master: Optional[ray.ObjectRef] = None

        # Execution state
        self.is_running = False

        self.logger.info(f"Job {job_id} initialized")

    def add_stage(
        self,
        stage: Stage,
        upstream_stages: Optional[List[str]] = None,
    ) -> "Job":
        """
        Add a stage to the job DAG.

        Args:
            stage: Stage to add
            upstream_stages: List of upstream stage IDs

        Returns:
            Self for chaining
        """
        if stage.stage_id in self.stages:
            raise ValueError(f"Stage {stage.stage_id} already exists")

        self.stages[stage.stage_id] = stage

        # Update DAG edges
        upstream_stages = upstream_stages or []
        for upstream_id in upstream_stages:
            if upstream_id not in self.stages:
                raise ValueError(f"Upstream stage {upstream_id} not found")

            if upstream_id not in self.dag_edges:
                self.dag_edges[upstream_id] = []
            self.dag_edges[upstream_id].append(stage.stage_id)

        self.logger.info(
            f"Added stage {stage.stage_id} with {len(upstream_stages)} upstream stages"
        )

        return self

    def initialize(self) -> None:
        """Initialize Ray actors for the job"""
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        self.logger.info("Initializing job actors...")

        # Create Meta Service
        self.meta_service = MetaService.remote(
            job_id=self.job_id,
            state_backend=self.state_backend,
            config=self.config,
        )

        # Create Global State Master
        self.global_state_master = GlobalStateMaster.remote(
            job_id=self.job_id,
            state_backend=self.state_backend,
            checkpoint_interval_secs=self.checkpoint_interval_secs,
            checkpoint_interval_records=self.checkpoint_interval_records,
        )

        # Register global state master with meta service
        ray.get(self.meta_service.set_global_state_master.remote(self.global_state_master))

        # Add stages to meta service
        reverse_dag = self._build_reverse_dag()
        for stage_id, stage in self.stages.items():
            ray.get(
                self.meta_service.add_stage.remote(
                    stage_id=stage_id,
                    stage_config=stage.to_dict(),
                    upstream_stages=reverse_dag.get(stage_id, []),
                )
            )

        # Create stage masters
        for stage_id, stage in self.stages.items():
            stage_master = StageMaster(stage, self.state_backend)
            actor_ref = stage_master.start()

            self.stage_masters[stage_id] = stage_master

            # Register with meta service
            ray.get(self.meta_service.register_stage_master.remote(stage_id, actor_ref))

        self.logger.info(f"Initialized {len(self.stages)} stages")

    def _build_reverse_dag(self) -> Dict[str, List[str]]:
        """Build reverse DAG (downstream -> upstream)"""
        reverse_dag = {stage_id: [] for stage_id in self.stages.keys()}

        for upstream_id, downstream_ids in self.dag_edges.items():
            for downstream_id in downstream_ids:
                reverse_dag[downstream_id].append(upstream_id)

        return reverse_dag

    def start(self) -> None:
        """Start job execution"""
        if self.is_running:
            self.logger.warning("Job is already running")
            return

        if not self.meta_service:
            self.initialize()

        self.logger.info("Starting job execution...")
        ray.get(self.meta_service.start_job.remote())
        self.is_running = True

        self.logger.info("Job started")

    def stop(self) -> None:
        """Stop job execution"""
        if not self.is_running:
            return

        self.logger.info("Stopping job...")
        ray.get(self.meta_service.stop_job.remote())
        self.is_running = False

        self.logger.info("Job stopped")

    def trigger_checkpoint(self) -> Optional[str]:
        """Manually trigger a checkpoint"""
        if not self.is_running:
            self.logger.warning("Job is not running")
            return None

        self.logger.info("Triggering manual checkpoint...")
        checkpoint_id = ray.get(self.meta_service.trigger_global_checkpoint.remote())

        return checkpoint_id

    def restore_from_checkpoint(self, checkpoint_id: Optional[str] = None) -> bool:
        """
        Restore job from a checkpoint.

        Args:
            checkpoint_id: Specific checkpoint to restore from (or latest if None)

        Returns:
            True if restoration was successful
        """
        if not self.global_state_master:
            self.initialize()

        # Get checkpoint to restore
        if not checkpoint_id:
            checkpoint_id = ray.get(self.global_state_master.get_latest_checkpoint.remote())
            if not checkpoint_id:
                self.logger.error("No checkpoint available to restore from")
                return False

        self.logger.info(f"Restoring from checkpoint {checkpoint_id}...")

        success = ray.get(self.global_state_master.restore_from_checkpoint.remote(checkpoint_id))

        if success:
            self.logger.info(f"Successfully restored from checkpoint {checkpoint_id}")
        else:
            self.logger.error(f"Failed to restore from checkpoint {checkpoint_id}")

        return success

    def get_status(self) -> Dict[str, Any]:
        """Get job status"""
        if not self.meta_service:
            return {
                "job_id": self.job_id,
                "is_running": False,
                "initialized": False,
            }

        try:
            status = ray.get(self.meta_service.get_job_status.remote(), timeout=5)
            return status
        except Exception as e:
            self.logger.error(f"Failed to get job status: {e}")
            return {
                "job_id": self.job_id,
                "error": str(e),
            }

    def get_metrics(self) -> Dict[str, Any]:
        """Get job metrics"""
        if not self.meta_service:
            return {}

        try:
            metrics = ray.get(self.meta_service.collect_all_metrics.remote(), timeout=10)
            return metrics
        except Exception as e:
            self.logger.error(f"Failed to get metrics: {e}")
            return {}

    def list_checkpoints(self) -> List[str]:
        """List available checkpoints"""
        if not self.global_state_master:
            return []

        return ray.get(self.global_state_master.list_checkpoints.remote())

    def cleanup_checkpoints(self, keep_last_n: int = 5) -> None:
        """Clean up old checkpoints"""
        if not self.global_state_master:
            return

        ray.get(self.global_state_master.cleanup_old_checkpoints.remote(keep_last_n))
        self.logger.info(f"Cleaned up old checkpoints, keeping last {keep_last_n}")

    def wait_for_completion(self, timeout: Optional[float] = None) -> None:
        """
        Wait for job to complete.

        Args:
            timeout: Maximum time to wait in seconds (None = wait forever)
        """
        start_time = time.time()

        while self.is_running:
            if timeout and (time.time() - start_time) > timeout:
                self.logger.warning(f"Job wait timed out after {timeout} seconds")
                break

            time.sleep(1)

    def __enter__(self):
        """Context manager entry"""
        self.initialize()
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()
