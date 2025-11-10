"""Meta Service for managing job DAG and global coordination"""

import time
import logging
from typing import Any, Dict, List, Optional
import ray

from solstice.state.backend import StateBackend


@ray.remote
class MetaService:
    """Global service for job management and DAG coordination"""

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        config: Dict[str, Any],
    ):
        self.job_id = job_id
        self.state_backend = state_backend
        self.config = config

        self.logger = logging.getLogger("MetaService")

        # DAG representation
        self.stages: Dict[str, Dict[str, Any]] = {}  # stage_id -> stage config
        self.stage_masters: Dict[str, ray.ObjectRef] = {}  # stage_id -> actor ref
        self.dag_edges: Dict[str, List[str]] = {}  # stage_id -> downstream stage_ids
        self.reverse_dag: Dict[str, List[str]] = {}  # stage_id -> upstream stage_ids

        # Global state master
        self.global_state_master: Optional[ray.ObjectRef] = None

        # Execution state
        self.is_running = False
        self.start_time: Optional[float] = None

        # Scheduling
        self.scheduling_policy = config.get("scheduling_policy", "fair")

        self.logger.info(f"Meta Service initialized for job {job_id}")

    def set_global_state_master(self, state_master_ref: ray.ObjectRef) -> None:
        """Set the global state master reference"""
        self.global_state_master = state_master_ref
        self.logger.info("Global state master registered")

    def add_stage(
        self,
        stage_id: str,
        stage_config: Dict[str, Any],
        upstream_stages: Optional[List[str]] = None,
    ) -> None:
        """Add a stage to the DAG"""
        self.stages[stage_id] = stage_config

        # Update DAG edges
        upstream_stages = upstream_stages or []
        self.reverse_dag[stage_id] = upstream_stages

        for upstream_id in upstream_stages:
            if upstream_id not in self.dag_edges:
                self.dag_edges[upstream_id] = []
            self.dag_edges[upstream_id].append(stage_id)

        self.logger.info(f"Added stage {stage_id} with {len(upstream_stages)} upstream stages")

    def register_stage_master(self, stage_id: str, stage_master_ref: ray.ObjectRef) -> None:
        """Register a stage master actor"""
        if stage_id not in self.stages:
            self.logger.error(f"Cannot register unknown stage {stage_id}")
            return

        self.stage_masters[stage_id] = stage_master_ref

        # Also register with global state master
        if self.global_state_master:
            self.global_state_master.register_stage.remote(stage_id, stage_master_ref)

        self.logger.info(f"Registered stage master for {stage_id}")

    def get_stage_order(self) -> List[str]:
        """Get topological order of stages"""
        # Simple topological sort
        visited = set()
        order = []

        def visit(stage_id):
            if stage_id in visited:
                return
            visited.add(stage_id)

            # Visit upstream first
            for upstream in self.reverse_dag.get(stage_id, []):
                visit(upstream)

            order.append(stage_id)

        for stage_id in self.stages.keys():
            visit(stage_id)

        return order

    def start_job(self) -> None:
        """Start job execution"""
        if self.is_running:
            self.logger.warning("Job is already running")
            return

        self.is_running = True
        self.start_time = time.time()

        self.logger.info(f"Started job {self.job_id}")

    def stop_job(self) -> None:
        """Stop job execution"""
        if not self.is_running:
            return

        self.is_running = False

        # Shutdown all stage masters
        shutdown_refs = []
        for stage_id, stage_master in self.stage_masters.items():
            ref = stage_master.shutdown.remote()
            shutdown_refs.append((stage_id, ref))

        # Wait for shutdown
        for stage_id, ref in shutdown_refs:
            try:
                ray.get(ref, timeout=30)
            except Exception as e:
                self.logger.error(f"Error shutting down stage {stage_id}: {e}")

        elapsed = time.time() - self.start_time if self.start_time else 0
        self.logger.info(f"Stopped job {self.job_id} after {elapsed:.2f} seconds")

    def get_downstream_stages(self, stage_id: str) -> List[str]:
        """Get downstream stages for a given stage"""
        return self.dag_edges.get(stage_id, [])

    def get_upstream_stages(self, stage_id: str) -> List[str]:
        """Get upstream stages for a given stage"""
        return self.reverse_dag.get(stage_id, [])

    def propagate_backpressure(self, from_stage: str, slow_down_factor: float) -> None:
        """Propagate backpressure signal upstream"""
        self.logger.info(
            f"Propagating backpressure from {from_stage} with factor {slow_down_factor}"
        )

        # Get upstream stages
        upstream = self.get_upstream_stages(from_stage)

        # For now, just log - in a full implementation, this would
        # send signals to upstream stage masters
        for stage_id in upstream:
            self.logger.debug(
                f"Backpressure signal to stage {stage_id}: slow down by {slow_down_factor}"
            )

    def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect metrics from all stages"""
        metrics_refs = []
        for stage_id, stage_master in self.stage_masters.items():
            ref = stage_master.collect_metrics.remote()
            metrics_refs.append((stage_id, ref))

        all_metrics = {}
        for stage_id, ref in metrics_refs:
            try:
                metrics = ray.get(ref, timeout=10)
                all_metrics[stage_id] = metrics
            except Exception as e:
                self.logger.warning(f"Failed to collect metrics from {stage_id}: {e}")

        # Add job-level metrics
        job_metrics = {
            "job_id": self.job_id,
            "is_running": self.is_running,
            "uptime_secs": time.time() - self.start_time if self.start_time else 0,
            "stage_count": len(self.stages),
            "stages": all_metrics,
        }

        return job_metrics

    def trigger_global_checkpoint(self) -> Optional[str]:
        """Trigger a global checkpoint"""
        if not self.global_state_master:
            self.logger.error("Global state master not set")
            return None

        try:
            checkpoint_id = ray.get(
                self.global_state_master.trigger_global_checkpoint.remote(), timeout=60
            )

            # Collect handles
            success = ray.get(
                self.global_state_master.collect_checkpoint_handles.remote(checkpoint_id),
                timeout=180,
            )

            if success:
                self.logger.info(f"Global checkpoint {checkpoint_id} completed")
                return checkpoint_id
            else:
                self.logger.error(f"Global checkpoint {checkpoint_id} failed")
                return None

        except Exception as e:
            self.logger.error(f"Error triggering global checkpoint: {e}")
            return None

    def handle_stage_failure(self, stage_id: str) -> None:
        """Handle stage master failure"""
        self.logger.warning(f"Handling failure of stage {stage_id}")

        # In a full implementation, this would:
        # 1. Detect the failure
        # 2. Get the latest checkpoint
        # 3. Recreate the stage master
        # 4. Restore from checkpoint
        # 5. Reconnect data flows

        if stage_id not in self.stages:
            self.logger.error(f"Unknown stage {stage_id}")
            return

        # For now, just log
        self.logger.info(f"Stage {stage_id} failure handling initiated")

    def get_job_status(self) -> Dict[str, Any]:
        """Get overall job status"""
        checkpoint_status = {}
        if self.global_state_master:
            try:
                checkpoint_status = ray.get(
                    self.global_state_master.get_checkpoint_status.remote(), timeout=5
                )
            except Exception:
                pass

        return {
            "job_id": self.job_id,
            "is_running": self.is_running,
            "uptime_secs": time.time() - self.start_time if self.start_time else 0,
            "stage_count": len(self.stages),
            "active_stage_masters": len(self.stage_masters),
            "checkpoint_status": checkpoint_status,
        }

    def health_check(self) -> bool:
        """Health check"""
        return True
