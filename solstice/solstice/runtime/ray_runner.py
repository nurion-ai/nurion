"""Ray runtime for executing Solstice jobs."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import ray
import ray.actor
from solstice.utils.logging import create_ray_logger
from solstice.core.stage_master import StageStatus
from solstice.actors.meta_service import MetaService
from solstice.core.job import Job
from solstice.state.checkpoint_manager import CheckpointManager


class RayJobRunner:
    """Control-plane responsible for running a :class:`Job` on Ray."""

    def __init__(self, job: Job, ray_init_kwargs: Optional[dict[str, Any]] = None) -> None:
        self.job = job
        self._ray_init_kwargs = ray_init_kwargs or {}

        self.logger = create_ray_logger(f"RayJobRunner-{job.job_id}")

        self.meta_service: Optional[ray.actor.ActorHandle] = None
        self.checkpoint_manager: Optional[CheckpointManager] = None
        self.stage_actor_refs: dict[str, ray.actor.ActorHandle] = {}
        self.stage_run_refs: dict[str, ray.ObjectRef] = {}

        self._initialized = False
        self._running = False
        self._topology: List[str] = []
        self._reverse_dag: Dict[str, List[str]] = {}
        self._sink_stage_ids: List[str] = []

        self.job.attach_ray_runner(self)

        self._stage_run_poll_interval = float(self.job.config.get("stage_run_poll_interval", 0.05))

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    def _ensure_ray(self) -> None:
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, **self._ray_init_kwargs)

    def initialize(self) -> None:
        if self._initialized:
            return

        self._ensure_ray()
        self.logger.info("Initializing job %s", self.job.job_id)

        # Initialize checkpoint manager
        self.checkpoint_manager = CheckpointManager(
            job_id=self.job.job_id,
            store=self.job.checkpoint_store,
            config=self.job.checkpoint_config,
        )

        self.meta_service = MetaService.remote(
            job_id=self.job.job_id,
            checkpoint_store=self.job.checkpoint_store,
            config=self.job.config,
        )

        self._reverse_dag = self.job.build_reverse_dag()
        for stage_id, stage in self.job.stages.items():
            ray.get(
                self.meta_service.add_stage.remote(
                    stage_id=stage_id,
                    stage_config=stage.to_dict(),
                    upstream_stages=self._reverse_dag.get(stage_id, []),
                )
            )

        for stage_id, stage in self.job.stages.items():
            actor_name = stage_id
            upstream_stages = self._reverse_dag.get(stage_id, [])
            stage_master = (
                ray.remote(stage.master_config.master_class)
                .options(name=actor_name, max_concurrency=10, num_cpus=0.2)
                .remote(
                    job_id=self.job.job_id,
                    checkpoint_store=self.job.checkpoint_store,
                    upstream_stages=upstream_stages,
                    stage=stage,
                )
            )
            self.stage_actor_refs[stage_id] = stage_master
            ray.get(self.meta_service.register_stage_master.remote(stage_id, stage_master))

            # Register stage with checkpoint manager (unless skipped)
            if not stage.skip_checkpoint:
                self.checkpoint_manager.register_stage(stage_id)

        for stage_id, actor_ref in self.stage_actor_refs.items():
            downstream_ids = self.job.dag_edges.get(stage_id, [])
            downstream_mapping = {
                downstream_id: self.stage_actor_refs[downstream_id]
                for downstream_id in downstream_ids
                if downstream_id in self.stage_actor_refs
            }
            ray.get(actor_ref.configure_downstream.remote(downstream_mapping))

        self._topology = self._compute_topology()
        self._sink_stage_ids = [
            stage_id for stage_id in self.job.stages if not self.job.dag_edges.get(stage_id)
        ]

        self._initialized = True
        self.logger.info(
            "Initialized %d stages for job %s", len(self.stage_actor_refs), self.job.job_id
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _compute_topology(self) -> list[str]:
        visited = set[str]()
        order = list[str]()

        def visit(stage_id: str) -> None:
            if stage_id in visited:
                return
            visited.add(stage_id)
            for upstream in self._reverse_dag.get(stage_id, []):
                visit(upstream)
            order.append(stage_id)

        for stage_id in self.job.stages.keys():
            visit(stage_id)
        return order

    def _start_stage_loops(self) -> None:
        if not self.stage_actor_refs:
            return
        started: List[str] = []
        for stage_id, actor_ref in self.stage_actor_refs.items():
            if stage_id in self.stage_run_refs:
                continue
            run_ref = actor_ref.run.remote(poll_interval=self._stage_run_poll_interval)
            self.stage_run_refs[stage_id] = run_ref
            started.append(stage_id)
        if started:
            self.logger.debug("Started stage run loops for: %s", ", ".join(sorted(started)))

    def _check_stage_run_refs(self) -> None:
        if not self.stage_run_refs:
            return
        for stage_id, run_ref in list(self.stage_run_refs.items()):
            ready_refs, _ = ray.wait([run_ref], timeout=0)
            if ready_refs:
                try:
                    ray.get(run_ref)
                except Exception as exc:
                    self.logger.exception(f"Stage {stage_id} run loop failed: {exc}")
                    raise
                # else:
                #     self.logger.error(
                #         "Stage %s run loop exited unexpectedly; stopping job", stage_id
                #     )
                #     raise RuntimeError(f"Stage {stage_id} run loop exited unexpectedly")

    def _stop_stage_loops(self) -> None:
        if not self.stage_actor_refs:
            return
        if self.stage_run_refs and self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                "Stopping stage run loops for: %s", ", ".join(sorted(self.stage_run_refs.keys()))
            )
        stop_refs = []
        for actor_ref in self.stage_actor_refs.values():
            stop_refs.append(actor_ref.stop.remote())
        if stop_refs:
            ray.get(stop_refs)

        if self.stage_run_refs:
            try:
                ray.get(list(self.stage_run_refs.values()), timeout=10)
            except Exception:
                pass
        self.stage_run_refs.clear()

    def _is_pipeline_idle(self) -> bool:
        if not self.stage_actor_refs:
            return True

        stage_statuses: Dict[str, StageStatus] = {}
        for stage_id, actor_ref in self.stage_actor_refs.items():
            stage_statuses[stage_id] = ray.get(actor_ref.get_stage_status.remote())

        # Check for failed stages (fail-fast)
        for stage_id, status in stage_statuses.items():
            if status.failed:
                self.logger.error(f"Stage {stage_id} failed: {status.failure_message}")
                raise RuntimeError(f"Stage {stage_id} failed: {status.failure_message}")

        return self._are_stage_statuses_idle(stage_statuses)

    @staticmethod
    def _stage_has_work(status: StageStatus) -> bool:
        return status.pending_splits > 0 or status.active_splits > 0 or status.inflight_results > 0

    @staticmethod
    def _upstreams_finished(status: StageStatus) -> bool:
        if not status.upstream_finished:
            return True
        return all(status.upstream_finished.values())

    @classmethod
    def _are_stage_statuses_idle(cls, stage_statuses: Dict[str, StageStatus]) -> bool:
        if not stage_statuses:
            return True

        for status in stage_statuses.values():
            if not cls._upstreams_finished(status):
                return False
            if cls._stage_has_work(status):
                return False
        return True

    def run(self, poll_interval: float = 0.05, timeout: Optional[float] = None) -> None:
        self.initialize()
        if not self._running:
            ray.get(self.meta_service.start_job.remote())
            self._running = True

        self._start_stage_loops()

        deadline = time.time() + timeout if timeout is not None else None
        last_checkpoint_check = time.time()
        checkpoint_check_interval = 1.0  # Check every 1 second

        try:
            while self._running:
                if deadline is not None and time.time() > deadline:
                    raise TimeoutError(
                        f"Timeout while waiting for job {self.job.job_id} to complete."
                    )
                self._check_stage_run_refs()

                # Periodic checkpoint trigger check
                if (
                    self.job.checkpoint_config.enabled
                    and time.time() - last_checkpoint_check >= checkpoint_check_interval
                ):
                    last_checkpoint_check = time.time()
                    self._maybe_trigger_checkpoint()

                if self._is_pipeline_idle():
                    self.logger.info("All stages idle; stopping job %s", self.job.job_id)
                    self._stop()
                    break

                time.sleep(poll_interval)
        except Exception:
            self._stop()
            raise

    def _maybe_trigger_checkpoint(self) -> None:
        """Check if a checkpoint should be triggered and trigger it if so."""
        if self.checkpoint_manager is None:
            return

        try:
            if not self.checkpoint_manager.should_trigger_checkpoint():
                return

            self.logger.info("Auto-triggering checkpoint for job %s", self.job.job_id)
            checkpoint_id = self.checkpoint_manager.trigger_checkpoint()

            if checkpoint_id:
                # Collect checkpoint data only from registered stages
                registered_stages = self.checkpoint_manager.get_registered_stages()
                for stage_id in registered_stages:
                    actor_ref = self.stage_actor_refs.get(stage_id)
                    if actor_ref is None:
                        continue
                    try:
                        ray.get(actor_ref.trigger_checkpoint.remote(checkpoint_id), timeout=30)
                        data = ray.get(actor_ref.get_checkpoint_data.remote(), timeout=30)
                        if data:
                            from solstice.state.store import StageCheckpointData

                            self.checkpoint_manager.collect_stage_checkpoint(
                                stage_id, StageCheckpointData.from_dict(data)
                            )
                    except Exception as e:
                        self.logger.warning(f"Failed to checkpoint stage {stage_id}: {e}")

                # Finalize checkpoint
                self.checkpoint_manager.finalize_checkpoint()

        except Exception as e:
            self.logger.warning("Error during checkpoint trigger: %s", e)

    def _stop(self) -> None:
        self.logger.debug("Stopping job %s (running=%s)", self.job.job_id, self._running)
        self._stop_stage_loops()
        if self._running and self.meta_service is not None:
            ray.get(self.meta_service.stop_job.remote())
            self._running = False

    def shutdown(self) -> None:
        self._stop()
        self.stage_actor_refs.clear()
        self.stage_run_refs.clear()
        self.meta_service = None
        self.checkpoint_manager = None
        self._initialized = False

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------
    def trigger_checkpoint(self) -> Optional[str]:
        """Manually trigger a checkpoint."""
        if not self._running:
            self.logger.warning("Job %s is not running", self.job.job_id)
            return None

        if self.checkpoint_manager is None:
            return None

        self.logger.info("Triggering checkpoint for job %s", self.job.job_id)
        self._maybe_trigger_checkpoint()
        return self.checkpoint_manager.get_latest_checkpoint_id()

    def restore_from_checkpoint(self, checkpoint_id: Optional[str] = None) -> bool:
        """Restore job state from a checkpoint."""
        if not self._initialized:
            self.initialize()

        if self.checkpoint_manager is None:
            self.logger.error("Checkpoint manager not initialized")
            return False

        if checkpoint_id is None:
            checkpoint_id = self.checkpoint_manager.get_latest_checkpoint_id()
            if not checkpoint_id:
                self.logger.error("No checkpoint available to restore job %s", self.job.job_id)
                return False

        self.logger.info("Restoring job %s from checkpoint %s", self.job.job_id, checkpoint_id)

        # Load checkpoint manifest
        manifest = self.checkpoint_manager.load_checkpoint(checkpoint_id)
        if not manifest:
            self.logger.error("Failed to load checkpoint %s", checkpoint_id)
            return False

        # Restore each stage
        for stage_id, stage_data in manifest.stages.items():
            if stage_id in self.stage_actor_refs:
                try:
                    ray.get(
                        self.stage_actor_refs[stage_id].restore_from_checkpoint.remote(
                            stage_data.to_dict()
                        ),
                        timeout=60,
                    )
                except Exception as e:
                    self.logger.error(f"Failed to restore stage {stage_id}: {e}")
                    return False

        self.logger.info("Successfully restored job %s from %s", self.job.job_id, checkpoint_id)
        return True

    def list_checkpoints(self) -> List[str]:
        """List available checkpoints."""
        if self.checkpoint_manager is None:
            return []
        return self.checkpoint_manager.list_checkpoints()

    def cleanup_checkpoints(self, keep_last_n: int = 5) -> None:
        """Clean up old checkpoints."""
        if self.checkpoint_manager is None:
            return
        self.checkpoint_manager.cleanup_old_checkpoints(keep_last_n)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    def get_status(self) -> Dict[str, Any]:
        if not self._initialized:
            return {
                "job_id": self.job.job_id,
                "is_running": False,
                "initialized": False,
            }

        try:
            status = ray.get(self.meta_service.get_job_status.remote(), timeout=5)
            status["is_running"] = self._running
            return status
        except Exception as exc:
            self.logger.error("Failed to fetch job status: %s", exc)
            return {"job_id": self.job.job_id, "error": str(exc)}

    def get_metrics(self) -> Dict[str, Any]:
        if not self._initialized:
            return {}
        try:
            return ray.get(self.meta_service.collect_all_metrics.remote(), timeout=10)
        except Exception as exc:
            self.logger.error("Failed to collect metrics: %s", exc)
            return {}

    def wait_for_completion(self, timeout: Optional[float] = None) -> None:
        if not self._running:
            self.run()
            return

        deadline = time.time() + timeout if timeout is not None else None
        while self._running:
            self._check_stage_run_refs()
            if self._is_pipeline_idle():
                self._stop()
                break

            time.sleep(0.5)

            if deadline is not None and time.time() > deadline:
                raise TimeoutError(f"Timeout while waiting for job {self.job.job_id} to complete.")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_initialized(self) -> bool:
        return self._initialized
