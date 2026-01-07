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

"""Ray runtime for executing Solstice jobs with queue-based architecture.

Architecture:
- Workers pull directly from upstream queues
- Masters manage their output queue
- Offset-based recovery via queue backends
- Optional autoscaling for dynamic worker management
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

import ray

from solstice.core.job import Job

if TYPE_CHECKING:
    from solstice.core.stage import Stage
from solstice.core.stage_master import (
    StageMaster,
    StageConfig,
)
from solstice.operators.sources.source import SourceMaster
from solstice.core.split_payload_store import RaySplitPayloadStore
from solstice.runtime.autoscaler import SimpleAutoscaler
from solstice.runtime.state_push import StatePushManager, StatePushConfig
from solstice.utils.logging import create_ray_logger


@dataclass
class JobStatus:
    """Status of the entire pipeline."""

    job_id: str
    is_running: bool
    stages: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    start_time: Optional[float] = None
    elapsed_time: float = 0.0
    error: Optional[str] = None


class RayJobRunner:
    """Job runner using queue-based architecture.

    Features:
    - StageMaster for simplified, output-queue only management
    - Workers pull from upstream queues
    - Offset-based recovery via queue backends
    - Async-first design

    Example:
        ```python
        job = Job(job_id="my_job")
        job.add_stage(source_stage)
        job.add_stage(transform_stage)
        job.add_stage(sink_stage)

        runner = RayJobRunner(job)
        await runner.run()
        ```
    """

    def __init__(self, job: Job):
        """Initialize the runner.

        Args:
            job: The job to run (configuration read from job.config)
        """
        self.job = job

        # Read configuration from job.config
        config = job.config
        self.queue_type = config.queue_type
        self.tansu_storage_url = config.tansu_storage_url
        self._ray_init_kwargs = config.ray_init_kwargs or {}

        self.logger = create_ray_logger(f"RayJobRunner-{job.job_id}")

        # SplitPayloadStore - shared across all stages
        self._payload_store: Optional[RaySplitPayloadStore] = None

        # Stage masters (not Ray actors - they manage their own workers)
        self._masters: Dict[str, Union[StageMaster, SourceMaster]] = {}
        self._master_tasks: Dict[str, asyncio.Task] = {}

        # Autoscaler (configured in run())
        self._autoscaler: Optional[SimpleAutoscaler] = None
        self._autoscale_task: Optional[asyncio.Task] = None

        # WebUI
        self._webui = None
        self._webui_port: Optional[int] = None

        # State push manager (encapsulates broker, producer, manager)
        self._state_push = StatePushManager(
            job_id=job.job_id,
            config=StatePushConfig(
                enabled=config.webui.enabled,
                storage_url=config.tansu_storage_url or "memory://state/",
                webui_storage_path=config.webui.storage_path,
            ),
        )

        # State
        self._initialized = False
        self._running = False
        self._start_time: Optional[float] = None
        self._error: Optional[str] = None

        # DAG info
        self._reverse_dag: Dict[str, List[str]] = {}

    def _ensure_ray(self) -> None:
        """Ensure Ray is initialized."""
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, **self._ray_init_kwargs)

    async def initialize(self) -> None:
        """Initialize the pipeline."""
        if self._initialized:
            return

        self._ensure_ray()
        self.logger.info(f"Initializing job {self.job.job_id}")

        # Create SplitPayloadStore - shared across all stages
        self._payload_store = RaySplitPayloadStore(name=f"payload_store_{self.job.job_id}")
        self.logger.info(f"Created SplitPayloadStore for job {self.job.job_id}")

        # Initialize state push infrastructure (if WebUI enabled)
        await self._state_push.start()

        # Build reverse DAG (stage -> its upstreams)
        self._reverse_dag = self.job.build_reverse_dag()

        # Create masters in topological order
        processing_order = self._get_topological_order()

        for stage_id in processing_order:
            stage = self.job.stages[stage_id]
            upstream_ids = self._reverse_dag.get(stage_id, [])
            is_source = not upstream_ids

            # Build config from stage settings (same for source and regular stages)
            config = self._build_stage_config(stage)

            # Set state push config (for WebUI metrics)
            config.state_endpoint = self._state_push.endpoint
            config.state_topic = self._state_push.topic

            if is_source:
                # Source stage: use SourceMaster from operator_config
                master = self._create_source_master(stage, config)
                self._masters[stage_id] = master
                self.logger.info(f"Created {type(master).__name__} for source stage {stage_id}")
            else:
                # Regular stage: use StageMaster
                # Get upstream endpoint and topic
                upstream_id = upstream_ids[0]  # TODO: handle multi-input
                upstream_master = self._masters[upstream_id]

                # Start upstream if needed to get its endpoint
                if not upstream_master._running:
                    await upstream_master.start()

                # Set upstream queue config
                config.upstream_endpoint = upstream_master._output_endpoint
                config.upstream_topic = upstream_master._output_topic

                master = StageMaster(
                    job_id=self.job.job_id,
                    stage=stage,
                    config=config,
                    payload_store=self._payload_store,
                )
                self._masters[stage_id] = master
                self.logger.info(f"Created StageMaster for stage {stage_id}")

        # Wire downstream references for backpressure propagation
        self._wire_downstream_refs()

        # Emit JOB_STARTED event
        await self._state_push.emit_job_started(
            dag_edges=self.job.dag_edges,
            stages=[self._stage_info(s) for s in self.job.stages.values()],
        )

        # Initialize WebUI if enabled
        if self.job.config.webui.enabled:
            await self._initialize_webui()

        self._initialized = True
        self.logger.info(f"Initialized {len(self._masters)} stages")

    def _wire_downstream_refs(self) -> None:
        """Connect masters with their downstream refs so backpressure works."""
        for upstream_id, downstream_ids in self.job.dag_edges.items():
            upstream_master = self._masters.get(upstream_id)
            if upstream_master is None:
                continue

            downstream_refs = {
                downstream_id: self._masters[downstream_id]
                for downstream_id in downstream_ids
                if downstream_id in self._masters
            }
            if downstream_refs:
                upstream_master.set_downstream_stage_refs(downstream_refs)

    def _build_stage_config(self, stage: "Stage") -> StageConfig:
        """Build StageConfig from stage settings including worker resources."""
        worker_res = stage.worker_resources or {}
        return StageConfig(
            queue_type=self.queue_type,
            tansu_storage_url=self.tansu_storage_url,
            min_workers=stage.min_parallelism,
            max_workers=stage.max_parallelism,
            num_cpus=worker_res.get("num_cpus", 1.0),
            num_gpus=worker_res.get("num_gpus", 0.0),
            memory_mb=int(worker_res.get("memory", 0) / (1024**2)),
        )

    def _stage_info(self, stage: "Stage") -> Dict[str, Any]:
        """Get stage info dict for state events."""
        p = stage.parallelism
        return {
            "stage_id": stage.stage_id,
            "operator_type": type(stage.operator_config).__name__,
            "min_parallelism": p[0] if isinstance(p, tuple) else p,
            "max_parallelism": p[1] if isinstance(p, tuple) else p,
        }

    def _create_source_master(self, stage: "Stage", config: StageConfig) -> SourceMaster:
        """Create appropriate SourceMaster for a source stage.

        The source operator_config must have a master_class attribute that
        specifies which SourceMaster class to use.
        """
        operator_config = stage.operator_config

        # Get master_class from operator_config
        master_class = operator_config.master_class
        if master_class is None:
            raise ValueError(
                f"Source stage '{stage.stage_id}' operator_config {type(operator_config).__name__} "
                f"does not have a master_class attribute. "
                f"Source configs must define master_class to specify the SourceMaster to use."
            )

        return master_class(
            job_id=self.job.job_id,
            stage=stage,
            payload_store=self._payload_store,
            config=config,
        )

    def _get_topological_order(self) -> List[str]:
        """Get stages in topological order (sources first)."""
        # Simple BFS from sources
        in_degree = {
            stage_id: len(self._reverse_dag.get(stage_id, [])) for stage_id in self.job.stages
        }

        # Start with sources (no upstreams)
        queue = [s for s, d in in_degree.items() if d == 0]
        result = []

        while queue:
            stage_id = queue.pop(0)
            result.append(stage_id)

            # Find downstream stages
            for downstream_id, upstreams in self._reverse_dag.items():
                if stage_id in upstreams:
                    in_degree[downstream_id] -= 1
                    if in_degree[downstream_id] == 0:
                        queue.append(downstream_id)

        return result

    def _notify_downstream_stages(self, finished_stage_id: str, all_finished: set) -> None:
        """Notify downstream stages that an upstream has finished.

        A downstream stage is notified when ALL its upstreams have finished.
        """
        # Find all stages that have this stage as an upstream
        for stage_id, upstream_ids in self._reverse_dag.items():
            if finished_stage_id in upstream_ids:
                # Check if ALL upstreams of this stage are finished
                all_upstreams_done = all(up_id in all_finished for up_id in upstream_ids)
                if all_upstreams_done and stage_id in self._masters:
                    self._masters[stage_id].notify_upstream_finished()
                    self.logger.info(f"Notified stage {stage_id}: all upstreams finished")

    async def run(self, timeout: Optional[float] = None) -> JobStatus:
        """Run the pipeline until completion.

        Args:
            timeout: Maximum time to wait (seconds), None for no timeout

        Returns:
            Final pipeline status
        """
        if not self._initialized:
            await self.initialize()

        self._running = True
        self._start_time = time.time()
        deadline = time.time() + timeout if timeout else None

        try:
            # Start all masters that haven't been started
            for stage_id, master in self._masters.items():
                if not master._running:
                    await master.start()

            # Create tasks for all master run loops
            for stage_id, master in self._masters.items():
                if stage_id not in self._master_tasks:
                    task = asyncio.create_task(
                        master.run(),
                        name=f"master_{stage_id}",
                    )
                    self._master_tasks[stage_id] = task

            # Start autoscaler if configured
            self._start_autoscaler()

            # Track which stages have finished (for upstream completion notification)
            finished_stages = set()

            # Wait for all masters to complete
            while self._running and self._master_tasks:
                # Check timeout
                if deadline and time.time() > deadline:
                    raise TimeoutError(f"Pipeline timeout after {timeout}s")

                # Check for completed tasks
                done_stages = []
                for stage_id, task in list(self._master_tasks.items()):
                    if task.done():
                        try:
                            result = task.result()
                            self.logger.info(f"Stage {stage_id} completed: {result}")
                        except Exception as e:
                            self._error = f"Stage {stage_id} failed: {e}"
                            self.logger.error(self._error)
                            raise
                        done_stages.append(stage_id)

                for stage_id in done_stages:
                    del self._master_tasks[stage_id]
                    finished_stages.add(stage_id)

                    # Notify downstream stages that this upstream has finished
                    self._notify_downstream_stages(stage_id, finished_stages)

                if not self._master_tasks:
                    break

                await asyncio.sleep(0.1)

            self.logger.info("Pipeline completed successfully")
            return self.get_status()

        except Exception as e:
            self._error = str(e)
            raise
        finally:
            self._running = False
            # Note: Don't stop WebUI here - let caller decide when to stop
            # via explicit stop() call after any wait period

    async def stop(self) -> None:
        """Stop the pipeline."""
        self._running = False

        # Stop autoscaler
        await self._stop_autoscaler()

        # Cancel all running tasks
        for stage_id, task in list(self._master_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._master_tasks.clear()

        # Stop all masters (but don't clean up queues yet)
        for stage_id, master in self._masters.items():
            try:
                await master.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping stage {stage_id}: {e}")

        # Now clean up all queues (after all consumers are done)
        for stage_id, master in self._masters.items():
            try:
                await master.cleanup_queue()
            except Exception as e:
                self.logger.warning(f"Error cleaning up queue for {stage_id}: {e}")

        # Clean up SplitPayloadStore
        if self._payload_store:
            try:
                self._payload_store.clear()
            except Exception as e:
                self.logger.warning(f"Error cleaning up SplitPayloadStore: {e}")
            self._payload_store = None

        # Emit job completed event before stopping state infrastructure
        status = "FAILED" if self._error else "COMPLETED"
        await self._state_push.emit_job_completed(status, self._start_time)

        # Clean up state push infrastructure
        await self._state_push.stop()

        # Stop WebUI
        await self._stop_webui()

        self.logger.info("Pipeline stopped")

    def _start_autoscaler(self) -> None:
        """Start the autoscaler if configured."""
        autoscale_config = self.job.config.autoscale_config
        if autoscale_config is None:
            return

        self._autoscaler = SimpleAutoscaler(autoscale_config)
        self._autoscale_task = asyncio.create_task(
            self._autoscaler.run_loop(self._masters),
            name="autoscaler",
        )
        self.logger.info("Autoscaler started")

    async def _stop_autoscaler(self) -> None:
        """Stop the autoscaler."""
        if self._autoscale_task and not self._autoscale_task.done():
            self._autoscale_task.cancel()
            try:
                await self._autoscale_task
            except asyncio.CancelledError:
                pass
            self._autoscale_task = None

        if self._autoscaler:
            self._autoscaler.stop()
            self._autoscaler = None

    def get_status(self) -> JobStatus:
        """Get current pipeline status."""
        stages = {}
        for stage_id, master in self._masters.items():
            status = master.get_status()
            stages[stage_id] = {
                "worker_count": status.worker_count,
                "output_queue_size": status.output_queue_size,
                "is_running": status.is_running,
                "is_finished": status.is_finished,
                "failed": status.failed,
            }

        elapsed = time.time() - self._start_time if self._start_time else 0

        return JobStatus(
            job_id=self.job.job_id,
            is_running=self._running,
            stages=stages,
            start_time=self._start_time,
            elapsed_time=elapsed,
            error=self._error,
        )

    async def get_status_async(self) -> JobStatus:
        """Get current pipeline status with queue metrics."""
        stages = {}
        for stage_id, master in self._masters.items():
            status = await master.get_status_async()
            stages[stage_id] = {
                "worker_count": status.worker_count,
                "output_queue_size": status.output_queue_size,
                "is_running": status.is_running,
                "is_finished": status.is_finished,
                "failed": status.failed,
            }

        elapsed = time.time() - self._start_time if self._start_time else 0

        return JobStatus(
            job_id=self.job.job_id,
            is_running=self._running,
            stages=stages,
            start_time=self._start_time,
            elapsed_time=elapsed,
            error=self._error,
        )

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    # === WebUI API ===

    def get_dag_edges(self) -> Dict[str, List[str]]:
        """Get the DAG edges for this job."""
        return self.job.dag_edges

    async def get_stages_for_webui(self) -> List[Dict[str, Any]]:
        """Get detailed stage info with metrics for the WebUI.

        Returns list of stage dicts with:
        - stage_id, operator_type, worker_count
        - input_count, output_count (from workers)
        - is_running, is_finished, failed
        - output_queue_size
        """
        stages = []
        for stage_id, master in self._masters.items():
            status = await master.get_status_async()
            metrics = await master.collect_metrics()

            stages.append(
                {
                    "stage_id": stage_id,
                    "operator_type": type(master.stage.operator_config).__name__,
                    "worker_count": status.worker_count,
                    "min_parallelism": master.config.min_workers,
                    "max_parallelism": master.config.max_workers,
                    "input_count": metrics.input_records,
                    "output_count": metrics.output_records,
                    "output_queue_size": status.output_queue_size,
                    "is_running": status.is_running,
                    "is_finished": status.is_finished,
                    "failed": status.failed,
                    "backpressure_active": status.backpressure_active,
                }
            )
        return stages

    # === Autoscaling Manual Intervention API ===

    def set_stage_workers(self, stage_id: str, count: int) -> None:
        """Set a fixed worker count for a stage (manual override).

        This will override automatic scaling decisions for the specified stage.
        Use `clear_stage_workers()` to return to automatic scaling.

        Args:
            stage_id: The stage to configure
            count: Fixed number of workers to maintain
        """
        if not self._autoscaler:
            self.logger.warning("Autoscaler not enabled, ignoring set_stage_workers")
            return
        self._autoscaler.set_fixed_workers(stage_id, count)

    def clear_stage_workers(self, stage_id: str) -> None:
        """Clear manual override, return stage to automatic scaling."""
        if not self._autoscaler:
            return
        self._autoscaler.clear_fixed_workers(stage_id)

    def freeze_stage(self, stage_id: str) -> None:
        """Freeze a stage (disable autoscaling for it)."""
        if not self._autoscaler:
            self.logger.warning("Autoscaler not enabled, ignoring freeze_stage")
            return
        self._autoscaler.freeze_stage(stage_id)

    def unfreeze_stage(self, stage_id: str) -> None:
        """Unfreeze a stage (re-enable autoscaling)."""
        if not self._autoscaler:
            return
        self._autoscaler.unfreeze_stage(stage_id)

    def pause_autoscaling(self) -> None:
        """Pause all automatic scaling decisions."""
        if not self._autoscaler:
            self.logger.warning("Autoscaler not enabled, ignoring pause_autoscaling")
            return
        self._autoscaler.pause()

    def resume_autoscaling(self) -> None:
        """Resume automatic scaling decisions."""
        if not self._autoscaler:
            return
        self._autoscaler.resume()

    def get_autoscale_status(self) -> Dict[str, Any]:
        """Get current autoscaler status and metrics."""
        if not self._autoscaler:
            return {"enabled": False, "reason": "autoscaler not configured"}
        return self._autoscaler.get_status()

    # === WebUI Integration ===

    async def _initialize_webui(self) -> None:
        """Initialize WebUI components.

        - Ensures Portal is running (starts if needed)
        - Creates JobWebUI instance with isolated storage
        - Starts collectors

        Storage Architecture:
        - SlateDB only supports single writer
        - Each job gets its own storage path: {base_path}/{job_id}/{attempt_id}/
        - Portal uses base_path for reading historical archives
        """
        import uuid

        try:
            from solstice.webui.job_webui import JobWebUI
            from solstice.webui.portal import portal_exists, start_portal
            from solstice.webui.storage import JobStorage

            # Generate attempt_id for this run (timestamp + short random suffix)
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            attempt_id = f"{timestamp}_{uuid.uuid4().hex[:4]}"

            # Ensure Portal is running
            if not portal_exists():
                self.logger.info("Starting Solstice Portal...")
                start_portal(
                    storage_path=self.job.config.webui.storage_path,
                    port=self.job.config.webui.port,
                )
            else:
                self.logger.info("Portal already running")

            self._webui_port = self.job.config.webui.port

            # Create isolated storage path for this job attempt
            # This avoids SlateDB single-writer conflicts
            base_path = self.job.config.webui.storage_path.rstrip("/")
            job_storage_path = f"{base_path}/{self.job.job_id}/{attempt_id}"

            storage = JobStorage(job_storage_path)
            self.logger.info(f"WebUI storage at {job_storage_path}")

            # Create JobWebUI with pre-generated attempt_id
            self._webui = JobWebUI(
                self,
                storage,
                prometheus_enabled=self.job.config.webui.prometheus_enabled,
                attempt_id=attempt_id,
            )

            # Start WebUI
            await self._webui.start()

            self.logger.info(
                f"WebUI available at Ray Serve port {self._webui_port}, "
                f"path: /solstice/jobs/{self.job.job_id}/"
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize WebUI: {e}")
            # Don't fail the job if WebUI fails
            self._webui = None

    async def _stop_webui(self) -> None:
        """Stop WebUI components."""
        if self._webui:
            try:
                await self._webui.stop()
                self.logger.info("WebUI stopped")
            except Exception as e:
                self.logger.warning(f"Error stopping WebUI: {e}")
            self._webui = None

    @property
    def webui_port(self) -> Optional[int]:
        """Get WebUI port if available.

        Returns:
            Ray Serve port where WebUI is accessible, or None
        """
        return self._webui_port

    @property
    def webui_path(self) -> Optional[str]:
        """Get WebUI path if available.

        Returns:
            WebUI path (e.g., "/solstice/jobs/{job_id}/"), or None
        """
        if self._webui_port:
            return f"/solstice/jobs/{self.job.job_id}/"
        return None


# Convenience function for simple pipeline execution
async def run_pipeline(
    job: Job,
    timeout: Optional[float] = None,
) -> JobStatus:
    """Run a pipeline and return its status.

    Configuration is read from job.config.

    Example:
        ```python
        job = Job(job_id="my_job", config=JobConfig(queue_type=QueueType.MEMORY))
        # ... add stages ...

        status = await run_pipeline(job)
        print(f"Completed in {status.elapsed_time:.2f}s")
        ```
    """
    runner = RayJobRunner(job)
    return await runner.run(timeout=timeout)
