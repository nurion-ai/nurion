"""Ray runtime for executing Solstice jobs with queue-based architecture.

Architecture:
- Workers pull directly from upstream queues
- Masters manage their output queue
- Offset-based recovery via queue backends
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import ray

from solstice.core.job import Job
from solstice.core.stage_master import (
    StageMaster,
    StageConfig,
    QueueEndpoint,
    QueueType,
)
from solstice.utils.logging import create_ray_logger


@dataclass
class PipelineStatus:
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
    
    def __init__(
        self,
        job: Job,
        queue_type: QueueType = QueueType.RAY,
        ray_init_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the runner.
        
        Args:
            job: The job to run
            queue_type: Type of queue backend (RAY for testing, TANSU for production)
            ray_init_kwargs: Arguments to pass to ray.init()
        """
        self.job = job
        self.queue_type = queue_type
        self._ray_init_kwargs = ray_init_kwargs or {}
        
        self.logger = create_ray_logger(f"RunnerV2-{job.job_id}")
        
        # Stage masters (not Ray actors - they manage their own workers)
        self._masters: Dict[str, StageMaster] = {}
        self._master_tasks: Dict[str, asyncio.Task] = {}
        
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
        
        # Build reverse DAG (stage -> its upstreams)
        self._reverse_dag = self.job.build_reverse_dag()
        
        # Create stage masters in topological order
        # First, create all masters without upstream connections
        for stage_id, stage in self.job.stages.items():
            # Get or create stage config
            if hasattr(stage, 'config_v2') and stage.config_v2:
                config = stage.config_v2
            else:
                # Create default config with specified queue type
                config = StageConfig(
                    queue_type=self.queue_type,
                    min_workers=stage.min_parallelism,
                    max_workers=stage.max_parallelism,
                )
            
            master = StageMaster(
                job_id=self.job.job_id,
                stage=stage,
                config=config,
            )
            self._masters[stage_id] = master
        
        # Connect upstreams in topological order
        # This ensures upstream masters are started before downstream
        processing_order = self._get_topological_order()
        
        for stage_id in processing_order:
            master = self._masters[stage_id]
            upstream_ids = self._reverse_dag.get(stage_id, [])
            
            if upstream_ids:
                # Get first upstream's endpoint and topic
                # (for multi-input stages, this would need more complex handling)
                upstream_id = upstream_ids[0]
                upstream_master = self._masters[upstream_id]
                
                # Start upstream to get its endpoint
                if not upstream_master._running:
                    await upstream_master.start()
                
                # Update master with upstream info
                master.upstream_endpoint = upstream_master._output_endpoint
                master.upstream_topic = upstream_master._output_topic
    
        self._initialized = True
        self.logger.info(f"Initialized {len(self._masters)} stages")
    
    def _get_topological_order(self) -> List[str]:
        """Get stages in topological order (sources first)."""
        # Simple BFS from sources
        in_degree = {stage_id: len(self._reverse_dag.get(stage_id, []))
                     for stage_id in self._masters}
        
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
    
    async def run(self, timeout: Optional[float] = None) -> PipelineStatus:
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
                
                if not self._master_tasks:
                    break
                
                await asyncio.sleep(0.1)
            
            self.logger.info(f"Pipeline completed successfully")
            return self.get_status()
            
        except Exception as e:
            self._error = str(e)
            raise
        finally:
            self._running = False
            await self.stop()
    
    async def stop(self) -> None:
        """Stop the pipeline."""
        self._running = False
        
        # Cancel all running tasks
        for stage_id, task in list(self._master_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._master_tasks.clear()
        
        # Stop all masters
        for stage_id, master in self._masters.items():
            try:
                await master.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping stage {stage_id}: {e}")
        
        self.logger.info("Pipeline stopped")
    
    def get_status(self) -> PipelineStatus:
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
        
        return PipelineStatus(
            job_id=self.job.job_id,
            is_running=self._running,
            stages=stages,
            start_time=self._start_time,
            elapsed_time=elapsed,
            error=self._error,
        )
    
    async def get_status_async(self) -> PipelineStatus:
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
        
        return PipelineStatus(
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


# Convenience function for simple pipeline execution
async def run_pipeline(
    job: Job,
    queue_type: QueueType = QueueType.RAY,
    timeout: Optional[float] = None,
) -> PipelineStatus:
    """Run a pipeline and return its status.
    
    Example:
        ```python
        job = Job(job_id="my_job")
        # ... add stages ...
        
        status = await run_pipeline(job)
        print(f"Completed in {status.elapsed_time:.2f}s")
        ```
    """
    runner = RayJobRunner(job, queue_type=queue_type)
    return await runner.run(timeout=timeout)



