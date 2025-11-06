"""Stage Master actor for managing a stage"""

import time
import logging
from typing import Any, Dict, List, Optional
import uuid
import ray
from collections import deque

from solstice.core.models import (
    Shard, ShardStatus, WorkerMetrics, Batch, Record, BackpressureSignal
)
from solstice.state.backend import StateBackend
from solstice.core.operator import Operator


@ray.remote
class StageMasterActor:
    """Ray actor that manages a processing stage"""
    
    def __init__(
        self,
        stage_id: str,
        operator_class: type,
        operator_config: Dict[str, Any],
        state_backend: StateBackend,
        worker_resources: Dict[str, float],
        initial_workers: int = 1,
        max_workers: int = 10,
        min_workers: int = 1,
    ):
        self.stage_id = stage_id
        self.operator_class = operator_class
        self.operator_config = operator_config
        self.state_backend = state_backend
        self.worker_resources = worker_resources
        self.max_workers = max_workers
        self.min_workers = min_workers
        
        self.logger = logging.getLogger(f"StageMaster-{stage_id}")
        
        # Worker management
        self.workers: Dict[str, ray.ObjectRef] = {}  # worker_id -> actor ref
        self.worker_metrics: Dict[str, WorkerMetrics] = {}
        self.worker_shards: Dict[str, List[str]] = {}  # worker_id -> shard_ids
        
        # Shard management
        self.shards: Dict[str, Shard] = {}
        self.pending_shards: deque = deque()
        self.shard_assignments: Dict[str, str] = {}  # shard_id -> worker_id
        
        # Data queues
        self.input_queue: deque = deque()
        self.output_buffer: deque = deque()
        
        # Checkpoint state
        self.current_checkpoint_id: Optional[str] = None
        self.checkpoint_handles: Dict[str, Dict[str, Any]] = {}  # worker_id -> handle
        
        # Backpressure
        self.backpressure_active = False
        self.max_queue_size = 1000
        
        # Metrics
        self.total_processed = 0
        self.start_time = time.time()
        
        # Initialize workers
        for i in range(initial_workers):
            self._create_worker()
        
        self.logger.info(
            f"Stage {stage_id} initialized with {initial_workers} workers"
        )
    
    def _create_worker(self) -> str:
        """Create a new worker"""
        worker_id = f"{self.stage_id}_worker_{len(self.workers)}_{uuid.uuid4().hex[:8]}"
        
        # Import worker actor
        from solstice.actors.worker import WorkerActor
        
        # Create operator instance
        operator = self.operator_class(self.operator_config)
        
        # Create worker actor
        worker_ref = WorkerActor.options(**self.worker_resources).remote(
            worker_id=worker_id,
            stage_id=self.stage_id,
            operator=operator,
            state_backend=self.state_backend,
            config=self.operator_config,
        )
        
        self.workers[worker_id] = worker_ref
        self.worker_shards[worker_id] = []
        
        self.logger.info(f"Created worker {worker_id}")
        return worker_id
    
    def _remove_worker(self, worker_id: str) -> None:
        """Remove a worker"""
        if worker_id not in self.workers:
            return
        
        # Shutdown worker
        worker_ref = self.workers[worker_id]
        try:
            ray.get(worker_ref.shutdown.remote(), timeout=10)
        except Exception as e:
            self.logger.error(f"Error shutting down worker {worker_id}: {e}")
        
        # Reassign its shards
        shards = self.worker_shards.get(worker_id, [])
        for shard_id in shards:
            if shard_id in self.shards:
                self.shards[shard_id].status = ShardStatus.PENDING
                self.shards[shard_id].worker_id = None
                self.pending_shards.append(shard_id)
        
        # Remove worker
        del self.workers[worker_id]
        del self.worker_shards[worker_id]
        if worker_id in self.worker_metrics:
            del self.worker_metrics[worker_id]
        
        self.logger.info(f"Removed worker {worker_id}")
    
    def scale_workers(self, target_count: int) -> None:
        """Scale workers to target count"""
        target_count = max(self.min_workers, min(target_count, self.max_workers))
        current_count = len(self.workers)
        
        if target_count > current_count:
            # Scale out
            for _ in range(target_count - current_count):
                self._create_worker()
            self.logger.info(f"Scaled out to {target_count} workers")
        
        elif target_count < current_count:
            # Scale in - remove idle workers
            workers_to_remove = []
            for worker_id in list(self.workers.keys()):
                if len(workers_to_remove) >= (current_count - target_count):
                    break
                # Only remove workers with no assigned shards
                if not self.worker_shards.get(worker_id):
                    workers_to_remove.append(worker_id)
            
            for worker_id in workers_to_remove:
                self._remove_worker(worker_id)
            
            self.logger.info(f"Scaled in to {len(self.workers)} workers")
    
    def add_input_batch(self, batch: Batch) -> None:
        """Add a batch to the input queue"""
        self.input_queue.append(batch)
        
        # Check backpressure
        if len(self.input_queue) > self.max_queue_size:
            self.backpressure_active = True
            self.logger.warning(
                f"Backpressure activated: queue size {len(self.input_queue)}"
            )
    
    def assign_work(self) -> None:
        """Assign pending work to idle workers"""
        if not self.input_queue:
            return
        
        # Find idle workers
        idle_workers = []
        for worker_id in self.workers.keys():
            # Simple heuristic: workers with fewer shards are more idle
            if len(self.worker_shards.get(worker_id, [])) < 2:
                idle_workers.append(worker_id)
        
        if not idle_workers:
            return
        
        # Distribute work
        while self.input_queue and idle_workers:
            batch = self.input_queue.popleft()
            
            # Round-robin assignment
            worker_id = idle_workers[0]
            idle_workers = idle_workers[1:] + [idle_workers[0]]
            
            # Send to worker asynchronously
            worker_ref = self.workers[worker_id]
            result_ref = worker_ref.process_batch.remote(batch)
            
            # Store for later retrieval
            # In a real system, we'd track these and collect results
            
            self.total_processed += len(batch)
    
    def get_output_batch(self) -> Optional[Batch]:
        """Get a batch from the output buffer"""
        if self.output_buffer:
            return self.output_buffer.popleft()
        return None
    
    def trigger_checkpoint(self, checkpoint_id: str) -> None:
        """Trigger checkpoint across all workers"""
        self.logger.info(f"Triggering checkpoint {checkpoint_id}")
        
        self.current_checkpoint_id = checkpoint_id
        self.checkpoint_handles = {}
        
        # Send barrier to all workers
        barrier_refs = []
        for worker_id, worker_ref in self.workers.items():
            barrier_ref = worker_ref.handle_barrier.remote(checkpoint_id)
            barrier_refs.append((worker_id, barrier_ref))
        
        # Wait for barriers to be processed
        for worker_id, barrier_ref in barrier_refs:
            try:
                ray.get(barrier_ref, timeout=30)
            except Exception as e:
                self.logger.error(
                    f"Error sending barrier to worker {worker_id}: {e}"
                )
    
    def collect_checkpoints(self) -> List[Dict[str, Any]]:
        """Collect checkpoint handles from all workers"""
        if not self.current_checkpoint_id:
            return []
        
        handles = []
        collect_refs = []
        
        for worker_id, worker_ref in self.workers.items():
            ref = worker_ref.create_checkpoint.remote()
            collect_refs.append((worker_id, ref))
        
        # Collect handles
        for worker_id, ref in collect_refs:
            try:
                handle = ray.get(ref, timeout=60)
                if handle:
                    handles.append(handle)
                    self.checkpoint_handles[worker_id] = handle
            except Exception as e:
                self.logger.error(
                    f"Error collecting checkpoint from worker {worker_id}: {e}"
                )
        
        self.logger.info(
            f"Collected {len(handles)} checkpoint handles for {self.current_checkpoint_id}"
        )
        
        return handles
    
    def restore_from_checkpoint(self, checkpoint_id: str) -> None:
        """Restore stage from checkpoint"""
        self.logger.info(f"Restoring stage {self.stage_id} from checkpoint {checkpoint_id}")
        
        # Restore each worker
        restore_refs = []
        for worker_id, worker_ref in self.workers.items():
            ref = worker_ref.restore_from_checkpoint.remote(checkpoint_id)
            restore_refs.append(ref)
        
        # Wait for restoration
        ray.get(restore_refs)
        
        self.logger.info(f"Restored stage {self.stage_id} from checkpoint {checkpoint_id}")
    
    def collect_metrics(self) -> Dict[str, Any]:
        """Collect metrics from all workers"""
        metric_refs = []
        for worker_id, worker_ref in self.workers.items():
            ref = worker_ref.get_metrics.remote()
            metric_refs.append((worker_id, ref))
        
        # Collect metrics
        for worker_id, ref in metric_refs:
            try:
                metrics = ray.get(ref, timeout=5)
                self.worker_metrics[worker_id] = WorkerMetrics(**metrics)
            except Exception as e:
                self.logger.warning(
                    f"Failed to collect metrics from worker {worker_id}: {e}"
                )
        
        # Aggregate metrics
        total_rate = sum(m.processing_rate for m in self.worker_metrics.values())
        
        return {
            'stage_id': self.stage_id,
            'worker_count': len(self.workers),
            'total_processed': self.total_processed,
            'total_processing_rate': total_rate,
            'input_queue_size': len(self.input_queue),
            'output_buffer_size': len(self.output_buffer),
            'backpressure_active': self.backpressure_active,
            'uptime_secs': time.time() - self.start_time,
        }
    
    def handle_worker_failure(self, worker_id: str) -> None:
        """Handle a worker failure"""
        self.logger.warning(f"Handling failure of worker {worker_id}")
        
        # Remove the failed worker
        self._remove_worker(worker_id)
        
        # Create a replacement
        self._create_worker()
    
    def get_backpressure_signal(self) -> Optional[BackpressureSignal]:
        """Get backpressure signal if active"""
        if self.backpressure_active:
            # Calculate slow down factor based on queue size
            queue_ratio = len(self.input_queue) / self.max_queue_size
            slow_down = max(0.0, 1.0 - queue_ratio)
            
            return BackpressureSignal(
                from_stage=self.stage_id,
                to_stage="",  # Will be filled by caller
                slow_down_factor=slow_down,
                reason=f"Queue size {len(self.input_queue)}/{self.max_queue_size}",
            )
        
        # Clear backpressure if queue is back to normal
        if len(self.input_queue) < self.max_queue_size * 0.5:
            self.backpressure_active = False
        
        return None
    
    def health_check(self) -> bool:
        """Health check"""
        return True
    
    def shutdown(self) -> None:
        """Shutdown the stage"""
        self.logger.info(f"Shutting down stage {self.stage_id}")
        
        # Shutdown all workers
        for worker_id in list(self.workers.keys()):
            self._remove_worker(worker_id)

