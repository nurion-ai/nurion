"""Stage Master actor for managing a stage"""

import time
import logging
from typing import Any, Dict, List, Optional
import uuid
import ray
from collections import deque

from solstice.core.models import Split, SplitStatus, WorkerMetrics, Batch, BackpressureSignal
from solstice.state.backend import StateBackend


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
        self.worker_splits: Dict[str, List[str]] = {}  # worker_id -> split_ids

        # Split management
        self.splits: Dict[str, Split] = {}
        self.pending_splits: deque = deque()
        self.split_assignments: Dict[str, str] = {}  # split_id -> worker_id

        # Data queues
        self.input_queue: deque = deque()
        self.output_buffer: deque = deque()
        self.inflight_batches: Dict[str, ray.ObjectRef] = {}  # batch_id -> result_ref
        self.result_to_batch: Dict[ray.ObjectRef, str] = {}
        self.batch_to_worker: Dict[str, str] = {}
        self.active_batches: Dict[str, Batch] = {}

        # Checkpoint state
        self.current_checkpoint_id: Optional[str] = None
        self.checkpoint_handles: Dict[str, Dict[str, Any]] = {}  # split_id -> handle

        # Backpressure
        self.backpressure_active = False
        self.max_queue_size = 1000

        # Metrics
        self.total_processed = 0
        self.start_time = time.time()

        # Initialize workers
        for i in range(initial_workers):
            self._create_worker()

        self.logger.info(f"Stage {stage_id} initialized with {initial_workers} workers")

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
        self.worker_splits[worker_id] = []

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

        # Reassign its splits
        splits = self.worker_splits.get(worker_id, [])
        for split_id in splits:
            if split_id in self.splits:
                self.splits[split_id].status = SplitStatus.PENDING
                self.splits[split_id].worker_id = None
                self.pending_splits.append(split_id)

        # Remove worker
        del self.workers[worker_id]
        del self.worker_splits[worker_id]
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
                # Only remove workers with no assigned splits
                if not self.worker_splits.get(worker_id):
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
            self.logger.warning(f"Backpressure activated: queue size {len(self.input_queue)}")

    def assign_work(self) -> None:
        """Assign pending work to idle workers"""
        if not self.input_queue:
            return

        # Find idle workers
        idle_workers = []
        for worker_id in self.workers.keys():
            # Simple heuristic: workers with fewer splits are more idle
            if len(self.worker_splits.get(worker_id, [])) < 2:
                idle_workers.append(worker_id)

        if not idle_workers:
            return

        # Distribute work
        while self.input_queue and idle_workers:
            batch = self.input_queue.popleft()
            split_id = batch.source_split or batch.batch_id

            # Round-robin assignment
            worker_id = idle_workers[0]
            idle_workers = idle_workers[1:] + [idle_workers[0]]

            # Send to worker asynchronously
            worker_ref = self.workers[worker_id]
            result_ref = worker_ref.process_batch.remote(batch)

            if split_id not in self.worker_splits[worker_id]:
                self.worker_splits[worker_id].append(split_id)
            self.inflight_batches[batch.batch_id] = result_ref
            self.result_to_batch[result_ref] = batch.batch_id
            self.batch_to_worker[batch.batch_id] = worker_id
            self.active_batches[batch.batch_id] = batch

            self.total_processed += len(batch)

        # Collect any completed work immediately
        self.collect_ready_results(timeout=0.0)

    def collect_ready_results(self, timeout: float = 0.0) -> None:
        """Collect ready results from workers and populate output buffer"""
        if not self.result_to_batch:
            return

        pending_refs = list(self.result_to_batch.keys())
        ready_refs, _ = ray.wait(
            pending_refs,
            num_returns=len(pending_refs),
            timeout=timeout,
        )

        for ref in ready_refs:
            batch_id = self.result_to_batch.pop(ref, None)
            if batch_id is None:
                continue

            self.inflight_batches.pop(batch_id, None)
            batch_payload = self.active_batches.pop(batch_id, None)

            worker_id = self.batch_to_worker.pop(batch_id, None)
            if worker_id:
                split_id = batch_payload.source_split if batch_payload else batch_id
                if split_id in self.worker_splits.get(worker_id, []):
                    self.worker_splits[worker_id].remove(split_id)

            try:
                output_batch = ray.get(ref, timeout=5)
            except Exception as e:
                self.logger.error(f"Failed to fetch processed batch {batch_id}: {e}")
                if batch_payload:
                    self.input_queue.appendleft(batch_payload)
                if worker_id:
                    self.handle_worker_failure(worker_id)
                continue

            if output_batch:
                self.output_buffer.append(output_batch)

    def tick(self, timeout: float = 0.0) -> None:
        """Run a scheduling iteration: assign work then collect results"""
        self.assign_work()
        self.collect_ready_results(timeout=timeout)

    def get_output_batch(self) -> Optional[Batch]:
        """Get a batch from the output buffer"""
        if self.output_buffer:
            return self.output_buffer.popleft()
        return None

    def get_next_output(self, timeout: float = 5.0, poll_interval: float = 0.1) -> Optional[Batch]:
        """Blocking helper used in tests to fetch the next output batch."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            batch = self.get_output_batch()
            if batch is not None:
                return batch
            self.collect_ready_results(timeout=poll_interval)
            time.sleep(poll_interval)
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
                self.logger.error(f"Error sending barrier to worker {worker_id}: {e}")

    def collect_checkpoints(self) -> List[Dict[str, Any]]:
        """Collect checkpoint handles from all workers"""
        if not self.current_checkpoint_id:
            return []

        handles: List[Dict[str, Any]] = []
        collect_refs = []

        for worker_id, worker_ref in self.workers.items():
            ref = worker_ref.create_checkpoint.remote()
            collect_refs.append((worker_id, ref))

        # Collect handles
        for worker_id, ref in collect_refs:
            try:
                result = ray.get(ref, timeout=60)
                if not result:
                    continue

                worker_handles = result.get("handles") if isinstance(result, dict) else result
                if not worker_handles:
                    continue

                for handle in worker_handles:
                    normalized = dict(handle)
                    normalized.setdefault("worker_id", worker_id)
                    split_id = normalized.get("split_id")
                    if split_id:
                        self.checkpoint_handles[split_id] = normalized
                    handles.append(normalized)

                metrics_payload = result.get("metrics") if isinstance(result, dict) else None
                if metrics_payload:
                    try:
                        self.worker_metrics[worker_id] = WorkerMetrics(**metrics_payload)
                    except TypeError:
                        self.logger.debug(
                            "Failed to parse worker metrics from %s: %s", worker_id, metrics_payload
                        )
            except Exception as e:
                self.logger.error(f"Error collecting checkpoint from worker {worker_id}: {e}")

        self.logger.info(
            f"Collected {len(handles)} checkpoint handles for {self.current_checkpoint_id}"
        )

        return handles

    def restore_from_checkpoint(
        self, checkpoint_id: str, handles: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """Restore stage from checkpoint"""
        self.logger.info(f"Restoring stage {self.stage_id} from checkpoint {checkpoint_id}")

        if not handles:
            self.logger.warning("No handles provided for stage restore; skipping")
            return

        worker_ids = list(self.workers.keys())
        if not worker_ids:
            self.logger.warning("No workers available to restore stage %s", self.stage_id)
            return

        assignments: Dict[str, List[Dict[str, Any]]] = {worker_id: [] for worker_id in worker_ids}
        for index, handle in enumerate(handles):
            worker_id = worker_ids[index % len(worker_ids)]
            assignments[worker_id].append(handle)

        restore_refs = []
        for worker_id, assigned_handles in assignments.items():
            if not assigned_handles:
                continue
            worker_ref = self.workers[worker_id]
            self.worker_splits[worker_id] = []
            ref = worker_ref.restore_from_checkpoint.remote(checkpoint_id, assigned_handles)
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
                self.logger.warning(f"Failed to collect metrics from worker {worker_id}: {e}")

        # Aggregate metrics
        total_rate = sum(m.processing_rate for m in self.worker_metrics.values())

        return {
            "stage_id": self.stage_id,
            "worker_count": len(self.workers),
            "total_processed": self.total_processed,
            "total_processing_rate": total_rate,
            "input_queue_size": len(self.input_queue),
            "output_buffer_size": len(self.output_buffer),
            "backpressure_active": self.backpressure_active,
            "uptime_secs": time.time() - self.start_time,
        }

    def handle_worker_failure(self, worker_id: str) -> None:
        """Handle a worker failure"""
        self.logger.warning(f"Handling failure of worker {worker_id}")

        stalled_batches = [
            batch_id for batch_id, owner in self.batch_to_worker.items() if owner == worker_id
        ]

        for batch_id in stalled_batches:
            split_identifier = batch_id
            ref = self.inflight_batches.pop(batch_id, None)
            if ref is not None:
                self.result_to_batch.pop(ref, None)
            batch_payload = self.active_batches.pop(batch_id, None)
            if batch_payload:
                self.input_queue.appendleft(batch_payload)
                split_identifier = batch_payload.source_split or batch_id
            self.batch_to_worker.pop(batch_id, None)
            if (
                worker_id in self.worker_splits
                and split_identifier in self.worker_splits[worker_id]
            ):
                self.worker_splits[worker_id].remove(split_identifier)

        # Clear worker assignment state; worker remains available for future work.
        if worker_id in self.worker_splits:
            self.worker_splits[worker_id].clear()

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
