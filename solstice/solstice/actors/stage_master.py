"""Stage Master actor orchestrating pipelined split execution."""

from __future__ import annotations

import logging
import time
import uuid
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

import ray  # type: ignore[import]
import ray.actor  # type: ignore[import]

from solstice.core.models import (
    BackpressureSignal,
    Batch,
    Split,
    SplitStatus,
    WorkerMetrics,
)
from solstice.state.backend import StateBackend
from solstice.state.manager import StateManager

EPHEMERAL_METADATA_KEYS = {"batch_id", "worker_id", "task_id"}
BACKPRESSURE_QUEUE_RATIO_THRESHOLD = 0.7

@ray.remote(max_concurrency=4)
class StageMasterActor:
    """Ray actor that manages split scheduling for a single stage."""

    def __init__(
        self,
        job_id: str,
        stage_id: str,
        operator_class: type,
        operator_config: Dict[str, Any],
        state_backend: StateBackend,
        worker_resources: Dict[str, float],
        actor_name: Optional[str] = None,
        max_workers: int = 16,
        min_workers: int = 1,
        max_active_splits_per_worker: int = 2,
        queue_capacity: int = 1000,
        upstream_stages: Optional[List[str]] = None,
    ):
        self.job_id = job_id
        self.stage_id = stage_id
        self.actor_name = actor_name
        self.operator_class = operator_class
        self.operator_config = operator_config
        self.state_backend = state_backend
        self.worker_resources = worker_resources
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.max_active_splits_per_worker = max_active_splits_per_worker
        self.max_queue_size = queue_capacity
        self.upstream_stages = upstream_stages or []

        self.logger = logging.getLogger(f"StageMaster-{stage_id}")

        # Create operator master if needed
        temp_operator = operator_class(operator_config)
        self.operator_master: Optional[Any] = None
        operator_master = temp_operator.create_operator_master(
            job_id=job_id,
            stage_id=stage_id,
            operator_class=operator_class,
            operator_config=operator_config,
        )
        if operator_master is not None:
            self.operator_master = operator_master
            self.logger.info(
                "Created operator master for stage %s", self.stage_id
            )
        
        del temp_operator

        # State & split tracking
        self.state_manager = StateManager(stage_id=stage_id, state_backend=state_backend)
        self.pending_splits: Deque[Split] = deque()
        self.split_payloads: Dict[str, ray.ObjectRef] = {}
        self.splits: Dict[str, Split] = {}
        self.downstream_stage_refs: Dict[str, ray.actor.ActorHandle] = {}
        self.downstream_split_counters: Dict[str, int] = {}

        # Worker management
        self.workers: Dict[str, ray.actor.ActorHandle] = {}
        self.worker_active_counts: Dict[str, int] = {}
        self.worker_metrics: Dict[str, WorkerMetrics] = {}

        # Assignment tracking
        self._pending_results: Dict[ray.ObjectRef, str] = {}
        self._split_to_worker: Dict[str, str] = {}

        # Runtime bookkeeping
        self.backpressure_active = False
        self.input_records = 0
        self.output_records = 0
        self.start_time = time.time()
        self._running = False
        self.current_checkpoint_id: Optional[str] = None

        # Spawn initial workers
        for _ in range(self.min_workers):
            self._create_worker()

        self.logger.info(
            "Stage %s initialised with %d workers (job=%s)",
            self.stage_id,
            len(self.workers),
            self.job_id,
        )

    # ------------------------------------------------------------------
    # Worker management
    # ------------------------------------------------------------------
    def _create_worker(self) -> str:
        worker_id = f"{self.stage_id}_worker_{len(self.workers)}_{uuid.uuid4().hex[:6]}"
        from solstice.actors.worker import StageWorker

        worker_ref = StageWorker.options(**self.worker_resources).remote(
            worker_id=worker_id,
            stage_id=self.stage_id,
            operator_class=self.operator_class,
            operator_config=self.operator_config,
        )

        self.workers[worker_id] = worker_ref
        self.worker_active_counts[worker_id] = 0
        self.logger.debug("Created StageWorker %s for stage %s", worker_id, self.stage_id)
        return worker_id

    def _remove_worker(self, worker_id: str) -> None:
        worker_ref = self.workers.pop(worker_id, None)
        if not worker_ref:
            return
        self.worker_active_counts.pop(worker_id, None)
        self.worker_metrics.pop(worker_id, None)
        try:
            ray.get(worker_ref.shutdown.remote(), timeout=10)
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("Failed to shutdown worker %s cleanly: %s", worker_id, exc)

    def scale_workers(self, target_count: int) -> None:
        target_count = max(self.min_workers, min(target_count, self.max_workers))
        current = len(self.workers)
        if target_count == current:
            return
        if target_count > current:
            for _ in range(target_count - current):
                self._create_worker()
            self.logger.info("Scaled stage %s out to %d workers", self.stage_id, target_count)
            return
        removable = max(0, current - target_count)
        for worker_id in list(self.workers.keys()):
            if removable <= 0:
                break
            if self.worker_active_counts.get(worker_id, 0) == 0:
                self._remove_worker(worker_id)
                removable -= 1
        self.logger.info("Scaled stage %s in to %d workers", self.stage_id, len(self.workers))

    # ------------------------------------------------------------------
    # Downstream wiring
    # ------------------------------------------------------------------
    def configure_downstream(self, downstream: Dict[str, ray.actor.ActorHandle]) -> None:
        self.downstream_stage_refs = dict(downstream)
        for stage_id in downstream:
            self.downstream_split_counters.setdefault(stage_id, 0)
        self.logger.info(
            "Stage %s connected to downstream stages: %s",
            self.stage_id,
            ", ".join(sorted(downstream.keys())) or "<none>",
        )

    # ------------------------------------------------------------------
    # Split lifecycle
    # ------------------------------------------------------------------
    def enqueue_split(self, split: Split, payload_ref: Optional[ray.ObjectRef] = None) -> None:
        """Receive a new split from upstream (or create one for source stages)."""
        split.metadata = self._sanitize_metadata(split.metadata)
        split.status = SplitStatus.PENDING
        self.splits[split.split_id] = split
        if payload_ref is not None:
            self.split_payloads[split.split_id] = payload_ref
        self.pending_splits.append(split)
        
        # Activate split state (for checkpointing)
        self.state_manager.activate_split(split.split_id, metadata=split.metadata)

        if len(self.pending_splits) >= self.max_queue_size and not self.backpressure_active:
            self.backpressure_active = True
            self.logger.warning(
                "Backpressure activated for stage %s (queue=%d)", self.stage_id, len(self.pending_splits)
            )

    def _sanitize_metadata(self, metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        clean = dict(metadata or {})
        for key in EPHEMERAL_METADATA_KEYS:
            clean.pop(key, None)
        return clean

    # ------------------------------------------------------------------
    # Run loop
    # ------------------------------------------------------------------
    def run(self, poll_interval: float = 0.05) -> None:
        if self._running:
            return
        self._running = True
        self.logger.info("Stage %s run loop started", self.stage_id)
        try:
            while self._running:
                # Request splits from operator master if available
                if self.operator_master is not None:
                    self._request_splits_from_master()
                
                self._schedule_pending_splits()
                self._drain_completed_results(timeout=0.0)
                time.sleep(poll_interval)
        finally:
            self.logger.info("Stage %s run loop stopped", self.stage_id)

    def stop(self) -> None:
        self._running = False

    def _request_splits_from_master(self) -> None:
        """Request splits from operator master."""
        if self.operator_master is None:
            return
        
        # Check if we have capacity
        available_capacity = self.max_queue_size - len(self.pending_splits)
        if available_capacity <= 0:
            return
        
        splits = self.operator_master.on_split_requested(max_count=available_capacity)
        for split in splits:
            self.enqueue_split(split, payload_ref=None)
    
    def _schedule_pending_splits(self) -> None:
        while self.pending_splits:
            worker_id = self._select_worker()
            if worker_id is None:
                break
            split = self.pending_splits.popleft()
            split.status = SplitStatus.RUNNING
            worker_ref = self.workers[worker_id]
            
            # For source stages, no payload_ref needed
            payload_ref = self.split_payloads.get(split.split_id)
            
            result_ref = worker_ref.process_split.remote(
                split, 
                payload_ref=payload_ref,
            )
            self._pending_results[result_ref] = split.split_id
            self._split_to_worker[split.split_id] = worker_id
            self.worker_active_counts[worker_id] += 1

        if len(self.pending_splits) < self.max_queue_size * BACKPRESSURE_QUEUE_RATIO_THRESHOLD:
            self.backpressure_active = False

    def _select_worker(self) -> Optional[str]:
        candidates = [
            (worker_id, active)
            for worker_id, active in self.worker_active_counts.items()
            if active < self.max_active_splits_per_worker
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[1])
        return candidates[0][0]

    def _drain_completed_results(self, timeout: float) -> None:
        if not self._pending_results:
            return

        pending_refs = list(self._pending_results.keys())
        ready_refs, _ = ray.wait(
            pending_refs,
            num_returns=len(pending_refs),
            timeout=timeout,
        )

        for ref in ready_refs:
            split_id = self._pending_results.pop(ref, None)
            if split_id is None:
                continue
            worker_id = self._split_to_worker.pop(split_id, None)
            if worker_id in self.worker_active_counts:
                self.worker_active_counts[worker_id] = max(
                    0, self.worker_active_counts[worker_id] - 1
                )
            result = ray.get(ref, timeout=5)
            self._handle_worker_result(split_id, result)

    def _requeue_split(self, split_id: str) -> None:
        split = self.splits.get(split_id)
        if not split:
            return
        split.status = SplitStatus.PENDING
        self.pending_splits.appendleft(split)

    def _handle_worker_result(self, split_id: str, result: Dict[str, Any]) -> None:
        split = self.splits.pop(split_id, None)
        self.split_payloads.pop(split_id, None)
        if not split:
            return

        metrics_payload = result.get("metrics")
        self.worker_metrics[result["worker_id"]] = WorkerMetrics(**metrics_payload)

        output_ref = result.get("output_ref")
        split.status = SplitStatus.COMPLETED
        
        self.input_records += split.record_count
        if output_ref is not None:
            output_batch = ray.get(output_ref, timeout=1)
            if output_batch is not None:
                self.output_records += len(output_batch)
        
        # Clear split state after processing
        self.state_manager.clear_split(split_id)

        self.operator_master.on_split_completed(split_id)
        
        if output_ref and self.downstream_stage_refs:
            self._fan_out_downstream(split, output_ref)

    def _fan_out_downstream(self, split: Split, output_ref: ray.ObjectRef) -> None:
        for downstream_id, actor_ref in self.downstream_stage_refs.items():
            next_id = self._next_downstream_split_id(downstream_id)
            downstream_split = split.with_output(
                target_stage_id=downstream_id,
                split_id=next_id,
                metadata=self._sanitize_metadata(split.metadata),
            )
            actor_ref.enqueue_split.remote(downstream_split, output_ref)

    def _next_downstream_split_id(self, downstream_stage: str) -> str:
        counter = self.downstream_split_counters.get(downstream_stage, 0)
        self.downstream_split_counters[downstream_stage] = counter + 1
        return f"{downstream_stage}_split_{counter}"

    # ------------------------------------------------------------------
    # Checkpointing (state managed by StageMaster)
    # ------------------------------------------------------------------
    def trigger_checkpoint(self, checkpoint_id: str) -> None:
        self.current_checkpoint_id = checkpoint_id
        self.logger.info("Stage %s preparing checkpoint %s", self.stage_id, checkpoint_id)

    def collect_checkpoints(self) -> List[Dict[str, Any]]:
        if not self.current_checkpoint_id:
            return []
        handles = self.state_manager.checkpoint(self.current_checkpoint_id)
        payloads = [
            {
                "checkpoint_id": handle.checkpoint_id,
                "stage_id": handle.stage_id,
                "split_id": handle.split_id,
                "split_attempt": handle.split_attempt,
                "state_path": handle.state_path,
                "offset": handle.offset,
                "size_bytes": handle.size_bytes,
                "timestamp": handle.timestamp,
                "metadata": handle.metadata,
            }
            for handle in handles
        ]
        self.logger.info(
            "Stage %s emitted %d split handles for checkpoint %s",
            self.stage_id,
            len(payloads),
            self.current_checkpoint_id,
        )
        return payloads

    def restore_from_checkpoint(
        self, checkpoint_id: str, handles: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        if not handles:
            self.logger.warning(
                "Stage %s restore requested for %s without handles", self.stage_id, checkpoint_id
            )
            return
        from solstice.core.models import CheckpointHandle

        converted = [
            CheckpointHandle(
                checkpoint_id=handle.get("checkpoint_id", checkpoint_id),
                stage_id=handle["stage_id"],
                split_id=handle["split_id"],
                split_attempt=handle.get("split_attempt", 0),
                state_path=handle["state_path"],
                offset=handle.get("offset", {}),
                size_bytes=handle.get("size_bytes", 0),
                timestamp=handle.get("timestamp", time.time()),
                metadata=self._sanitize_metadata(handle.get("metadata", {})),
            )
            for handle in handles
        ]
        self.state_manager.restore_many(converted)
        self.logger.info(
            "Stage %s restored %d split states from checkpoint %s",
            self.stage_id,
            len(converted),
            checkpoint_id,
        )

    # ------------------------------------------------------------------
    # Metrics & status
    # ------------------------------------------------------------------
    def get_split_counters(self) -> Dict[str, int]:
        return {
            "pending": len(self.pending_splits),
            "active": len(self._split_to_worker),
            "inflight": len(self._pending_results),
            "output": len(self.output_buffer),
        }

    def collect_metrics(self) -> Dict[str, Any]:
        metric_refs = []
        for worker_id, worker_ref in self.workers.items():
            metric_refs.append((worker_id, worker_ref.get_metrics.remote()))

        for worker_id, ref in metric_refs:
            try:
                metrics = ray.get(ref, timeout=5)
                self.worker_metrics[worker_id] = WorkerMetrics(**metrics)
            except Exception:
                continue

        total_rate = sum(metric.processing_rate for metric in self.worker_metrics.values())

        return {
            "stage_id": self.stage_id,
            "worker_count": len(self.workers),
            "input_records": self.input_records,
            "output_records": self.output_records,
            "total_processing_rate": total_rate,
            "pending_splits": len(self.pending_splits),
            "inflight_results": len(self._pending_results),
            "output_buffer_size": len(self.output_buffer),
            "backpressure_active": self.backpressure_active,
            "uptime_secs": time.time() - self.start_time,
        }

    def get_backpressure_signal(self) -> Optional[BackpressureSignal]:
        if not self.backpressure_active:
            return None
        queue_ratio = len(self.pending_splits) / float(self.max_queue_size)
        slow_down = max(0.0, min(1.0, 1.0 - queue_ratio))
        return BackpressureSignal(
            from_stage=self.stage_id,
            to_stage="",
            slow_down_factor=slow_down,
            reason=f"pending_splits={len(self.pending_splits)}",
        )

    def health_check(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        self.logger.info("Shutting down stage %s", self.stage_id)
        self.stop()
        
        # Shutdown operator master if exists
        if self.operator_master is not None:
            try:
                self.operator_master.shutdown()
            except Exception as exc:
                self.logger.warning("Error shutting down operator master: %s", exc)
        
        for worker_id in list(self.workers.keys()):
            self._remove_worker(worker_id)
        self.pending_splits.clear()
        self._pending_results.clear()
        self._split_to_worker.clear()
        self.output_buffer.clear()
