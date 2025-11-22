"""Stage Master actor orchestrating pipelined split execution."""

from __future__ import annotations

from dataclasses import dataclass
import time
import uuid
from collections import deque
from collections import defaultdict
from typing import Any, Deque, Dict, List, Optional, Tuple

import ray 
import ray.actor

from solstice.core.models import (
    BackpressureSignal,
    Split,
    WorkerMetrics,
    StageMetrics,
)
from solstice.state.backend import StateBackend
from solstice.state.manager import StateManager
from solstice.utils.logging import create_ray_logger
from solstice.core.stage import Stage
from solstice.core.worker import ProcessResult

BACKPRESSURE_QUEUE_RATIO_THRESHOLD = 0.7

@dataclass
class StageStatus:
    pending_splits: int
    active_splits: int
    inflight_results: int
    backpressure_active: bool
    upstream_finished: dict[str, bool]

class StageMasterActor:

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        stage: Stage,
        upstream_stages: List[str] | None,
    ):
        self.job_id = job_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.state_backend = state_backend
        self.upstream_stages = upstream_stages

        self.logger = create_ray_logger(f"StageMaster-{self.stage_id}")

        # State & split tracking
        self.state_manager = StateManager(stage_id=self.stage_id, state_backend=state_backend)
        self._pending_splits: Deque[Split] = deque()
        self.max_split_attempts = self.stage.operator_config.get("max_split_attempts", 3)
        self.downstream_stage_refs: Dict[str, ray.actor.ActorHandle] = {}
        self.downstream_split_counters: Dict[str, int] = {}
        self.upstream_finished: dict[str, bool] = {stage_id: False for stage_id in upstream_stages}

        # Worker management
        self.workers: Dict[str, ray.actor.ActorHandle] = {}
        self.worker_active_splits = defaultdict(int)
        self.worker_metrics: Dict[str, WorkerMetrics] = {}
        self.max_active_splits_per_worker = self.stage.operator_config.get("max_active_splits_per_worker", 100)

        # Assignment tracking
        self._inflight_results: Dict[ray.ObjectRef, Split] = {}
        self._split_to_worker: Dict[str, str] = {}

        # Runtime bookkeeping
        self.backpressure_active = False
        self.start_time = time.time()
        self._running = False
        self.current_checkpoint_id: Optional[str] = None
        self.max_queue_size = self.stage.operator_config.get("max_queue_size", 1000)

        # Spawn initial workers
        for _ in range(self.stage.min_parallelism):
            self._create_worker()

        self.logger.info(
            f"Stage {self.stage_id} initialised with {len(self.workers)} workers (job={self.job_id})",
        )

    # ------------------------------------------------------------------
    # Worker management
    # ------------------------------------------------------------------
    def _create_worker(self) -> str:
        worker_id = f"{self.stage_id}_worker_{len(self.workers)}_{uuid.uuid4().hex[:6]}"
        from solstice.core.worker import StageWorker

        worker_name = f"{self.stage_id}:{worker_id}"
        worker_ref = StageWorker.options(
            name=worker_name,
            **self.stage.worker_resources
        ).remote(
            worker_id=worker_id,
            stage=self.stage,
        )

        self.workers[worker_id] = worker_ref
        self.logger.debug("Created StageWorker %s for stage %s", worker_id, self.stage_id)
        return worker_id

    def _remove_worker(self, worker_id: str) -> None:
        worker_ref = self.workers.pop(worker_id, None)
        if not worker_ref:
            return
        self.worker_active_splits[worker_id] = 0
        self.worker_metrics.pop(worker_id, None)
        try:
            ray.get(worker_ref.shutdown.remote(), timeout=10)
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("Failed to shutdown worker %s cleanly: %s", worker_id, exc)
        else:
            self.logger.info(
                f"Removed worker {worker_id} from stage {self.stage_id} (workers={len(self.workers)})",
            )

    def scale_workers(self, target_count: int) -> None:
        target_count = max(self.stage.min_parallelism, min(target_count, self.stage.max_parallelism))
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
        if removable > 0:
            self.logger.debug(
                "Unable to remove %d workers for stage %s because they are busy",
                removable,
                self.stage_id,
            )
        self.logger.info("Scaled stage %s in to %d workers", self.stage_id, len(self.workers))

    # ------------------------------------------------------------------
    # Downstream management, called by upstream stages or runner
    # ------------------------------------------------------------------
    def configure_downstream(self, downstream: Dict[str, ray.actor.ActorHandle]) -> None:
        self.downstream_stage_refs = dict(downstream)
        for stage_id in downstream:
            self.downstream_split_counters.setdefault(stage_id, 0)
        self.logger.info( 
            f"Stage {self.stage_id} connected to downstream stages: {', '.join(sorted(downstream.keys())) or '<none>'}"
        )

    def enqueue_split(
        self,
        split: Split,
        payload_ref: Optional[ray.ObjectRef] = None,
    ) -> None:
        """Receive a new split from upstream (or create one for source stages)."""
        # payload_ref is intentionally ignored; object references are carried inside split.data_range.
        self._pending_splits.append(split)
        obj_ref = split.data_range.get("object_ref")
        self.logger.debug(
            f"Enqueued split {split.split_id} (object_ref={obj_ref}) "
            f"(pending={len(self._pending_splits)})"
        )

        self.state_manager.activate_split(split.split_id)

        if len(self._pending_splits) >= self.max_queue_size and not self.backpressure_active:
            self.backpressure_active = True
            self.logger.warning(
                f"Backpressure activated for stage {self.stage_id} (queue={len(self._pending_splits)})",
            )

    def set_upstream_finished(self, upstream_stage_id: str) -> None:
        self.upstream_finished[upstream_stage_id] = True

    # ------------------------------------------------------------------
    # Run loop
    # ------------------------------------------------------------------
    def run(self, poll_interval: float = 0.05) -> bool:
        if self._running:
            return False
        self._running = True
        self.logger.info("Stage %s run loop started", self.stage_id)
        try:
            need_running = lambda: self._running and \
                    (not all(self.upstream_finished.values()) or \
                        len(self._pending_splits) > 0 or \
                        len(self._inflight_results) > 0)
            while need_running():
                self._schedule_pending_splits()
                self._drain_completed_results(timeout=poll_interval*2)
                time.sleep(poll_interval)
            for actor_ref in self.downstream_stage_refs.values():
                actor_ref.set_upstream_finished.remote(self.stage_id)
            self._running = False
            self.logger.info("Stage %s run loop stopped", self.stage_id)
            return True
        finally:
            self.logger.info("Stage %s run loop stopped", self.stage_id)
            return False

    def _schedule_pending_splits(self) -> None:
        while self._pending_splits:
            worker_id = self._select_worker()
            if worker_id is None:
                self.logger.debug(f"No worker available for stage {self.stage_id}")
                break
            split = self._pending_splits.popleft()
            self.worker_active_splits[worker_id] = self.worker_active_splits[worker_id] + 1
            worker_ref = self.workers[worker_id]
            self.logger.debug(
                f"Worker {worker_id} selected for split {split.split_id}, "
                f"pending={len(self._pending_splits)}, inflight={len(self._inflight_results)}"
            )
            process_result_ref = worker_ref.process_split.remote(split)
            self._inflight_results[process_result_ref] = split
            self._split_to_worker[split.split_id] = worker_id
        if len(self._pending_splits) < self.max_queue_size * BACKPRESSURE_QUEUE_RATIO_THRESHOLD:
            self.backpressure_active = False

    def _select_worker(self) -> Optional[str]:
        candidates = [(worker_id, self.worker_active_splits[worker_id]) for worker_id in self.workers.keys()]
        candidates = [item for item in candidates if item[1] < self.max_active_splits_per_worker]
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[1])
        return candidates[0][0]

    def _drain_completed_results(self, timeout: float) -> None:
        if not self._inflight_results:
            return

        pending_refs = list(self._inflight_results.keys())
        ready_refs, _ = ray.wait(
            pending_refs,
            num_returns=len(pending_refs),
            timeout=timeout,
        )

        if ready_refs:
            self.logger.debug(f"Stage {self.stage_id} draining {len(ready_refs)} completed results")
        for ref in ready_refs:
            split = self._inflight_results.pop(ref, None)
            if split is None:
                self.logger.error(f"Result {ref} not found in inflight results")
                continue
            split_id = split.split_id
            worker_id = self._split_to_worker.pop(split_id, None)
            self.worker_active_splits[worker_id] = self.worker_active_splits[worker_id] - 1
            try:
                process_result = ray.get(ref, timeout=5)
            except Exception as exc:
                self.logger.error(
                    f"Stage {self.stage_id} failed to fetch result for split {split_id} from worker {worker_id}: {exc}",
                )
                if not self._requeue_split(split):
                    raise
                continue
            self.logger.debug(f"Stage {self.stage_id} received result for split {split_id} from worker {worker_id}")
            self._handle_worker_result(process_result)

    def _requeue_split(self, split: Split) -> bool:
        split.attempt += 1
        split_id = split.split_id
        if split.attempt > self.max_split_attempts:
            self.logger.error(f"Split {split_id} has exceeded the maximum number of attempts ({self.max_split_attempts}), giving up")
            return False
        self._pending_splits.append(split)
        self.logger.info(f"Requeued split {split_id} for stage {self.stage_id} (pending={len(self._pending_splits)})")
        return True

    def _handle_worker_result(self, process_result: ProcessResult) -> None:
        input_split_id = process_result.input_split_id
        # Clear split state after processing
        self.state_manager.clear_split(input_split_id)

        self.logger.debug(
            f"Stage {self.stage_id} handling result for input split {input_split_id} "
            f"and output split {process_result.output_split.split_id}, "
            f"output_records={process_result.output_records}, "
            f"downstreams are {list(self.downstream_stage_refs.keys())}"
        )
        if process_result.output_split and self.downstream_stage_refs:
            object_ref = process_result.output_split.data_range.get("object_ref")
            if object_ref:
                self.logger.debug(
                    f"Stage {self.stage_id} forwarding split {process_result.output_split.split_id} "
                    f"with object_ref={object_ref} to downstream stages"
                )
            self._fan_out_downstream(process_result.output_split)
        worker_metrics = process_result.worker_metrics
        self.worker_active_splits[worker_metrics.worker_id] = self.worker_active_splits[worker_metrics.worker_id] - 1
        self.logger.debug(
            f"Completed split {input_split_id} on worker {worker_metrics.worker_id} (input={worker_metrics.input_records}, output={worker_metrics.output_records})",
        )
        self.worker_metrics[worker_metrics.worker_id] = worker_metrics

    def _fan_out_downstream(self, split: Split) -> None:
        for downstream_id, actor_ref in self.downstream_stage_refs.items():
            actor_ref.enqueue_split.remote(split)
            self.logger.debug(
                f"Forwarded split {split.split_id} to downstream {downstream_id}",
            )

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
    def get_stage_status(self) -> StageStatus:
        return StageStatus(
            pending_splits=len(self._pending_splits),
            active_splits=len(self._split_to_worker),
            inflight_results=len(self._inflight_results),
            backpressure_active=self.backpressure_active,
            upstream_finished=self.upstream_finished,
        )

    def collect_metrics(self) -> StageMetrics:
        metric_refs = []
        for worker_id, worker_ref in self.workers.items():
            metric_refs.append((worker_id, worker_ref.get_metrics.remote()))

        for worker_id, ref in metric_refs:
            try:
                metrics = ray.get(ref, timeout=5)
                self.worker_metrics[worker_id] = metrics
            except Exception:
                continue


        return StageMetrics(
            stage_id=self.stage_id,
            worker_count=len(self.workers),
            input_records=sum(metric.input_records for metric in self.worker_metrics.values()),
            output_records=sum(metric.output_records for metric in self.worker_metrics.values()),
            total_processing_time=sum(metric.processing_time for metric in self.worker_metrics.values()),
            pending_splits=len(self._pending_splits),
            inflight_results=len(self._inflight_results),
            backpressure_active=self.backpressure_active,
            uptime_secs=time.time() - self.start_time,
        )

    def get_backpressure_signal(self) -> Optional[BackpressureSignal]:
        if not self.backpressure_active:
            return None
        queue_ratio = len(self._pending_splits) / float(self.max_queue_size)
        slow_down = max(0.0, min(1.0, 1.0 - queue_ratio))
        return BackpressureSignal(
            from_stage=self.stage_id,
            to_stage="",
            slow_down_factor=slow_down,
            reason=f"pending_splits={len(self._pending_splits)}",
        )

    def health_check(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        self.logger.info("Shutting down stage %s", self.stage_id)
        self._running = False

        for worker_id in list(self.workers.keys()):
            self._remove_worker(worker_id)
        self._pending_splits.clear()
        self._inflight_results.clear()
        self._split_to_worker.clear()
        self.logger.info("Stage %s shutdown complete", self.stage_id)

    def stop(self) -> None:
        self._running = False
