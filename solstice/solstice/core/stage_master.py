"""Stage Master actor orchestrating pipelined split execution with Pull-based data flow.

.. deprecated::
    This module is deprecated. Use :mod:`solstice.core.stage_master_v2` instead.
    
    The v2 architecture provides:
    - Simpler design: Master only manages output queue
    - Worker pull model: Workers fetch directly from upstream queue
    - Better exactly-once semantics via offset tracking
    - Cleaner code with async-first design
    
    Migration:
        # Old (deprecated)
        from solstice.core.stage_master import StageMasterActor
        
        # New (recommended)
        from solstice.core.stage_master_v2 import StageMasterV2, StageConfigV2
"""

from __future__ import annotations

import warnings

warnings.warn(
    "solstice.core.stage_master is deprecated. Use solstice.core.stage_master_v2 instead.",
    DeprecationWarning,
    stacklevel=2,
)

from dataclasses import dataclass, fields
import time
import uuid
from collections import deque
from collections import defaultdict
from typing import TYPE_CHECKING, Any, ClassVar, Deque, Dict, List, Optional, Tuple, Type, TypeVar

import ray
import ray.actor

from solstice.core.models import Split, WorkerMetrics, StageMetrics
from solstice.core.output_buffer import OutputBuffer
from solstice.state.checkpoint_manager import StageCheckpointTracker
from solstice.utils.logging import create_ray_logger
from solstice.core.worker import ProcessResult

if TYPE_CHECKING:
    from solstice.core.stage import Stage

BACKPRESSURE_QUEUE_RATIO_THRESHOLD = 0.7


T = TypeVar("T", bound="StageMasterActor")


@dataclass
class StageMasterConfig:
    """Base configuration class for stage masters.

    Subclasses should define their configuration fields as dataclass fields,
    and set the `master_class` class variable to the corresponding master class.

    Example:
        @dataclass
        class MyMasterConfig(StageMasterConfig):
            master_class = MyStageMasterActor

            custom_param: str = "default"

        # Usage:
        config = MyMasterConfig(custom_param="value")
        master = config.setup(job_id, state_backend, stage, upstream_stages)
    """

    master_class: ClassVar[Type["StageMasterActor"]]

    # Common config fields with defaults
    max_split_attempts: int = 3
    max_active_splits_per_worker: int = 100
    max_queue_size: int = 1000
    max_output_buffer_size: int = 1000
    max_consumer_lag: int = 500
    fetch_batch_size: int = 100
    fetch_timeout: float = 1.0
    fail_fast: bool = True  # Stop immediately on exception instead of retrying

    def setup(
        self,
        job_id: str,
        stage: "Stage",
        upstream_stages: List[str] | None,
    ) -> "StageMasterActor":
        """Create and return a stage master instance with this configuration.

        Args:
            job_id: The job ID
            stage: The stage this master will manage
            upstream_stages: List of upstream stage IDs

        Returns:
            Configured stage master instance
        """
        return self.master_class(
            job_id=job_id,
            stage=stage,
            upstream_stages=upstream_stages,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary representation."""
        result = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(value, StageMasterConfig):
                result[f.name] = value.to_dict()
            else:
                result[f.name] = value
        return result


@dataclass
class DefaultStageMasterConfig(StageMasterConfig):
    """Default stage master configuration using the standard StageMasterActor."""

    # master_class will be set after StageMasterActor is defined
    pass


@dataclass
class StageStatus:
    pending_splits: int
    active_splits: int
    inflight_results: int
    output_buffer_size: int
    backpressure_active: bool
    upstream_finished: dict[str, bool]
    failed: bool = False
    failure_message: Optional[str] = None


@dataclass
class UpstreamCursor:
    """Tracks pull cursor for an upstream stage."""

    stage_id: str
    cursor: int = 0
    finished: bool = False


class StageMasterActor:
    """Stage master with Pull-based data flow.

    Data flows via pull model:
    - This stage pulls from upstream stages via fetch_splits()
    - Downstream stages pull from this stage's output_buffer

    This eliminates the need for upstream to know about downstream,
    and provides natural backpressure.
    """

    def __init__(
        self,
        job_id: str,
        stage: "Stage",
        upstream_stages: List[str] | None,
    ):
        self.job_id = job_id
        self.stage_id = stage.stage_id
        self.stage = stage
        self.logger = create_ray_logger(f"StageMaster-{self.stage_id}")

        # Config (cache commonly used values)
        config = stage.master_config
        self._max_queue_size = config.max_queue_size
        self._max_split_attempts = config.max_split_attempts
        self._fetch_batch_size = config.fetch_batch_size
        self._fetch_timeout = config.fetch_timeout
        self._max_active_splits_per_worker = config.max_active_splits_per_worker
        self._fail_fast = config.fail_fast

        # Checkpoint tracking
        self.checkpoint_tracker = StageCheckpointTracker(stage_id=self.stage_id)
        self._current_checkpoint_id: Optional[str] = None

        # Input queue
        self._pending_splits: Deque[Split] = deque()

        # Output buffer (for downstream to pull)
        self.output_buffer = OutputBuffer(
            stage_id=self.stage_id,
            max_size=config.max_output_buffer_size,
            max_consumer_lag=config.max_consumer_lag,
        )

        # Upstream (for pulling data)
        self._upstream_refs: Dict[str, ray.actor.ActorHandle] = {}
        self._upstream_cursors: Dict[str, UpstreamCursor] = {
            sid: UpstreamCursor(stage_id=sid) for sid in (upstream_stages or [])
        }

        # Workers
        self._workers: Dict[str, ray.actor.ActorHandle] = {}
        self._worker_active_splits: Dict[str, int] = defaultdict(int)
        self._worker_metrics: Dict[str, WorkerMetrics] = {}

        # Inflight tracking
        self._inflight_results: Dict[ray.ObjectRef, Split] = {}
        self._split_to_worker: Dict[str, str] = {}

        # Runtime state
        self._running = False
        self._backpressure = False
        self._start_time = time.time()

        # Failure state (fail-fast mode)
        self._failed = False
        self._failure_exception: Optional[Exception] = None

        # Spawn initial workers
        for _ in range(self.stage.min_parallelism):
            self._create_worker()

        self.logger.info(f"Stage {self.stage_id} initialized with {len(self._workers)} workers")

    # ------------------------------------------------------------------
    # Worker management
    # ------------------------------------------------------------------
    def _create_worker(self) -> str:
        worker_id = f"{self.stage_id}_worker_{len(self._workers)}_{uuid.uuid4().hex[:6]}"
        from solstice.core.worker import StageWorker

        worker_name = f"{self.stage_id}:{worker_id}"
        worker_ref = StageWorker.options(name=worker_name, **self.stage.worker_resources).remote(
            worker_id=worker_id,
            stage=self.stage,
        )
        self._workers[worker_id] = worker_ref
        return worker_id

    def _remove_worker(self, worker_id: str) -> None:
        worker_ref = self._workers.pop(worker_id, None)
        if not worker_ref:
            return
        self._worker_active_splits.pop(worker_id, None)
        self._worker_metrics.pop(worker_id, None)
        try:
            ray.get(worker_ref.shutdown.remote(), timeout=10)
        except Exception:
            pass

    def _cleanup_workers(self) -> None:
        """Release all workers after run completes."""
        for worker_id in list(self._workers.keys()):
            self._remove_worker(worker_id)

    # ------------------------------------------------------------------
    # Upstream configuration (Pull model)
    # ------------------------------------------------------------------
    def configure_upstream(self, upstream: Dict[str, ray.actor.ActorHandle]) -> None:
        """Configure upstream stage references for pulling data."""
        self._upstream_refs = dict(upstream)
        for stage_id in upstream:
            if stage_id not in self._upstream_cursors:
                self._upstream_cursors[stage_id] = UpstreamCursor(stage_id=stage_id)

    # ------------------------------------------------------------------
    # Output buffer interface (for downstream to pull)
    # ------------------------------------------------------------------
    def fetch_splits(
        self,
        consumer_id: str,
        cursor: int = 0,
        max_splits: int = 100,
    ) -> Tuple[List[Split], int, bool]:
        """Fetch splits from output buffer (called by downstream stages).

        This is the Pull interface - downstream stages call this to get data.

        Args:
            consumer_id: ID of the consumer (downstream stage)
            cursor: Last fetched sequence number
            max_splits: Maximum splits to return

        Returns:
            Tuple of (splits, new_cursor, upstream_finished)
        """
        splits, new_cursor = self.output_buffer.fetch(
            consumer_id=consumer_id,
            cursor=cursor,
            max_splits=max_splits,
        )
        upstream_finished = (
            self.output_buffer.is_upstream_finished()
            and self.output_buffer.is_empty_after(new_cursor)
        )
        return splits, new_cursor, upstream_finished

    # ------------------------------------------------------------------
    # Run loop (Pull-based, synchronous)
    # ------------------------------------------------------------------
    def run(self, poll_interval: float = 0.05) -> bool:
        """Synchronous run loop with Pull-based data fetching."""
        if self._running:
            return False
        self._running = True

        try:
            while self._should_continue():
                self._pull_from_upstreams()
                self._schedule_pending_splits()
                self._drain_completed_results(timeout=poll_interval * 2)
                self.output_buffer.gc()
                time.sleep(poll_interval)

            if self._failed and self._failure_exception:
                raise self._failure_exception

            self.output_buffer.mark_upstream_finished()
            return True
        finally:
            self._running = False
            self._cleanup_workers()

    def _should_continue(self) -> bool:
        """Check if run loop should continue."""
        if self._failed:
            return False
        all_done = (
            all(c.finished for c in self._upstream_cursors.values())
            if self._upstream_cursors
            else True
        )
        has_work = self._pending_splits or self._inflight_results
        return self._running and (not all_done or has_work)

    def _pull_from_upstreams(self) -> None:
        """Pull splits from all upstream stages."""
        if not self._upstream_refs or self._backpressure:
            return

        for upstream_id, actor_ref in self._upstream_refs.items():
            cursor = self._upstream_cursors.get(upstream_id)
            if cursor is None or cursor.finished:
                continue

            try:
                splits, new_cursor, finished = ray.get(
                    actor_ref.fetch_splits.remote(
                        consumer_id=self.stage_id,
                        cursor=cursor.cursor,
                        max_splits=self._fetch_batch_size,
                    ),
                    timeout=self._fetch_timeout,
                )
                cursor.cursor = new_cursor
                if finished:
                    cursor.finished = True
                for split in splits:
                    self._pending_splits.append(split)
            except ray.exceptions.GetTimeoutError:
                pass
            except Exception as exc:
                self.logger.warning(f"Failed to fetch from {upstream_id}: {exc}")

        # Update backpressure
        queue_len = len(self._pending_splits)
        if queue_len >= self._max_queue_size:
            self._backpressure = True
        elif queue_len < self._max_queue_size * BACKPRESSURE_QUEUE_RATIO_THRESHOLD:
            self._backpressure = False

    def _schedule_pending_splits(self) -> None:
        while self._pending_splits:
            worker_id = self._select_worker()
            if worker_id is None:
                break
            split = self._pending_splits.popleft()

            if self.checkpoint_tracker.is_split_completed(split.split_id):
                continue

            self.checkpoint_tracker.mark_split_started(split.split_id)
            self._worker_active_splits[worker_id] += 1
            ref = self._workers[worker_id].process_split.remote(split)
            self._inflight_results[ref] = split
            self._split_to_worker[split.split_id] = worker_id

        if len(self._pending_splits) < self._max_queue_size * BACKPRESSURE_QUEUE_RATIO_THRESHOLD:
            self._backpressure = False

    def _select_worker(self) -> Optional[str]:
        candidates = [
            (wid, self._worker_active_splits[wid])
            for wid in self._workers
            if self._worker_active_splits[wid] < self._max_active_splits_per_worker
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda x: x[1])[0]

    def _drain_completed_results(self, timeout: float) -> None:
        if not self._inflight_results:
            return

        ready_refs, _ = ray.wait(
            list(self._inflight_results.keys()),
            num_returns=len(self._inflight_results),
            timeout=timeout,
        )

        for ref in ready_refs:
            split = self._inflight_results.pop(ref, None)
            if split is None:
                continue

            worker_id = self._split_to_worker.pop(split.split_id, None)
            if worker_id:
                self._worker_active_splits[worker_id] -= 1

            try:
                result = ray.get(ref, timeout=5)
                self._handle_worker_result(result)
            except Exception as exc:
                if self._fail_fast:
                    self._failed = True
                    self._failure_exception = exc
                    return
                if not self._requeue_split(split):
                    raise

    def _requeue_split(self, split: Split) -> bool:
        split.attempt += 1
        self.checkpoint_tracker.mark_split_failed(split.split_id)
        if split.attempt > self._max_split_attempts:
            return False
        self._pending_splits.append(split)
        return True

    def _handle_worker_result(self, result: ProcessResult) -> None:
        self.checkpoint_tracker.mark_split_completed(result.input_split_id)
        if result.output_split:
            self.output_buffer.append(result.output_split)
        self._worker_metrics[result.worker_metrics.worker_id] = result.worker_metrics

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------
    def trigger_checkpoint(self, checkpoint_id: str) -> None:
        """Prepare stage for checkpoint."""
        self._current_checkpoint_id = checkpoint_id

    def get_checkpoint_data(self) -> Dict[str, Any]:
        """Get checkpoint data for this stage."""
        if not self._current_checkpoint_id:
            return {}

        data = self.checkpoint_tracker.prepare_checkpoint(self._current_checkpoint_id)
        result = data.to_dict()
        result["upstream_cursors"] = {
            sid: {"cursor": c.cursor, "finished": c.finished}
            for sid, c in self._upstream_cursors.items()
        }
        return result

    def restore_from_checkpoint(self, checkpoint_data: Optional[Dict[str, Any]] = None) -> None:
        """Restore stage state from checkpoint data."""
        if not checkpoint_data:
            return

        from solstice.state.store import StageCheckpointData

        data = StageCheckpointData.from_dict(checkpoint_data)
        self.checkpoint_tracker.restore_from_checkpoint(data)

        # Restore cursor positions but NOT finished state
        # Upstream stages are new instances on restart, so we reset cursor to 0
        # and mark them as not finished to allow re-pulling
        for sid, cursor_data in checkpoint_data.get("upstream_cursors", {}).items():
            if sid in self._upstream_cursors:
                # Reset cursor to 0 - upstream will regenerate splits
                self._upstream_cursors[sid].cursor = 0
                # Don't restore finished state - upstream is a new instance
                self._upstream_cursors[sid].finished = False

    # ------------------------------------------------------------------
    # Metrics & status
    # ------------------------------------------------------------------
    def get_stage_status(self) -> StageStatus:
        return StageStatus(
            pending_splits=len(self._pending_splits),
            active_splits=len(self._split_to_worker),
            inflight_results=len(self._inflight_results),
            output_buffer_size=len(self.output_buffer),
            backpressure_active=self._backpressure,
            upstream_finished={sid: c.finished for sid, c in self._upstream_cursors.items()}
            if self._upstream_cursors
            else {},
            failed=self._failed,
            failure_message=str(self._failure_exception) if self._failure_exception else None,
        )

    def collect_metrics(self) -> StageMetrics:
        # Collect fresh metrics from workers
        refs = [(wid, w.get_metrics.remote()) for wid, w in self._workers.items()]
        for wid, ref in refs:
            try:
                self._worker_metrics[wid] = ray.get(ref, timeout=5)
            except Exception:
                pass

        metrics = self._worker_metrics.values()
        return StageMetrics(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            input_records=sum(m.input_records for m in metrics),
            output_records=sum(m.output_records for m in metrics),
            total_processing_time=sum(m.processing_time for m in metrics),
            pending_splits=len(self._pending_splits),
            inflight_results=len(self._inflight_results),
            output_buffer_size=len(self.output_buffer),
            backpressure_active=self._backpressure,
            uptime_secs=time.time() - self._start_time,
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        self._running = False
        self.output_buffer.close()
        for worker_id in list(self._workers.keys()):
            self._remove_worker(worker_id)
        self._pending_splits.clear()
        self._inflight_results.clear()
        self._split_to_worker.clear()

    def stop(self) -> None:
        self._running = False


# Set the master_class on DefaultStageMasterConfig after class definition
DefaultStageMasterConfig.master_class = StageMasterActor
