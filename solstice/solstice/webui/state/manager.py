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

"""Job state manager for consuming and aggregating state.

JobStateManager consumes state messages from Tansu and maintains
an in-memory view of the job's current state. It handles:
- Time-window aggregation for metrics
- Gap filling for missing data
- Snapshot to SlateDB for history
- Query API for WebUI
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from solstice.webui.state.messages import StateMessage, StateMessageType
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.queue import QueueClient
    from solstice.webui.storage import JobStorage


@dataclass
class WorkerState:
    """Current state of a worker."""

    worker_id: str
    stage_id: str
    status: str = "RUNNING"  # RUNNING, STOPPED
    start_time: float = 0.0
    end_time: Optional[float] = None

    # Latest metrics
    input_records: int = 0
    output_records: int = 0
    processing_time: float = 0.0
    processed_count: int = 0
    assigned_partitions: List[int] = field(default_factory=list)

    # Last update time (for staleness detection)
    last_update: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "input_records": self.input_records,
            "output_records": self.output_records,
            "processing_time": self.processing_time,
            "processed_count": self.processed_count,
            "assigned_partitions": self.assigned_partitions,
            "last_update": self.last_update,
        }


@dataclass
class StageState:
    """Current state of a stage."""

    stage_id: str
    operator_type: str = ""
    status: str = "RUNNING"  # RUNNING, COMPLETED
    start_time: float = 0.0
    end_time: Optional[float] = None

    # Configuration
    min_parallelism: int = 1
    max_parallelism: int = 1

    # Aggregated metrics
    worker_count: int = 0
    input_records: int = 0
    output_records: int = 0
    input_throughput: float = 0.0
    output_throughput: float = 0.0
    queue_lag: int = 0
    backpressure_active: bool = False

    # Partition metrics
    partition_metrics: Dict[int, Any] = field(default_factory=dict)

    # Last update time
    last_update: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "operator_type": self.operator_type,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "min_parallelism": self.min_parallelism,
            "max_parallelism": self.max_parallelism,
            "worker_count": self.worker_count,
            "input_records": self.input_records,
            "output_records": self.output_records,
            "input_throughput": self.input_throughput,
            "output_throughput": self.output_throughput,
            "queue_lag": self.queue_lag,
            "backpressure_active": self.backpressure_active,
            "partition_metrics": self.partition_metrics,
            "last_update": self.last_update,
        }


@dataclass
class JobState:
    """Current state of a job."""

    job_id: str
    status: str = "PENDING"  # PENDING, RUNNING, COMPLETED, FAILED
    start_time: float = 0.0
    end_time: Optional[float] = None

    # DAG structure
    dag_edges: Dict[str, List[str]] = field(default_factory=dict)

    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)

    # Aggregated counts
    stage_count: int = 0
    worker_count: int = 0
    total_input_records: int = 0
    total_output_records: int = 0

    # Last update time
    last_update: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "dag_edges": self.dag_edges,
            "config": self.config,
            "stage_count": self.stage_count,
            "worker_count": self.worker_count,
            "total_input_records": self.total_input_records,
            "total_output_records": self.total_output_records,
            "last_update": self.last_update,
            "duration_ms": int((self.end_time or time.time()) - self.start_time) * 1000
            if self.start_time
            else 0,
        }


class JobStateManager:
    """Manages job state by consuming from Tansu state topic.

    Responsibilities:
    1. Consume state messages from Tansu
    2. Maintain in-memory state view
    3. Time-window aggregation for metrics
    4. Provide query API for WebUI
    5. Snapshot to SlateDB for history

    This replaces:
    - Job metadata in SplitPayloadStore
    - MetricsCollector's ray.get() polling
    - ray_state.py's manual actor discovery

    Usage:
        manager = JobStateManager(job_id, queue_client, state_topic, storage)
        await manager.start()

        # Query current state
        job_info = manager.get_job_info()
        stage_info = manager.get_stage_info(stage_id)

        await manager.stop()
    """

    def __init__(
        self,
        job_id: str,
        queue_client: "QueueClient",
        state_topic: str,
        storage: Optional["JobStorage"] = None,
        window_size_s: float = 1.0,
        max_lag_s: float = 3.0,
        snapshot_interval_s: float = 30.0,
    ):
        """Initialize state manager.

        Args:
            job_id: Job identifier
            queue_client: Tansu queue client for consuming
            state_topic: Topic name to consume from
            storage: Optional SlateDB storage for snapshots
            window_size_s: Time window size for aggregation
            max_lag_s: Maximum wait time for late arrivals
            snapshot_interval_s: Interval for SlateDB snapshots
        """
        self.job_id = job_id
        self.queue_client = queue_client
        self.state_topic = state_topic
        self.storage = storage
        self.window_size_s = window_size_s
        self.max_lag_s = max_lag_s
        self.snapshot_interval_s = snapshot_interval_s

        self.logger = create_ray_logger(f"JobStateManager-{job_id}")

        # In-memory state
        self._job_state = JobState(job_id=job_id)
        self._stage_states: Dict[str, StageState] = {}
        self._worker_states: Dict[str, WorkerState] = {}

        # Exception tracking
        self._exceptions: List[Dict[str, Any]] = []
        self._max_exceptions = 1000

        # Metrics time windows for aggregation
        # window_start -> source_id -> latest metrics
        self._metrics_windows: Dict[float, Dict[str, StateMessage]] = {}

        # Background task
        self._running = False
        self._consume_task: Optional[asyncio.Task] = None
        self._last_snapshot_time = 0.0

    async def start(self) -> None:
        """Start consuming from state topic."""
        if self._running:
            return

        self._running = True
        self._consume_task = asyncio.create_task(self._consume_loop())
        self.logger.info("JobStateManager started")

    async def stop(self) -> None:
        """Stop consuming and flush final snapshot."""
        self._running = False

        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None

        # Final snapshot
        if self.storage:
            await self._snapshot_to_storage()

        self.logger.info("JobStateManager stopped")

    # =========================================================================
    # Query API
    # =========================================================================

    def get_job_info(self) -> Dict[str, Any]:
        """Get current job state."""
        # Aggregate from stages
        self._job_state.stage_count = len(self._stage_states)
        self._job_state.worker_count = sum(s.worker_count for s in self._stage_states.values())
        self._job_state.total_input_records = sum(
            s.input_records for s in self._stage_states.values()
        )
        self._job_state.total_output_records = sum(
            s.output_records for s in self._stage_states.values()
        )

        result = self._job_state.to_dict()
        result["stages"] = [s.to_dict() for s in self._stage_states.values()]
        return result

    def get_stage_info(self, stage_id: str) -> Optional[Dict[str, Any]]:
        """Get current state for a stage."""
        state = self._stage_states.get(stage_id)
        if not state:
            return None

        result = state.to_dict()
        # Add workers for this stage
        result["workers"] = [
            w.to_dict() for w in self._worker_states.values() if w.stage_id == stage_id
        ]
        return result

    def get_worker_info(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get current state for a worker."""
        state = self._worker_states.get(worker_id)
        return state.to_dict() if state else None

    def list_stages(self) -> List[Dict[str, Any]]:
        """List all stages."""
        return [s.to_dict() for s in self._stage_states.values()]

    def list_workers(self, stage_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List workers, optionally filtered by stage."""
        workers = self._worker_states.values()
        if stage_id:
            workers = [w for w in workers if w.stage_id == stage_id]
        return [w.to_dict() for w in workers]

    def list_exceptions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """List recent exceptions."""
        return self._exceptions[-limit:]

    def is_running(self) -> bool:
        """Check if job is still running."""
        return self._job_state.status == "RUNNING"

    # =========================================================================
    # Message handling
    # =========================================================================

    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        while self._running:
            try:
                records = await self.queue_client.fetch(
                    self.state_topic,
                    max_records=100,
                    timeout_ms=100,
                )

                for record in records:
                    try:
                        message = StateMessage.from_bytes(record.value)
                        await self._handle_message(message)
                    except Exception as e:
                        self.logger.warning(f"Failed to handle message: {e}")

                # Flush completed time windows
                await self._flush_metrics_windows()

                # Periodic snapshot
                now = time.time()
                if now - self._last_snapshot_time >= self.snapshot_interval_s:
                    if self.storage:
                        await self._snapshot_to_storage()
                    self._last_snapshot_time = now

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in consume loop: {e}")
                await asyncio.sleep(0.1)

    async def _handle_message(self, msg: StateMessage) -> None:
        """Handle a single state message."""
        now = time.time()

        match msg.message_type:
            case StateMessageType.JOB_STARTED:
                self._job_state.status = "RUNNING"
                self._job_state.start_time = msg.timestamp
                self._job_state.dag_edges = msg.payload.get("dag_edges", {})
                self._job_state.config = msg.payload.get("config", {})
                self._job_state.last_update = now

                # Initialize stages from payload
                for stage_info in msg.payload.get("stages", []):
                    stage_id = stage_info.get("stage_id")
                    if stage_id:
                        self._stage_states[stage_id] = StageState(
                            stage_id=stage_id,
                            operator_type=stage_info.get("operator_type", ""),
                            min_parallelism=stage_info.get("min_parallelism", 1),
                            max_parallelism=stage_info.get("max_parallelism", 1),
                            start_time=msg.timestamp,
                        )

                # Immediate snapshot so Portal can see the job right away
                await self._snapshot_to_storage()
                self._last_snapshot_time = now

            case StateMessageType.JOB_COMPLETED:
                self._job_state.status = "COMPLETED"
                self._job_state.end_time = msg.timestamp
                self._job_state.last_update = now

            case StateMessageType.JOB_FAILED:
                self._job_state.status = "FAILED"
                self._job_state.end_time = msg.timestamp
                self._job_state.last_update = now

            case StateMessageType.STAGE_STARTED:
                stage_id = msg.source_id
                if stage_id not in self._stage_states:
                    self._stage_states[stage_id] = StageState(stage_id=stage_id)

                state = self._stage_states[stage_id]
                state.status = "RUNNING"
                state.start_time = msg.timestamp
                state.operator_type = msg.payload.get("operator_type", "")
                state.min_parallelism = msg.payload.get("min_parallelism", 1)
                state.max_parallelism = msg.payload.get("max_parallelism", 1)
                state.last_update = now

            case StateMessageType.STAGE_COMPLETED:
                stage_id = msg.source_id
                if stage_id in self._stage_states:
                    state = self._stage_states[stage_id]
                    state.status = "COMPLETED"
                    state.end_time = msg.timestamp
                    state.last_update = now

            case StateMessageType.STAGE_METRICS:
                stage_id = msg.source_id
                if stage_id not in self._stage_states:
                    self._stage_states[stage_id] = StageState(stage_id=stage_id)

                state = self._stage_states[stage_id]
                state.worker_count = msg.payload.get("worker_count", 0)
                state.input_records = msg.payload.get("input_records", 0)
                state.output_records = msg.payload.get("output_records", 0)
                state.input_throughput = msg.payload.get("input_throughput", 0.0)
                state.output_throughput = msg.payload.get("output_throughput", 0.0)
                state.queue_lag = msg.payload.get("queue_lag", 0)
                state.backpressure_active = msg.payload.get("backpressure_active", False)
                state.partition_metrics = msg.payload.get("partition_metrics", {})
                state.last_update = now

            case StateMessageType.WORKER_STARTED:
                worker_id = msg.source_id
                stage_id = msg.payload.get("stage_id", "")

                self._worker_states[worker_id] = WorkerState(
                    worker_id=worker_id,
                    stage_id=stage_id,
                    status="RUNNING",
                    start_time=msg.timestamp,
                    assigned_partitions=msg.payload.get("assigned_partitions", []),
                    last_update=now,
                )

            case StateMessageType.WORKER_STOPPED:
                worker_id = msg.source_id
                if worker_id in self._worker_states:
                    state = self._worker_states[worker_id]
                    state.status = "STOPPED"
                    state.end_time = msg.timestamp
                    state.processed_count = msg.payload.get("processed_count", 0)
                    state.last_update = now

            case StateMessageType.WORKER_METRICS:
                worker_id = msg.source_id
                stage_id = msg.payload.get("stage_id", "")

                if worker_id not in self._worker_states:
                    self._worker_states[worker_id] = WorkerState(
                        worker_id=worker_id,
                        stage_id=stage_id,
                    )

                state = self._worker_states[worker_id]
                state.input_records = msg.payload.get("input_records", 0)
                state.output_records = msg.payload.get("output_records", 0)
                state.processing_time = msg.payload.get("processing_time", 0.0)
                state.processed_count = msg.payload.get("processed_count", 0)
                state.assigned_partitions = msg.payload.get("assigned_partitions", [])
                if msg.payload.get("is_running", True):
                    state.status = "RUNNING"
                state.last_update = now

                # Immediately update stage metrics (aggregate all workers for this stage)
                self._update_stage_metrics_from_workers(stage_id)

                # Add to time window for aggregation
                self._add_to_window(msg)

            case StateMessageType.EXCEPTION:
                self._exceptions.append(
                    {
                        "timestamp": msg.timestamp,
                        "stage_id": msg.payload.get("stage_id"),
                        "worker_id": msg.payload.get("worker_id"),
                        "exception_type": msg.payload.get("exception_type"),
                        "message": msg.payload.get("message"),
                        "stacktrace": msg.payload.get("stacktrace"),
                        "split_id": msg.payload.get("split_id"),
                    }
                )
                # Trim if too many
                if len(self._exceptions) > self._max_exceptions:
                    self._exceptions = self._exceptions[-self._max_exceptions :]

            case StateMessageType.BACKPRESSURE:
                stage_id = msg.source_id
                if stage_id in self._stage_states:
                    state = self._stage_states[stage_id]
                    state.backpressure_active = msg.payload.get("active", False)
                    state.queue_lag = msg.payload.get("queue_lag", 0)
                    state.last_update = now

    def _update_stage_metrics_from_workers(self, stage_id: str) -> None:
        """Aggregate worker metrics to update stage metrics immediately."""
        if stage_id not in self._stage_states:
            return

        stage_state = self._stage_states[stage_id]

        # Sum metrics from all workers belonging to this stage
        total_input = 0
        total_output = 0
        worker_count = 0

        for worker in self._worker_states.values():
            if worker.stage_id == stage_id:
                total_input += worker.input_records
                total_output += worker.output_records
                worker_count += 1

        stage_state.input_records = total_input
        stage_state.output_records = total_output
        stage_state.worker_count = worker_count

    def _get_window_start(self, timestamp: float) -> float:
        """Get the start time of the window containing timestamp."""
        return math.floor(timestamp / self.window_size_s) * self.window_size_s

    def _add_to_window(self, msg: StateMessage) -> None:
        """Add a metrics message to the appropriate time window."""
        window_start = self._get_window_start(msg.timestamp)

        if window_start not in self._metrics_windows:
            self._metrics_windows[window_start] = {}

        # Keep latest value per source_id
        existing = self._metrics_windows[window_start].get(msg.source_id)
        if existing is None or msg.timestamp > existing.timestamp:
            self._metrics_windows[window_start][msg.source_id] = msg

    async def _flush_metrics_windows(self) -> None:
        """Flush completed time windows."""
        now = time.time()

        for window_start in list(self._metrics_windows.keys()):
            window_end = window_start + self.window_size_s

            # Window is complete when: window_end + max_lag has passed
            if now >= window_end + self.max_lag_s:
                window_data = self._metrics_windows.pop(window_start)

                # Aggregate workers by stage
                stage_metrics: Dict[str, Dict[str, Any]] = {}
                for source_id, msg in window_data.items():
                    stage_id = msg.payload.get("stage_id")
                    if stage_id:
                        if stage_id not in stage_metrics:
                            stage_metrics[stage_id] = {
                                "worker_count": 0,
                                "input_records": 0,
                                "output_records": 0,
                            }
                        stage_metrics[stage_id]["worker_count"] += 1
                        stage_metrics[stage_id]["input_records"] += msg.payload.get(
                            "input_records", 0
                        )
                        stage_metrics[stage_id]["output_records"] += msg.payload.get(
                            "output_records", 0
                        )

                # Update stage states with aggregated metrics
                for stage_id, metrics in stage_metrics.items():
                    if stage_id in self._stage_states:
                        state = self._stage_states[stage_id]
                        state.worker_count = metrics["worker_count"]
                        state.input_records = metrics["input_records"]
                        state.output_records = metrics["output_records"]

    async def _snapshot_to_storage(self) -> None:
        """Snapshot current state to SlateDB.

        Stores job state, stage metrics, and worker history.
        This enables Portal to read all job info from storage.
        """
        if not self.storage:
            return

        try:
            now = time.time()

            # Store job state (enables Portal to list running jobs from storage)
            job_info = self.get_job_info()
            job_archive = {
                "job_id": self.job_id,
                "status": self._job_state.status,
                "start_time": self._job_state.start_time,
                "end_time": self._job_state.end_time,
                "last_update": now,
                "config": self._job_state.config,
                "dag_edges": self._job_state.dag_edges,
                "stages": job_info.get("stages", []),
            }
            self.storage.store_job_archive(job_archive)

            # Store metrics snapshot for each stage
            for stage_id, state in self._stage_states.items():
                self.storage.store_metrics_snapshot(
                    stage_id,
                    now,
                    state.to_dict(),
                )

            # Store worker history
            for worker_id, state in self._worker_states.items():
                self.storage.store_worker_history(worker_id, state.to_dict())

            self.logger.debug(f"Snapshot stored at {now}")

        except Exception as e:
            self.logger.warning(f"Failed to snapshot to storage: {e}")
