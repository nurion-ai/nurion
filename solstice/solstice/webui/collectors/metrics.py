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

"""Metrics collector for WebUI."""

import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

import ray

from solstice.webui.storage import JobStorage
from solstice.webui.storage.prometheus_exporter import PrometheusMetricsExporter
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner


class MetricsCollector:
    """Collect metrics from running job and export to Prometheus + SlateDB.

    Responsibilities:
    1. Poll StageMaster for metrics every second
    2. Export to Prometheus for real-time monitoring
    3. Snapshot to SlateDB every 30 seconds for history
    4. Track worker lifecycle and store to SlateDB

    Note: No centralized registry updates. Job metadata is stored in
    the payload_store actor and metrics are obtained via real-time
    queries to workers.

    Usage:
        collector = MetricsCollector(job_runner, storage, prometheus_enabled=True)
        asyncio.create_task(collector.run_loop())
    """

    def __init__(
        self,
        job_runner: "RayJobRunner",
        storage: JobStorage,
        prometheus_enabled: bool = True,
        snapshot_interval_s: float = 30.0,
    ):
        """Initialize metrics collector.

        Args:
            job_runner: RayJobRunner instance to collect from
            storage: SlateDB storage for snapshots
            prometheus_enabled: Whether to export to Prometheus
            snapshot_interval_s: Interval between SlateDB snapshots
        """
        self.job_runner = job_runner
        self.storage = storage
        self.prometheus_enabled = prometheus_enabled
        self.snapshot_interval_s = snapshot_interval_s

        self.job_id = job_runner.job.job_id
        self.logger = create_ray_logger(f"MetricsCollector-{self.job_id}")

        # Prometheus exporter
        self.prometheus: Optional[PrometheusMetricsExporter] = None
        if prometheus_enabled:
            self.prometheus = PrometheusMetricsExporter(self.job_id)

        # State
        self._running = False
        self._last_snapshot_time = 0.0
        self._last_worker_tracking_time = 0.0
        self._last_worker_snapshot_time = 0.0

        # Rate calculation
        self._last_metrics: Dict[str, Dict[str, Any]] = {}
        self._last_poll_time = 0.0

        # Worker tracking
        self._known_workers: Dict[str, Dict[str, Any]] = {}  # worker_id -> worker_data

    async def run_loop(self) -> None:
        """Main collection loop.

        Runs until job completes:
        - Collect metrics every 1 second
        - Export to Prometheus
        - Snapshot to SlateDB every 30 seconds
        - Track workers every 2 seconds
        """
        self._running = True
        self.logger.info("Metrics collector started")

        try:
            while self._running and self.job_runner.is_running:
                await self._collect_and_export()
                await asyncio.sleep(1)  # Poll every second

        except Exception as e:
            self.logger.error(f"Metrics collector error: {e}")
        finally:
            self._running = False
            self.logger.info("Metrics collector stopped")

    def stop(self) -> None:
        """Stop the collector."""
        self._running = False

    async def _collect_and_export(self) -> None:
        """Collect metrics from all stages and export."""
        current_time = time.time()

        for stage_id, master in self.job_runner._masters.items():
            try:
                # Collect metrics
                metrics = await master.collect_metrics()
                metrics_dict = metrics.to_dict()

                # Calculate rates if we have previous data
                if stage_id in self._last_metrics and self._last_poll_time > 0:
                    time_delta = current_time - self._last_poll_time
                    if time_delta > 0:
                        last = self._last_metrics[stage_id]

                        # Calculate throughput
                        input_delta = metrics_dict["input_records"] - last.get("input_records", 0)
                        output_delta = metrics_dict["output_records"] - last.get(
                            "output_records", 0
                        )

                        metrics_dict["input_throughput"] = input_delta / time_delta
                        metrics_dict["output_throughput"] = output_delta / time_delta

                # Export to Prometheus
                if self.prometheus:
                    self.prometheus.update_stage_metrics(stage_id, metrics_dict)

                    if "partition_metrics" in metrics_dict:
                        self.prometheus.update_partition_metrics(
                            stage_id, metrics_dict["partition_metrics"]
                        )

                # Snapshot to SlateDB periodically
                if current_time - self._last_snapshot_time >= self.snapshot_interval_s:
                    self.storage.store_metrics_snapshot(
                        stage_id,
                        current_time,
                        metrics_dict,
                    )

                # Update cache
                self._last_metrics[stage_id] = metrics_dict

            except Exception as e:
                self.logger.warning(f"Failed to collect metrics for {stage_id}: {e}")

        # Update snapshot time
        if current_time - self._last_snapshot_time >= self.snapshot_interval_s:
            self._last_snapshot_time = current_time

        self._last_poll_time = current_time

        # Track workers every 2 seconds
        if current_time - self._last_worker_tracking_time >= 2.0:
            await self._track_workers(current_time)
            self._last_worker_tracking_time = current_time

    async def _track_workers(self, current_time: float) -> None:
        """Track worker lifecycle and metrics."""
        for stage_id, master in self.job_runner._masters.items():
            try:
                # Track active workers
                for worker_id, worker_handle in master._workers.items():
                    try:
                        # Get worker status and metrics
                        worker_status = ray.get(worker_handle.get_status.remote(), timeout=0.5)
                        worker_metrics = ray.get(worker_handle.get_metrics.remote(), timeout=0.5)

                        worker_data = {
                            "worker_id": worker_id,
                            "stage_id": stage_id,
                            "status": "RUNNING" if worker_status.get("running") else "IDLE",
                            "processed_count": worker_status.get("processed_count", 0),
                            "assigned_partitions": worker_status.get("assigned_partitions", []),
                            "input_records": worker_metrics.input_records,
                            "output_records": worker_metrics.output_records,
                            "processing_time": worker_metrics.processing_time,
                        }

                        await self._update_worker_history(worker_id, worker_data, current_time)

                    except Exception as e:
                        self.logger.debug(f"Failed to get worker {worker_id} metrics: {e}")

                # Check for removed workers (completed or failed)
                await self._check_removed_workers(stage_id, master._workers.keys(), current_time)

            except Exception as e:
                self.logger.warning(f"Failed to track workers for {stage_id}: {e}")

        # Periodically snapshot worker history to SlateDB (every 10 seconds)
        if current_time - self._last_worker_snapshot_time >= 10.0:
            await self._snapshot_worker_history()
            self._last_worker_snapshot_time = current_time

    async def _update_worker_history(
        self, worker_id: str, worker_data: Dict[str, Any], current_time: float
    ) -> None:
        """Update worker history tracking."""
        if worker_id not in self._known_workers:
            # New worker - record start time
            self._known_workers[worker_id] = {
                **worker_data,
                "start_time": current_time,
                "end_time": None,
                "processed_splits": [],
            }
            # Store worker event: STARTED
            self.storage.store_worker_event(
                worker_id,
                current_time,
                {"event_type": "STARTED", "stage_id": worker_data.get("stage_id")},
            )
        else:
            # Update existing worker data
            self._known_workers[worker_id].update(
                {
                    "status": worker_data.get("status", "UNKNOWN"),
                    "input_records": worker_data.get("input_records", 0),
                    "output_records": worker_data.get("output_records", 0),
                    "processing_time": worker_data.get("processing_time", 0),
                    "processed_count": worker_data.get("processed_count", 0),
                    "assigned_partitions": worker_data.get("assigned_partitions", []),
                }
            )

    async def _check_removed_workers(
        self, stage_id: str, current_worker_ids: set, current_time: float
    ) -> None:
        """Check for workers that have been removed (completed or failed)."""
        # Find workers from this stage that are no longer active
        for worker_id, worker_data in list(self._known_workers.items()):
            if worker_data.get("stage_id") == stage_id and worker_id not in current_worker_ids:
                if worker_data.get("end_time") is None:
                    # Worker has been removed - mark as completed
                    self._known_workers[worker_id]["status"] = "COMPLETED"
                    self._known_workers[worker_id]["end_time"] = current_time

                    # Store worker event: COMPLETED
                    self.storage.store_worker_event(
                        worker_id,
                        current_time,
                        {"event_type": "COMPLETED", "stage_id": stage_id},
                    )

                    # Immediately store final worker history
                    self.storage.store_worker_history(worker_id, self._known_workers[worker_id])

    async def _snapshot_worker_history(self) -> None:
        """Snapshot all known workers to SlateDB."""
        for worker_id, worker_data in self._known_workers.items():
            try:
                self.storage.store_worker_history(worker_id, worker_data)
            except Exception as e:
                self.logger.warning(f"Failed to snapshot worker {worker_id}: {e}")
