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

from solstice.webui.registry import get_or_create_registry
from solstice.webui.storage import SlateDBStorage
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
    4. Calculate derived metrics (rates, ETA)

    Usage:
        collector = MetricsCollector(job_runner, storage, prometheus_enabled=True)
        asyncio.create_task(collector.run_loop())
    """

    def __init__(
        self,
        job_runner: "RayJobRunner",
        storage: SlateDBStorage,
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

        # Registry for updating stages info
        self.registry = get_or_create_registry()

        # Prometheus exporter
        self.prometheus: Optional[PrometheusMetricsExporter] = None
        if prometheus_enabled:
            self.prometheus = PrometheusMetricsExporter(self.job_id)

        # State
        self._running = False
        self._last_snapshot_time = 0.0
        self._last_registry_update = 0.0

        # Rate calculation
        self._last_metrics: Dict[str, Dict[str, Any]] = {}
        self._last_poll_time = 0.0

    async def run_loop(self) -> None:
        """Main collection loop.

        Runs until job completes:
        - Collect metrics every 1 second
        - Export to Prometheus
        - Snapshot to SlateDB every 30 seconds
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
                        self.job_id,
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

        # Update Registry with stages info (every 2 seconds)
        if current_time - self._last_registry_update >= 2.0:
            await self._update_registry()
            self._last_registry_update = current_time

    async def _update_registry(self) -> None:
        """Update the Registry with current stages info."""
        stages_info = []
        total_workers = 0

        for stage_id, master in self.job_runner._masters.items():
            try:
                status = master.get_status()
                stages_info.append(
                    {
                        "stage_id": stage_id,
                        "worker_count": status.worker_count,
                        "output_queue_size": status.output_queue_size,
                        "is_running": status.is_running,
                        "is_finished": status.is_finished,
                        "failed": False,
                        "input_count": self._last_metrics.get(stage_id, {}).get("input_records", 0),
                        "output_count": self._last_metrics.get(stage_id, {}).get(
                            "output_records", 0
                        ),
                    }
                )
                total_workers += status.worker_count
            except Exception as e:
                self.logger.warning(f"Failed to get status for {stage_id}: {e}")

        # Update registry
        try:
            ray.get(
                self.registry.update.remote(
                    self.job_id,
                    {
                        "stages": stages_info,
                        "worker_count": total_workers,
                        "last_update": time.time(),
                    },
                ),
                timeout=1,
            )
        except Exception as e:
            self.logger.warning(f"Failed to update registry: {e}")
