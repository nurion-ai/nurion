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

"""Prometheus metrics exporter for WebUI.

This module exports metrics to Prometheus for real-time monitoring.
Metrics are obtained from JobStateManager (push-based architecture).

Note: Worker tracking and SlateDB snapshots are handled by JobStateManager.
This collector only handles Prometheus export.
"""

import asyncio
from typing import TYPE_CHECKING, Any, Dict

from solstice.webui.storage.prometheus_exporter import PrometheusMetricsExporter
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.webui.state.manager import JobStateManager


class PrometheusCollector:
    """Export metrics to Prometheus from JobStateManager.

    Responsibilities:
    1. Read metrics from JobStateManager (push-based)
    2. Export to Prometheus for real-time monitoring

    Usage:
        collector = PrometheusCollector(state_manager)
        asyncio.create_task(collector.run_loop())
    """

    def __init__(
        self,
        state_manager: "JobStateManager",
        job_id: str,
    ):
        """Initialize Prometheus collector.

        Args:
            state_manager: JobStateManager instance to read metrics from
            job_id: Job identifier
        """
        self.state_manager = state_manager
        self.job_id = job_id
        self.logger = create_ray_logger(f"PrometheusCollector-{job_id}")

        self.prometheus = PrometheusMetricsExporter(job_id)

        self._running = False
        self._last_metrics: Dict[str, Dict[str, Any]] = {}

    async def run_loop(self) -> None:
        """Main export loop.

        Runs until stopped:
        - Read metrics from JobStateManager every 1 second
        - Export to Prometheus
        """
        self._running = True
        self.logger.info("Prometheus collector started")

        try:
            while self._running:
                self._export_metrics()
                await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"Prometheus collector error: {e}")
        finally:
            self._running = False
            self.logger.info("Prometheus collector stopped")

    def stop(self) -> None:
        """Stop the collector."""
        self._running = False

    def _export_metrics(self) -> None:
        """Export metrics from JobStateManager to Prometheus."""
        try:
            # Get all stage info from state manager
            job_info = self.state_manager.get_job_info()
            stages = job_info.get("stages", [])

            for stage_data in stages:
                stage_id = stage_data.get("stage_id", "")
                if not stage_id:
                    continue

                metrics_dict = {
                    "stage_id": stage_id,
                    "worker_count": stage_data.get("worker_count", 0),
                    "input_records": stage_data.get("input_records", 0),
                    "output_records": stage_data.get("output_records", 0),
                    "output_queue_size": stage_data.get("output_queue_size", 0),
                    "is_running": stage_data.get("is_running", False),
                    "is_finished": stage_data.get("is_finished", False),
                }

                # Calculate throughput if we have previous data
                if stage_id in self._last_metrics:
                    last = self._last_metrics[stage_id]
                    input_delta = metrics_dict["input_records"] - last.get("input_records", 0)
                    output_delta = metrics_dict["output_records"] - last.get("output_records", 0)
                    # Assuming 1 second interval
                    metrics_dict["input_throughput"] = max(0, input_delta)
                    metrics_dict["output_throughput"] = max(0, output_delta)

                self.prometheus.update_stage_metrics(stage_id, metrics_dict)
                self._last_metrics[stage_id] = metrics_dict

        except Exception as e:
            self.logger.warning(f"Failed to export metrics: {e}")
