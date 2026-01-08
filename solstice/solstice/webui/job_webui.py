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

"""Job WebUI - per-job WebUI instance."""

import asyncio
import os
from typing import TYPE_CHECKING, Optional

from solstice.webui.collectors.metrics import PrometheusCollector
from solstice.webui.storage import JobStorage
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner
    from solstice.webui.state.manager import JobStateManager


class JobWebUI:
    """WebUI instance for a single Solstice job.

    This component:
    1. Stores job configuration
    2. Starts Prometheus exporter (if enabled)

    Note: Metrics collection, worker tracking, and job archiving are handled
    by JobStateManager (push-based architecture).
    """

    def __init__(
        self,
        job_runner: "RayJobRunner",
        storage: JobStorage,
        attempt_id: str,
        state_manager: Optional["JobStateManager"] = None,
        prometheus_enabled: bool = True,
    ):
        """Initialize job WebUI.

        Args:
            job_runner: RayJobRunner instance
            storage: SlateDB storage instance
            attempt_id: Unique attempt ID for this run
            state_manager: JobStateManager for reading metrics (push-based)
            prometheus_enabled: Whether to export Prometheus metrics
        """
        self.job_runner = job_runner
        self.storage = storage
        self.job_id = job_runner.job.job_id
        self.attempt_id = attempt_id
        self.state_manager = state_manager

        self.logger = create_ray_logger(f"JobWebUI-{self.job_id}")

        # Prometheus collector (optional)
        self.prometheus_collector: Optional[PrometheusCollector] = None
        if prometheus_enabled and state_manager:
            self.prometheus_collector = PrometheusCollector(state_manager, self.job_id)

        # Background tasks
        self._collector_tasks: list = []

        self.logger.info("Job WebUI initialized")

    async def start(self) -> None:
        """Start the WebUI components."""
        # Store configuration at job start
        self._store_configuration()

        # Start Prometheus collector if enabled
        if self.prometheus_collector:
            self._collector_tasks.append(asyncio.create_task(self.prometheus_collector.run_loop()))

        self.logger.info("Job WebUI started")

    def _store_configuration(self) -> None:
        """Store job configuration to storage."""
        try:
            job_runner = self.job_runner

            # Build stage configs
            stage_configs = {}
            for stage_id, master in job_runner._masters.items():
                stage_configs[stage_id] = {
                    "operator_type": type(master.stage.operator_config).__name__,
                    "min_parallelism": master.config.min_workers,
                    "max_parallelism": master.config.max_workers,
                    "num_cpus": master.config.num_cpus,
                    "num_gpus": master.config.num_gpus,
                    "memory_mb": master.config.memory_mb,
                }

            config_data = {
                "job_config": {
                    "job_id": job_runner.job.job_id,
                    "queue_type": job_runner.queue_type.value,
                    "tansu_storage_url": job_runner.tansu_storage_url,
                },
                "stage_configs": stage_configs,
                "dag_edges": job_runner.job.dag_edges,
                "environment": {
                    "SOLSTICE_LOG_LEVEL": os.getenv("SOLSTICE_LOG_LEVEL", "INFO"),
                    "RAY_PROMETHEUS_HOST": os.getenv("RAY_PROMETHEUS_HOST"),
                    "SOLSTICE_GRAFANA_URL": os.getenv("SOLSTICE_GRAFANA_URL"),
                },
            }

            self.storage.store_configuration(config_data)
            self.logger.debug("Configuration stored")

        except Exception as e:
            self.logger.warning(f"Failed to store configuration: {e}")

    async def stop(self) -> None:
        """Stop the WebUI components."""
        self.logger.info("Stopping Job WebUI")

        # Stop Prometheus collector
        if self.prometheus_collector:
            self.prometheus_collector.stop()

        # Wait for tasks to complete
        for task in self._collector_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._collector_tasks.clear()
        self.logger.info("Job WebUI stopped")
