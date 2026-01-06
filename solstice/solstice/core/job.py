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

"""Job definition and DAG specification."""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional

from solstice.core.stage import Stage
from solstice.queue import QueueType

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner
    from solstice.core.stage_master import AutoscaleConfig


@dataclass
class WebUIConfig:
    """Configuration for WebUI debugging interface.

    Attributes:
        enabled: Whether to enable WebUI
        storage_path: SlateDB storage path (local or s3://)
        prometheus_enabled: Whether to export Prometheus metrics
        prometheus_pushgateway: Optional Prometheus Pushgateway URL
        metrics_snapshot_interval_s: Interval between SlateDB metrics snapshots
        archive_on_completion: Whether to archive job data when complete
        port: Ray Serve port (default 8000)
    """

    enabled: bool = False
    storage_path: str = "/tmp/solstice-webui/"
    prometheus_enabled: bool = True
    prometheus_pushgateway: Optional[str] = None
    metrics_snapshot_interval_s: float = 30.0
    archive_on_completion: bool = True
    port: int = 8000


@dataclass
class JobConfig:
    """Configuration for a Solstice job.

    Attributes:
        queue_type: Type of queue backend (TANSU for production, MEMORY for testing)
        tansu_storage_url: Storage URL for Tansu backend (memory://, s3://)
        ray_init_kwargs: Arguments to pass to ray.init()
        autoscale_config: Configuration for autoscaling (None to disable)
        webui: WebUI debugging interface configuration
    """

    queue_type: QueueType = QueueType.TANSU
    tansu_storage_url: str = "memory://"
    ray_init_kwargs: Dict[str, Any] = field(default_factory=dict)
    autoscale_config: Optional["AutoscaleConfig"] = None
    webui: WebUIConfig = field(default_factory=WebUIConfig)


class Job:
    """Represents the logical definition of a streaming job (stages + DAG)."""

    def __init__(
        self,
        job_id: str,
        config: Optional[JobConfig] = None,
    ):
        """
        Initialize a streaming job.

        Args:
            job_id: Unique identifier for the job
            config: Job configuration (queue type, autoscaling, etc.)

        Examples:
            >>> job = Job(job_id="etl_pipeline")

            >>> job = Job(
            ...     job_id="etl_pipeline",
            ...     config=JobConfig(queue_type=QueueType.MEMORY),
            ... )
        """
        self.job_id = job_id
        self.config = config or JobConfig()

        self.logger = logging.getLogger(f"Job-{job_id}")

        # DAG components
        self.stages: dict[str, Stage] = {}
        self.dag_edges: dict[str, list[str]] = {}  # stage_id -> downstream stages

        self.logger.debug("Job %s initialized", job_id)

    def add_stage(
        self,
        stage: Stage,
        upstream_stages: Optional[list[str]] = None,
    ) -> "Job":
        """
        Add a stage to the job DAG.

        Args:
            stage: Stage to add
            upstream_stages: List of upstream stage IDs

        Returns:
            Self for chaining
        """
        if stage.stage_id in self.stages:
            raise ValueError(f"Stage {stage.stage_id} already exists")

        self.stages[stage.stage_id] = stage

        # Update DAG edges
        upstream_stages = upstream_stages or []
        for upstream_id in upstream_stages:
            if upstream_id not in self.stages:
                raise ValueError(f"Upstream stage {upstream_id} not found")

            if upstream_id not in self.dag_edges:
                self.dag_edges[upstream_id] = []
            self.dag_edges[upstream_id].append(stage.stage_id)

        self.logger.info(
            f"Added stage {stage.stage_id} with {len(upstream_stages)} upstream stages"
        )

        return self

    def build_reverse_dag(self) -> dict[str, list[str]]:
        """Build reverse DAG mapping (downstream -> upstream)."""
        reverse_dag = {stage_id: [] for stage_id in self.stages.keys()}

        for upstream_id, downstream_ids in self.dag_edges.items():
            for downstream_id in downstream_ids:
                reverse_dag[downstream_id].append(upstream_id)

        return reverse_dag

    def create_ray_runner(self) -> "RayJobRunner":
        """Create a RayJobRunner for this job.

        Configuration is read from self.config (JobConfig).
        """
        from solstice.runtime.ray_runner import RayJobRunner

        return RayJobRunner(self)
