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
from typing import TYPE_CHECKING, Any, Dict, Optional

from solstice.core.stage import Stage

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner


class Job:
    """Represents the logical definition of a streaming job (stages + DAG)."""

    def __init__(
        self,
        job_id: str,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a streaming job.

        Args:
            job_id: Unique identifier for the job
            config: Additional job configuration

        Examples:
            >>> job = Job(job_id="etl_pipeline")

            >>> job = Job(
            ...     job_id="etl_pipeline",
            ...     config={"parallelism": 4},
            ... )
        """
        self.job_id = job_id
        self.config = config or {}

        self.logger = logging.getLogger(f"Job-{job_id}")

        # DAG components
        self.stages: dict[str, Stage] = {}
        self.dag_edges: dict[str, list[str]] = {}  # stage_id -> downstream stages

        # Runtime hook (optional, populated when a runner is attached)
        self._ray_runner: Optional[RayJobRunner] = None

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

    # ------------------------------------------------------------------
    # Runner helpers
    # ------------------------------------------------------------------
    def attach_ray_runner(self, runner: "RayJobRunner") -> None:
        self._ray_runner = runner

    @property
    def ray_runner(self) -> Optional["RayJobRunner"]:
        return self._ray_runner

    def create_ray_runner(self, **ray_runner_kwargs: Any) -> "RayJobRunner":
        from solstice.runtime.ray_runner import RayJobRunner

        runner = RayJobRunner(self, **ray_runner_kwargs)
        self.attach_ray_runner(runner)
        return runner
