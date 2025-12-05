"""Job definition and DAG specification."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

import os

from solstice.core.stage import Stage
from solstice.core.models import JobCheckpointConfig
from solstice.state.store import CheckpointStore, create_checkpoint_store

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner


class Job:
    """Represents the logical definition of a streaming job (stages + DAG)."""

    def __init__(
        self,
        job_id: str,
        checkpoint_store: Optional[CheckpointStore] = None,
        checkpoint_store_uri: Optional[str] = None,
        checkpoint_config: Optional[JobCheckpointConfig] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a streaming job.

        Args:
            job_id: Unique identifier for the job
            checkpoint_store: Store instance for checkpoint persistence.
            checkpoint_store_uri: URI to create checkpoint store. Ignored if
                checkpoint_store is provided. Supports:
                - "/path/to/dir" or "local:/path" - Local filesystem
                - "s3://bucket/prefix" - S3 (via fsspec)
                - "slatedb://memory:///" - SlateDB in-memory (testing)
                - "slatedb://file:///path" - SlateDB local file
                - "slatedb://s3://bucket/prefix" - SlateDB with S3 (recommended)
                Can also be set via SOLSTICE_CHECKPOINT_STORE_URI env var.
            checkpoint_config: Global checkpoint configuration. Controls checkpoint
                triggering strategy, coordination mode, and timeouts.
            config: Additional job configuration

        Examples:
            >>> # Default: SlateDB with local storage
            >>> job = Job(job_id="etl_pipeline")

            >>> # SlateDB with S3 (recommended for production)
            >>> job = Job(
            ...     job_id="etl_pipeline",
            ...     checkpoint_store_uri="slatedb://s3://my-bucket/checkpoints",
            ... )

            >>> # Or via environment variable
            >>> # export SOLSTICE_CHECKPOINT_STORE_URI="slatedb://s3://bucket/ckpt"
            >>> job = Job(job_id="etl_pipeline")

            >>> # Custom checkpoint settings
            >>> job = Job(
            ...     job_id="etl_pipeline",
            ...     checkpoint_config=JobCheckpointConfig(
            ...         enabled=True,
            ...         interval_secs=300,
            ...     ),
            ... )
        """
        self.job_id = job_id

        # Resolve checkpoint store: explicit store > URI param > env var > default
        if checkpoint_store is not None:
            self.checkpoint_store = checkpoint_store
        else:
            uri = (
                checkpoint_store_uri
                or os.environ.get("SOLSTICE_CHECKPOINT_STORE_URI")
                or f"slatedb://file:///tmp/solstice/{job_id}/slatedb"
            )
            self.checkpoint_store = create_checkpoint_store(uri)

        self.checkpoint_config = checkpoint_config or JobCheckpointConfig()
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
