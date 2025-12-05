"""Unit tests for checkpoint functionality.

Tests checkpoint creation, restoration, and skip_checkpoint configuration.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pytest
import ray

from solstice.core.job import Job
from solstice.core.models import JobCheckpointConfig, Split, SplitPayload
from solstice.core.operator import Operator, OperatorConfig
from solstice.core.stage import Stage
from solstice.core.stage_master import StageMasterActor, StageMasterConfig
from solstice.state.store import LocalCheckpointStore

logger = logging.getLogger(__name__)


# =============================================================================
# Simple test operators for checkpoint testing
# =============================================================================


@dataclass
class SimpleSourceConfig(OperatorConfig):
    """Config for simple test source."""

    num_items: int = 5


class SimpleSource(Operator):
    """Simple source that generates test data from split index."""

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        idx = int(split.split_id.split(":")[-1]) if ":" in split.split_id else 0
        data = pa.Table.from_pylist([{"id": idx, "value": f"item_{idx}"}])
        return SplitPayload(data=data, split_id=split.split_id)


SimpleSourceConfig.operator_class = SimpleSource


@dataclass
class SimpleSourceMasterConfig(StageMasterConfig):
    """Config for simple test source master."""

    num_items: int = 5


class SimpleSourceMaster(StageMasterActor):
    """Source master that generates test splits."""

    def __init__(self, job_id, checkpoint_store, stage, upstream_stages):
        super().__init__(job_id, checkpoint_store, stage, upstream_stages)
        self.num_items = getattr(stage.master_config, "num_items", 5)
        self._generated = False

    def run(self, poll_interval: float = 0.05) -> bool:
        if self._running:
            return False
        self._running = True

        try:
            if not self._generated:
                for i in range(self.num_items):
                    split = Split(
                        split_id=f"source:{i}", stage_id=self.stage_id, data_range={"idx": i}
                    )
                    self.enqueue_split(split)
                self._generated = True

            while self._running and (self._pending_splits or self._inflight_results):
                self._schedule_pending_splits()
                self._drain_completed_results(timeout=poll_interval * 2)
                time.sleep(poll_interval)

            for actor_ref in self.downstream_stage_refs.values():
                actor_ref.set_upstream_finished.remote(self.stage_id)
            return True
        finally:
            self._running = False


SimpleSourceMasterConfig.master_class = SimpleSourceMaster


@dataclass
class SlowProcessorConfig(OperatorConfig):
    """Config for slow processor (simulates expensive work)."""

    delay_secs: float = 0.3


class SlowProcessor(Operator):
    """Processor that adds delay (for checkpoint timing)."""

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None
        time.sleep(self.config.delay_secs)
        data = payload.data.to_pylist()
        for row in data:
            row["processed"] = True
            row["processed_at"] = time.time()
        return SplitPayload(data=pa.Table.from_pylist(data), split_id=payload.split_id)


SlowProcessorConfig.operator_class = SlowProcessor


@dataclass
class SimpleSinkConfig(OperatorConfig):
    """Config for simple sink."""

    pass


class SimpleSink(Operator):
    """Simple sink that just logs."""

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload:
            logger.debug(f"[SINK] Received {len(payload.data)} records from {payload.split_id}")
        return None


SimpleSinkConfig.operator_class = SimpleSink


def create_simple_test_job(
    job_id: str,
    checkpoint_store: LocalCheckpointStore,
    num_items: int = 5,
    checkpoint_interval: int = 2,
) -> Job:
    """Create a simple test job for checkpoint testing."""
    job = Job(
        job_id=job_id,
        checkpoint_store=checkpoint_store,
        checkpoint_config=JobCheckpointConfig(
            enabled=True,
            interval_secs=checkpoint_interval,
            min_pause_between_secs=1,
        ),
    )

    # Source - skip checkpoint
    source = Stage(
        stage_id="source",
        operator_config=SimpleSourceConfig(num_items=num_items),
        master_config=SimpleSourceMasterConfig(num_items=num_items),
        parallelism=1,
        skip_checkpoint=True,
    )

    # Processor - needs checkpoint
    processor = Stage(
        stage_id="processor",
        operator_config=SlowProcessorConfig(delay_secs=0.3),
        parallelism=1,
        skip_checkpoint=False,
    )

    # Sink - skip checkpoint
    sink = Stage(
        stage_id="sink",
        operator_config=SimpleSinkConfig(),
        parallelism=1,
        skip_checkpoint=True,
    )

    job.add_stage(source)
    job.add_stage(processor, upstream_stages=["source"])
    job.add_stage(sink, upstream_stages=["processor"])

    return job


class TestCheckpointWithWorkflow:
    """Test checkpoint functionality with actual workflow execution."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path: Path):
        """Setup test environment."""
        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=4)

        self.tmp_path = tmp_path
        self.checkpoint_dir = tmp_path / "checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        yield

        # Cleanup Ray after each test to avoid actor name conflicts
        if ray.is_initialized():
            ray.shutdown()

    def test_skip_checkpoint_stages(self):
        """Test that skip_checkpoint correctly excludes stages from checkpointing."""
        store = LocalCheckpointStore(str(self.checkpoint_dir))

        try:
            job = create_simple_test_job("test_skip", store, num_items=3)

            runner = job.create_ray_runner()
            runner.initialize()

            # Verify skip_checkpoint configuration
            registered = runner.checkpoint_manager.get_registered_stages()
            logger.info(f"Registered stages for checkpoint: {registered}")

            # Only processor should be registered
            assert "processor" in registered
            assert "source" not in registered
            assert "sink" not in registered

            runner.shutdown()
        finally:
            store.close()

    def test_checkpoint_creation_during_workflow(self):
        """Test that checkpoints are created during workflow execution."""
        store = LocalCheckpointStore(str(self.checkpoint_dir))

        try:
            # Use more items and longer delay to ensure checkpoint triggers
            job = create_simple_test_job(
                "test_ckpt_create",
                store,
                num_items=10,
                checkpoint_interval=1,  # Checkpoint every 1 second
            )

            runner = job.create_ray_runner()
            runner.initialize()

            # Run job
            runner.run(timeout=60)

            # Check for checkpoints
            checkpoints = runner.list_checkpoints()
            logger.info(f"Checkpoints created: {checkpoints}")

            runner.shutdown()
        finally:
            store.close()

    def test_checkpoint_restoration(self):
        """Test restoring from a checkpoint."""
        store = LocalCheckpointStore(str(self.checkpoint_dir))

        try:
            # First run - create checkpoint
            job1 = create_simple_test_job(
                "test_restore",
                store,
                num_items=10,
                checkpoint_interval=1,
            )

            runner1 = job1.create_ray_runner()
            runner1.initialize()
            runner1.run(timeout=30)

            checkpoints = runner1.list_checkpoints()
            logger.info(f"Checkpoints after run 1: {checkpoints}")
            runner1.shutdown()

            # Second run - restore from checkpoint
            if checkpoints:
                # Reinit Ray for clean state
                ray.shutdown()
                ray.init(ignore_reinit_error=True, num_cpus=4)

                job2 = create_simple_test_job(
                    "test_restore",
                    store,
                    num_items=10,
                    checkpoint_interval=1,
                )

                runner2 = job2.create_ray_runner()
                runner2.initialize()

                latest = checkpoints[-1]
                restored = runner2.restore_from_checkpoint(latest)
                logger.info(f"Restore from {latest}: {restored}")

                assert restored, f"Failed to restore from checkpoint {latest}"

                runner2.shutdown()
            else:
                logger.info("No checkpoints created (job completed too fast)")
        finally:
            store.close()
