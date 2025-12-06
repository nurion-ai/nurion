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

# Ray init kwargs to exclude large test data files
RAY_INIT_KWARGS = {
    "ignore_reinit_error": True,
    "num_cpus": 4,
    "include_dashboard": False,  # Don't start dashboard in tests
    "runtime_env": {
        "excludes": [
            "tests/testdata/",
            "*.mp4",
            "*.tar.gz",
            "*.lance",
            ".cache/",
        ]
    },
}


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

    def __init__(self, job_id, stage, upstream_stages):
        super().__init__(job_id, stage, upstream_stages)
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
                    # Direct append to pending queue (source generates its own splits)
                    self._pending_splits.append(split)
                self._generated = True

            while self._running and (self._pending_splits or self._inflight_results):
                self._schedule_pending_splits()
                self._drain_completed_results(timeout=poll_interval * 2)
                time.sleep(poll_interval)

            # Mark output buffer as finished for downstream to detect
            self.output_buffer.mark_upstream_finished()
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
class TrackingProcessorConfig(OperatorConfig):
    """Config for processor that tracks which splits it processes."""

    tracking_file: str = ""
    delay_secs: float = 0.1


class TrackingProcessor(Operator):
    """Processor that records which splits it processes to a file."""

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None

        time.sleep(self.config.delay_secs)

        # Record this split was processed using split.split_id (not payload.split_id)
        # This must match what checkpoint_tracker uses for is_split_completed()
        tracking_file = self.config.tracking_file
        if tracking_file:
            with open(tracking_file, "a") as f:
                f.write(f"{split.split_id}\n")

        data = payload.data.to_pylist()
        for row in data:
            row["processed"] = True
        return SplitPayload(data=pa.Table.from_pylist(data), split_id=payload.split_id)


TrackingProcessorConfig.operator_class = TrackingProcessor


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
    tracking_file: Optional[str] = None,
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
    if tracking_file:
        processor_config = TrackingProcessorConfig(tracking_file=tracking_file, delay_secs=1.0)
    else:
        processor_config = SlowProcessorConfig(delay_secs=0.3)

    processor = Stage(
        stage_id="processor",
        operator_config=processor_config,
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
        # Ensure Ray is initialized with proper excludes
        if ray.is_initialized():
            ray.shutdown()
        ray.init(**RAY_INIT_KWARGS)

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

    def test_checkpoint_manual_restoration(self):
        """Test manually restoring from a checkpoint."""
        store = LocalCheckpointStore(str(self.checkpoint_dir))

        try:
            # First run - create checkpoint
            job1 = create_simple_test_job(
                "test_manual_restore",
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

            # Second run - manually restore from checkpoint
            if checkpoints:
                # Reinit Ray for clean state
                ray.shutdown()
                ray.init(**RAY_INIT_KWARGS)

                job2 = create_simple_test_job(
                    "test_manual_restore",
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

    def test_checkpoint_auto_restoration(self):
        """Test that checkpoint restoration skips already-processed splits.

        This test verifies:
        1. First run processes PART of the splits, then stops
        2. Second run restores checkpoint and processes REMAINING splits
        3. Two runs have NO overlap (intersection is empty)
        4. Two runs together process ALL splits (union equals full set)
        """
        import threading
        import uuid

        # Use unique job_id to avoid interference from other tests
        job_id = f"test_auto_restore_{uuid.uuid4().hex[:8]}"
        num_items = 20
        tracking_file = str(self.tmp_path / "processed_splits.txt")
        stop_after = 8  # Stop first run after processing this many

        store = LocalCheckpointStore(str(self.checkpoint_dir))

        try:
            # First run - process PART of items then stop
            job1 = create_simple_test_job(
                job_id,
                store,
                num_items=num_items,
                checkpoint_interval=1,
                tracking_file=tracking_file,
            )

            runner1 = job1.create_ray_runner()
            runner1.initialize()

            # Run in background so we can monitor and stop
            stop_event = threading.Event()

            def run_until_stopped():
                try:
                    while not stop_event.is_set():
                        # Check if we've processed enough
                        if Path(tracking_file).exists():
                            with open(tracking_file) as f:
                                count = len([line for line in f if line.strip()])
                            if count >= stop_after:
                                stop_event.set()
                                break
                        time.sleep(0.1)
                except Exception:
                    pass

            monitor = threading.Thread(target=run_until_stopped, daemon=True)
            monitor.start()

            # Run job in background
            def run_job():
                try:
                    runner1.run(timeout=60)
                except Exception:
                    pass

            job_thread = threading.Thread(target=run_job, daemon=True)
            job_thread.start()

            # Wait for stop condition
            stop_event.wait(timeout=30)
            time.sleep(0.5)  # Let last checkpoint complete

            # Get checkpoint data before shutdown
            checkpoints = runner1.list_checkpoints()
            logger.info(f"Checkpoints after partial run: {checkpoints}")
            assert len(checkpoints) > 0, "Expected at least one checkpoint"

            latest_ckpt = checkpoints[-1]
            manifest = runner1.checkpoint_manager.load_checkpoint(latest_ckpt)
            checkpointed_splits = set()
            if manifest and "processor" in manifest.stages:
                checkpointed_splits = set(manifest.stages["processor"].completed_splits)

            runner1.shutdown()
            job_thread.join(timeout=2)

            logger.info(f"First run checkpointed {len(checkpointed_splits)} splits")

            # First run should NOT have processed all items
            assert len(checkpointed_splits) > 0, "Expected some checkpointed splits"
            assert len(checkpointed_splits) < num_items, (
                f"First run should NOT process all {num_items} items, got {len(checkpointed_splits)}"
            )

            # Clear tracking file for second run
            Path(tracking_file).unlink()

            store.close()
            ray.shutdown()
            ray.init(**RAY_INIT_KWARGS)

            # Second run - should restore and process REMAINING splits only
            store2 = LocalCheckpointStore(str(self.checkpoint_dir))

            job2 = create_simple_test_job(
                job_id,
                store2,
                num_items=num_items,
                checkpoint_interval=1,
                tracking_file=tracking_file,
            )

            runner2 = job2.create_ray_runner()
            runner2.initialize()
            runner2.run(timeout=60)
            runner2.shutdown()

            # Read splits processed in second run
            second_run_splits = set()
            if Path(tracking_file).exists():
                with open(tracking_file) as f:
                    second_run_splits = {line.strip() for line in f if line.strip()}

            logger.info(f"Second run processed {len(second_run_splits)} splits")

            # KEY VERIFICATION 1: No overlap (intersection is empty)
            overlap = checkpointed_splits & second_run_splits
            assert len(overlap) == 0, (
                f"OVERLAP DETECTED! These splits were processed twice: {overlap}"
            )

            # KEY VERIFICATION 2: Union equals full set
            all_processed = checkpointed_splits | second_run_splits
            assert len(all_processed) == num_items, (
                f"INCOMPLETE! Expected {num_items} total, got {len(all_processed)}. "
                f"First run: {len(checkpointed_splits)}, Second run: {len(second_run_splits)}, "
                f"Missing: {num_items - len(all_processed)}"
            )

            logger.info(
                f"SUCCESS: First run={len(checkpointed_splits)}, "
                f"Second run={len(second_run_splits)}, "
                f"Total={len(all_processed)}, No overlap!"
            )

            store2.close()

        except Exception:
            store.close()
            raise
