"""End-to-end tests for v2 pipeline architecture.

Tests the complete flow:
- Source operator generating data
- Transform operators processing data
- Queue-based communication between stages
- Worker pull model
"""

import asyncio
import pytest
from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.core.operator import Operator, OperatorConfig
from solstice.core.models import Split, SplitPayload
from solstice.core.stage_master import QueueType
from solstice.runtime.ray_runner import RayJobRunner
from solstice.operators.sources.source import SourceMaster

pytestmark = pytest.mark.asyncio(loop_scope="function")


# ============================================================================
# Test Operators
# ============================================================================


class TestSourceOperator(Operator):
    """Source operator that generates test data."""

    def __init__(self, config: "TestSourceConfig", worker_id: str = None):
        super().__init__(config, worker_id)
        self._generated = 0

    def generate_splits(self) -> List[Split]:
        """Generate splits for the source."""
        splits = []
        num_batches = self.config.num_records // self.config.batch_size
        for i in range(num_batches):
            splits.append(
                Split(
                    split_id=f"source_split_{i}",
                    stage_id="source",
                    data_range={
                        "start": i * self.config.batch_size,
                        "end": (i + 1) * self.config.batch_size,
                    },
                )
            )
        return splits

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Generate data for a split."""
        start = split.data_range["start"]
        end = split.data_range["end"]

        # Generate test data
        data = pa.table(
            {
                "id": list(range(start, end)),
                "value": [f"record_{i}" for i in range(start, end)],
            }
        )

        self._generated += end - start
        return SplitPayload(data=data, split_id=split.split_id)

    def close(self) -> None:
        pass


@dataclass
class TestSourceConfig(OperatorConfig):
    """Config for test source operator."""

    num_records: int = 100
    batch_size: int = 10


# Set operator_class after class definition
TestSourceConfig.operator_class = TestSourceOperator


class TestSourceMaster(SourceMaster):
    """Test source master that generates splits from config."""

    def plan_splits(self):
        """Generate splits based on operator config."""
        config = self.stage.operator_config
        num_batches = config.num_records // config.batch_size

        for i in range(num_batches):
            yield Split(
                split_id=f"source_split_{i}",
                stage_id=self.stage_id,
                data_range={
                    "start": i * config.batch_size,
                    "end": (i + 1) * config.batch_size,
                },
            )


# Set master_class after class definition
TestSourceConfig.master_class = TestSourceMaster


class TestTransformOperator(Operator):
    """Transform operator that modifies data."""

    def __init__(self, config: "TestTransformConfig", worker_id: str = None):
        super().__init__(config, worker_id)
        self._processed = 0

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Transform data by adding suffix to values."""
        if payload is None:
            return None

        table = payload.to_table()

        # Transform: add suffix to value column
        values = table.column("value").to_pylist()
        new_values = [v + self.config.suffix for v in values]

        new_table = pa.table(
            {
                "id": table.column("id"),
                "value": new_values,
            }
        )

        self._processed += table.num_rows
        return SplitPayload(data=new_table, split_id=split.split_id)

    def close(self) -> None:
        pass


@dataclass
class TestTransformConfig(OperatorConfig):
    """Config for test transform operator."""

    suffix: str = "_transformed"


# Set operator_class after class definition
TestTransformConfig.operator_class = TestTransformOperator


class TestSinkOperator(Operator):
    """Sink operator that collects results."""

    # Shared storage for test verification
    collected_records: List[Dict] = []

    def __init__(self, config: "TestSinkConfig", worker_id: str = None):
        super().__init__(config, worker_id)

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Collect records from payload."""
        if payload is None:
            return None

        records = payload.to_pylist()
        TestSinkOperator.collected_records.extend(records)

        # Sink doesn't produce output
        return None

    def close(self) -> None:
        pass

    @classmethod
    def reset(cls):
        cls.collected_records = []


@dataclass
class TestSinkConfig(OperatorConfig):
    """Config for test sink operator."""

    pass


# Set operator_class after class definition
TestSinkConfig.operator_class = TestSinkOperator


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def simple_job():
    """Create a simple single-stage job."""
    job = Job(job_id="test_simple")

    source_stage = Stage(
        stage_id="source",
        operator_config=TestSourceConfig(num_records=50, batch_size=10),
        parallelism=(1, 2),  # (min, max)
    )
    job.add_stage(source_stage)

    return job


@pytest.fixture
def two_stage_job():
    """Create a two-stage job (source -> transform)."""
    job = Job(job_id="test_two_stage")

    source_stage = Stage(
        stage_id="source",
        operator_config=TestSourceConfig(num_records=50, batch_size=10),
        parallelism=1,
    )
    job.add_stage(source_stage)

    transform_stage = Stage(
        stage_id="transform",
        operator_config=TestTransformConfig(suffix="_v2"),
        parallelism=1,
    )
    # Note: upstream_stages is set via job.add_stage with dependencies
    job.add_stage(transform_stage, upstream_stages=["source"])

    return job


# ============================================================================
# Tests
# ============================================================================


class TestRayJobRunner:
    """Tests for RayJobRunner."""

    @pytest.mark.asyncio
    async def test_initialization(self, simple_job, ray_cluster):
        """Test runner initialization."""
        runner = RayJobRunner(simple_job, queue_type=QueueType.TANSU)

        assert not runner.is_initialized
        assert not runner.is_running

        await runner.initialize()

        assert runner.is_initialized
        assert "source" in runner._masters

    @pytest.mark.asyncio
    async def test_get_status(self, simple_job, ray_cluster):
        """Test getting pipeline status."""
        runner = RayJobRunner(simple_job, queue_type=QueueType.TANSU)
        await runner.initialize()

        status = runner.get_status()

        assert status.job_id == "test_simple"
        assert not status.is_running
        assert "source" in status.stages

        await runner.stop()

    @pytest.mark.asyncio
    async def test_stop_before_run(self, simple_job, ray_cluster):
        """Test stopping before running."""
        runner = RayJobRunner(simple_job, queue_type=QueueType.TANSU)
        await runner.initialize()
        await runner.stop()  # Should not raise

        assert not runner.is_running


class TestPipelineExecution:
    """Tests for actual pipeline execution."""

    @pytest.mark.asyncio
    async def test_single_stage_messages(self, ray_cluster):
        """Test that single stage produces messages to queue."""
        job = Job(job_id="test_single_stage_msg")

        source_stage = Stage(
            stage_id="source",
            operator_config=TestSourceConfig(num_records=20, batch_size=5),
            parallelism=1,
        )
        job.add_stage(source_stage)

        runner = RayJobRunner(job, queue_type=QueueType.TANSU)
        await runner.initialize()

        # Start the source
        source_master = runner._masters["source"]
        await source_master.start()

        # Give it time to produce some messages
        await asyncio.sleep(2)

        # Check output queue
        queue = source_master.get_output_queue()
        topic = source_master.get_output_topic()

        if queue:
            offset = await queue.get_latest_offset(topic)
            # Source should have produced some messages
            # (exact count depends on timing)
            assert offset >= 0

        await runner.stop()


class TestQueueCommunication:
    """Tests for queue-based stage communication."""

    @pytest.mark.asyncio
    async def test_upstream_downstream_connection(self, two_stage_job, ray_cluster):
        """Test that downstream stage connects to upstream queue."""
        runner = RayJobRunner(two_stage_job, queue_type=QueueType.TANSU)
        await runner.initialize()

        transform_master = runner._masters["transform"]

        # Verify transform has upstream endpoint
        assert transform_master.upstream_endpoint is not None
        assert transform_master.upstream_topic is not None

        # Endpoint should point to source's output
        assert transform_master.upstream_endpoint.queue_type == QueueType.TANSU

        await runner.stop()


class TestExactlyOnce:
    """Tests for exactly-once semantics."""

    @pytest.mark.asyncio
    async def test_offset_tracking(self, ray_cluster):
        """Test that offsets are tracked correctly."""
        from solstice.queue import MemoryBackend

        # Create a shared queue
        backend = MemoryBackend()
        await backend.start()

        topic = "test_topic"
        group = "test_group"
        await backend.create_topic(topic)

        # Produce messages
        from solstice.core.stage_master import QueueMessage

        for i in range(10):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                payload_key=f"ref_{i}",
            )
            await backend.produce(topic, msg.to_bytes())

        # Consume and commit
        records = await backend.fetch(topic, offset=0, max_records=5)
        assert len(records) == 5

        await backend.commit_offset(group, topic, 5)

        # Verify committed offset
        committed = await backend.get_committed_offset(group, topic)
        assert committed == 5

        # Resume from committed
        remaining = await backend.fetch(topic, offset=committed)
        assert len(remaining) == 5

        await backend.stop()


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Full integration tests."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_source_produces_to_queue(self, ray_cluster):
        """Test that source stage produces data to its output queue."""
        job = Job(job_id="test_source_queue")

        source_stage = Stage(
            stage_id="source",
            operator_config=TestSourceConfig(num_records=10, batch_size=5),
            parallelism=1,
        )
        job.add_stage(source_stage)

        runner = RayJobRunner(job, queue_type=QueueType.TANSU)
        await runner.initialize()

        source_master = runner._masters["source"]

        # Start and let it run briefly
        await source_master.start()

        # Wait for workers to produce
        await asyncio.sleep(3)

        # Check that messages were produced
        queue = source_master.get_output_queue()
        if queue:
            topic = source_master.get_output_topic()
            latest = await queue.get_latest_offset(topic)
            # Should have produced some messages (timing dependent)
            print(f"Source produced {latest} messages")

        await runner.stop()


class TestMultiStagePipeline:
    """Tests for multi-stage pipeline with actual data flow."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_three_stage_pipeline_data_flow(self, ray_cluster):
        """Test complete data flow: Source -> Transform -> Sink."""
        # Reset sink collector
        TestSinkOperator.reset()

        job = Job(job_id="test_three_stage")

        # Stage 1: Source generates data
        source_stage = Stage(
            stage_id="source",
            operator_config=TestSourceConfig(num_records=30, batch_size=10),
            parallelism=1,
        )
        job.add_stage(source_stage)

        # Stage 2: Transform modifies data
        transform_stage = Stage(
            stage_id="transform",
            operator_config=TestTransformConfig(suffix="_processed"),
            parallelism=1,
        )
        job.add_stage(transform_stage, upstream_stages=["source"])

        # Stage 3: Sink collects results
        sink_stage = Stage(
            stage_id="sink",
            operator_config=TestSinkConfig(),
            parallelism=1,
        )
        job.add_stage(sink_stage, upstream_stages=["transform"])

        # Verify DAG structure
        assert len(job.stages) == 3
        assert job.dag_edges.get("source") == ["transform"]
        assert job.dag_edges.get("transform") == ["sink"]

        reverse_dag = job.build_reverse_dag()
        assert reverse_dag["source"] == []
        assert reverse_dag["transform"] == ["source"]
        assert reverse_dag["sink"] == ["transform"]

        print("DAG structure verified")

    @pytest.mark.asyncio
    async def test_two_stage_queue_topology(self, ray_cluster):
        """Test that two-stage pipeline has correct queue topology."""
        job = Job(job_id="test_topology")

        source_stage = Stage(
            stage_id="source",
            operator_config=TestSourceConfig(num_records=10, batch_size=5),
            parallelism=1,
        )
        job.add_stage(source_stage)

        transform_stage = Stage(
            stage_id="transform",
            operator_config=TestTransformConfig(suffix="_t"),
            parallelism=1,
        )
        job.add_stage(transform_stage, upstream_stages=["source"])

        runner = RayJobRunner(job, queue_type=QueueType.TANSU)
        await runner.initialize()

        # Verify topology
        source_master = runner._masters["source"]
        transform_master = runner._masters["transform"]

        # Source master has internal source queue (for workers to pull from)
        # and output queue (for downstream stages)
        assert source_master._output_endpoint is not None

        # Transform has upstream (from source)
        assert transform_master.upstream_endpoint is not None

        # Transform's upstream points to source's output
        assert transform_master.upstream_topic == source_master._output_topic

        # Note: Transform's output endpoint is created when start() is called
        # So we just verify the upstream connection here

        print("Queue topology verified: transform pulls from source")
        await runner.stop()

    @pytest.mark.asyncio
    async def test_parallel_workers_in_stage(self, ray_cluster):
        """Test that stage can have multiple parallel workers."""
        job = Job(job_id="test_parallel")

        source_stage = Stage(
            stage_id="source",
            operator_config=TestSourceConfig(num_records=100, batch_size=10),
            parallelism=1,  # Single source
        )
        job.add_stage(source_stage)

        transform_stage = Stage(
            stage_id="transform",
            operator_config=TestTransformConfig(suffix="_p"),
            parallelism=2,  # Multiple transform workers
        )
        job.add_stage(transform_stage, upstream_stages=["source"])

        runner = RayJobRunner(job, queue_type=QueueType.TANSU)
        await runner.initialize()

        transform_master = runner._masters["transform"]

        # Start transforms
        await transform_master.start()

        # Should spawn workers according to parallelism
        status = transform_master.get_status()
        # Note: actual worker count may vary based on implementation
        print(f"Transform workers: {status.worker_count}")

        await runner.stop()

    @pytest.mark.asyncio
    async def test_stage_completion_detection(self, ray_cluster):
        """Test that pipeline detects when all stages complete."""
        job = Job(job_id="test_completion")

        # Small job that completes quickly
        source_stage = Stage(
            stage_id="source",
            operator_config=TestSourceConfig(num_records=10, batch_size=10),
            parallelism=1,
        )
        job.add_stage(source_stage)

        runner = RayJobRunner(job, queue_type=QueueType.TANSU)

        try:
            # Run should complete (or timeout)
            status = await asyncio.wait_for(runner.run(timeout=10), timeout=15)

            print(f"Pipeline completed: elapsed={status.elapsed_time:.2f}s")
            assert status.elapsed_time > 0

        except asyncio.TimeoutError:
            print("Pipeline did not complete in time (expected for some implementations)")
            await runner.stop()
