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

"""End-to-end tests for v2 pipeline architecture.

Tests the complete flow:
- Source operator generating data
- Transform operators processing data
- Queue-based communication between stages
- Worker pull model
"""

import pytest
from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from solstice.core.job import Job, JobConfig
from solstice.core.stage import Stage
from solstice.core.operator import Operator, OperatorConfig
from solstice.core.models import Split, SplitPayload
from solstice.queue import QueueType
from solstice.runtime.ray_runner import RayJobRunner
from solstice.operators.sources.source import SourceMaster

pytestmark = pytest.mark.asyncio(loop_scope="function")


# ============================================================================
# Test Operators
# ============================================================================


class MockSourceOperator(Operator):
    """Source operator that generates test data."""

    def __init__(self, config: "MockSourceConfig", worker_id: str = None):
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
class MockSourceConfig(OperatorConfig):
    """Config for test source operator."""

    num_records: int = 100
    batch_size: int = 10


# Set operator_class after class definition
MockSourceConfig.operator_class = MockSourceOperator


class MockSourceMaster(SourceMaster):
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
MockSourceConfig.master_class = MockSourceMaster


class MockTransformOperator(Operator):
    """Transform operator that modifies data."""

    def __init__(self, config: "MockTransformConfig", worker_id: str = None):
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
class MockTransformConfig(OperatorConfig):
    """Config for test transform operator."""

    suffix: str = "_transformed"


# Set operator_class after class definition
MockTransformConfig.operator_class = MockTransformOperator


class MockSinkOperator(Operator):
    """Sink operator that collects results."""

    # Shared storage for test verification
    collected_records: List[Dict] = []

    def __init__(self, config: "MockSinkConfig", worker_id: str = None):
        super().__init__(config, worker_id)

    def process_split(
        self, split: Split, payload: Optional[SplitPayload]
    ) -> Optional[SplitPayload]:
        """Collect records from payload."""
        if payload is None:
            return None

        records = payload.to_pylist()
        MockSinkOperator.collected_records.extend(records)

        # Sink doesn't produce output
        return None

    def close(self) -> None:
        pass

    @classmethod
    def reset(cls):
        cls.collected_records = []


@dataclass
class MockSinkConfig(OperatorConfig):
    """Config for test sink operator."""

    pass


# Set operator_class after class definition
MockSinkConfig.operator_class = MockSinkOperator


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def simple_job():
    """Create a simple single-stage job."""
    job = Job(
        job_id="test_simple",
        config=JobConfig(queue_type=QueueType.TANSU),
    )

    source_stage = Stage(
        stage_id="source",
        operator_config=MockSourceConfig(num_records=50, batch_size=10),
        parallelism=(1, 2),  # (min, max)
    )
    job.add_stage(source_stage)

    return job


# ============================================================================
# Tests
# ============================================================================


class TestRayJobRunner:
    """Tests for RayJobRunner."""

    @pytest.mark.asyncio
    async def test_initialization(self, simple_job, ray_cluster):
        """Test runner initialization."""
        runner = RayJobRunner(simple_job)

        assert not runner.is_initialized
        assert not runner.is_running

        await runner.initialize()

        assert runner.is_initialized
        assert "source" in runner._masters

    @pytest.mark.asyncio
    async def test_get_status(self, simple_job, ray_cluster):
        """Test getting pipeline status."""
        runner = RayJobRunner(simple_job)
        await runner.initialize()

        status = runner.get_status()

        assert status.job_id == "test_simple"
        assert not status.is_running
        assert "source" in status.stages

        await runner.stop()

    @pytest.mark.asyncio
    async def test_stop_before_run(self, simple_job, ray_cluster):
        """Test stopping before running."""
        runner = RayJobRunner(simple_job)
        await runner.initialize()
        await runner.stop()  # Should not raise

        assert not runner.is_running


class TestExactlyOnce:
    """Tests for exactly-once semantics."""

    @pytest.mark.asyncio
    async def test_offset_tracking(self):
        """Test that offsets are tracked correctly."""
        from solstice.queue import MemoryBroker, MemoryClient

        # Create a shared queue
        broker = MemoryBroker()
        await broker.start()
        client = MemoryClient(broker)
        await client.start()

        topic = "test_topic"
        group = "test_group"
        await client.create_topic(topic)

        # Produce messages
        from solstice.core.stage_master import QueueMessage

        for i in range(10):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                payload_key=f"ref_{i}",
            )
            await client.produce(topic, msg.to_bytes())

        # Consume and commit
        records = await client.fetch(topic, offset=0, max_records=5)
        assert len(records) == 5

        await client.commit_offset(group, topic, 5)

        # Verify committed offset
        committed = await client.get_committed_offset(group, topic)
        assert committed == 5

        # Resume from committed
        remaining = await client.fetch(topic, offset=committed)
        assert len(remaining) == 5

        await client.stop()
        await broker.stop()


# ============================================================================
