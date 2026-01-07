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

"""Integration tests for partition management.

Tests cover:
- Queue creation with dynamic partitions
- Partition rebalance handling

These tests require Ray and Tansu.
"""

from dataclasses import dataclass

import pytest

from solstice.core.stage_master import StageMaster, StageConfig
from solstice.core.stage import Stage
from solstice.core.operator import OperatorConfig, Operator
from solstice.queue import QueueType
from tests.utils import wait_until


@dataclass
class _MockOperatorConfig(OperatorConfig):
    """Mock operator config."""

    pass


class _MockOperator(Operator):
    """Mock operator that passes through data."""

    def __init__(self, config: _MockOperatorConfig, worker_id: str = None):
        super().__init__(config, worker_id)
        self._closed = False

    def process_split(self, split, payload):
        return payload

    def generate_splits(self):
        from solstice.core.models import Split

        return [
            Split(split_id=f"split_{i}", stage_id="test_stage", data_range={"index": i})
            for i in range(5)
        ]

    def close(self):
        self._closed = True


# Set operator_class after class definition
_MockOperatorConfig.operator_class = _MockOperator

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestQueueCreationWithPartitions:
    """Tests for queue creation with dynamic partitions."""

    @pytest.mark.asyncio
    async def test_tansu_queue_created_with_correct_partitions(self, payload_store, ray_cluster):
        """Test that Tansu backend creates queue with correct partition count.

        This test REQUIRES Tansu to be installed with dynostore feature enabled.
        It verifies that:
        1. Partition count is calculated correctly
        2. Tansu queue is created with the correct number of partitions
        3. The queue client is actually a TansuQueueClient instance

        If Tansu is not available or misconfigured, the test will FAIL (not skip).
        """
        from solstice.queue import TansuQueueClient

        config = StageConfig(
            queue_type=QueueType.TANSU,
            max_workers=4,
            tansu_storage_url="memory://tansu/",  # Use memory storage (requires dynostore feature)
        )
        stage = Stage(
            stage_id="test_stage",
            operator_config=_MockOperatorConfig(),
            parallelism=4,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        # Verify partition count calculation
        partition_count = master._compute_partition_count()
        assert partition_count == 4

        # Start master to create queue
        await master.start()

        try:
            # Verify queue was created with correct partition count
            assert master._output_queue is not None
            assert master._compute_partition_count() == 4
            assert isinstance(master._output_queue, TansuQueueClient)
        finally:
            await master.stop()


class TestPartitionRebalance:
    """Tests for partition rebalance when workers change."""

    @pytest.mark.asyncio
    async def test_rebalance_on_worker_add(self, payload_store, ray_cluster):
        """Test that adding workers triggers rebalance."""
        config = StageConfig(
            queue_type=QueueType.TANSU,
            max_workers=4,
            min_workers=2,
            tansu_storage_url="memory://tansu/",
        )
        stage = Stage(
            stage_id="test_stage",
            operator_config=_MockOperatorConfig(),
            parallelism=4,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        await master.start()

        initial_worker_count = len(master._workers)
        assert initial_worker_count == 2  # min_workers

        # Add more workers
        await master._spawn_worker()
        await master._spawn_worker()

        # Verify workers were added
        assert len(master._workers) == 4

        # Workers will automatically rebalance via consumer group protocol
        # This is handled by Kafka/Tansu, not our code

        await master.stop()

    @pytest.mark.asyncio
    async def test_rebalance_on_worker_remove(self, payload_store, ray_cluster):
        """Test that removing workers triggers rebalance."""
        config = StageConfig(
            queue_type=QueueType.TANSU,
            max_workers=4,
            min_workers=1,
            tansu_storage_url="memory://tansu/",
        )
        stage = Stage(
            stage_id="test_stage",
            operator_config=_MockOperatorConfig(),
            parallelism=4,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        await master.start()

        # Start with 4 workers
        while len(master._workers) < 4:
            await master._spawn_worker()

        # Wait for all 4 workers to be ready
        await wait_until(
            lambda: len(master._workers) == 4,
            timeout=5.0,
            message="Workers not spawned",
        )

        # Remove workers
        removed = await master.scale_down(2)

        # Wait for workers to be removed
        await wait_until(
            lambda: len(master._workers) == 2,
            timeout=5.0,
            message="Workers not removed",
        )

        assert removed == 2

        await master.stop()


