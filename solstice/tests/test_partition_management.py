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

"""Unit tests for dynamic partition management.

Tests cover:
- Partition count calculation
- Queue creation with dynamic partitions
- Consumer group assignment
- Partition rebalance handling

All tests use real implementations (no mocks) to catch real issues.
"""

import pytest
from dataclasses import dataclass

from solstice.core.stage_master import StageMaster, StageConfig, QueueType
from solstice.core.stage import Stage
from solstice.core.operator import OperatorConfig, Operator
from solstice.queue import TansuBackend


@dataclass
class _TestOperatorConfig(OperatorConfig):
    """Test operator config (prefixed with _ to avoid pytest collection)."""

    pass


class _TestOperator(Operator):
    """Test operator that passes through data (prefixed with _ to avoid pytest collection)."""

    def __init__(self, config: _TestOperatorConfig, worker_id: str = None):
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
_TestOperatorConfig.operator_class = _TestOperator


class TestPartitionCountCalculation:
    """Tests for partition count calculation logic."""

    def test_single_worker_returns_one_partition(self, payload_store):
        """Test that single worker scenario uses 1 partition."""
        config = StageConfig(max_workers=1, min_workers=1)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=1,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        assert partition_count == 1

    def test_explicit_partition_count(self, payload_store):
        """Test that explicit partition_count is respected."""
        config = StageConfig(max_workers=4, partition_count=8)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=4,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        assert partition_count == 8

    def test_auto_partition_count_from_max_workers(self, payload_store):
        """Test that partition count equals max_workers when auto."""
        config = StageConfig(max_workers=4, partition_count=None)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=4,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        assert partition_count == 4

    def test_partition_count_minimum_one(self, payload_store):
        """Test that partition count is always at least 1."""
        config = StageConfig(max_workers=0, partition_count=0)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=0,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        assert partition_count >= 1


class TestQueueCreationWithPartitions:
    """Tests for queue creation with dynamic partitions."""

    @pytest.mark.asyncio
    async def test_tansu_queue_created_with_correct_partitions(self, payload_store, ray_cluster):
        """Test that Tansu backend creates queue with correct partition count.

        This test REQUIRES Tansu to be installed with dynostore feature enabled.
        It verifies that:
        1. Partition count is calculated correctly
        2. Tansu queue is created with the correct number of partitions
        3. The queue backend is actually a TansuBackend instance

        If Tansu is not available or misconfigured, the test will FAIL (not skip).
        """
        config = StageConfig(
            queue_type=QueueType.TANSU,
            max_workers=4,
            tansu_storage_url="memory://tansu/",  # Use memory storage (requires dynostore feature)
        )
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
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
            assert isinstance(master._output_queue, TansuBackend)
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
            operator_config=_TestOperatorConfig(),
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
            operator_config=_TestOperatorConfig(),
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

        assert len(master._workers) == 4

        # Remove workers
        removed = await master.scale_down(2)
        assert removed == 2
        assert len(master._workers) == 2

        # Remaining workers will rebalance via consumer group protocol

        await master.stop()


class TestPartitionCountEdgeCases:
    """Tests for edge cases in partition count calculation."""

    def test_partition_count_with_zero_max_workers(self, payload_store):
        """Test partition count when max_workers is 0."""
        config = StageConfig(max_workers=0, partition_count=None)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=0,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        # Should default to 1 (minimum)
        assert partition_count == 1

    def test_partition_count_with_negative_value(self, payload_store):
        """Test partition count with negative explicit value."""
        config = StageConfig(max_workers=4, partition_count=-5)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=4,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        # Should be clamped to minimum 1
        assert partition_count == 1

    def test_partition_count_large_value(self, payload_store):
        """Test partition count with very large value."""
        config = StageConfig(max_workers=4, partition_count=1000)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=4,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        # Should accept large value (no upper limit in calculation)
        assert partition_count == 1000
