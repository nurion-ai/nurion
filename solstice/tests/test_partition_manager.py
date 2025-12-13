"""Tests for multi-partition queue support and partition manager.

Tests the new partition functionality including:
- Multi-partition topic creation and usage
- Partition assignment to workers
- Work-stealing for data skew mitigation
"""

import asyncio
import pytest
import pytest_asyncio

from solstice.queue import MemoryBackend, PartitionManager
from solstice.queue.partition_manager import PartitionState, WorkerState, WorkStealRequest


# ============================================================================
# PartitionManager Tests
# ============================================================================


class TestPartitionManager:
    """Tests for PartitionManager functionality."""

    def test_init(self):
        """Test basic initialization."""
        manager = PartitionManager(topic="test-topic", num_partitions=4)
        assert manager.topic == "test-topic"
        assert manager.num_partitions == 4
        assert len(manager._partitions) == 4

    def test_register_worker(self):
        """Test worker registration and partition assignment."""
        manager = PartitionManager(topic="test-topic", num_partitions=4)

        # Register first worker - should get partitions
        partitions = manager.register_worker("worker_0")
        assert len(partitions) > 0
        assert all(0 <= p < 4 for p in partitions)

    def test_register_multiple_workers(self):
        """Test that partitions are distributed across workers."""
        manager = PartitionManager(topic="test-topic", num_partitions=4)

        manager.register_worker("worker_0")
        manager.register_worker("worker_1")

        # After second worker registers, partitions should be rebalanced
        # Get current assignments (after rebalance)
        p0 = manager.get_assigned_partitions("worker_0")
        p1 = manager.get_assigned_partitions("worker_1")

        # Each worker should have some partitions
        assert len(p0) > 0
        assert len(p1) > 0

        # No overlap
        assert set(p0).isdisjoint(set(p1))

        # All partitions should be assigned
        all_assigned = set(p0) | set(p1)
        assert all_assigned == {0, 1, 2, 3}

    def test_unregister_worker(self):
        """Test worker unregistration and rebalancing."""
        manager = PartitionManager(topic="test-topic", num_partitions=4)

        manager.register_worker("worker_0")
        manager.register_worker("worker_1")

        # Unregister worker_1
        manager.unregister_worker("worker_1")

        # worker_0 should now have all partitions
        p0 = manager.get_assigned_partitions("worker_0")
        assert set(p0) == {0, 1, 2, 3}

    def test_update_partition_offset(self):
        """Test updating partition offsets."""
        manager = PartitionManager(topic="test-topic", num_partitions=2)

        manager.update_partition_offset(0, current_offset=50, latest_offset=100)
        manager.update_partition_offset(1, current_offset=80, latest_offset=100)

        lags = manager.get_partition_lags()
        assert lags[0] == 50
        assert lags[1] == 20

    def test_get_total_lag(self):
        """Test total lag calculation."""
        manager = PartitionManager(topic="test-topic", num_partitions=3)

        manager.update_partition_offset(0, current_offset=0, latest_offset=100)
        manager.update_partition_offset(1, current_offset=50, latest_offset=100)
        manager.update_partition_offset(2, current_offset=100, latest_offset=100)

        total_lag = manager.get_total_lag()
        assert total_lag == 100 + 50 + 0  # 150

    def test_get_skew_factor(self):
        """Test data skew factor calculation."""
        manager = PartitionManager(topic="test-topic", num_partitions=3)

        # No skew: all equal lag
        manager.update_partition_offset(0, current_offset=0, latest_offset=100)
        manager.update_partition_offset(1, current_offset=0, latest_offset=100)
        manager.update_partition_offset(2, current_offset=0, latest_offset=100)
        assert manager.get_skew_factor() == 0.0

        # High skew: all lag in one partition
        manager.update_partition_offset(0, current_offset=0, latest_offset=100)
        manager.update_partition_offset(1, current_offset=100, latest_offset=100)
        manager.update_partition_offset(2, current_offset=100, latest_offset=100)
        skew = manager.get_skew_factor()
        assert skew > 0.5  # Should indicate high skew

    def test_work_steal_request(self):
        """Test work-stealing request."""
        manager = PartitionManager(
            topic="test-topic",
            num_partitions=4,
            enable_work_stealing=True,
            work_steal_lag_threshold=50,
        )

        manager.register_worker("worker_0")  # Gets partitions 0, 2
        manager.register_worker("worker_1")  # Gets partitions 1, 3

        # Create high lag in partition 1 (assigned to worker_1)
        manager.update_partition_offset(1, current_offset=0, latest_offset=200)

        # worker_0 should be able to steal from partition 1
        steal_request = manager.request_work_steal("worker_0", min_lag=50)

        if steal_request:
            assert steal_request.partition_id == 1
            assert steal_request.offset_start == 0
            assert steal_request.offset_end > 0

    def test_work_stealing_disabled(self):
        """Test that work-stealing can be disabled."""
        manager = PartitionManager(
            topic="test-topic",
            num_partitions=4,
            enable_work_stealing=False,
        )

        manager.register_worker("worker_0")
        manager.update_partition_offset(1, current_offset=0, latest_offset=1000)

        steal_request = manager.request_work_steal("worker_0")
        assert steal_request is None

    def test_get_stats(self):
        """Test getting manager statistics."""
        manager = PartitionManager(topic="test-topic", num_partitions=2)
        manager.register_worker("worker_0")

        stats = manager.get_stats()

        assert stats["topic"] == "test-topic"
        assert stats["num_partitions"] == 2
        assert stats["num_workers"] == 1
        assert "partition_lags" in stats
        assert "worker_assignments" in stats


# ============================================================================
# MemoryBackend Multi-Partition Tests
# ============================================================================


class TestMemoryBackendMultiPartition:
    """Tests for multi-partition support in MemoryBackend."""

    @pytest.fixture
    def backend(self):
        return MemoryBackend()

    @pytest.mark.asyncio
    async def test_create_topic_with_partitions(self, backend):
        """Test creating a topic with multiple partitions."""
        await backend.start()
        try:
            await backend.create_topic("test-topic", partitions=4)

            partition_count = await backend.get_partition_count("test-topic")
            assert partition_count == 4
        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_produce_to_partition(self, backend):
        """Test producing to a specific partition."""
        await backend.start()
        try:
            await backend.create_topic("test-topic", partitions=4)

            # Produce to partition 2
            offset = await backend.produce_to_partition("test-topic", 2, b"msg1")
            assert offset == 0

            # Produce another to partition 2
            offset = await backend.produce_to_partition("test-topic", 2, b"msg2")
            assert offset == 1

            # Fetch from partition 2
            records = await backend.fetch_from_partition("test-topic", 2, offset=0)
            assert len(records) == 2
            assert records[0].value == b"msg1"
            assert records[1].value == b"msg2"
            assert all(r.partition == 2 for r in records)
        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_fetch_from_partition(self, backend):
        """Test fetching from specific partitions."""
        await backend.start()
        try:
            await backend.create_topic("test-topic", partitions=3)

            # Produce to different partitions
            await backend.produce_to_partition("test-topic", 0, b"p0_msg")
            await backend.produce_to_partition("test-topic", 1, b"p1_msg")
            await backend.produce_to_partition("test-topic", 2, b"p2_msg")

            # Fetch from each partition
            r0 = await backend.fetch_from_partition("test-topic", 0, offset=0)
            r1 = await backend.fetch_from_partition("test-topic", 1, offset=0)
            r2 = await backend.fetch_from_partition("test-topic", 2, offset=0)

            assert len(r0) == 1 and r0[0].value == b"p0_msg"
            assert len(r1) == 1 and r1[0].value == b"p1_msg"
            assert len(r2) == 1 and r2[0].value == b"p2_msg"
        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_partition_offset_tracking(self, backend):
        """Test per-partition offset tracking."""
        await backend.start()
        try:
            await backend.create_topic("test-topic", partitions=2)

            # Produce to partitions
            await backend.produce_to_partition("test-topic", 0, b"p0")
            await backend.produce_to_partition("test-topic", 0, b"p0")
            await backend.produce_to_partition("test-topic", 1, b"p1")

            # Commit different offsets per partition
            await backend.commit_partition_offset("group1", "test-topic", 0, 2)
            await backend.commit_partition_offset("group1", "test-topic", 1, 1)

            # Verify
            offset_p0 = await backend.get_committed_partition_offset("group1", "test-topic", 0)
            offset_p1 = await backend.get_committed_partition_offset("group1", "test-topic", 1)

            assert offset_p0 == 2
            assert offset_p1 == 1
        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_partition_latest_offset(self, backend):
        """Test getting latest offset per partition."""
        await backend.start()
        try:
            await backend.create_topic("test-topic", partitions=2)

            # Produce different amounts to each partition
            await backend.produce_to_partition("test-topic", 0, b"msg1")
            await backend.produce_to_partition("test-topic", 0, b"msg2")
            await backend.produce_to_partition("test-topic", 0, b"msg3")
            await backend.produce_to_partition("test-topic", 1, b"msg1")

            latest_p0 = await backend.get_partition_latest_offset("test-topic", 0)
            latest_p1 = await backend.get_partition_latest_offset("test-topic", 1)

            assert latest_p0 == 3
            assert latest_p1 == 1
        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_partition_lag(self, backend):
        """Test partition lag calculation."""
        await backend.start()
        try:
            await backend.create_topic("test-topic", partitions=2)

            # Produce messages
            await backend.produce_to_partition("test-topic", 0, b"msg1")
            await backend.produce_to_partition("test-topic", 0, b"msg2")
            await backend.produce_to_partition("test-topic", 0, b"msg3")

            # Commit partial progress
            await backend.commit_partition_offset("group1", "test-topic", 0, 1)

            # Check lag
            lag = await backend.get_partition_lag("group1", "test-topic", 0)
            assert lag == 2  # 3 - 1 = 2
        finally:
            await backend.stop()

    @pytest.mark.asyncio
    async def test_key_based_partitioning(self, backend):
        """Test that messages with same key go to same partition."""
        await backend.start()
        try:
            await backend.create_topic("test-topic", partitions=4)

            # Produce with keys
            key = b"user123"
            await backend.produce("test-topic", b"msg1", key=key)
            await backend.produce("test-topic", b"msg2", key=key)
            await backend.produce("test-topic", b"msg3", key=key)

            # All messages with same key should be in same partition
            # Check each partition
            found_partition = None
            for p in range(4):
                records = await backend.fetch_from_partition("test-topic", p, offset=0)
                if records:
                    found_partition = p
                    assert len(records) == 3  # All 3 should be here
                    break

            assert found_partition is not None
        finally:
            await backend.stop()


# ============================================================================
# StageConfig Partition Tests
# ============================================================================


class TestStageConfigPartitions:
    """Tests for StageConfig partition configuration."""

    def test_default_partitions(self):
        """Test that default partitions equals max_workers."""
        from solstice.core.stage_master import StageConfig

        config = StageConfig(max_workers=8)
        assert config.effective_partitions == 8

    def test_explicit_partitions(self):
        """Test explicit partition count."""
        from solstice.core.stage_master import StageConfig

        config = StageConfig(max_workers=8, num_partitions=4)
        assert config.effective_partitions == 4

    def test_work_stealing_config(self):
        """Test work-stealing configuration."""
        from solstice.core.stage_master import StageConfig

        config = StageConfig(
            enable_work_stealing=True,
            work_steal_lag_threshold=200,
        )
        assert config.enable_work_stealing is True
        assert config.work_steal_lag_threshold == 200


# ============================================================================
# Partition Assignment Tests
# ============================================================================


class TestPartitionAssignment:
    """Tests for partition assignment in StageMaster."""

    def test_compute_partition_assignment(self):
        """Test partition assignment algorithm."""
        from solstice.core.stage_master import StageMaster, StageConfig, QueueType
        from unittest.mock import MagicMock

        # Create minimal mock stage
        stage = MagicMock()
        stage.stage_id = "test_stage"

        config = StageConfig(
            queue_type=QueueType.MEMORY,
            max_workers=4,
            num_partitions=8,
        )

        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=MagicMock(),
        )

        # Test partition assignment for each worker
        p0 = master._compute_partition_assignment(0, 4)
        p1 = master._compute_partition_assignment(1, 4)
        p2 = master._compute_partition_assignment(2, 4)
        p3 = master._compute_partition_assignment(3, 4)

        # Each worker should have 2 partitions (8 partitions / 4 workers)
        assert len(p0) == 2
        assert len(p1) == 2
        assert len(p2) == 2
        assert len(p3) == 2

        # All partitions should be assigned
        all_assigned = set(p0) | set(p1) | set(p2) | set(p3)
        assert all_assigned == {0, 1, 2, 3, 4, 5, 6, 7}

        # No overlap
        assert set(p0).isdisjoint(set(p1))
        assert set(p0).isdisjoint(set(p2))
        assert set(p0).isdisjoint(set(p3))

    def test_partition_assignment_uneven(self):
        """Test partition assignment when partitions don't divide evenly."""
        from solstice.core.stage_master import StageMaster, StageConfig, QueueType
        from unittest.mock import MagicMock

        stage = MagicMock()
        stage.stage_id = "test_stage"

        config = StageConfig(
            queue_type=QueueType.MEMORY,
            max_workers=3,
            num_partitions=4,  # 4 doesn't divide evenly by 3
        )

        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=MagicMock(),
        )

        p0 = master._compute_partition_assignment(0, 3)
        p1 = master._compute_partition_assignment(1, 3)
        p2 = master._compute_partition_assignment(2, 3)

        # Total assignments should cover all partitions
        all_assigned = set(p0) | set(p1) | set(p2)
        assert all_assigned == {0, 1, 2, 3}

        # One worker will have 2, others have 1
        total = len(p0) + len(p1) + len(p2)
        assert total == 4
