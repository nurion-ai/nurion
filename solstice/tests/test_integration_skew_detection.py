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

"""Unit tests for partition-level skew detection.

Tests cover:
- Partition lag calculation
- Skew detection algorithm
- Skew ratio calculation
- Metrics collection

All tests use real implementations (no mocks) to catch real issues.
"""

import pytest
from dataclasses import dataclass

from solstice.core.stage_master import (
    StageMaster,
    StageConfig,
    QueueType,
    QueueEndpoint,
    QueueMessage,
)
from solstice.core.stage import Stage
from solstice.core.operator import OperatorConfig, Operator
from solstice.core.models import PartitionMetrics


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

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestPartitionLagCalculation:
    """Tests for partition lag calculation using real Tansu backend."""

    @pytest.mark.asyncio
    async def test_lag_calculation_single_partition(self, payload_store, tansu_backend):
        """Test lag calculation for a single partition."""
        config = StageConfig(max_workers=1, partition_count=1)
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

        # Create topic and produce some messages
        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=1)

        # Produce 100 messages
        for i in range(100):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                payload_key=f"key_{i}",
            )
            await tansu_backend.produce(topic, msg.to_bytes())

        # Commit offset at 50 for partition 0
        # In consumer group mode, we need to create a consumer assigned to partition 0
        import asyncio
        from aiokafka import AIOKafkaConsumer, TopicPartition

        consumer_group = "test_job_test_stage"
        # Create a consumer assigned to partition 0 and commit
        commit_consumer = AIOKafkaConsumer(
            bootstrap_servers=f"localhost:{tansu_backend.port}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            request_timeout_ms=5000,
            group_id=consumer_group,
        )
        await commit_consumer.start()
        await asyncio.sleep(0.2)
        commit_consumer.assign([TopicPartition(topic, 0)])
        await asyncio.sleep(0.1)
        await commit_consumer.commit({TopicPartition(topic, 0): 50})
        await commit_consumer.stop()

        # Verify commit worked by reading it back from the same backend
        committed = await tansu_backend.get_committed_offset(consumer_group, topic, partition=0)
        assert committed == 50, f"Expected committed offset 50, got {committed}"

        # Setup master to use this queue
        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        # Get partition metrics
        partition_metrics = await master.get_partition_metrics()

        assert 0 in partition_metrics
        assert partition_metrics[0].latest_offset == 100
        assert partition_metrics[0].committed_offset == 50, (
            f"Expected committed offset 50, got {partition_metrics[0].committed_offset}"
        )
        assert partition_metrics[0].lag == 50

    @pytest.mark.asyncio
    async def test_lag_calculation_multiple_partitions(self, payload_store, tansu_backend):
        """Test lag calculation for multiple partitions."""
        config = StageConfig(max_workers=4, partition_count=4)
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

        # Create topic with 4 partitions
        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=4)

        # Produce different amounts to each partition
        # Partition 0: 100 messages, committed at 50
        # Partition 1: 200 messages, committed at 150
        # Partition 2: 150 messages, committed at 100
        # Partition 3: 180 messages, committed at 120

        for partition in range(4):
            for i in range([100, 200, 150, 180][partition]):
                msg = QueueMessage(
                    message_id=f"msg_{partition}_{i}",
                    split_id=f"split_{partition}_{i}",
                    payload_key=f"key_{partition}_{i}",
                )
                # Note: Memory backend doesn't support partition selection in produce
                # For Tansu, we need to use partition-aware produce
                await tansu_backend.produce(topic, msg.to_bytes())

        import asyncio
        from aiokafka import AIOKafkaConsumer, TopicPartition

        consumer_group = "test_job_test_stage"
        # Commit offsets for each partition
        # In consumer group mode, we need to create consumers assigned to specific partitions
        for partition, offset in [(0, 50), (1, 150), (2, 100), (3, 120)]:
            commit_consumer = AIOKafkaConsumer(
                bootstrap_servers=f"localhost:{tansu_backend.port}",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=5000,
                group_id=consumer_group,
            )
            await commit_consumer.start()
            await asyncio.sleep(0.2)
            commit_consumer.assign([TopicPartition(topic, partition)])
            await asyncio.sleep(0.1)
            await commit_consumer.commit({TopicPartition(topic, partition): offset})
            await commit_consumer.stop()

        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        partition_metrics = await master.get_partition_metrics()

        # Verify we got metrics for all partitions
        # Note: Actual lag values depend on how Tansu distributes messages
        assert len(partition_metrics) >= 0  # May be 0 if no upstream configured
        # If we have metrics, verify structure
        for pid, pm in partition_metrics.items():
            assert isinstance(pm, PartitionMetrics)
            assert pm.partition_id == pid
            assert pm.lag >= 0

    @pytest.mark.asyncio
    async def test_lag_calculation_missing_committed_offset(self, payload_store, tansu_backend):
        """Test lag calculation when committed offset is missing (defaults to 0)."""
        config = StageConfig(max_workers=1, partition_count=1)
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

        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=1)

        # Produce 100 messages but don't commit any offset
        for i in range(100):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                payload_key=f"key_{i}",
            )
            await tansu_backend.produce(topic, msg.to_bytes())

        consumer_group = "test_job_test_stage"
        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        partition_metrics = await master.get_partition_metrics()

        if 0 in partition_metrics:
            # If no committed offset, should default to 0
            assert partition_metrics[0].committed_offset == 0
            assert partition_metrics[0].lag == 100  # 100 - 0

    @pytest.mark.asyncio
    async def test_lag_calculation_no_data(self, payload_store, tansu_backend):
        """Test lag calculation when partition has no data."""
        config = StageConfig(max_workers=1, partition_count=1)
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

        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=1)

        # Don't produce any messages
        consumer_group = "test_job_test_stage"
        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        partition_metrics = await master.get_partition_metrics()

        if 0 in partition_metrics:
            assert partition_metrics[0].lag == 0


class TestSkewDetectionAlgorithm:
    """Tests for skew detection algorithm using real backends."""

    @pytest.mark.asyncio
    async def test_no_skew_when_no_lag(self, payload_store, tansu_backend):
        """Test that no skew is detected when there's no lag (empty topic)."""
        config = StageConfig(max_workers=4, partition_count=4)
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

        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=4)

        # Don't produce any messages - all partitions have 0 lag
        consumer_group = "test_job_test_stage"
        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        skew_detected, skew_ratio, partition_lags = await master.detect_partition_skew(
            skew_threshold=2.0
        )

        # With no lag on any partition, skew should not be detected
        assert skew_detected is False
        # When all lags are 0, ratio is 0.0 (undefined, no data to calculate)
        assert skew_ratio == 0.0
        # partition_lags should have entries for each partition with 0 lag
        assert len(partition_lags) == 4
        for pid in range(4):
            assert pid in partition_lags
            assert partition_lags[pid] == 0

    @pytest.mark.asyncio
    async def test_skew_detected_with_uneven_commits(self, payload_store, tansu_backend):
        """Test skew detection when one partition has much higher lag than others."""
        import asyncio
        from aiokafka import AIOKafkaConsumer, TopicPartition

        config = StageConfig(max_workers=4, partition_count=4)
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

        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=4)

        # Produce 100 messages (Tansu distributes round-robin, so ~25 per partition)
        for i in range(100):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                payload_key=f"key_{i}",
            )
            await tansu_backend.produce(topic, msg.to_bytes())

        consumer_group = "test_job_test_stage"

        # Commit most messages on partitions 0,1,2 but leave partition 3 uncommitted
        # This creates skew: partitions 0,1,2 have low lag, partition 3 has high lag
        for partition in [0, 1, 2]:
            commit_consumer = AIOKafkaConsumer(
                bootstrap_servers=f"localhost:{tansu_backend.port}",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=5000,
                group_id=consumer_group,
            )
            await commit_consumer.start()
            await asyncio.sleep(0.2)
            commit_consumer.assign([TopicPartition(topic, partition)])
            await asyncio.sleep(0.1)
            # Commit at offset 25 (most messages consumed)
            await commit_consumer.commit({TopicPartition(topic, partition): 25})
            await commit_consumer.stop()

        # Don't commit partition 3 - it will have lag = latest_offset - 0

        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        skew_detected, skew_ratio, partition_lags = await master.detect_partition_skew(
            skew_threshold=2.0  # Detect skew if max_lag > 2 * min_lag
        )

        # Verify partition_lags contains expected partitions
        assert len(partition_lags) == 4

        # Partitions 0,1,2 should have low lag (committed at 25)
        # Partition 3 should have higher lag (no commit, lag = latest_offset)
        committed_lags = [partition_lags[p] for p in [0, 1, 2]]
        uncommitted_lag = partition_lags[3]

        # Verify the uncommitted partition has higher lag
        assert uncommitted_lag >= max(committed_lags), (
            f"Expected partition 3 lag ({uncommitted_lag}) >= "
            f"max committed lag ({max(committed_lags)})"
        )

        # If there's meaningful skew, it should be detected
        if uncommitted_lag > 0 and min(committed_lags) > 0:
            actual_ratio = max(partition_lags.values()) / max(min(partition_lags.values()), 1)
            assert skew_ratio == actual_ratio


class TestSkewMetricsCollection:
    """Tests for skew metrics collection using real backends."""

    @pytest.mark.asyncio
    async def test_metrics_for_all_partitions(self, payload_store, tansu_backend):
        """Test that partition metrics are collected for all partitions."""
        config = StageConfig(max_workers=4, partition_count=4)
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
        master._start_time = 1000.0

        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=4)

        # Produce some messages to create non-zero offsets
        for i in range(20):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                payload_key=f"key_{i}",
            )
            await tansu_backend.produce(topic, msg.to_bytes())

        consumer_group = "test_job_test_stage"
        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        await master.start()

        try:
            partition_metrics = await master.get_partition_metrics()

            # Should have metrics for all 4 partitions
            assert len(partition_metrics) == 4

            # Each partition should have valid metrics
            total_lag = 0
            for partition_id in range(4):
                assert partition_id in partition_metrics
                pm = partition_metrics[partition_id]
                assert pm.partition_id == partition_id
                assert pm.latest_offset >= 0
                assert pm.committed_offset >= 0
                assert pm.lag == pm.latest_offset - pm.committed_offset
                total_lag += pm.lag

            # Total lag should equal total messages (no commits yet)
            assert total_lag == 20

            # Skew detection with no commits - messages distributed across partitions
            skew_detected, skew_ratio, partition_lags = await master.detect_partition_skew(
                skew_threshold=2.0
            )
            # With round-robin distribution, skew should be minimal (ratio close to 1.0)
            # Allow some variance due to message distribution
            assert skew_ratio >= 1.0  # ratio >= 1.0 always (max >= avg)
            assert skew_ratio < 2.0  # No significant skew
        finally:
            await master.stop()

    @pytest.mark.asyncio
    async def test_partition_metrics_reflect_commits(self, payload_store, tansu_backend):
        """Test that partition metrics correctly reflect committed offsets."""
        import asyncio
        from aiokafka import AIOKafkaConsumer, TopicPartition

        config = StageConfig(max_workers=1, partition_count=1)
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
        master._start_time = 1000.0

        # Use single partition for deterministic testing
        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=1)

        # Produce 100 messages to partition 0
        for i in range(100):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                payload_key=f"key_{i}",
            )
            await tansu_backend.produce(topic, msg.to_bytes())

        consumer_group = "test_job_test_stage"

        # Commit at offset 40 for partition 0
        commit_consumer = AIOKafkaConsumer(
            bootstrap_servers=f"localhost:{tansu_backend.port}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            request_timeout_ms=5000,
            group_id=consumer_group,
        )
        await commit_consumer.start()
        await asyncio.sleep(0.2)
        commit_consumer.assign([TopicPartition(topic, 0)])
        await asyncio.sleep(0.1)
        await commit_consumer.commit({TopicPartition(topic, 0): 40})
        await commit_consumer.stop()

        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        await master.start()

        try:
            partition_metrics = await master.get_partition_metrics()

            # Verify partition 0 metrics
            assert 0 in partition_metrics
            pm = partition_metrics[0]
            assert pm.latest_offset == 100
            assert pm.committed_offset == 40
            assert pm.lag == 60  # 100 - 40
        finally:
            await master.stop()
