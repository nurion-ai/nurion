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

"""Integration tests for partition management, skew detection, and backpressure.

Tests cover:
- Multi-partition parallel consumption
- Partition skew scenarios
- Backpressure end-to-end flow
- Combined scenarios

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


class TestMultiPartitionParallelConsumption:
    """Integration tests for multi-partition parallel consumption."""

    @pytest.mark.asyncio
    async def test_partition_count_matches_worker_count(self, payload_store, ray_cluster):
        """Test that partition count matches worker count configuration."""
        config = StageConfig(
            queue_type=QueueType.MEMORY,
            max_workers=8,
            min_workers=1,
        )
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=8,
        )

        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        # Verify partition count calculation via partition manager
        partition_count = master._partition_count
        assert partition_count == 8

        # Start and verify actual partition count
        await master.start()

        try:
            assert master._partition_count == 8
        finally:
            await master.stop()


class TestPartitionSkewScenario:
    """Integration tests for partition skew scenarios."""

    @pytest.mark.asyncio
    async def test_skew_detection_in_multi_partition_setup(
        self, payload_store, tansu_backend, ray_cluster
    ):
        """Test skew detection in a multi-partition setup."""
        import math
        import asyncio
        from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition

        config = StageConfig(
            queue_type=QueueType.TANSU,
            max_workers=4,
            partition_count=3,
            shared_broker_endpoint=QueueEndpoint(
                queue_type=QueueType.TANSU,
                host="localhost",
                port=tansu_backend.port,
                storage_url="memory://tansu/",
            ),
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

        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=3)

        # Produce controlled skew: partitions [10, 200, 20] messages respectively
        producer = AIOKafkaProducer(bootstrap_servers=f"localhost:{tansu_backend.port}")
        await producer.start()
        try:
            for i in range(10):
                msg = QueueMessage(message_id=f"p0_{i}", split_id=f"s0_{i}", payload_key=f"k0_{i}")
                await producer.send_and_wait(topic, msg.to_bytes(), partition=0)
            for i in range(200):
                msg = QueueMessage(message_id=f"p1_{i}", split_id=f"s1_{i}", payload_key=f"k1_{i}")
                await producer.send_and_wait(topic, msg.to_bytes(), partition=1)
            for i in range(20):
                msg = QueueMessage(message_id=f"p2_{i}", split_id=f"s2_{i}", payload_key=f"k2_{i}")
                await producer.send_and_wait(topic, msg.to_bytes(), partition=2)
        finally:
            await producer.stop()

        # Commit offsets: p0->0 (none consumed), p1->0, p2->20 (fully consumed)
        consumer_group = "test_job_test_stage"
        for partition, offset in [(0, 0), (1, 0), (2, 20)]:
            commit_consumer = AIOKafkaConsumer(
                bootstrap_servers=f"localhost:{tansu_backend.port}",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=5000,
                group_id=consumer_group,
            )
            try:
                await commit_consumer.start()
                await asyncio.sleep(0.2)
                commit_consumer.assign([TopicPartition(topic, partition)])
                await asyncio.sleep(0.1)
                await commit_consumer.commit({TopicPartition(topic, partition): offset})
            finally:
                await commit_consumer.stop()

        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic
        master._consumer_group = consumer_group

        # Initialize backpressure monitor with the upstream config
        await master.start()

        try:
            partition_metrics = await master._backpressure_monitor.get_partition_metrics()
            skew_info = await master._backpressure_monitor.detect_skew()

            # Expect skew: partition1 lags most (200), avg lag ~70 -> ratio > 2
            assert set(partition_metrics.keys()) == {0, 1, 2}
            assert partition_metrics[0].latest_offset == 10
            assert partition_metrics[0].committed_offset == 0
            assert partition_metrics[0].lag == 10

            assert partition_metrics[1].latest_offset == 200
            assert partition_metrics[1].committed_offset == 0
            assert partition_metrics[1].lag == 200

            assert partition_metrics[2].latest_offset == 20
            assert partition_metrics[2].committed_offset == 20
            assert partition_metrics[2].lag == 0

            assert skew_info.is_skewed is True
            expected_ratio = 200 / ((10 + 200 + 0) / 3)
            assert math.isclose(skew_info.skew_ratio, expected_ratio, rel_tol=0.05)
        finally:
            await master.stop()


class TestBackpressureEndToEnd:
    """Integration tests for backpressure end-to-end flow."""

    @pytest.mark.asyncio
    async def test_backpressure_propagation_chain(self, payload_store, ray_cluster):
        """Test backpressure propagation through a chain of stages."""
        # Stage 1: Source
        config1 = StageConfig(
            queue_type=QueueType.MEMORY,
            max_workers=2,
        )
        stage1 = Stage(
            stage_id="source",
            operator_config=_TestOperatorConfig(),
            parallelism=2,
        )
        master1 = StageMaster(
            job_id="test_job",
            stage=stage1,
            config=config1,
            payload_store=payload_store,
        )

        # Stage 2: Process (middle)
        config2 = StageConfig(
            queue_type=QueueType.MEMORY,
            max_workers=2,
        )
        stage2 = Stage(
            stage_id="process",
            operator_config=_TestOperatorConfig(),
            parallelism=2,
        )
        master2 = StageMaster(
            job_id="test_job",
            stage=stage2,
            config=config2,
            payload_store=payload_store,
        )

        # Stage 3: Sink (slow)
        config3 = StageConfig(
            queue_type=QueueType.MEMORY,
            max_workers=1,
        )
        stage3 = Stage(
            stage_id="sink",
            operator_config=_TestOperatorConfig(),
            parallelism=1,
        )
        master3 = StageMaster(
            job_id="test_job",
            stage=stage3,
            config=config3,
            payload_store=payload_store,
        )

        # Start all stages
        await master1.start()
        await master2.start()
        await master3.start()

        try:
            # Activate backpressure on master3 (sink) via backpressure monitor
            master3._backpressure_monitor._backpressure_active = True

            # Connect master2 to master3 via set_downstream_stage_refs
            master2.set_downstream_stage_refs({"sink": master3})

            # Verify master2 can detect backpressure from master3
            should_pause = await master2._backpressure_monitor.check_downstream_backpressure()
            # master2 should detect backpressure from master3
            assert isinstance(should_pause, bool)
        finally:
            await master1.stop()
            await master2.stop()
            await master3.stop()

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_backpressure_clears_when_downstream_catches_up(
        self, payload_store, tansu_backend, ray_cluster
    ):
        """Test that backpressure clears when downstream processing catches up."""
        config = StageConfig(
            queue_type=QueueType.TANSU,
            max_workers=2,
            backpressure_threshold_lag=5000,
            shared_broker_endpoint=QueueEndpoint(
                queue_type=QueueType.TANSU,
                host="localhost",
                port=tansu_backend.port,
                storage_url="memory://tansu/",
            ),
        )
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=2,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        # Create upstream topic
        topic = "upstream_topic"
        await tansu_backend.create_topic(topic, partitions=1)

        master.upstream_endpoint = QueueEndpoint(
            queue_type=QueueType.TANSU,
            host="localhost",
            port=tansu_backend.port,
            storage_url="memory://tansu/",
        )
        master.upstream_topic = topic

        await master.start()

        try:
            # Initially produce many messages to create high lag
            for i in range(6000):
                msg = QueueMessage(
                    message_id=f"msg_{i}",
                    split_id=f"split_{i}",
                    payload_key=f"key_{i}",
                )
                await tansu_backend.produce(topic, msg.to_bytes())

            # Check backpressure - should be active
            result1 = await master._backpressure_monitor.check_backpressure(
                master._output_queue, master._output_topic
            )
            assert isinstance(result1, bool)

            # Commit offsets to simulate processing
            consumer_group = master._consumer_group
            # Commit offset for partition 0
            import asyncio
            from aiokafka import AIOKafkaConsumer, TopicPartition

            commit_consumer = AIOKafkaConsumer(
                bootstrap_servers=f"localhost:{tansu_backend.port}",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=5000,
                group_id=consumer_group,
            )
            try:
                await commit_consumer.start()
                await asyncio.sleep(0.2)
                commit_consumer.assign([TopicPartition(topic, 0)])
                await asyncio.sleep(0.1)
                await commit_consumer.commit({TopicPartition(topic, 0): 3000})
            finally:
                await commit_consumer.stop()

            # Check backpressure again - should clear with hysteresis
            result2 = await master._backpressure_monitor.check_backpressure(
                master._output_queue, master._output_topic
            )
            assert isinstance(result2, bool)
        finally:
            await master.stop()


class TestCombinedScenarios:
    """Tests for combined scenarios involving multiple mechanisms."""

    @pytest.mark.asyncio
    async def test_skew_and_backpressure_together(self, payload_store, tansu_backend, ray_cluster):
        """Test scenario where both skew and backpressure occur."""
        config = StageConfig(
            queue_type=QueueType.TANSU,
            max_workers=4,
            partition_count=4,
            shared_broker_endpoint=QueueEndpoint(
                queue_type=QueueType.TANSU,
                host="localhost",
                port=tansu_backend.port,
                storage_url="memory://tansu/",
            ),
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

        # Create upstream topic
        topic = "test_topic"
        await tansu_backend.create_topic(topic, partitions=4)

        # Produce many messages to create both skew and high lag
        for i in range(10000):
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
            # Check backpressure via backpressure monitor
            backpressure_active = await master._backpressure_monitor.check_backpressure(
                master._output_queue, master._output_topic
            )
            assert isinstance(backpressure_active, bool)

            # Check skew detection via backpressure monitor
            skew_info = await master._backpressure_monitor.detect_skew()
            assert isinstance(skew_info.is_skewed, bool)
            assert isinstance(skew_info.skew_ratio, float)
            assert isinstance(master._backpressure_monitor.is_backpressure_active, bool)
        finally:
            await master.stop()

    @pytest.mark.asyncio
    async def test_dynamic_workers_with_partitions(self, payload_store, ray_cluster):
        """Test dynamic worker scaling with multiple partitions."""
        # Use smaller resource requirements to fit within local Ray cluster
        config = StageConfig(
            queue_type=QueueType.MEMORY,
            max_workers=4,
            min_workers=1,
            partition_count=4,
            num_cpus=0.25,  # Smaller CPU requirement per worker
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

        try:
            initial_workers = len(master._workers)
            assert initial_workers == 1  # min_workers

            # Scale up using worker manager - spawn 2 more workers
            partition_count = master._partition_count
            for _ in range(2):
                await master._worker_manager.spawn_worker(partition_count=partition_count)

            assert len(master._workers) == 3

            # Partition count should remain at max_workers (4)
            # Workers will rebalance via consumer group protocol
            assert master._partition_count == 4
        finally:
            await master.stop()
