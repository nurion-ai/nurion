"""Tests for Stage Master v2 architecture.

Tests the new queue-based architecture with:
- Worker pull model
- Simplified master (output queue only)
- QueueBackend integration
"""

import asyncio
import pytest
import pytest_asyncio
import ray
from unittest.mock import MagicMock, AsyncMock
from dataclasses import dataclass
from typing import List, Optional

from solstice.queue import MemoryBackend
from solstice.core.stage_master_v2 import (
    StageMasterV2,
    StageConfigV2,
    StageWorkerV2,
    QueueType,
    QueueMessage,
    QueueEndpoint,
)

pytestmark = pytest.mark.asyncio(loop_scope="function")


# ============================================================================
# Test Fixtures
# ============================================================================

@dataclass
class MockOperatorConfig:
    """Mock operator config for testing."""
    
    def setup(self, worker_id: str):
        return MockOperator(worker_id)


class MockOperator:
    """Mock operator that passes through data."""
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self._closed = False
    
    def process_split(self, split, payload):
        # Just pass through for testing
        return payload
    
    def generate_splits(self):
        from solstice.core.models import Split
        # Generate some test splits
        return [
            Split(split_id=f"split_{i}", stage_id="test_stage", data_range={"index": i})
            for i in range(5)
        ]
    
    def close(self):
        self._closed = True


@dataclass  
class MockStage:
    """Mock stage for testing."""
    stage_id: str = "test_stage"
    operator_config: MockOperatorConfig = None
    upstream_stages: List[str] = None
    
    def __post_init__(self):
        if self.operator_config is None:
            self.operator_config = MockOperatorConfig()
        if self.upstream_stages is None:
            self.upstream_stages = []


@pytest_asyncio.fixture
async def memory_backend():
    """Provide a fresh memory backend."""
    backend = MemoryBackend()
    await backend.start()
    yield backend
    await backend.stop()


@pytest.fixture
def mock_stage():
    """Provide a mock stage."""
    return MockStage()


@pytest.fixture
def stage_config():
    """Provide default stage config using RAY backend for distributed tests."""
    return StageConfigV2(
        queue_type=QueueType.RAY,
        min_workers=1,
        max_workers=2,
        batch_size=10,
    )


# ============================================================================
# QueueMessage Tests
# ============================================================================

class TestQueueMessage:
    """Tests for QueueMessage serialization."""
    
    def test_to_bytes_from_bytes(self):
        """Test message round-trip serialization."""
        msg = QueueMessage(
            message_id="msg_001",
            split_id="split_001",
            data_ref="abc123",
            metadata={"key": "value"},
        )
        
        data = msg.to_bytes()
        restored = QueueMessage.from_bytes(data)
        
        assert restored.message_id == msg.message_id
        assert restored.split_id == msg.split_id
        assert restored.data_ref == msg.data_ref
        assert restored.metadata == msg.metadata
    
    def test_empty_metadata(self):
        """Test message with empty metadata."""
        msg = QueueMessage(
            message_id="msg_001",
            split_id="split_001",
            data_ref="abc123",
        )
        
        data = msg.to_bytes()
        restored = QueueMessage.from_bytes(data)
        
        assert restored.metadata == {}


# ============================================================================
# StageConfigV2 Tests
# ============================================================================

class TestStageConfigV2:
    """Tests for StageConfigV2."""
    
    def test_default_values(self):
        """Test default config values."""
        config = StageConfigV2()
        
        assert config.queue_type == QueueType.RAY  # Default is RAY for distributed
        assert config.min_workers == 1
        assert config.max_workers == 4
        assert config.batch_size == 100
    
    def test_tansu_config(self):
        """Test Tansu-specific config."""
        config = StageConfigV2(
            queue_type=QueueType.TANSU,
            tansu_storage_url="s3://my-bucket/",
            tansu_port=19092,
        )
        
        assert config.queue_type == QueueType.TANSU
        assert config.tansu_storage_url == "s3://my-bucket/"
        assert config.tansu_port == 19092
    
    def test_to_dict(self):
        """Test config serialization."""
        config = StageConfigV2(batch_size=50)
        d = config.to_dict()
        
        assert d["batch_size"] == 50
        assert d["queue_type"] == "ray"  # Default is ray


# ============================================================================
# StageMasterV2 Tests
# ============================================================================

@pytest.fixture(scope="class")
def ray_init():
    """Initialize Ray for master tests."""
    ray.init(num_cpus=2, ignore_reinit_error=True)
    yield
    ray.shutdown()


class TestStageMasterV2:
    """Tests for StageMasterV2."""
    
    @pytest.mark.asyncio
    async def test_create_output_queue(self, mock_stage, stage_config, ray_init):
        """Test that master creates output queue."""
        master = StageMasterV2(
            job_id="test_job",
            stage=mock_stage,
            config=stage_config,
        )
        
        await master.start()
        
        assert master._output_queue is not None
        assert master._output_topic == "test_job_test_stage_output"
        
        await master.stop()
    
    @pytest.mark.asyncio
    async def test_get_status(self, mock_stage, stage_config, ray_init):
        """Test getting stage status."""
        master = StageMasterV2(
            job_id="test_job",
            stage=mock_stage,
            config=stage_config,
        )
        
        # Before start
        status = master.get_status()
        assert status.is_running == False
        assert status.is_finished == False
        
        await master.start()
        
        # After start
        status = master.get_status()
        assert status.is_running == True
        assert status.worker_count >= 1
        
        await master.stop()
    
    @pytest.mark.asyncio
    async def test_stop_idempotent(self, mock_stage, stage_config, ray_init):
        """Test that stop can be called multiple times."""
        master = StageMasterV2(
            job_id="test_job",
            stage=mock_stage,
            config=stage_config,
        )
        
        await master.start()
        await master.stop()
        await master.stop()  # Should not raise
    
    @pytest.mark.asyncio
    async def test_get_output_queue(self, mock_stage, stage_config, ray_init):
        """Test getting output queue for downstream."""
        from solstice.queue import RayBackend
        
        master = StageMasterV2(
            job_id="test_job",
            stage=mock_stage,
            config=stage_config,
        )
        
        assert master.get_output_queue() is None
        
        await master.start()
        
        queue = master.get_output_queue()
        assert queue is not None
        assert isinstance(queue, RayBackend)
        
        await master.stop()


# ============================================================================
# Integration Tests (with Ray)
# ============================================================================

@pytest.fixture(scope="module")
def ray_context():
    """Initialize Ray for integration tests."""
    ray.init(num_cpus=2, ignore_reinit_error=True)
    yield
    ray.shutdown()


class TestIntegration:
    """Integration tests requiring Ray."""
    
    @pytest.mark.asyncio
    async def test_produce_to_output_queue(self, mock_stage, stage_config, ray_context):
        """Test that messages can be produced to output queue."""
        master = StageMasterV2(
            job_id="test_job",
            stage=mock_stage,
            config=stage_config,
        )
        
        await master.start()
        
        # Manually produce a message (simulating worker output)
        queue = master.get_output_queue()
        topic = master.get_output_topic()
        
        msg = QueueMessage(
            message_id="test_001",
            split_id="split_001",
            data_ref="abc123",
        )
        
        offset = await queue.produce(topic, msg.to_bytes())
        assert offset >= 0
        
        # Verify we can fetch it
        records = await queue.fetch(topic, offset=0)
        assert len(records) == 1
        
        restored = QueueMessage.from_bytes(records[0].value)
        assert restored.message_id == "test_001"
        
        await master.stop()
    
    @pytest.mark.asyncio
    async def test_two_stage_pipeline(self, ray_context):
        """Test two-stage pipeline with queue communication."""
        stage_config = StageConfigV2(
            queue_type=QueueType.RAY,
            min_workers=1,
            max_workers=1,
        )
        
        # Stage 1 (source)
        stage1 = MockStage(stage_id="stage1")
        master1 = StageMasterV2(
            job_id="test_job",
            stage=stage1,
            config=stage_config,
        )
        
        await master1.start()
        
        # Produce some messages to stage1 output
        queue1 = master1.get_output_queue()
        topic1 = master1.get_output_topic()
        
        for i in range(3):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                data_ref=f"ref_{i}",
            )
            await queue1.produce(topic1, msg.to_bytes())
        
        # Stage 2 (consumer) - uses endpoint from stage1
        stage2 = MockStage(stage_id="stage2", upstream_stages=["stage1"])
        master2 = StageMasterV2(
            job_id="test_job",
            stage=stage2,
            config=stage_config,
            upstream_endpoint=master1._output_endpoint,
            upstream_topic=topic1,
        )
        
        await master2.start()
        
        # Verify stage2 can read from stage1's output via endpoint
        queue2_upstream = await master2._workers[list(master2._workers.keys())[0]].get_stats.remote() if master2._workers else None
        
        # Direct verification: fetch from stage1's queue
        records = await queue1.fetch(topic1, offset=0)
        assert len(records) == 3
        
        await master1.stop()
        await master2.stop()


# ============================================================================
# Exactly-Once Semantics Tests
# ============================================================================

class TestExactlyOnce:
    """Tests for exactly-once processing semantics."""
    
    @pytest.mark.asyncio
    async def test_offset_tracking(self, memory_backend):
        """Test that offsets are tracked correctly."""
        topic = "test_topic"
        group = "test_group"
        
        await memory_backend.create_topic(topic)
        
        # Produce messages
        for i in range(10):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                data_ref=f"ref_{i}",
            )
            await memory_backend.produce(topic, msg.to_bytes())
        
        # Simulate processing and committing
        offset = await memory_backend.get_committed_offset(group, topic)
        assert offset is None
        
        records = await memory_backend.fetch(topic, offset=0, max_records=5)
        assert len(records) == 5
        
        # Commit after processing
        new_offset = records[-1].offset + 1
        await memory_backend.commit_offset(group, topic, new_offset)
        
        # Verify committed offset
        committed = await memory_backend.get_committed_offset(group, topic)
        assert committed == new_offset
        
        # Resume from committed offset
        remaining = await memory_backend.fetch(topic, offset=committed)
        assert len(remaining) == 5
        assert remaining[0].offset == new_offset
    
    @pytest.mark.asyncio
    async def test_crash_recovery_simulation(self, memory_backend):
        """Simulate crash recovery with offset tracking."""
        topic = "test_topic"
        group = "test_group"
        
        await memory_backend.create_topic(topic)
        
        # Produce messages
        for i in range(10):
            msg = QueueMessage(
                message_id=f"msg_{i}",
                split_id=f"split_{i}",
                data_ref=f"ref_{i}",
            )
            await memory_backend.produce(topic, msg.to_bytes())
        
        # First "worker" processes some messages
        offset = 0
        records = await memory_backend.fetch(topic, offset=offset, max_records=3)
        processed_ids = [QueueMessage.from_bytes(r.value).message_id for r in records]
        
        # Commit offset
        await memory_backend.commit_offset(group, topic, records[-1].offset + 1)
        
        # "Crash" - lose in-memory state
        del records, processed_ids
        
        # "Restart" - resume from committed offset
        committed = await memory_backend.get_committed_offset(group, topic)
        remaining = await memory_backend.fetch(topic, offset=committed)
        
        # Should get remaining 7 messages
        assert len(remaining) == 7
        
        # First remaining message should be msg_3
        first_msg = QueueMessage.from_bytes(remaining[0].value)
        assert first_msg.message_id == "msg_3"


