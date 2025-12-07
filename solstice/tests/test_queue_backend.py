"""Tests for queue backends.

This module contains unit tests for the QueueBackend implementations:
- MemoryBackend: Fast in-memory queue
- TansuBackend: Persistent queue with Tansu subprocess

Test categories:
1. Basic operations: produce, fetch, offset tracking
2. Batch operations: produce_batch, fetch batches
3. Exactly-once semantics: offset commit/recovery
4. Edge cases: empty queues, concurrent access
"""

import asyncio
import pytest
import pytest_asyncio
import time

from solstice.queue import MemoryBackend, QueueBackend, Record

# Configure pytest-asyncio
pytestmark = pytest.mark.asyncio(loop_scope="function")


# ============================================================================
# Fixtures
# ============================================================================

@pytest_asyncio.fixture
async def memory_backend():
    """Provide a fresh memory backend for each test."""
    backend = MemoryBackend(gc_interval_seconds=3600)  # Disable auto-GC
    await backend.start()
    yield backend
    await backend.stop()


# ============================================================================
# MemoryBackend Tests
# ============================================================================

class TestMemoryBackendBasic:
    """Basic operations for MemoryBackend."""
    
    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test backend lifecycle."""
        backend = MemoryBackend()
        await backend.start()
        assert await backend.health_check()
        await backend.stop()
        assert not await backend.health_check()
    
    @pytest.mark.asyncio
    async def test_create_topic(self, memory_backend: MemoryBackend):
        """Test topic creation."""
        await memory_backend.create_topic("test-topic")
        # Creating again should be a no-op
        await memory_backend.create_topic("test-topic")
    
    @pytest.mark.asyncio
    async def test_delete_topic(self, memory_backend: MemoryBackend):
        """Test topic deletion."""
        await memory_backend.create_topic("test-topic")
        await memory_backend.produce("test-topic", b"data")
        
        await memory_backend.delete_topic("test-topic")
        
        # Fetch from deleted topic should return empty
        records = await memory_backend.fetch("test-topic")
        assert records == []
    
    @pytest.mark.asyncio
    async def test_produce_fetch_single(self, memory_backend: MemoryBackend):
        """Test single message produce and fetch."""
        topic = "test-topic"
        
        # Produce
        offset = await memory_backend.produce(topic, b"hello world")
        assert offset == 0
        
        # Fetch
        records = await memory_backend.fetch(topic, offset=0)
        assert len(records) == 1
        assert records[0].offset == 0
        assert records[0].value == b"hello world"
    
    @pytest.mark.asyncio
    async def test_produce_fetch_multiple(self, memory_backend: MemoryBackend):
        """Test multiple messages."""
        topic = "test-topic"
        
        # Produce 10 messages
        offsets = []
        for i in range(10):
            offset = await memory_backend.produce(topic, f"msg-{i}".encode())
            offsets.append(offset)
        
        assert offsets == list(range(10))
        
        # Fetch all
        records = await memory_backend.fetch(topic, offset=0, max_records=100)
        assert len(records) == 10
        
        for i, record in enumerate(records):
            assert record.offset == i
            assert record.value == f"msg-{i}".encode()
    
    @pytest.mark.asyncio
    async def test_fetch_with_offset(self, memory_backend: MemoryBackend):
        """Test fetching from a specific offset."""
        topic = "test-topic"
        
        # Produce 10 messages
        for i in range(10):
            await memory_backend.produce(topic, f"msg-{i}".encode())
        
        # Fetch from offset 5
        records = await memory_backend.fetch(topic, offset=5)
        assert len(records) == 5
        assert records[0].offset == 5
        assert records[0].value == b"msg-5"
    
    @pytest.mark.asyncio
    async def test_fetch_max_records(self, memory_backend: MemoryBackend):
        """Test max_records limit."""
        topic = "test-topic"
        
        # Produce 100 messages
        for i in range(100):
            await memory_backend.produce(topic, f"msg-{i}".encode())
        
        # Fetch with limit
        records = await memory_backend.fetch(topic, offset=0, max_records=10)
        assert len(records) == 10
    
    @pytest.mark.asyncio
    async def test_fetch_empty_topic(self, memory_backend: MemoryBackend):
        """Test fetching from empty/non-existent topic."""
        records = await memory_backend.fetch("non-existent")
        assert records == []
    
    @pytest.mark.asyncio
    async def test_get_latest_offset(self, memory_backend: MemoryBackend):
        """Test getting latest offset."""
        topic = "test-topic"
        
        # Empty topic
        assert await memory_backend.get_latest_offset(topic) == 0
        
        # After producing
        await memory_backend.produce(topic, b"msg1")
        assert await memory_backend.get_latest_offset(topic) == 1
        
        await memory_backend.produce(topic, b"msg2")
        assert await memory_backend.get_latest_offset(topic) == 2


class TestMemoryBackendBatch:
    """Batch operations for MemoryBackend."""
    
    @pytest.mark.asyncio
    async def test_produce_batch(self, memory_backend: MemoryBackend):
        """Test batch produce."""
        topic = "test-topic"
        
        values = [f"msg-{i}".encode() for i in range(10)]
        offsets = await memory_backend.produce_batch(topic, values)
        
        assert offsets == list(range(10))
        
        # Verify all messages
        records = await memory_backend.fetch(topic, offset=0, max_records=100)
        assert len(records) == 10
    
    @pytest.mark.asyncio
    async def test_produce_batch_with_keys(self, memory_backend: MemoryBackend):
        """Test batch produce with keys."""
        topic = "test-topic"
        
        values = [b"v1", b"v2", b"v3"]
        keys = [b"k1", b"k2", b"k3"]
        
        offsets = await memory_backend.produce_batch(topic, values, keys=keys)
        assert len(offsets) == 3
        
        records = await memory_backend.fetch(topic, offset=0)
        assert records[0].key == b"k1"
        assert records[1].key == b"k2"
        assert records[2].key == b"k3"
    
    @pytest.mark.asyncio
    async def test_produce_batch_mismatched_keys(self, memory_backend: MemoryBackend):
        """Test batch produce with mismatched keys raises error."""
        topic = "test-topic"
        
        values = [b"v1", b"v2"]
        keys = [b"k1"]  # Wrong length
        
        with pytest.raises(ValueError):
            await memory_backend.produce_batch(topic, values, keys=keys)
    
    @pytest.mark.asyncio
    async def test_produce_batch_empty(self, memory_backend: MemoryBackend):
        """Test batch produce with empty list."""
        topic = "test-topic"
        
        offsets = await memory_backend.produce_batch(topic, [])
        assert offsets == []


class TestMemoryBackendOffsetTracking:
    """Offset commit/fetch for exactly-once semantics."""
    
    @pytest.mark.asyncio
    async def test_commit_offset(self, memory_backend: MemoryBackend):
        """Test offset commit."""
        group = "my-group"
        topic = "test-topic"
        
        # Initial: no committed offset
        offset = await memory_backend.get_committed_offset(group, topic)
        assert offset is None
        
        # Commit offset
        await memory_backend.commit_offset(group, topic, 42)
        
        # Get committed offset
        offset = await memory_backend.get_committed_offset(group, topic)
        assert offset == 42
    
    @pytest.mark.asyncio
    async def test_commit_offset_multiple_groups(self, memory_backend: MemoryBackend):
        """Test offset commit for multiple consumer groups."""
        topic = "test-topic"
        
        await memory_backend.commit_offset("group-a", topic, 10)
        await memory_backend.commit_offset("group-b", topic, 20)
        
        assert await memory_backend.get_committed_offset("group-a", topic) == 10
        assert await memory_backend.get_committed_offset("group-b", topic) == 20
    
    @pytest.mark.asyncio
    async def test_offset_commit_update(self, memory_backend: MemoryBackend):
        """Test updating committed offset."""
        group = "my-group"
        topic = "test-topic"
        
        await memory_backend.commit_offset(group, topic, 10)
        assert await memory_backend.get_committed_offset(group, topic) == 10
        
        await memory_backend.commit_offset(group, topic, 20)
        assert await memory_backend.get_committed_offset(group, topic) == 20


class TestMemoryBackendExactlyOnce:
    """Exactly-once processing simulation."""
    
    @pytest.mark.asyncio
    async def test_exactly_once_flow(self, memory_backend: MemoryBackend):
        """Test complete exactly-once processing flow."""
        input_topic = "input"
        output_topic = "output"
        group = "processor"
        
        # Produce input messages
        for i in range(10):
            await memory_backend.produce(input_topic, f"input-{i}".encode())
        
        # Simulate processing
        offset = await memory_backend.get_committed_offset(group, input_topic) or 0
        
        while True:
            records = await memory_backend.fetch(input_topic, offset=offset, max_records=3)
            if not records:
                break
            
            # Process and produce output
            for record in records:
                output = b"processed-" + record.value
                await memory_backend.produce(output_topic, output)
            
            # Commit offset AFTER output is produced
            offset = records[-1].offset + 1
            await memory_backend.commit_offset(group, input_topic, offset)
        
        # Verify output
        output_records = await memory_backend.fetch(output_topic, offset=0, max_records=100)
        assert len(output_records) == 10
        
        # Verify committed offset
        assert await memory_backend.get_committed_offset(group, input_topic) == 10
    
    @pytest.mark.asyncio
    async def test_resume_after_crash(self, memory_backend: MemoryBackend):
        """Test resuming from committed offset (simulating crash recovery)."""
        input_topic = "input"
        group = "processor"
        
        # Produce messages
        for i in range(10):
            await memory_backend.produce(input_topic, f"msg-{i}".encode())
        
        # Process first half and commit
        records = await memory_backend.fetch(input_topic, offset=0, max_records=5)
        await memory_backend.commit_offset(group, input_topic, 5)
        
        # "Crash" - lose in-progress state
        # But committed offset survives
        
        # Resume: get committed offset
        resume_offset = await memory_backend.get_committed_offset(group, input_topic)
        assert resume_offset == 5
        
        # Continue processing from committed offset
        remaining = await memory_backend.fetch(input_topic, offset=resume_offset)
        assert len(remaining) == 5
        assert remaining[0].value == b"msg-5"


class TestMemoryBackendConcurrency:
    """Concurrent access tests."""
    
    @pytest.mark.asyncio
    async def test_concurrent_produce(self, memory_backend: MemoryBackend):
        """Test concurrent produce from multiple tasks."""
        topic = "test-topic"
        num_tasks = 10
        msgs_per_task = 100
        
        async def producer(task_id: int):
            for i in range(msgs_per_task):
                await memory_backend.produce(topic, f"task-{task_id}-msg-{i}".encode())
        
        await asyncio.gather(*[producer(i) for i in range(num_tasks)])
        
        # Verify total count
        records = await memory_backend.fetch(topic, offset=0, max_records=num_tasks * msgs_per_task)
        assert len(records) == num_tasks * msgs_per_task
        
        # Verify offsets are unique and sequential
        offsets = [r.offset for r in records]
        assert offsets == list(range(num_tasks * msgs_per_task))
    
    @pytest.mark.asyncio
    async def test_concurrent_produce_fetch(self, memory_backend: MemoryBackend):
        """Test concurrent produce and fetch."""
        topic = "test-topic"
        produced = []
        consumed = []
        
        async def producer():
            for i in range(100):
                offset = await memory_backend.produce(topic, f"msg-{i}".encode())
                produced.append(offset)
                await asyncio.sleep(0.001)
        
        async def consumer():
            offset = 0
            while len(consumed) < 100:
                records = await memory_backend.fetch(topic, offset=offset, max_records=10)
                for r in records:
                    consumed.append(r.offset)
                    offset = r.offset + 1
                if not records:
                    await asyncio.sleep(0.01)
        
        await asyncio.gather(producer(), consumer())
        
        assert len(produced) == 100
        assert len(consumed) == 100


class TestMemoryBackendProperties:
    """Property tests."""
    
    @pytest.mark.asyncio
    async def test_is_persistent(self, memory_backend: MemoryBackend):
        """Memory backend is not persistent."""
        assert memory_backend.is_persistent is False
    
    @pytest.mark.asyncio
    async def test_record_has_timestamp(self, memory_backend: MemoryBackend):
        """Records should have timestamps."""
        topic = "test-topic"
        
        before = int(time.time() * 1000)
        await memory_backend.produce(topic, b"test")
        after = int(time.time() * 1000)
        
        records = await memory_backend.fetch(topic, offset=0)
        assert before <= records[0].timestamp <= after
    
    @pytest.mark.asyncio
    async def test_get_stats(self, memory_backend: MemoryBackend):
        """Test getting backend stats."""
        topic = "test-topic"
        
        await memory_backend.produce(topic, b"msg1")
        await memory_backend.produce(topic, b"msg2")
        await memory_backend.commit_offset("group1", topic, 1)
        
        stats = memory_backend.get_stats()
        
        assert "topics" in stats
        assert topic in stats["topics"]
        assert stats["topics"][topic]["record_count"] == 2
        assert stats["topics"][topic]["next_offset"] == 2
        assert ("group1", topic) in stats["committed_offsets"]


# ============================================================================
# TansuBackend Tests (requires tansu binary)
# ============================================================================

@pytest_asyncio.fixture
async def tansu_backend():
    """Provide a fresh Tansu backend (skip if tansu not available)."""
    import shutil
    
    if not shutil.which("tansu"):
        pytest.skip("tansu binary not found")
    
    from solstice.queue import TansuBackend
    backend = TansuBackend(storage_url="memory://", port=19092)
    await backend.start()
    yield backend
    await backend.stop()


class TestTansuBackend:
    """Tests for TansuBackend (requires tansu binary)."""
    
    @pytest.mark.asyncio
    async def test_start_stop(self, tansu_backend):
        """Test Tansu backend lifecycle."""
        assert await tansu_backend.health_check()
    
    @pytest.mark.asyncio
    async def test_produce_fetch(self, tansu_backend):
        """Test basic produce/fetch with Tansu."""
        topic = "test-topic"
        await tansu_backend.create_topic(topic)
        
        offset = await tansu_backend.produce(topic, b"hello tansu")
        assert offset >= 0
        
        records = await tansu_backend.fetch(topic, offset=offset)
        assert len(records) >= 1
        assert records[0].value == b"hello tansu"


# Import TansuBackend for skip check
try:
    from solstice.queue import TansuBackend
except ImportError:
    TansuBackend = None



