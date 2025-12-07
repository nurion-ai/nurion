"""Crash recovery tests for exactly-once semantics.

These tests verify that:
1. Output is persisted BEFORE input offset is committed
2. After crash, processing resumes from last committed offset
3. No data is lost (at-least-once guarantee)
4. With idempotent sinks, exactly-once semantics is achieved
"""

import asyncio
import pytest
import ray

from solstice.queue import MemoryBackend, RayBackend
from solstice.core.stage_master_v2 import QueueMessage


pytestmark = pytest.mark.asyncio(loop_scope="function")


class TestExactlyOnceSemantics:
    """Tests for exactly-once processing semantics."""
    
    @pytest.fixture
    def input_queue(self):
        """Create input queue with test messages."""
        queue = MemoryBackend()
        return queue
    
    @pytest.fixture
    def output_queue(self):
        """Create output queue."""
        queue = MemoryBackend()
        return queue
    
    @pytest.mark.asyncio
    async def test_offset_tracks_position(self, input_queue, output_queue):
        """Test that offset correctly tracks consumer position."""
        await input_queue.start()
        await output_queue.start()
        
        topic = "test-topic"
        group = "test-group"
        
        try:
            await input_queue.create_topic(topic)
            
            # Produce 10 messages
            for i in range(10):
                msg = QueueMessage(
                    message_id=str(i),
                    split_id=f"split_{i}",
                    data_ref="dummy",
                    metadata={},
                )
                await input_queue.produce(topic, msg.to_bytes())
            
            # Read first 5 messages
            records = await input_queue.fetch(topic, offset=0, max_records=5)
            assert len(records) == 5
            
            # Commit offset 5
            await input_queue.commit_offset(group, topic, 5)
            
            # Verify committed offset
            committed = await input_queue.get_committed_offset(group, topic)
            assert committed == 5
            
            # Read from committed offset
            records = await input_queue.fetch(topic, offset=committed, max_records=10)
            assert len(records) == 5  # Remaining 5 messages
            assert records[0].offset == 5  # First record is at offset 5
            
        finally:
            await input_queue.stop()
            await output_queue.stop()
    
    @pytest.mark.asyncio
    async def test_resume_from_committed_offset(self, input_queue, output_queue):
        """Test that processing resumes from last committed offset after restart."""
        await input_queue.start()
        await output_queue.start()
        
        topic = "test-topic"
        out_topic = "out-topic"
        group = "test-group"
        
        try:
            await input_queue.create_topic(topic)
            await output_queue.create_topic(out_topic)
            
            # Produce 10 messages
            for i in range(10):
                msg = QueueMessage(
                    message_id=str(i),
                    split_id=f"split_{i}",
                    data_ref="dummy",
                    metadata={"value": i},
                )
                await input_queue.produce(topic, msg.to_bytes())
            
            # Simulate first run: process 5 messages
            offset = 0
            for _ in range(5):
                records = await input_queue.fetch(topic, offset=offset, max_records=1)
                if records:
                    # Process and produce output
                    msg = QueueMessage.from_bytes(records[0].value)
                    out_msg = QueueMessage(
                        message_id=f"out_{msg.message_id}",
                        split_id=f"out_{msg.split_id}",
                        data_ref="processed",
                        metadata=msg.metadata,
                    )
                    await output_queue.produce(out_topic, out_msg.to_bytes())
                    offset = records[0].offset + 1
            
            # Commit offset after processing
            await input_queue.commit_offset(group, topic, offset)
            
            # Verify: output queue has 5 messages
            output_records = await output_queue.fetch(out_topic, offset=0, max_records=10)
            assert len(output_records) == 5
            
            # Simulate "crash and restart": get committed offset
            restart_offset = await input_queue.get_committed_offset(group, topic)
            assert restart_offset == 5
            
            # Process remaining messages
            offset = restart_offset
            for _ in range(5):
                records = await input_queue.fetch(topic, offset=offset, max_records=1)
                if records:
                    msg = QueueMessage.from_bytes(records[0].value)
                    out_msg = QueueMessage(
                        message_id=f"out_{msg.message_id}",
                        split_id=f"out_{msg.split_id}",
                        data_ref="processed",
                        metadata=msg.metadata,
                    )
                    await output_queue.produce(out_topic, out_msg.to_bytes())
                    offset = records[0].offset + 1
            
            await input_queue.commit_offset(group, topic, offset)
            
            # Verify: output queue has 10 messages total
            output_records = await output_queue.fetch(out_topic, offset=0, max_records=20)
            assert len(output_records) == 10
            
        finally:
            await input_queue.stop()
            await output_queue.stop()
    
    @pytest.mark.asyncio
    async def test_crash_before_commit_causes_reprocess(self, input_queue, output_queue):
        """Test that crash before commit causes reprocessing (at-least-once)."""
        await input_queue.start()
        await output_queue.start()
        
        topic = "test-topic"
        out_topic = "out-topic"
        group = "test-group"
        
        try:
            await input_queue.create_topic(topic)
            await output_queue.create_topic(out_topic)
            
            # Produce 5 messages
            for i in range(5):
                msg = QueueMessage(
                    message_id=str(i),
                    split_id=f"split_{i}",
                    data_ref="dummy",
                    metadata={"value": i},
                )
                await input_queue.produce(topic, msg.to_bytes())
            
            # First run: process 3 messages but DON'T commit
            offset = 0
            for _ in range(3):
                records = await input_queue.fetch(topic, offset=offset, max_records=1)
                if records:
                    msg = QueueMessage.from_bytes(records[0].value)
                    out_msg = QueueMessage(
                        message_id=f"out_{msg.message_id}",
                        split_id=f"out_{msg.split_id}",
                        data_ref="processed",
                        metadata=msg.metadata,
                    )
                    await output_queue.produce(out_topic, out_msg.to_bytes())
                    offset = records[0].offset + 1
            
            # CRASH! Don't commit offset
            # Output queue has 3 messages, but input offset is still 0
            
            # Restart: get committed offset (should be 0 or None)
            restart_offset = await input_queue.get_committed_offset(group, topic) or 0
            assert restart_offset == 0  # No commit was made
            
            # Re-process all messages
            offset = restart_offset
            for _ in range(5):
                records = await input_queue.fetch(topic, offset=offset, max_records=1)
                if records:
                    msg = QueueMessage.from_bytes(records[0].value)
                    out_msg = QueueMessage(
                        message_id=f"out_{msg.message_id}",
                        split_id=f"out_{msg.split_id}",
                        data_ref="processed",
                        metadata=msg.metadata,
                    )
                    await output_queue.produce(out_topic, out_msg.to_bytes())
                    offset = records[0].offset + 1
            
            await input_queue.commit_offset(group, topic, offset)
            
            # Verify: output queue has 8 messages (3 from first run + 5 from second)
            # This is at-least-once semantics - some messages were processed twice
            output_records = await output_queue.fetch(out_topic, offset=0, max_records=20)
            assert len(output_records) == 8  # 3 duplicates + 5 = 8
            
        finally:
            await input_queue.stop()
            await output_queue.stop()
    
    @pytest.mark.asyncio
    async def test_idempotent_processing_gives_exactly_once(self, input_queue, output_queue):
        """Test that idempotent processing achieves exactly-once results."""
        await input_queue.start()
        await output_queue.start()
        
        topic = "test-topic"
        group = "test-group"
        
        try:
            await input_queue.create_topic(topic)
            
            # Produce 5 messages
            for i in range(5):
                msg = QueueMessage(
                    message_id=str(i),
                    split_id=f"split_{i}",
                    data_ref="dummy",
                    metadata={"value": i},
                )
                await input_queue.produce(topic, msg.to_bytes())
            
            # Simulate idempotent processing with a set
            processed_ids = set()
            results = []
            
            # First run: process 3 messages without commit
            offset = 0
            for _ in range(3):
                records = await input_queue.fetch(topic, offset=offset, max_records=1)
                if records:
                    msg = QueueMessage.from_bytes(records[0].value)
                    # Idempotent: only process if not already processed
                    if msg.message_id not in processed_ids:
                        processed_ids.add(msg.message_id)
                        results.append(msg.metadata["value"])
                    offset = records[0].offset + 1
            
            # CRASH - don't commit
            
            # Restart: re-process from offset 0
            offset = 0
            for _ in range(5):
                records = await input_queue.fetch(topic, offset=offset, max_records=1)
                if records:
                    msg = QueueMessage.from_bytes(records[0].value)
                    # Idempotent: skip if already processed
                    if msg.message_id not in processed_ids:
                        processed_ids.add(msg.message_id)
                        results.append(msg.metadata["value"])
                    offset = records[0].offset + 1
            
            await input_queue.commit_offset(group, topic, offset)
            
            # Verify: exactly 5 unique results (exactly-once with idempotent processing)
            assert len(results) == 5
            assert sorted(results) == [0, 1, 2, 3, 4]
            
        finally:
            await input_queue.stop()
            await output_queue.stop()


class TestRayBackendRecovery:
    """Test crash recovery with RayBackend (distributed queue)."""
    
    @pytest.fixture
    def ray_cluster(self):
        """Initialize Ray for distributed tests."""
        if not ray.is_initialized():
            ray.init(num_cpus=2, ignore_reinit_error=True)
        yield
        # Don't shutdown - other tests may need it
    
    @pytest.mark.asyncio
    async def test_ray_backend_offset_persistence(self, ray_cluster):
        """Test that RayBackend persists offsets correctly."""
        backend = RayBackend()
        await backend.start()
        
        topic = "test-topic"
        group = "test-group"
        
        try:
            await backend.create_topic(topic)
            
            # Produce messages
            for i in range(5):
                msg = QueueMessage(
                    message_id=str(i),
                    split_id=f"split_{i}",
                    data_ref="dummy",
                    metadata={},
                )
                await backend.produce(topic, msg.to_bytes())
            
            # Commit offset
            await backend.commit_offset(group, topic, 3)
            
            # Create new backend from same actor
            actor_ref = backend.get_actor_ref()
            backend2 = RayBackend.from_actor_ref(actor_ref)
            await backend2.start()
            
            # Verify offset is preserved
            offset = await backend2.get_committed_offset(group, topic)
            assert offset == 3
            
        finally:
            await backend.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

