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

"""Tests for queue backends.

This module contains unit tests for the queue implementations:
- MemoryBroker + MemoryClient: Fast in-memory queue
- TansuBrokerManager + TansuQueueClient: Kafka-compatible broker

Test categories:
1. Basic operations: produce, fetch, offset tracking
2. Batch operations: fetch batches
3. Exactly-once semantics: offset commit/recovery
4. Edge cases: empty queues, concurrent access

Note: Queue methods are now synchronous (confluent-kafka is inherently sync).
"""

import asyncio
import pytest
import time

from solstice.queue import MemoryBroker, MemoryClient


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def memory_broker_and_client():
    """Provide a fresh MemoryBroker and MemoryClient pair."""
    broker = MemoryBroker(gc_interval_seconds=3600)  # Disable auto-GC
    broker.start()
    client = MemoryClient(broker)
    client.start()
    yield broker, client
    client.stop()
    broker.stop()


@pytest.fixture
def memory_client(memory_broker_and_client):
    """Provide just the client for simple tests."""
    broker, client = memory_broker_and_client
    return client


# ============================================================================
# Memory Tests (Broker + Client)
# ============================================================================


class TestMemoryBroker:
    """Tests for MemoryBroker."""

    def test_start_stop(self):
        """Test broker lifecycle."""
        broker = MemoryBroker()
        broker.start()
        assert broker.is_running()
        broker.stop()
        assert not broker.is_running()

    def test_get_broker_url(self):
        """Test broker URL generation."""
        broker = MemoryBroker()
        broker.start()
        url = broker.get_broker_url()
        assert url.startswith("memory://")
        broker.stop()


class TestMemoryClient:
    """Tests for MemoryClient."""

    def test_health_check(self, memory_client):
        """Test client health check."""
        assert memory_client.health_check()

    def test_create_topic(self, memory_client):
        """Test topic creation."""
        memory_client.create_topic("test-topic")
        # Creating again should be a no-op
        memory_client.create_topic("test-topic")

    def test_delete_topic(self, memory_client):
        """Test topic deletion."""
        memory_client.create_topic("test-topic")
        memory_client.produce("test-topic", b"data")

        memory_client.delete_topic("test-topic")

        # Fetch from deleted topic should return empty
        records = memory_client.fetch("test-topic")
        assert records == []

    def test_produce_fetch_single(self, memory_client):
        """Test single message produce and fetch."""
        topic = "test-topic"

        # Produce
        offset = memory_client.produce(topic, b"hello world")
        assert offset == 0

        # Fetch
        records = memory_client.fetch(topic, offset=0)
        assert len(records) == 1
        assert records[0].offset == 0
        assert records[0].value == b"hello world"

    def test_produce_fetch_multiple(self, memory_client):
        """Test multiple messages."""
        topic = "test-topic"

        # Produce 10 messages
        offsets = []
        for i in range(10):
            offset = memory_client.produce(topic, f"msg-{i}".encode())
            offsets.append(offset)

        assert offsets == list(range(10))

        # Fetch all
        records = memory_client.fetch(topic, offset=0, max_records=100)
        assert len(records) == 10

        for i, record in enumerate(records):
            assert record.offset == i
            assert record.value == f"msg-{i}".encode()

    def test_fetch_with_offset(self, memory_client):
        """Test fetching from a specific offset."""
        topic = "test-topic"

        # Produce 10 messages
        for i in range(10):
            memory_client.produce(topic, f"msg-{i}".encode())

        # Fetch from offset 5
        records = memory_client.fetch(topic, offset=5)
        assert len(records) == 5
        assert records[0].offset == 5
        assert records[0].value == b"msg-5"

    def test_fetch_max_records(self, memory_client):
        """Test max_records limit."""
        topic = "test-topic"

        # Produce 100 messages
        for i in range(100):
            memory_client.produce(topic, f"msg-{i}".encode())

        # Fetch with limit
        records = memory_client.fetch(topic, offset=0, max_records=10)
        assert len(records) == 10

    def test_fetch_empty_topic(self, memory_client):
        """Test fetching from empty/non-existent topic."""
        records = memory_client.fetch("non-existent")
        assert records == []

    def test_get_latest_offset(self, memory_client):
        """Test getting latest offset."""
        topic = "test-topic"

        # Empty topic
        assert memory_client.get_latest_offset(topic) == 0

        # After producing
        memory_client.produce(topic, b"msg1")
        assert memory_client.get_latest_offset(topic) == 1

        memory_client.produce(topic, b"msg2")
        assert memory_client.get_latest_offset(topic) == 2


class TestMemoryClientOffsetTracking:
    """Offset commit/fetch for exactly-once semantics."""

    def test_commit_offset(self, memory_client):
        """Test offset commit."""
        group = "my-group"
        topic = "test-topic"

        # Initial: no committed offset
        offset = memory_client.get_committed_offset(group, topic)
        assert offset is None

        # Commit offset
        memory_client.commit_offset(group, topic, 42)

        # Get committed offset
        offset = memory_client.get_committed_offset(group, topic)
        assert offset == 42

    def test_commit_offset_multiple_groups(self, memory_client):
        """Test offset commit for multiple consumer groups."""
        topic = "test-topic"

        memory_client.commit_offset("group-a", topic, 10)
        memory_client.commit_offset("group-b", topic, 20)

        assert memory_client.get_committed_offset("group-a", topic) == 10
        assert memory_client.get_committed_offset("group-b", topic) == 20

    def test_offset_commit_update(self, memory_client):
        """Test updating committed offset."""
        group = "my-group"
        topic = "test-topic"

        memory_client.commit_offset(group, topic, 10)
        assert memory_client.get_committed_offset(group, topic) == 10

        memory_client.commit_offset(group, topic, 20)
        assert memory_client.get_committed_offset(group, topic) == 20


class TestMemoryClientExactlyOnce:
    """Exactly-once processing simulation."""

    def test_exactly_once_flow(self, memory_client):
        """Test complete exactly-once processing flow."""
        input_topic = "input"
        output_topic = "output"
        group = "processor"

        # Produce input messages
        for i in range(10):
            memory_client.produce(input_topic, f"input-{i}".encode())

        # Simulate processing
        offset = memory_client.get_committed_offset(group, input_topic) or 0

        while True:
            records = memory_client.fetch(input_topic, offset=offset, max_records=3)
            if not records:
                break

            # Process and produce output
            for record in records:
                output = b"processed-" + record.value
                memory_client.produce(output_topic, output)

            # Commit offset AFTER output is produced
            offset = records[-1].offset + 1
            memory_client.commit_offset(group, input_topic, offset)

        # Verify output
        output_records = memory_client.fetch(output_topic, offset=0, max_records=100)
        assert len(output_records) == 10

        # Verify committed offset
        assert memory_client.get_committed_offset(group, input_topic) == 10

    def test_resume_after_crash(self, memory_client):
        """Test resuming from committed offset (simulating crash recovery)."""
        input_topic = "input"
        group = "processor"

        # Produce messages
        for i in range(10):
            memory_client.produce(input_topic, f"msg-{i}".encode())

        # Process first half and commit
        memory_client.fetch(input_topic, offset=0, max_records=5)
        memory_client.commit_offset(group, input_topic, 5)

        # "Crash" - lose in-progress state
        # But committed offset survives

        # Resume: get committed offset
        resume_offset = memory_client.get_committed_offset(group, input_topic)
        assert resume_offset == 5

        # Continue processing from committed offset
        remaining = memory_client.fetch(input_topic, offset=resume_offset)
        assert len(remaining) == 5
        assert remaining[0].value == b"msg-5"

    def test_crash_before_commit_causes_reprocess(self, memory_client):
        """Test that crash before commit causes reprocessing (at-least-once).

        This demonstrates that without commit, messages are reprocessed,
        which is the expected at-least-once semantics.
        """
        input_topic = "input"
        output_topic = "output"
        group = "processor"

        # Produce 5 messages
        for i in range(5):
            memory_client.produce(input_topic, f"msg-{i}".encode())

        # First run: process 3 messages but DON'T commit
        offset = 0
        for _ in range(3):
            records = memory_client.fetch(input_topic, offset=offset, max_records=1)
            if records:
                memory_client.produce(output_topic, b"processed-" + records[0].value)
                offset = records[0].offset + 1

        # CRASH! Don't commit offset
        # Output has 3 messages, but input offset is still uncommitted

        # Restart: get committed offset (should be 0 or None)
        restart_offset = memory_client.get_committed_offset(group, input_topic) or 0
        assert restart_offset == 0  # No commit was made

        # Re-process all messages from beginning
        offset = restart_offset
        for _ in range(5):
            records = memory_client.fetch(input_topic, offset=offset, max_records=1)
            if records:
                memory_client.produce(output_topic, b"processed-" + records[0].value)
                offset = records[0].offset + 1

        memory_client.commit_offset(group, input_topic, offset)

        # Verify: output has 8 messages (3 from first run + 5 from second)
        # This is at-least-once semantics - some messages were processed twice
        output_records = memory_client.fetch(output_topic, offset=0, max_records=20)
        assert len(output_records) == 8

    def test_idempotent_processing_achieves_exactly_once(self, memory_client):
        """Test that idempotent processing achieves exactly-once results.

        With at-least-once delivery + idempotent processing = exactly-once semantics.
        """
        input_topic = "input"
        group = "processor"

        # Produce 5 messages
        for i in range(5):
            memory_client.produce(input_topic, f"msg-{i}".encode())

        # Simulate idempotent processing with a set
        processed_ids = set()
        results = []

        # First run: process 3 messages without commit
        offset = 0
        for _ in range(3):
            records = memory_client.fetch(input_topic, offset=offset, max_records=1)
            if records:
                msg_id = records[0].value.decode()
                # Idempotent: only process if not already processed
                if msg_id not in processed_ids:
                    processed_ids.add(msg_id)
                    results.append(msg_id)
                offset = records[0].offset + 1

        # CRASH - don't commit

        # Restart: re-process from offset 0
        offset = 0
        for _ in range(5):
            records = memory_client.fetch(input_topic, offset=offset, max_records=1)
            if records:
                msg_id = records[0].value.decode()
                # Idempotent: skip if already processed
                if msg_id not in processed_ids:
                    processed_ids.add(msg_id)
                    results.append(msg_id)
                offset = records[0].offset + 1

        memory_client.commit_offset(group, input_topic, offset)

        # Verify: exactly 5 unique results (exactly-once with idempotent processing)
        assert len(results) == 5
        assert sorted(results) == ["msg-0", "msg-1", "msg-2", "msg-3", "msg-4"]


class TestMemoryClientConcurrency:
    """Concurrent access tests."""

    @pytest.mark.asyncio
    async def test_concurrent_produce(self, memory_client):
        """Test concurrent produce from multiple tasks."""
        topic = "test-topic"
        num_tasks = 10
        msgs_per_task = 100

        async def producer(task_id: int):
            for i in range(msgs_per_task):
                memory_client.produce(topic, f"task-{task_id}-msg-{i}".encode())

        await asyncio.gather(*[producer(i) for i in range(num_tasks)])

        # Verify total count
        records = memory_client.fetch(topic, offset=0, max_records=num_tasks * msgs_per_task)
        assert len(records) == num_tasks * msgs_per_task

        # Verify offsets are unique and sequential
        offsets = [r.offset for r in records]
        assert offsets == list(range(num_tasks * msgs_per_task))

    @pytest.mark.asyncio
    async def test_concurrent_produce_fetch(self, memory_client):
        """Test concurrent produce and fetch."""
        topic = "test-topic"
        produced = []
        consumed = []

        async def producer():
            for i in range(100):
                offset = memory_client.produce(topic, f"msg-{i}".encode())
                produced.append(offset)
                await asyncio.sleep(0.001)

        async def consumer():
            offset = 0
            while len(consumed) < 100:
                records = memory_client.fetch(topic, offset=offset, max_records=10)
                for r in records:
                    consumed.append(r.offset)
                    offset = r.offset + 1
                if not records:
                    await asyncio.sleep(0.01)

        await asyncio.gather(producer(), consumer())

        assert len(produced) == 100
        assert len(consumed) == 100


class TestMemoryClientProperties:
    """Property tests."""

    def test_record_has_timestamp(self, memory_client):
        """Records should have timestamps."""
        topic = "test-topic"

        before = int(time.time() * 1000)
        memory_client.produce(topic, b"test")
        after = int(time.time() * 1000)

        records = memory_client.fetch(topic, offset=0)
        assert before <= records[0].timestamp <= after


# ============================================================================
# Tansu Tests (Broker + Client)
# ============================================================================


@pytest.fixture
def tansu_broker_and_client():
    """Provide a Tansu broker and client pair."""
    import socket
    from solstice.queue import TansuBrokerManager, TansuQueueClient

    # Find a free port dynamically
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]

    # Start broker with shorter timeout for tests
    broker = TansuBrokerManager(storage_url="memory://tansu/", port=port, startup_timeout=5.0)
    broker.start()

    # Create and start client
    client = TansuQueueClient(broker.get_broker_url())
    client.start()

    yield broker, client

    # Cleanup
    client.stop()
    broker.stop()
    time.sleep(0.1)  # Brief pause for cleanup


@pytest.mark.slow
class TestTansuBrokerManager:
    """Tests for TansuBrokerManager (QueueBroker implementation)."""

    def test_start_stop(self, tansu_broker_and_client):
        """Test broker lifecycle."""
        broker, client = tansu_broker_and_client
        assert broker.is_running()

    def test_get_broker_url(self, tansu_broker_and_client):
        """Test getting broker URL."""
        broker, client = tansu_broker_and_client
        broker_url = broker.get_broker_url()
        assert broker_url.startswith("127.0.0.1:")
        port = int(broker_url.split(":")[1])
        assert 1024 < port < 65535


@pytest.mark.slow
class TestTansuQueueClient:
    """Tests for TansuQueueClient (QueueClient implementation)."""

    def test_health_check(self, tansu_broker_and_client):
        """Test client health check."""
        broker, client = tansu_broker_and_client
        assert client.health_check()

    def test_create_topic(self, tansu_broker_and_client):
        """Test topic creation."""
        broker, client = tansu_broker_and_client
        client.create_topic("test-topic")
        # Should not raise

    def test_produce_fetch(self, tansu_broker_and_client):
        """Test produce and fetch."""
        broker, client = tansu_broker_and_client
        topic = "test-topic"
        client.create_topic(topic)

        # Produce
        offset = client.produce(topic, b"hello tansu")
        assert offset == 0

        # Fetch
        records = client.fetch(topic, offset=0, timeout_ms=1000)
        assert len(records) == 1
        assert records[0].value == b"hello tansu"
        assert records[0].offset == 0

    def test_get_latest_offset(self, tansu_broker_and_client):
        """Test getting latest offset."""
        broker, client = tansu_broker_and_client
        topic = "offset-topic"
        client.create_topic(topic)

        # Initially should be 0
        latest = client.get_latest_offset(topic)
        assert latest == 0

        # After producing
        client.produce(topic, b"msg1")
        client.produce(topic, b"msg2")
        latest = client.get_latest_offset(topic)
        assert latest == 2

    def test_commit_and_get_offset(self, tansu_broker_and_client):
        """Test offset commit and retrieval."""
        broker, client = tansu_broker_and_client
        topic = "commit-topic"
        group = "test-group"
        client.create_topic(topic)

        # Produce some messages
        client.produce(topic, b"msg1")
        client.produce(topic, b"msg2")

        # Commit offset
        client.commit_offset(group, topic, offset=1)

        # Get committed offset
        committed = client.get_committed_offset(group, topic)
        assert committed == 1


@pytest.mark.slow
class TestTansuMultiClient:
    """Tests for multiple clients connecting to same broker."""

    def test_two_clients_communication(self, tansu_broker_and_client):
        """Test two clients producing and consuming."""
        broker, client1 = tansu_broker_and_client
        from solstice.queue import TansuQueueClient

        # Create second client
        client2 = TansuQueueClient(broker.get_broker_url())
        client2.start()

        try:
            topic = "shared-topic"
            client1.create_topic(topic)

            # Client 1 produces
            client1.produce(topic, b"from client1")

            # Client 2 produces
            offset = client2.produce(topic, b"from client2")
            assert offset == 1

            # Both clients can fetch all messages
            records1 = client1.fetch(topic, offset=0, timeout_ms=1000)
            records2 = client2.fetch(topic, offset=0, timeout_ms=1000)

            assert len(records1) == 2
            assert len(records2) == 2
            assert records1[0].value == b"from client1"
            assert records1[1].value == b"from client2"

        finally:
            client2.stop()


# Import check
try:
    from solstice.queue import TansuBrokerManager, TansuQueueClient
except ImportError:
    TansuBrokerManager = None
    TansuQueueClient = None
