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

"""Tests for queue garbage collection (GC) functionality.

These tests verify that:
1. truncate_before correctly removes old records
2. get_min_committed_offset returns the minimum across consumer groups
3. GC preserves records that haven't been processed by all consumers
"""

import pytest

from solstice.queue import MemoryBackend


pytestmark = pytest.mark.asyncio(loop_scope="function")


class TestMemoryBackendGC:
    """Test GC functionality in MemoryBackend."""

    @pytest.mark.asyncio
    async def test_truncate_before_removes_old_records(self):
        """Truncate should remove records before the given offset."""
        backend = MemoryBackend()
        await backend.start()

        topic = "gc-test"
        await backend.create_topic(topic)

        # Produce 10 messages
        for i in range(10):
            await backend.produce(topic, f"msg-{i}".encode())

        # Truncate before offset 5
        deleted = await backend.truncate_before(topic, 5)
        assert deleted == 5, f"Expected 5 deleted, got {deleted}"

        # Fetch should only return records 5-9
        records = await backend.fetch(topic, offset=0, max_records=20)
        assert len(records) == 5
        assert records[0].offset == 5
        assert records[-1].offset == 9

        await backend.stop()

    @pytest.mark.asyncio
    async def test_truncate_nonexistent_topic(self):
        """Truncate on nonexistent topic should return 0."""
        backend = MemoryBackend()
        await backend.start()

        deleted = await backend.truncate_before("nonexistent", 100)
        assert deleted == 0

        await backend.stop()

    @pytest.mark.asyncio
    async def test_get_min_committed_offset_single_group(self):
        """get_min_committed_offset with single consumer group."""
        backend = MemoryBackend()
        await backend.start()

        topic = "min-offset-test"
        await backend.create_topic(topic)

        # Commit offset for one group
        await backend.commit_offset("group1", topic, 50)

        min_offset = await backend.get_min_committed_offset(topic)
        assert min_offset == 50

        await backend.stop()

    @pytest.mark.asyncio
    async def test_get_min_committed_offset_multiple_groups(self):
        """get_min_committed_offset should return minimum across groups."""
        backend = MemoryBackend()
        await backend.start()

        topic = "multi-group-test"
        await backend.create_topic(topic)

        # Multiple consumer groups at different offsets
        await backend.commit_offset("group1", topic, 100)
        await backend.commit_offset("group2", topic, 50)  # Slowest
        await backend.commit_offset("group3", topic, 75)

        min_offset = await backend.get_min_committed_offset(topic)
        assert min_offset == 50, f"Expected 50, got {min_offset}"

        await backend.stop()

    @pytest.mark.asyncio
    async def test_get_min_committed_offset_no_commits(self):
        """get_min_committed_offset returns None when no offsets committed."""
        backend = MemoryBackend()
        await backend.start()

        topic = "no-commits-test"
        await backend.create_topic(topic)

        min_offset = await backend.get_min_committed_offset(topic)
        assert min_offset is None

        await backend.stop()

    @pytest.mark.asyncio
    async def test_gc_workflow(self):
        """Full GC workflow: produce, consume, commit, truncate."""
        backend = MemoryBackend()
        await backend.start()

        topic = "gc-workflow"
        await backend.create_topic(topic)

        # Produce 100 messages
        for i in range(100):
            await backend.produce(topic, f"msg-{i}".encode())

        # Two consumer groups processing at different rates
        await backend.commit_offset("fast-consumer", topic, 80)
        await backend.commit_offset("slow-consumer", topic, 30)

        # Get minimum (safe to GC before this)
        min_offset = await backend.get_min_committed_offset(topic)
        assert min_offset == 30

        # GC before min offset
        deleted = await backend.truncate_before(topic, min_offset)
        assert deleted == 30

        # Slow consumer can still read its next record
        records = await backend.fetch(topic, offset=30, max_records=1)
        assert len(records) == 1
        assert records[0].offset == 30

        # Fast consumer can continue from where it was
        records = await backend.fetch(topic, offset=80, max_records=100)
        assert len(records) == 20  # 80-99

        await backend.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
