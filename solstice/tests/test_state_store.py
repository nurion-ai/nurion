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

"""Tests for partition state store."""

import tempfile

import pytest

from slatedb import ClosedError

from solstice.state import SlateDBPartitionStateStore


class TestSlateDBPartitionStateStore:
    """Tests for SlateDBPartitionStateStore."""

    @pytest.fixture
    def temp_path(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def store(self, temp_path):
        """Create a state store for testing."""
        store = SlateDBPartitionStateStore(
            base_path=temp_path,
            job_id="test_job",
            stage_id="test_stage",
        )
        yield store
        store.close()

    def test_acquire_release_partition(self, store):
        """Test acquiring and releasing partitions."""
        # Acquire partition
        result = store.acquire_partition(0)
        assert result is True
        assert 0 in store._dbs

        # Acquire same partition again should succeed
        result = store.acquire_partition(0)
        assert result is True

        # Release partition
        store.release_partition(0)
        assert 0 not in store._dbs

    def test_basic_get_put(self, store):
        """Test basic get/put operations."""
        store.acquire_partition(0)

        # Put a value
        store.put(0, b"key1", b"value1")

        # Get should return the value
        value = store.get(0, b"key1")
        assert value == b"value1"

        store.release_partition(0)

    def test_get_nonexistent(self, store):
        """Test getting a nonexistent key."""
        store.acquire_partition(0)

        value = store.get(0, b"nonexistent")
        assert value is None

        store.release_partition(0)

    def test_multiple_partitions(self, store):
        """Test working with multiple partitions."""
        # Acquire multiple partitions
        store.acquire_partition(0)
        store.acquire_partition(1)
        store.acquire_partition(2)

        # Write to each
        store.put(0, b"key", b"value0")
        store.put(1, b"key", b"value1")
        store.put(2, b"key", b"value2")

        # Read back
        assert store.get(0, b"key") == b"value0"
        assert store.get(1, b"key") == b"value1"
        assert store.get(2, b"key") == b"value2"

        # Release all
        store.release_partition(0)
        store.release_partition(1)
        store.release_partition(2)

    def test_partition_not_acquired_error(self, store):
        """Test that operations fail if partition not acquired."""
        with pytest.raises(ValueError, match="not acquired"):
            store.get(0, b"key")

        with pytest.raises(ValueError, match="not acquired"):
            store.put(0, b"key", b"value")

    def test_persistence_across_reopen(self, temp_path):
        """Test that data persists across store reopen."""
        # First store writes data
        store1 = SlateDBPartitionStateStore(
            base_path=temp_path,
            job_id="test_job",
            stage_id="test_stage",
        )
        store1.acquire_partition(0)
        store1.put(0, b"key1", b"value1")
        store1.close()

        # Second store reads data
        store2 = SlateDBPartitionStateStore(
            base_path=temp_path,
            job_id="test_job",
            stage_id="test_stage",
        )
        store2.acquire_partition(0)
        value = store2.get(0, b"key1")
        assert value == b"value1"
        store2.close()

    def test_fencing(self, temp_path):
        """Test that SlateDB fencing works correctly.

        When two writers open the same partition, the second writer
        should fence out the first on subsequent writes.
        """
        # Create two stores pointing to the same location
        store1 = SlateDBPartitionStateStore(
            base_path=temp_path,
            job_id="test_job",
            stage_id="test_stage",
        )
        store2 = SlateDBPartitionStateStore(
            base_path=temp_path,
            job_id="test_job",
            stage_id="test_stage",
        )

        try:
            # Store1 acquires and writes
            store1.acquire_partition(0)
            store1.put(0, b"key1", b"value1")

            # Store2 acquires same partition (this will fence out store1)
            store2.acquire_partition(0)
            store2.put(0, b"key2", b"value2")

            # Store1 should be fenced out on next write
            with pytest.raises(ClosedError):
                store1.put(0, b"key3", b"value3")

            # Store2 should still work
            value = store2.get(0, b"key2")
            assert value == b"value2"

        finally:
            store1.close()
            store2.close()

    def test_release_not_acquired(self, store):
        """Test releasing a partition that was never acquired."""
        # Should not raise, just return
        store.release_partition(999)

    def test_close_releases_all(self, store):
        """Test that close releases all partitions."""
        store.acquire_partition(0)
        store.acquire_partition(1)
        store.put(0, b"key", b"value")
        store.put(1, b"key", b"value")

        store.close()

        assert len(store._dbs) == 0
        assert len(store._dbs) == 0
