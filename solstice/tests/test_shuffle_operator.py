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

"""Tests for shuffle operators."""

import pyarrow as pa
import pytest

from solstice.core.models import Split, SplitPayload
from solstice.operators.shuffle import (
    RepartitionConfig,
    RepartitionOperator,
    ShuffleOperator,
    ShuffleOperatorConfig,
    is_shuffle_operator,
    split_by_partition,
)
from solstice.operators.map import MapOperatorConfig


class TestRepartitionOperator:
    """Tests for RepartitionOperator."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table for testing."""
        return pa.table({
            "user_id": [1, 2, 1, 3, 2, 1, 4, 5],
            "value": [10, 20, 30, 40, 50, 60, 70, 80],
        })

    @pytest.fixture
    def sample_payload(self, sample_table):
        """Create a sample payload for testing."""
        return SplitPayload(data=sample_table, split_id="test_split")

    @pytest.fixture
    def sample_split(self):
        """Create a sample split for testing."""
        return Split(split_id="test_split", stage_id="test_stage", data_range={})

    def test_repartition_basic(self, sample_split, sample_payload):
        """Test basic repartition operation."""
        config = RepartitionConfig(partition_keys=["user_id"])
        operator = config.setup()
        operator.set_num_partitions(4)

        result = operator.process_split(sample_split, sample_payload)

        assert result is not None
        table = result.to_table()

        # Should have partition column added
        assert ShuffleOperator.PARTITION_COLUMN in table.column_names

        # All rows should be present
        assert table.num_rows == 8

        # Partition IDs should be in range
        partition_ids = table.column(ShuffleOperator.PARTITION_COLUMN).to_pylist()
        for pid in partition_ids:
            assert 0 <= pid < 4

        operator.close()

    def test_repartition_deterministic(self, sample_split, sample_payload):
        """Test that repartition is deterministic."""
        config = RepartitionConfig(partition_keys=["user_id"])

        operator1 = config.setup()
        operator1.set_num_partitions(4)
        result1 = operator1.process_split(sample_split, sample_payload)

        operator2 = config.setup()
        operator2.set_num_partitions(4)
        result2 = operator2.process_split(sample_split, sample_payload)

        # Same partition assignments
        pids1 = result1.to_table().column(ShuffleOperator.PARTITION_COLUMN).to_pylist()
        pids2 = result2.to_table().column(ShuffleOperator.PARTITION_COLUMN).to_pylist()
        assert pids1 == pids2

        operator1.close()
        operator2.close()

    def test_repartition_same_key_same_partition(self, sample_split, sample_payload):
        """Test that rows with same key go to same partition."""
        config = RepartitionConfig(partition_keys=["user_id"])
        operator = config.setup()
        operator.set_num_partitions(8)

        result = operator.process_split(sample_split, sample_payload)
        table = result.to_table()

        # Group by user_id and check partition consistency
        user_partitions = {}
        for i in range(table.num_rows):
            user_id = table.column("user_id")[i].as_py()
            partition_id = table.column(ShuffleOperator.PARTITION_COLUMN)[i].as_py()

            if user_id in user_partitions:
                # Same user should always go to same partition
                assert user_partitions[user_id] == partition_id
            else:
                user_partitions[user_id] = partition_id

        operator.close()

    def test_repartition_empty_payload(self, sample_split):
        """Test repartition with empty payload."""
        config = RepartitionConfig(partition_keys=["user_id"])
        operator = config.setup()
        operator.set_num_partitions(4)

        result = operator.process_split(sample_split, None)
        assert result is None

        operator.close()

    def test_repartition_empty_table(self, sample_split):
        """Test repartition with empty table."""
        config = RepartitionConfig(partition_keys=["user_id"])
        operator = config.setup()
        operator.set_num_partitions(4)

        empty_table = pa.table({"user_id": [], "value": []})
        empty_payload = SplitPayload(data=empty_table, split_id="test")

        result = operator.process_split(sample_split, empty_payload)
        assert result is None

        operator.close()

    def test_repartition_multiple_keys(self, sample_split):
        """Test repartition with multiple partition keys."""
        table = pa.table({
            "user_id": [1, 1, 2, 2],
            "category": ["A", "B", "A", "B"],
            "value": [10, 20, 30, 40],
        })
        payload = SplitPayload(data=table, split_id="test")

        config = RepartitionConfig(partition_keys=["user_id", "category"])
        operator = config.setup()
        operator.set_num_partitions(4)

        result = operator.process_split(sample_split, payload)
        assert result is not None

        result_table = result.to_table()
        # Each (user_id, category) combination should have consistent partition
        partition_ids = result_table.column(ShuffleOperator.PARTITION_COLUMN).to_pylist()
        # All 4 rows have different (user_id, category) combinations
        # so they may or may not be in different partitions

        operator.close()


class TestSplitByPartition:
    """Tests for split_by_partition utility."""

    def test_split_basic(self):
        """Test basic partition splitting."""
        table = pa.table({
            "user_id": [1, 2, 3, 4],
            "value": [10, 20, 30, 40],
            ShuffleOperator.PARTITION_COLUMN: [0, 1, 0, 1],
        })

        partitions = split_by_partition(table)

        assert len(partitions) == 2
        assert 0 in partitions
        assert 1 in partitions

        # Check partition 0
        p0 = partitions[0]
        assert p0.num_rows == 2
        assert ShuffleOperator.PARTITION_COLUMN not in p0.column_names
        assert set(p0.column("user_id").to_pylist()) == {1, 3}

        # Check partition 1
        p1 = partitions[1]
        assert p1.num_rows == 2
        assert set(p1.column("user_id").to_pylist()) == {2, 4}

    def test_split_single_partition(self):
        """Test splitting when all rows go to same partition."""
        table = pa.table({
            "user_id": [1, 2, 3],
            ShuffleOperator.PARTITION_COLUMN: [0, 0, 0],
        })

        partitions = split_by_partition(table)

        assert len(partitions) == 1
        assert 0 in partitions
        assert partitions[0].num_rows == 3

    def test_split_missing_column_error(self):
        """Test that missing partition column raises error."""
        table = pa.table({
            "user_id": [1, 2, 3],
        })

        with pytest.raises(ValueError, match="missing"):
            split_by_partition(table)


class TestIsShuffleOperator:
    """Tests for is_shuffle_operator utility."""

    def test_shuffle_config(self):
        """Test that shuffle configs are detected."""
        config = RepartitionConfig(partition_keys=["user_id"])
        assert is_shuffle_operator(config) is True

    def test_non_shuffle_config(self):
        """Test that non-shuffle configs are not detected."""
        config = MapOperatorConfig(map_fn=lambda x: x)
        assert is_shuffle_operator(config) is False
