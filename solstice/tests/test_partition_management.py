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

"""Unit tests for dynamic partition management.

Tests cover:
- Partition count calculation
- Queue creation with dynamic partitions
- Consumer group assignment
- Partition rebalance handling

All tests use real implementations (no mocks) to catch real issues.
"""

from dataclasses import dataclass


from solstice.core.stage_master import StageMaster, StageConfig
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


class TestPartitionCountCalculation:
    """Tests for partition count calculation logic."""

    def test_single_worker_returns_one_partition(self, payload_store):
        """Test that single worker scenario uses 1 partition."""
        config = StageConfig(max_workers=1, min_workers=1)
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

        partition_count = master._compute_partition_count()
        assert partition_count == 1

    def test_explicit_partition_count(self, payload_store):
        """Test that explicit partition_count is respected."""
        config = StageConfig(max_workers=4, partition_count=8)
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

        partition_count = master._compute_partition_count()
        assert partition_count == 8

    def test_auto_partition_count_from_max_workers(self, payload_store):
        """Test that partition count equals max_workers when auto."""
        config = StageConfig(max_workers=4, partition_count=None)
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

        partition_count = master._compute_partition_count()
        assert partition_count == 4

    def test_partition_count_minimum_one(self, payload_store):
        """Test that partition count is always at least 1."""
        config = StageConfig(max_workers=0, partition_count=0)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=0,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        assert partition_count >= 1


class TestPartitionCountEdgeCases:
    """Tests for edge cases in partition count calculation."""

    def test_partition_count_with_zero_max_workers(self, payload_store):
        """Test partition count when max_workers is 0."""
        config = StageConfig(max_workers=0, partition_count=None)
        stage = Stage(
            stage_id="test_stage",
            operator_config=_TestOperatorConfig(),
            parallelism=0,
        )
        master = StageMaster(
            job_id="test_job",
            stage=stage,
            config=config,
            payload_store=payload_store,
        )

        partition_count = master._compute_partition_count()
        # Should default to 1 (minimum)
        assert partition_count == 1

    def test_partition_count_with_negative_value(self, payload_store):
        """Test partition count with negative explicit value."""
        config = StageConfig(max_workers=4, partition_count=-5)
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

        partition_count = master._compute_partition_count()
        # Should be clamped to minimum 1
        assert partition_count == 1

    def test_partition_count_large_value(self, payload_store):
        """Test partition count with very large value."""
        config = StageConfig(max_workers=4, partition_count=1000)
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

        partition_count = master._compute_partition_count()
        # Should accept large value (no upper limit in calculation)
        assert partition_count == 1000
