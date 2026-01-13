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

"""Tests for checkpoint module."""

import tempfile

import pytest

from solstice.checkpoint import (
    CheckpointStatus,
    FsspecCheckpointStorage,
    JobCheckpointData,
    PartitionCheckpointData,
    RecoveryResult,
    StageCheckpointData,
    get_partition_offset,
    recover_from_checkpoint,
)


class TestCheckpointModels:
    """Tests for checkpoint data models."""

    def test_partition_checkpoint_serialization(self):
        """Test PartitionCheckpointData serialization."""
        data = PartitionCheckpointData(
            partition_id=0,
            input_offset=100,
            state_snapshot_id="snap-123",
            state_snapshot_path="/path/to/snapshot",
        )

        # Serialize
        d = data.to_dict()
        assert d["partition_id"] == 0
        assert d["input_offset"] == 100
        assert d["state_snapshot_id"] == "snap-123"

        # Deserialize
        restored = PartitionCheckpointData.from_dict(d)
        assert restored.partition_id == data.partition_id
        assert restored.input_offset == data.input_offset
        assert restored.state_snapshot_id == data.state_snapshot_id

    def test_stage_checkpoint_serialization(self):
        """Test StageCheckpointData serialization."""
        data = StageCheckpointData(stage_id="groupby")
        data.partitions[0] = PartitionCheckpointData(partition_id=0, input_offset=100)
        data.partitions[1] = PartitionCheckpointData(partition_id=1, input_offset=200)

        # Serialize
        d = data.to_dict()
        assert d["stage_id"] == "groupby"
        assert len(d["partitions"]) == 2

        # Deserialize
        restored = StageCheckpointData.from_dict(d)
        assert restored.stage_id == data.stage_id
        assert len(restored.partitions) == 2
        assert restored.partitions[0].input_offset == 100
        assert restored.partitions[1].input_offset == 200

    def test_job_checkpoint_serialization(self):
        """Test JobCheckpointData serialization."""
        checkpoint = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="my_job",
            status=CheckpointStatus.COMPLETED,
            iteration=5,
            metadata={"key": "value"},
        )

        stage = StageCheckpointData(stage_id="groupby")
        stage.partitions[0] = PartitionCheckpointData(partition_id=0, input_offset=100)
        checkpoint.stages["groupby"] = stage

        # Serialize to JSON
        json_str = checkpoint.to_json()
        assert "ckpt-123" in json_str
        assert "COMPLETED" in json_str

        # Deserialize from JSON
        restored = JobCheckpointData.from_json(json_str)
        assert restored.checkpoint_id == "ckpt-123"
        assert restored.job_id == "my_job"
        assert restored.status == CheckpointStatus.COMPLETED
        assert restored.iteration == 5
        assert len(restored.stages) == 1
        assert restored.stages["groupby"].partitions[0].input_offset == 100

    def test_job_checkpoint_lifecycle(self):
        """Test JobCheckpointData status transitions."""
        checkpoint = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="my_job",
        )

        # Initially IN_PROGRESS
        assert checkpoint.status == CheckpointStatus.IN_PROGRESS
        assert not checkpoint.is_complete()

        # Mark completed
        checkpoint.mark_completed()
        assert checkpoint.status == CheckpointStatus.COMPLETED
        assert checkpoint.is_complete()
        assert checkpoint.completed_at is not None

    def test_get_partition_data(self):
        """Test getting partition data from checkpoint."""
        checkpoint = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="my_job",
        )

        stage = StageCheckpointData(stage_id="groupby")
        stage.partitions[0] = PartitionCheckpointData(partition_id=0, input_offset=100)
        checkpoint.stages["groupby"] = stage

        # Get existing partition
        data = checkpoint.get_partition_data("groupby", 0)
        assert data is not None
        assert data.input_offset == 100

        # Get non-existing partition
        data = checkpoint.get_partition_data("groupby", 99)
        assert data is None

        # Get from non-existing stage
        data = checkpoint.get_partition_data("nonexistent", 0)
        assert data is None


class TestFsspecCheckpointStorage:
    """Tests for checkpoint storage."""

    @pytest.fixture
    def storage(self):
        """Create a temporary storage for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield FsspecCheckpointStorage(tmpdir, "test_job")

    @pytest.mark.asyncio
    async def test_save_and_load(self, storage):
        """Test saving and loading a checkpoint."""
        checkpoint = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="test_job",
        )
        checkpoint.mark_completed()

        await storage.save(checkpoint)

        # Load
        loaded = await storage.load()
        assert loaded is not None
        assert loaded.checkpoint_id == "ckpt-123"
        assert loaded.is_complete()

    @pytest.mark.asyncio
    async def test_save_overwrites(self, storage):
        """Test that save overwrites existing checkpoint."""
        # Save first checkpoint
        checkpoint1 = JobCheckpointData(
            checkpoint_id="ckpt-1",
            job_id="test_job",
        )
        checkpoint1.mark_completed()
        await storage.save(checkpoint1)

        # Save second checkpoint (should overwrite)
        checkpoint2 = JobCheckpointData(
            checkpoint_id="ckpt-2",
            job_id="test_job",
        )
        checkpoint2.mark_completed()
        await storage.save(checkpoint2)

        # Load should return the second one
        loaded = await storage.load()
        assert loaded is not None
        assert loaded.checkpoint_id == "ckpt-2"

    @pytest.mark.asyncio
    async def test_load_empty(self, storage):
        """Test loading when no checkpoint exists."""
        loaded = await storage.load()
        assert loaded is None

    @pytest.mark.asyncio
    async def test_load_skips_incomplete(self, storage):
        """Test that incomplete checkpoints are skipped."""
        # Save incomplete checkpoint
        checkpoint = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="test_job",
            status=CheckpointStatus.IN_PROGRESS,
        )
        await storage.save(checkpoint)

        # Load should return None
        loaded = await storage.load()
        assert loaded is None


class TestRecovery:
    """Tests for checkpoint recovery."""

    @pytest.fixture
    def storage(self):
        """Create a temporary storage for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield FsspecCheckpointStorage(tmpdir, "test_job")

    @pytest.mark.asyncio
    async def test_recover_no_checkpoint(self, storage):
        """Test recovery when no checkpoint exists."""
        checkpoint, result = await recover_from_checkpoint(
            storage=storage,
            job_id="test_job",
        )

        assert checkpoint is None
        assert result.recovered is False
        assert result.error is None

    @pytest.mark.asyncio
    async def test_recover_with_checkpoint(self, storage):
        """Test recovery from a valid checkpoint."""
        # Create and save a checkpoint
        saved = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="test_job",
            iteration=5,
        )
        stage = StageCheckpointData(stage_id="groupby")
        stage.partitions[0] = PartitionCheckpointData(partition_id=0, input_offset=100)
        saved.stages["groupby"] = stage
        saved.mark_completed()
        await storage.save(saved)

        # Recover
        checkpoint, result = await recover_from_checkpoint(
            storage=storage,
            job_id="test_job",
        )

        assert result.recovered is True
        assert result.checkpoint_id == "ckpt-123"
        assert checkpoint is not None
        assert checkpoint.iteration == 5

    @pytest.mark.asyncio
    async def test_recover_job_id_mismatch(self, storage):
        """Test recovery fails on job_id mismatch."""
        # Create checkpoint with different job_id
        saved = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="different_job",
        )
        saved.mark_completed()
        await storage.save(saved)

        # Try to recover with mismatched job_id
        checkpoint, result = await recover_from_checkpoint(
            storage=storage,
            job_id="test_job",
        )

        assert result.recovered is False
        assert "mismatch" in result.error


class TestGetPartitionOffset:
    """Tests for get_partition_offset utility."""

    def test_get_offset_no_checkpoint(self):
        """Test getting offset when no checkpoint."""
        offset = get_partition_offset(None, "stage", 0)
        assert offset is None

    def test_get_offset_from_checkpoint(self):
        """Test getting offset from checkpoint."""
        checkpoint = JobCheckpointData(
            checkpoint_id="ckpt-123",
            job_id="test_job",
        )
        stage = StageCheckpointData(stage_id="groupby")
        stage.partitions[0] = PartitionCheckpointData(partition_id=0, input_offset=100)
        stage.partitions[1] = PartitionCheckpointData(partition_id=1, input_offset=200)
        checkpoint.stages["groupby"] = stage

        assert get_partition_offset(checkpoint, "groupby", 0) == 100
        assert get_partition_offset(checkpoint, "groupby", 1) == 200
        assert get_partition_offset(checkpoint, "groupby", 2) is None
        assert get_partition_offset(checkpoint, "other_stage", 0) is None
