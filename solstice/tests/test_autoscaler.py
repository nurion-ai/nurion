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

"""Tests for the SimpleAutoscaler.

Tests the autoscaling functionality including:
- Threshold-based scaling decisions
- Cooldown periods
- Manual overrides
- Stage freezing
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from solstice.runtime.autoscaler import (
    AutoscaleConfig,
    SimpleAutoscaler,
    StageMetrics,
)
from solstice.core.stage_master import StageStatus, StageConfig


# ============================================================================
# Mock Classes
# ============================================================================


class MockStageMaster:
    """Mock StageMaster for testing autoscaler."""

    def __init__(
        self,
        stage_id: str = "test_stage",
        worker_count: int = 2,
        min_workers: int = 1,
        max_workers: int = 8,
        input_queue_lag: int = 0,
    ):
        self.stage_id = stage_id
        self._workers = {f"worker_{i}": MagicMock() for i in range(worker_count)}
        self._running = True
        self._finished = False

        # Config
        self.config = MagicMock()
        self.config.min_workers = min_workers
        self.config.max_workers = max_workers

        # For lag simulation
        self._input_queue_lag = input_queue_lag

    def get_status(self) -> StageStatus:
        return StageStatus(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            output_queue_size=0,
            is_running=self._running,
            is_finished=self._finished,
        )

    async def get_input_queue_lag(self) -> int:
        return self._input_queue_lag

    async def _spawn_worker(self) -> str:
        worker_id = f"worker_{len(self._workers)}"
        self._workers[worker_id] = MagicMock()
        return worker_id

    async def scale_down(self, count: int) -> int:
        to_remove = min(count, len(self._workers) - self.config.min_workers)
        for _ in range(to_remove):
            if self._workers:
                key = list(self._workers.keys())[-1]
                del self._workers[key]
        return to_remove

    def _rebalance_partitions(self) -> None:
        """Mock partition rebalancing."""
        pass

    async def _notify_workers_partition_update(self) -> None:
        """Mock worker partition notification."""
        pass


class MockSourceMaster:
    """Mock SourceMaster for testing (should be skipped by autoscaler)."""

    def __init__(self, stage_id: str = "source_stage"):
        self.stage_id = stage_id
        self._workers = {"worker_0": MagicMock()}
        self._running = True
        self._finished = False

    def get_status(self) -> StageStatus:
        return StageStatus(
            stage_id=self.stage_id,
            worker_count=len(self._workers),
            output_queue_size=100,
            is_running=self._running,
            is_finished=self._finished,
        )


# ============================================================================
# Unit Tests
# ============================================================================


class TestAutoscaleConfig:
    """Tests for AutoscaleConfig."""

    def test_default_config(self):
        config = AutoscaleConfig()
        assert config.enabled is True
        assert config.check_interval_s == 15.0
        assert config.scale_up_lag_threshold == 1000
        assert config.scale_down_lag_threshold == 100
        assert config.cooldown_s == 60.0
        assert config.max_scale_step == 2

    def test_custom_config(self):
        config = AutoscaleConfig(
            enabled=False,
            check_interval_s=30.0,
            scale_up_lag_threshold=500,
            fixed_workers={"stage_a": 5},
        )
        assert config.enabled is False
        assert config.check_interval_s == 30.0
        assert config.scale_up_lag_threshold == 500
        assert config.fixed_workers == {"stage_a": 5}


class TestStageMetrics:
    """Tests for StageMetrics."""

    def test_metrics_creation(self):
        metrics = StageMetrics(
            stage_id="test",
            worker_count=3,
            min_workers=1,
            max_workers=10,
            input_queue_lag=500,
        )
        assert metrics.stage_id == "test"
        assert metrics.worker_count == 3
        assert metrics.input_queue_lag == 500
        assert metrics.is_source is False


class TestSimpleAutoscaler:
    """Tests for SimpleAutoscaler."""

    def test_init(self):
        autoscaler = SimpleAutoscaler()
        assert autoscaler.config.enabled is True
        assert autoscaler._running is False

    def test_init_with_config(self):
        config = AutoscaleConfig(check_interval_s=10.0)
        autoscaler = SimpleAutoscaler(config)
        assert autoscaler.config.check_interval_s == 10.0


class TestScalingDecisions:
    """Tests for scaling decision logic."""

    @pytest.fixture
    def autoscaler(self):
        config = AutoscaleConfig(
            scale_up_lag_threshold=1000,
            scale_down_lag_threshold=100,
            max_scale_step=2,
        )
        return SimpleAutoscaler(config)

    def test_scale_up_on_high_lag(self, autoscaler):
        """Should scale up when lag exceeds threshold."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=1500,  # Above threshold
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" in decisions
        assert decisions["stage_a"] == 4  # 2 + max_scale_step(2)

    def test_scale_down_on_low_lag(self, autoscaler):
        """Should scale down when lag is below threshold."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=4,
                min_workers=1,
                max_workers=8,
                input_queue_lag=50,  # Below threshold
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" in decisions
        assert decisions["stage_a"] == 3  # 4 - 1

    def test_no_scale_in_normal_range(self, autoscaler):
        """Should not scale when lag is in normal range."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=500,  # Between thresholds
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions

    def test_respect_max_workers(self, autoscaler):
        """Should not scale above max_workers."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=7,
                min_workers=1,
                max_workers=8,
                input_queue_lag=2000,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert decisions["stage_a"] == 8  # Not 9

    def test_respect_min_workers(self, autoscaler):
        """Should not scale below min_workers."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=1,
                min_workers=1,
                max_workers=8,
                input_queue_lag=10,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions  # Already at min

    def test_skip_source_stages(self, autoscaler):
        """Should skip source stages."""
        metrics = {
            "source": StageMetrics(
                stage_id="source",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=5000,
                is_source=True,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "source" not in decisions

    def test_skip_finished_stages(self, autoscaler):
        """Should skip finished stages."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=5000,
                is_finished=True,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions


class TestManualOverrides:
    """Tests for manual intervention API."""

    @pytest.fixture
    def autoscaler(self):
        return SimpleAutoscaler()

    def test_set_fixed_workers(self, autoscaler):
        """Manual override should take priority."""
        autoscaler.set_fixed_workers("stage_a", 5)

        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=0,  # Would normally not scale
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert decisions["stage_a"] == 5

    def test_clear_fixed_workers(self, autoscaler):
        autoscaler.set_fixed_workers("stage_a", 5)
        autoscaler.clear_fixed_workers("stage_a")

        assert autoscaler.config.fixed_workers.get("stage_a") is None

    def test_freeze_stage(self, autoscaler):
        autoscaler.freeze_stage("stage_a")

        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=5000,  # Would normally scale up
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions

    def test_unfreeze_stage(self, autoscaler):
        autoscaler.freeze_stage("stage_a")
        autoscaler.unfreeze_stage("stage_a")

        assert "stage_a" not in autoscaler.config.frozen_stages

    def test_pause_resume(self, autoscaler):
        autoscaler.pause()
        assert autoscaler.config.enabled is False

        autoscaler.resume()
        assert autoscaler.config.enabled is True

    def test_get_status(self, autoscaler):
        autoscaler.freeze_stage("stage_a")
        autoscaler.set_fixed_workers("stage_b", 10)

        # Simulate some metrics
        autoscaler._current_metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
            )
        }

        status = autoscaler.get_status()

        assert status["enabled"] is True
        assert "stage_a" in status["frozen_stages"]
        assert status["fixed_workers"]["stage_b"] == 10


@pytest.mark.asyncio
class TestCooldown:
    """Tests for cooldown behavior."""

    async def test_cooldown_prevents_rapid_scaling(self):
        """Scaling should be blocked during cooldown period."""
        config = AutoscaleConfig(cooldown_s=60.0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(
            stage_id="stage_a",
            worker_count=2,
            input_queue_lag=2000,
        )
        masters = {"stage_a": master}

        # First scale
        decisions = {"stage_a": 4}
        await autoscaler._execute_decisions(masters, decisions)

        assert len(master._workers) == 4

        # Try to scale again immediately
        decisions = {"stage_a": 6}
        await autoscaler._execute_decisions(masters, decisions)

        # Should still be 4 due to cooldown
        assert len(master._workers) == 4

    async def test_scaling_after_cooldown(self):
        """Scaling should work after cooldown period."""
        config = AutoscaleConfig(cooldown_s=0.1)  # Short cooldown for testing
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(
            stage_id="stage_a",
            worker_count=2,
            input_queue_lag=2000,
        )
        masters = {"stage_a": master}

        # First scale
        await autoscaler._execute_decisions(masters, {"stage_a": 4})
        assert len(master._workers) == 4

        # Wait for cooldown
        await asyncio.sleep(0.15)

        # Should be able to scale now
        await autoscaler._execute_decisions(masters, {"stage_a": 6})
        assert len(master._workers) == 6


@pytest.mark.asyncio
class TestMetricsCollection:
    """Tests for metrics collection."""

    async def test_collect_metrics_from_masters(self):
        autoscaler = SimpleAutoscaler()

        master = MockStageMaster(
            stage_id="stage_a",
            worker_count=3,
            input_queue_lag=500,
        )

        # Collect metrics - MockStageMaster is not a SourceMaster
        metrics = await autoscaler._collect_metrics({"stage_a": master})

        assert "stage_a" in metrics
        assert metrics["stage_a"].worker_count == 3
        assert metrics["stage_a"].input_queue_lag == 500
        assert metrics["stage_a"].is_source is False

    async def test_source_stage_marked_correctly(self):
        from solstice.operators.sources.source import SourceMaster

        autoscaler = SimpleAutoscaler()

        # Create a mock that passes isinstance check
        source = MagicMock(spec=SourceMaster)
        source.stage_id = "source"
        source._workers = {"worker_0": MagicMock()}
        source._running = True
        source._finished = False
        source.config = StageConfig(min_workers=1, max_workers=1)
        source.get_status.return_value = StageStatus(
            stage_id="source",
            worker_count=1,
            output_queue_size=100,
            is_running=True,
            is_finished=False,
        )

        metrics = await autoscaler._collect_metrics({"source": source})

        assert metrics["source"].is_source is True


@pytest.mark.asyncio
class TestScaleExecution:
    """Tests for scale up/down execution."""

    async def test_scale_up_spawns_workers(self):
        autoscaler = SimpleAutoscaler()

        master = MockStageMaster(worker_count=2)
        masters = {"stage_a": master}

        await autoscaler._execute_decisions(masters, {"stage_a": 5})

        assert len(master._workers) == 5

    async def test_scale_down_removes_workers(self):
        config = AutoscaleConfig(cooldown_s=0)  # No cooldown for testing
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=5, min_workers=1)
        masters = {"stage_a": master}

        await autoscaler._execute_decisions(masters, {"stage_a": 2})

        assert len(master._workers) == 2

    async def test_scale_down_respects_min_workers(self):
        config = AutoscaleConfig(cooldown_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=3, min_workers=2)
        masters = {"stage_a": master}

        await autoscaler._execute_decisions(masters, {"stage_a": 1})

        # Should stop at min_workers
        assert len(master._workers) == 2
