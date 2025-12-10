"""Simple autoscaler for dynamic worker scaling.

This module implements a simple threshold-based autoscaler suitable for
offline/batch processing workloads. It prioritizes simplicity over complexity,
using in-memory state and slow-paced decisions (15-30 second intervals).

Design principles:
1. Single coordinator - runs within RayJobRunner, not distributed
2. In-memory state - no persistence needed, reconstructs on restart
3. Slow-paced decisions - 15-30 seconds is sufficient for batch workloads
4. Simple threshold rules - no complex algorithms

See design-docs/dynamic-worker-scaling.md for full design documentation.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Set, Union


from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.core.stage_master import StageMaster
    from solstice.operators.sources.source import SourceMaster


@dataclass
class AutoscaleConfig:
    """Configuration for the autoscaler.

    Attributes:
        enabled: Whether autoscaling is enabled
        check_interval_s: How often to make scaling decisions (seconds)
        scale_up_lag_threshold: Scale up if input queue lag exceeds this
        scale_down_lag_threshold: Scale down if lag is below this
        cooldown_s: Minimum time between scaling operations for a stage
        max_scale_step: Maximum workers to add/remove per decision
        fixed_workers: Manual override for specific stages {"stage_id": count}
        frozen_stages: Stages excluded from autoscaling
    """

    enabled: bool = True
    check_interval_s: float = 15.0

    # Scaling thresholds
    scale_up_lag_threshold: int = 1000
    scale_down_lag_threshold: int = 100

    # Damping
    cooldown_s: float = 60.0
    max_scale_step: int = 2

    # Manual overrides
    fixed_workers: Optional[Dict[str, int]] = None
    frozen_stages: Set[str] = field(default_factory=set)


@dataclass
class StageMetrics:
    """Metrics collected from a stage for scaling decisions."""

    stage_id: str
    worker_count: int
    min_workers: int
    max_workers: int
    input_queue_lag: int = 0
    output_queue_size: int = 0
    is_running: bool = True
    is_finished: bool = False
    is_source: bool = False


class SimpleAutoscaler:
    """Simple threshold-based autoscaler for batch workloads.

    This autoscaler:
    - Runs as a background task within RayJobRunner
    - Makes decisions every check_interval_s seconds
    - Uses simple threshold-based rules
    - Supports manual overrides and stage freezing

    Example:
        ```python
        config = AutoscaleConfig(
            check_interval_s=15.0,
            scale_up_lag_threshold=1000,
        )
        autoscaler = SimpleAutoscaler(config)

        # In RayJobRunner.run():
        autoscale_task = asyncio.create_task(
            autoscaler.run_loop(masters)
        )
        ```
    """

    def __init__(self, config: Optional[AutoscaleConfig] = None):
        self.config = config or AutoscaleConfig()
        self.logger = create_ray_logger("Autoscaler")

        # Scaling state (in-memory only)
        self._last_scale_time: Dict[str, float] = {}
        self._current_metrics: Dict[str, StageMetrics] = {}

        # Control
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def run_loop(
        self,
        masters: Dict[str, Union["StageMaster", "SourceMaster"]],
    ) -> None:
        """Main autoscaling loop.

        Args:
            masters: Dictionary of stage_id -> StageMaster/SourceMaster
        """
        self._running = True
        self.logger.info(
            f"Autoscaler started (interval={self.config.check_interval_s}s, "
            f"enabled={self.config.enabled})"
        )

        try:
            while self._running:
                await asyncio.sleep(self.config.check_interval_s)

                if not self.config.enabled:
                    continue

                try:
                    # Collect metrics from all stages
                    metrics = await self._collect_metrics(masters)
                    self._current_metrics = metrics

                    # Compute scaling decisions
                    decisions = self._compute_decisions(metrics)

                    # Execute scaling (with cooldown protection)
                    await self._execute_decisions(masters, decisions)

                except Exception as e:
                    self.logger.error(f"Autoscaler error: {e}")
                    # Continue running, don't crash the loop

        except asyncio.CancelledError:
            self.logger.info("Autoscaler stopped")
            raise

    def stop(self) -> None:
        """Stop the autoscaler loop."""
        self._running = False

    async def _collect_metrics(
        self,
        masters: Dict[str, Union["StageMaster", "SourceMaster"]],
    ) -> Dict[str, StageMetrics]:
        """Collect metrics from all stages.

        For non-source stages, we need to get the input queue lag.
        This requires checking the upstream queue's latest offset vs
        the stage's committed offset.
        """
        from solstice.operators.sources.source import SourceMaster

        metrics = {}

        for stage_id, master in masters.items():
            is_source = isinstance(master, SourceMaster)

            # Get basic status
            status = master.get_status()

            # Get config (min/max workers)
            if hasattr(master, "config"):
                config = master.config
                min_workers = config.min_workers
                max_workers = config.max_workers
            else:
                min_workers = 1
                max_workers = 4

            # For non-source stages, try to get input queue lag
            input_lag = 0
            if not is_source and hasattr(master, "get_input_queue_lag"):
                try:
                    input_lag = await master.get_input_queue_lag()
                except Exception:
                    pass  # Use 0 if we can't get lag

            metrics[stage_id] = StageMetrics(
                stage_id=stage_id,
                worker_count=status.worker_count,
                min_workers=min_workers,
                max_workers=max_workers,
                input_queue_lag=input_lag,
                output_queue_size=status.output_queue_size,
                is_running=status.is_running,
                is_finished=status.is_finished,
                is_source=is_source,
            )

        return metrics

    def _compute_decisions(
        self,
        metrics: Dict[str, StageMetrics],
    ) -> Dict[str, int]:
        """Compute scaling decisions for each stage.

        Rules:
        1. Manual override (fixed_workers) has highest priority
        2. Frozen stages are skipped
        3. Source stages are skipped (they control their own rate)
        4. Scale up if input_queue_lag > threshold
        5. Scale down if lag < threshold and workers > min
        """
        decisions = {}

        for stage_id, m in metrics.items():
            # Skip source stages (they don't have input queues to scale based on)
            if m.is_source:
                continue

            # Skip finished stages
            if m.is_finished or not m.is_running:
                continue

            # Skip frozen stages
            if stage_id in self.config.frozen_stages:
                continue

            current = m.worker_count

            # Rule 1: Manual override
            if self.config.fixed_workers and stage_id in self.config.fixed_workers:
                target = self.config.fixed_workers[stage_id]
                target = max(m.min_workers, min(target, m.max_workers))
                if target != current:
                    decisions[stage_id] = target
                continue

            # Rule 2: Scale up on high lag
            if m.input_queue_lag > self.config.scale_up_lag_threshold:
                target = min(current + self.config.max_scale_step, m.max_workers)
                if target > current:
                    decisions[stage_id] = target
                    self.logger.debug(
                        f"Stage {stage_id}: scale up {current} -> {target} "
                        f"(lag={m.input_queue_lag})"
                    )
                continue

            # Rule 3: Scale down on low lag
            if m.input_queue_lag < self.config.scale_down_lag_threshold:
                if current > m.min_workers:
                    target = max(current - 1, m.min_workers)
                    decisions[stage_id] = target
                    self.logger.debug(
                        f"Stage {stage_id}: scale down {current} -> {target} "
                        f"(lag={m.input_queue_lag})"
                    )

        return decisions

    async def _execute_decisions(
        self,
        masters: Dict[str, Union["StageMaster", "SourceMaster"]],
        decisions: Dict[str, int],
    ) -> None:
        """Execute scaling decisions with cooldown protection."""
        now = time.time()

        for stage_id, target in decisions.items():
            # Check cooldown
            last_scale = self._last_scale_time.get(stage_id, 0)
            if now - last_scale < self.config.cooldown_s:
                self.logger.debug(
                    f"Stage {stage_id}: skipping scale (cooldown, "
                    f"{self.config.cooldown_s - (now - last_scale):.1f}s remaining)"
                )
                continue

            master = masters.get(stage_id)
            if not master:
                continue

            current = len(master._workers) if hasattr(master, "_workers") else 0

            try:
                if target > current:
                    # Scale up
                    to_add = target - current
                    for _ in range(to_add):
                        await master._spawn_worker()
                    self._last_scale_time[stage_id] = now
                    self.logger.info(f"Scaled UP {stage_id}: {current} -> {target} workers")

                elif target < current:
                    # Scale down
                    to_remove = current - target
                    await master.scale_down(to_remove)
                    self._last_scale_time[stage_id] = now
                    self.logger.info(f"Scaled DOWN {stage_id}: {current} -> {target} workers")

            except Exception as e:
                self.logger.error(f"Failed to scale {stage_id}: {e}")

    # === Manual Intervention API ===

    def set_fixed_workers(self, stage_id: str, count: int) -> None:
        """Set a fixed worker count for a stage (manual override)."""
        if self.config.fixed_workers is None:
            self.config.fixed_workers = {}
        self.config.fixed_workers[stage_id] = count
        self.logger.info(f"Set fixed workers for {stage_id}: {count}")

    def clear_fixed_workers(self, stage_id: str) -> None:
        """Clear manual override, restore automatic scaling."""
        if self.config.fixed_workers:
            self.config.fixed_workers.pop(stage_id, None)
            self.logger.info(f"Cleared fixed workers for {stage_id}")

    def freeze_stage(self, stage_id: str) -> None:
        """Freeze a stage (disable autoscaling for it)."""
        self.config.frozen_stages.add(stage_id)
        self.logger.info(f"Froze stage {stage_id}")

    def unfreeze_stage(self, stage_id: str) -> None:
        """Unfreeze a stage (re-enable autoscaling)."""
        self.config.frozen_stages.discard(stage_id)
        self.logger.info(f"Unfroze stage {stage_id}")

    def pause(self) -> None:
        """Pause all autoscaling."""
        self.config.enabled = False
        self.logger.info("Autoscaling paused")

    def resume(self) -> None:
        """Resume autoscaling."""
        self.config.enabled = True
        self.logger.info("Autoscaling resumed")

    def get_status(self) -> Dict[str, Any]:
        """Get current autoscaler status."""
        return {
            "enabled": self.config.enabled,
            "check_interval_s": self.config.check_interval_s,
            "frozen_stages": list(self.config.frozen_stages),
            "fixed_workers": self.config.fixed_workers or {},
            "stages": {
                stage_id: {
                    "worker_count": m.worker_count,
                    "min_workers": m.min_workers,
                    "max_workers": m.max_workers,
                    "input_queue_lag": m.input_queue_lag,
                    "is_source": m.is_source,
                    "last_scale_time": self._last_scale_time.get(stage_id),
                    "is_frozen": stage_id in self.config.frozen_stages,
                }
                for stage_id, m in self._current_metrics.items()
            },
        }
