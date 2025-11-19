"""StageWorker actor for executing operator logic over splits."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Type

import ray  # type: ignore[import]

from solstice.core.models import Split, SplitPayload, WorkerMetrics
from solstice.core.operator import Operator
from solstice.utils.logging import create_ray_logger


@ray.remote
class StageWorker:
    """Ray actor that executes an operator over batches without persisting state.

    StageWorker is completely stateless - it only maintains ephemeral in-memory state
    during batch processing. All persistent state management is handled by StageMaster.
    """

    def __init__(
        self,
        worker_id: str,
        stage_id: str,
        operator_class: Type[Operator],
        operator_config: Optional[Dict[str, Any]] = None,
    ):
        self.worker_id = worker_id
        self.stage_id = stage_id
        self.operator_config = operator_config or {}
        self.operator: Operator = operator_class(self.operator_config)

        self.logger = create_ray_logger(f"StageWorker-{stage_id}-{worker_id}")

        # Ephemeral metrics (not persisted)
        self.processed_count = 0
        self.processing_times: List[float] = []

        self.logger.info(f"StageWorker {worker_id} initialised for stage {stage_id}")

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def process_split(
        self,
        split: Split,
        payload_ref: Optional[ray.ObjectRef] = None,
    ) -> Dict[str, Any]:
        """Process a split with the operator.

        Args:
            split: The split metadata
            payload_ref: Optional batch payload reference (None for source operators)

        Returns:
            Dictionary with split_id, output_ref (for downstream), and metrics
        """
        start_time = time.time()

        # For source operators, batch is None
        # For other operators, get the batch from payload_ref
        batch: Optional[SplitPayload] = None
        if payload_ref is not None:
            try:
                batch = ray.get(payload_ref)
            except Exception as exc:
                self.logger.error(
                    "Worker %s failed to fetch payload for split %s: %s",
                    self.worker_id,
                    split.split_id,
                    exc,
                )
                raise

        self.logger.debug(
            "Worker %s processing split %s (payload=%s)",
            self.worker_id,
            split.split_id,
            payload_ref is not None,
        )

        # Unified processing: all operators use process_split
        try:
            output_batch = self.operator.process_split(split, batch)
        except Exception as exc:
            self.logger.exception(
                "Operator %s failed to process split %s on worker %s",
                type(self.operator).__name__,
                split.split_id,
                self.worker_id,
            )
            raise

        # Update metrics
        if output_batch is not None:
            self.processed_count += len(output_batch)
        else:
            # For sinks or operators that produce no output, count input
            if batch is not None:
                self.processed_count += len(batch)

        self.processing_times.append(time.time() - start_time)
        if len(self.processing_times) > 100:
            self.processing_times = self.processing_times[-100:]

        output_ref: Optional[ray.ObjectRef] = None
        if output_batch is not None and len(output_batch):
            output_ref = ray.put(output_batch)

        metrics = self.get_metrics()

        duration = time.time() - start_time
        input_size = len(batch) if batch is not None else 0
        output_size = len(output_batch) if output_batch is not None else 0
        self.logger.debug(
            "Worker %s processed split %s in %.3fs (in=%d, out=%d)",
            self.worker_id,
            split.split_id,
            duration,
            input_size,
            output_size,
        )

        return {
            "split_id": split.split_id,
            "output_ref": output_ref,
            "metrics": metrics,
            "worker_id": self.worker_id,
        }

    # ------------------------------------------------------------------
    # Metrics / lifecycle
    # ------------------------------------------------------------------
    def get_metrics(self) -> WorkerMetrics:
        """Return current worker metrics."""
        if self.processing_times:
            avg_time = sum(self.processing_times) / len(self.processing_times)
            processing_rate = 1.0 / avg_time if avg_time > 0 else 0.0
        else:
            processing_rate = 0.0

        return WorkerMetrics(
            worker_id=self.worker_id,
            stage_id=self.stage_id,
            processing_rate=processing_rate,
            backlog_size=0,
        )

    def health_check(self) -> bool:
        """Ray health check hook."""
        return True

    def shutdown(self) -> None:
        """Gracefully close the operator."""
        self.logger.info(f"Shutting down StageWorker {self.worker_id}")
        try:
            self.operator.close()
        except Exception as exc:
            self.logger.error(f"Error closing operator in worker {self.worker_id}: {exc}")
