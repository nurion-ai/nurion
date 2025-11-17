"""StageWorker actor for executing operator logic over splits."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Type

import pyarrow as pa
import ray  # type: ignore[import]

from solstice.core.models import Batch, Record, Split, WorkerMetrics
from solstice.core.operator import Operator


@ray.remote
class StageWorker:
    """Ray actor that executes an operator over batches without persisting state."""

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

        self.logger = logging.getLogger(f"StageWorker-{stage_id}-{worker_id}")

        # Metrics
        self.processed_count = 0
        self.processing_times: List[float] = []

        self.logger.info(f"StageWorker {worker_id} initialised for stage {stage_id}")

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def process_split(self, split: Split, payload_ref: ray.ObjectRef) -> Dict[str, Any]:
        """Materialise the batch referenced by ``payload_ref`` and run the operator."""
        start_time = time.time()

        try:
            batch: Batch = ray.get(payload_ref)
        except Exception as exc:  # pragma: no cover - ray transport errors
            self.logger.error(f"Failed to fetch payload for split {split.split_id}: {exc}")
            raise

        output_batch = self._run_operator(batch)

        self.processed_count += len(batch)
        self.processing_times.append(time.time() - start_time)
        if len(self.processing_times) > 100:
            self.processing_times = self.processing_times[-100:]

        output_ref: Optional[ray.ObjectRef] = None
        if output_batch is not None and len(output_batch):
            output_ref = ray.put(output_batch)

        metrics = self.get_metrics()

        self.logger.debug(
            f"Worker {self.worker_id} processed split {split.split_id} ({len(batch)} records)",
        )

        return {
            "split_id": split.split_id,
            "output_ref": output_ref,
            "metrics": metrics,
        }

    def _run_operator(self, batch: Batch) -> Optional[Batch]:
        """Normalize operator outputs to a :class:`Batch`."""
        try:
            output = self.operator.process_batch(batch)
        except Exception as exc:
            self.logger.error(f"Operator error in stage {self.stage_id}: {exc}", exc_info=True)
            raise

        if output is None:
            return None
        if isinstance(output, Batch):
            return output
        if isinstance(output, (pa.Table, pa.RecordBatch)):
            return batch.with_new_data(output)
        raise TypeError(f"Unsupported operator output type {type(output)!r}")

    # ------------------------------------------------------------------
    # Metrics / lifecycle
    # ------------------------------------------------------------------
    def get_metrics(self) -> Dict[str, Any]:
        """Return current worker metrics."""
        if self.processing_times:
            avg_time = sum(self.processing_times) / len(self.processing_times)
            processing_rate = 1.0 / avg_time if avg_time > 0 else 0.0
        else:
            processing_rate = 0.0

        metrics = WorkerMetrics(
            worker_id=self.worker_id,
            stage_id=self.stage_id,
            processing_rate=processing_rate,
            backlog_size=0,
        )

        return {
            "worker_id": metrics.worker_id,
            "stage_id": metrics.stage_id,
            "processing_rate": metrics.processing_rate,
            "backlog_size": metrics.backlog_size,
            "cpu_usage": metrics.cpu_usage,
            "memory_usage": metrics.memory_usage,
            "timestamp": metrics.timestamp,
        }

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
