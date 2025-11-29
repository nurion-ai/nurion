"""StageWorker actor for executing operator logic over splits."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

import ray

from solstice.core.models import Split, SplitPayload, WorkerMetrics
from solstice.core.operator import Operator
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.core.stage import Stage


@dataclass
class ProcessResult:
    """Result of a split processing"""

    input_split_id: str
    input_records: int
    output_records: int
    processing_time: float
    output_split: Split
    worker_metrics: WorkerMetrics = field(default_factory=WorkerMetrics)


@ray.remote
class StageWorker:
    """Ray actor that executes an operator over batches without persisting state.

    StageWorker is completely stateless - it only maintains ephemeral in-memory state
    during batch processing. All persistent state management is handled by StageMaster.
    """

    def __init__(
        self,
        worker_id: str,
        stage: "Stage",
    ):
        self.worker_id = worker_id
        self.stage_id = stage.stage_id
        self.operator: Operator = stage.operator_config.setup(worker_id=worker_id)

        self.logger = create_ray_logger(f"StageWorker-{self.stage_id}-{self.worker_id}")

        # Ephemeral metrics (not persisted)
        self.total_input_records = 0
        self.total_output_records = 0
        self.total_processing_time = 0.0

        self.logger.info(f"StageWorker {worker_id} initialised for stage {self.stage_id}")

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def process_split(
        self,
        split: Split,
        payload_ref: Optional[SplitPayload] = None,
    ) -> ProcessResult:
        """Process a split with the operator.

        Args:
            split: The split metadata
            payload_ref: Optional batch payload reference (None for source operators)

        Returns:
            Dictionary with split_id, output_ref (for downstream), and metrics
        """
        self.logger.debug(
            f"Worker {self.worker_id} processing split {split.split_id} (payload={payload_ref})",
        )
        start_time = time.time()

        payload: Optional[SplitPayload] = None
        payload_ref = payload_ref or split.data_range.get("object_ref")
        if payload_ref is not None:
            if not isinstance(payload_ref, ray.ObjectRef):
                self.logger.error(
                    f"Worker {self.worker_id} received invalid payload reference type "
                    f"{type(payload_ref)} for split {split.split_id}",
                )
                raise TypeError(
                    f"payload_ref must be a ray.ObjectRef, got {type(payload_ref)}",
                )
            try:
                self.logger.debug(
                    f"Worker {self.worker_id} fetching payload for split {split.split_id}",
                )
                payload = ray.get(payload_ref, timeout=300)
                self.logger.debug(
                    f"Worker {self.worker_id} fetched payload for split {split.split_id}, "
                    f"records={len(payload) if payload else 0}",
                )
            except Exception as exc:
                self.logger.error(
                    f"Worker {self.worker_id} failed to fetch payload for split {split.split_id}: {exc}",
                    exc_info=True,
                )
                raise

        try:
            self.logger.debug(
                f"Worker {self.worker_id} calling operator.process_split for split {split.split_id}",
            )
            output_payload = self.operator.process_split(split, payload)
            self.logger.debug(
                f"Worker {self.worker_id} operator.process_split completed for split {split.split_id}, output_records={len(output_payload) if output_payload else 0}",
            )
        except Exception:
            self.logger.error(
                f"Operator {type(self.operator).__name__} failed to process split {split.split_id} on worker {self.worker_id}",
            )
            raise

        input_records = len(payload) if payload is not None else 0
        output_records = len(output_payload) if output_payload is not None else 0

        output_ref: Optional[ray.ObjectRef] = None
        if output_payload:
            self.logger.debug(
                f"Worker {self.worker_id} putting output payload to Ray object store for split {split.split_id}, records={len(output_payload)}",
            )
            output_ref = ray.put(output_payload)
            # ray.put() is synchronous, object is available immediately after return
            self.logger.debug(
                f"Worker {self.worker_id} put output payload to Ray object store for split {split.split_id}, object_ref={output_ref}",
            )

        # Update metrics
        self.total_input_records += input_records
        self.total_output_records += output_records

        duration = time.time() - start_time
        self.total_processing_time += duration

        self.logger.debug(
            f"Worker {self.worker_id} processed split {split.split_id} in {duration:.3f}s (in={input_records}, out={output_records})",
        )
        output_split = split.derive_output_split(
            target_split_id=f"{split.split_id}:read_{self.worker_id}",
            data_range={
                "object_ref": output_ref,
            },
        )

        return ProcessResult(
            input_split_id=split.split_id,
            input_records=input_records,
            output_records=output_records,
            processing_time=duration,
            output_split=output_split,
            worker_metrics=self.get_metrics(),
        )

    # ------------------------------------------------------------------
    # Metrics / lifecycle
    # ------------------------------------------------------------------
    def get_metrics(self) -> WorkerMetrics:
        """Return current worker metrics."""
        return WorkerMetrics(
            worker_id=self.worker_id,
            stage_id=self.stage_id,
            processing_time=self.total_processing_time,
            input_records=self.total_input_records,
            output_records=self.total_output_records,
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
