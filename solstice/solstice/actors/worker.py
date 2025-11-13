"""Worker actor for processing data"""

import time
import logging
from typing import Any, Dict, List, Optional
import ray  # type: ignore[import]

from solstice.core.operator import Operator, OperatorContext
from solstice.core.models import Batch, CheckpointHandle, WorkerMetrics
from solstice.state.manager import StateManager
from solstice.state.backend import StateBackend


@ray.remote
class WorkerActor:
    """Ray actor that executes operator logic on data"""

    def __init__(
        self,
        worker_id: str,
        stage_id: str,
        operator: Operator,
        state_backend: StateBackend,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.worker_id = worker_id
        self.stage_id = stage_id
        self.operator = operator
        self.state_backend = state_backend
        self.config = config or {}

        self.logger = logging.getLogger(f"Worker-{worker_id}")

        # State management
        self.state_manager = StateManager(
            stage_id=stage_id,
            state_backend=state_backend,
            worker_id=worker_id,
        )
        self.state_manager.activate_split(f"{stage_id}_bootstrap")

        # Metrics
        self.processed_count = 0
        self.processing_times: List[float] = []
        self.key_counts: Dict[str, int] = {}

        # Operator initialization
        context = OperatorContext(
            task_id=f"{stage_id}_{worker_id}",
            stage_id=stage_id,
            worker_id=worker_id,
            state_manager=self.state_manager,
        )
        self.operator.open(context)

        # Checkpoint state
        self.pending_checkpoint_id: Optional[str] = None
        self.checkpoint_frozen_state: Optional[Dict[str, Any]] = None

        self.logger.info(f"Worker {worker_id} initialized for stage {stage_id}")

    def process_batch(self, batch: Batch) -> Batch:
        """Process a batch of records"""
        start_time = time.time()

        try:
            split_id = batch.source_split or f"{self.stage_id}:{batch.batch_id}"
            extra_metadata: Dict[str, Any] = {}
            if isinstance(batch.metadata, dict):
                extra_metadata.update(batch.metadata)
            extra_metadata.setdefault("batch_id", batch.batch_id)
            extra_metadata.setdefault("stage_id", self.stage_id)

            self.state_manager.activate_split(
                split_id,
                metadata=extra_metadata,
            )

            # Process batch through operator
            output_batch = self.operator.process_batch(batch)

            # Update metrics
            self.processed_count += len(batch)
            self.processing_times.append(time.time() - start_time)

            # Track key distribution
            key_column_name = Batch.SOLSTICE_KEY_COLUMN
            if key_column_name in batch.column_names:
                for key in batch.column(key_column_name).to_pylist():
                    if key:
                        self.key_counts[key] = self.key_counts.get(key, 0) + 1
            else:
                # Fallback to materialized records when key column is unavailable
                for record in batch.to_records():
                    if record.key:
                        self.key_counts[record.key] = self.key_counts.get(record.key, 0) + 1

            # Keep only recent timing data
            if len(self.processing_times) > 100:
                self.processing_times = self.processing_times[-100:]

            self.logger.debug(
                f"Processed batch {batch.batch_id}: "
                f"{len(batch)} records -> {len(output_batch)} records"
            )

            return output_batch

        except Exception as e:
            self.logger.error(f"Error processing batch {batch.batch_id}: {e}", exc_info=True)
            raise

    def handle_barrier(self, checkpoint_id: str) -> None:
        """Handle a checkpoint barrier"""
        self.logger.info(f"Received checkpoint barrier: {checkpoint_id}")

        # Freeze current state
        self.pending_checkpoint_id = checkpoint_id
        self.checkpoint_frozen_state = {
            "worker_metrics": self.get_metrics(),
        }

    def create_checkpoint(self) -> Dict[str, Any]:
        """Create a checkpoint and return the handle"""
        if not self.pending_checkpoint_id:
            self.logger.warning("No pending checkpoint to create")
            return {}

        checkpoint_id = self.pending_checkpoint_id

        # Create checkpoint
        handles = self.state_manager.checkpoint(checkpoint_id, worker_id=self.worker_id)

        # Clear pending checkpoint
        self.pending_checkpoint_id = None
        self.checkpoint_frozen_state = None

        handle_dicts = [
            {
                "checkpoint_id": handle.checkpoint_id,
                "stage_id": handle.stage_id,
                "split_id": handle.split_id,
                "split_attempt": handle.split_attempt,
                "state_path": handle.state_path,
                "offset": handle.offset,
                "size_bytes": handle.size_bytes,
                "timestamp": handle.timestamp,
                "metadata": handle.metadata,
                "worker_id": handle.worker_id,
            }
            for handle in handles
            if handle is not None
        ]

        self.logger.info(
            "Created checkpoint %s with %d split handles", checkpoint_id, len(handle_dicts)
        )

        return {"handles": handle_dicts, "metrics": self.get_metrics()}

    def restore_from_checkpoint(
        self, checkpoint_id: str, handles: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """Restore state from a checkpoint"""
        self.logger.info(f"Restoring from checkpoint {checkpoint_id}")

        if not handles:
            self.logger.warning("No split handles provided for restore; skipping")
            return

        checkpoint_handles = [
            CheckpointHandle(
                checkpoint_id=handle.get("checkpoint_id", checkpoint_id),
                stage_id=handle["stage_id"],
                split_id=handle["split_id"],
                split_attempt=handle.get("split_attempt", 0),
                state_path=handle["state_path"],
                offset=handle.get("offset", {}),
                size_bytes=handle.get("size_bytes", 0),
                timestamp=handle.get("timestamp", time.time()),
                metadata=handle.get("metadata", {}),
                worker_id=handle.get("worker_id"),
            )
            for handle in handles
        ]

        self.state_manager.restore_many(checkpoint_handles)

        for handle in checkpoint_handles:
            self.state_manager.activate_split(
                handle.split_id,
                attempt=handle.split_attempt,
                metadata=handle.metadata,
            )
            operator_state = self.state_manager.get_operator_state()
            if operator_state:
                self.operator.restore(operator_state)

        self.logger.info(
            "Restored %d splits from checkpoint %s", len(checkpoint_handles), checkpoint_id
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get current worker metrics"""
        # Calculate processing rate
        if self.processing_times:
            avg_time = sum(self.processing_times) / len(self.processing_times)
            processing_rate = 1.0 / avg_time if avg_time > 0 else 0.0
        else:
            processing_rate = 0.0

        metrics = WorkerMetrics(
            worker_id=self.worker_id,
            stage_id=self.stage_id,
            processing_rate=processing_rate,
            backlog_size=0,  # Will be set by stage master
            key_distribution=self.key_counts.copy(),
        )

        return {
            "worker_id": metrics.worker_id,
            "stage_id": metrics.stage_id,
            "processing_rate": metrics.processing_rate,
            "backlog_size": metrics.backlog_size,
            "key_distribution": metrics.key_distribution,
            "cpu_usage": metrics.cpu_usage,
            "memory_usage": metrics.memory_usage,
            "timestamp": metrics.timestamp,
        }

    def get_key_distribution(self) -> Dict[str, int]:
        """Get the distribution of keys processed by this worker"""
        return self.key_counts.copy()

    def health_check(self) -> bool:
        """Health check"""
        return True

    def shutdown(self) -> None:
        """Gracefully shutdown the worker"""
        self.logger.info(f"Shutting down worker {self.worker_id}")

        try:
            self.operator.close()
        except Exception as e:
            self.logger.error(f"Error closing operator: {e}")

        self.state_manager.clear()
