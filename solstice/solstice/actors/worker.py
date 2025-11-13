"""Worker actor for processing data"""

import time
import logging
from typing import Any, Dict, List, Optional
import ray  # type: ignore[import]

from solstice.core.operator import Operator, OperatorContext
from solstice.core.models import Batch, WorkerMetrics
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
            worker_id=worker_id,
            stage_id=stage_id,
            state_backend=state_backend,
        )

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
            # Process batch through operator
            output_batch = self.operator.process_batch(batch)

            # Update metrics
            self.processed_count += len(batch)
            self.processing_times.append(time.time() - start_time)

            # Track key distribution
            for record in batch.records:
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
            "operator_state": self.operator.checkpoint(),
            "worker_metrics": self.get_metrics(),
        }

    def create_checkpoint(self) -> Dict[str, Any]:
        """Create a checkpoint and return the handle"""
        if not self.pending_checkpoint_id:
            self.logger.warning("No pending checkpoint to create")
            return {}

        checkpoint_id = self.pending_checkpoint_id

        # Update state manager with operator state
        if self.checkpoint_frozen_state:
            self.state_manager.update_operator_state(self.checkpoint_frozen_state["operator_state"])

        # Create checkpoint
        handle = self.state_manager.checkpoint(checkpoint_id)

        # Clear pending checkpoint
        self.pending_checkpoint_id = None
        self.checkpoint_frozen_state = None

        self.logger.info(f"Created checkpoint {checkpoint_id}")

        # Return handle as dict for serialization
        return {
            "checkpoint_id": handle.checkpoint_id,
            "stage_id": handle.stage_id,
            "worker_id": handle.worker_id,
            "state_path": handle.state_path,
            "offset": handle.offset,
            "size_bytes": handle.size_bytes,
            "timestamp": handle.timestamp,
            "metadata": handle.metadata,
        }

    def restore_from_checkpoint(self, checkpoint_id: str) -> None:
        """Restore state from a checkpoint"""
        self.logger.info(f"Restoring from checkpoint {checkpoint_id}")

        # Restore state
        self.state_manager.restore(checkpoint_id)

        # Restore operator state
        operator_state = self.state_manager.get_operator_state()
        if operator_state:
            self.operator.restore(operator_state)

        self.logger.info(f"Restored from checkpoint {checkpoint_id}")

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
