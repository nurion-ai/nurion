"""State management for workers"""

import logging
from typing import Any, Dict, Optional

from solstice.state.backend import StateBackend
from solstice.core.models import CheckpointHandle


class StateManager:
    """Manages state for a worker"""

    def __init__(
        self,
        worker_id: str,
        stage_id: str,
        state_backend: StateBackend,
    ):
        self.worker_id = worker_id
        self.stage_id = stage_id
        self.state_backend = state_backend
        self.logger = logging.getLogger(self.__class__.__name__)

        # In-memory state
        self.keyed_state: Dict[str, Dict[str, Any]] = {}  # key -> state dict
        self.operator_state: Dict[str, Any] = {}
        self.offset: Dict[str, Any] = {}

        # Checkpoint tracking
        self.last_checkpoint_state: Optional[Dict[str, Any]] = None

    def get_keyed_state(self, key: str) -> Dict[str, Any]:
        """Get state for a specific key"""
        if key not in self.keyed_state:
            self.keyed_state[key] = {}
        return self.keyed_state[key]

    def update_keyed_state(self, key: str, state: Dict[str, Any]) -> None:
        """Update state for a specific key"""
        self.keyed_state[key] = state

    def get_operator_state(self) -> Dict[str, Any]:
        """Get operator-level state"""
        return self.operator_state

    def update_operator_state(self, state: Dict[str, Any]) -> None:
        """Update operator-level state"""
        self.operator_state.update(state)

    def update_offset(self, offset: Dict[str, Any]) -> None:
        """Update processing offset"""
        self.offset.update(offset)

    def checkpoint(self, checkpoint_id: str) -> CheckpointHandle:
        """Create a checkpoint of current state"""
        # Collect all state
        state = {
            "keyed_state": self.keyed_state,
            "operator_state": self.operator_state,
            "offset": self.offset,
            "worker_id": self.worker_id,
            "stage_id": self.stage_id,
        }

        # Calculate delta from last checkpoint
        if self.last_checkpoint_state is not None:
            # Only save changed keys for efficiency
            delta_keyed_state = {
                key: value
                for key, value in self.keyed_state.items()
                if key not in self.last_checkpoint_state.get("keyed_state", {})
                or value != self.last_checkpoint_state["keyed_state"][key]
            }
            state["keyed_state"] = delta_keyed_state
            state["is_delta"] = True
        else:
            state["is_delta"] = False

        # Save to remote storage
        state_path = (
            f"{self.stage_id}/checkpoints/{checkpoint_id}/worker_{self.worker_id}_state.pkl"
        )
        self.state_backend.save_state(state_path, state)

        # Calculate size
        import pickle

        size_bytes = len(pickle.dumps(state))

        # Update last checkpoint
        self.last_checkpoint_state = {
            "keyed_state": self.keyed_state.copy(),
            "operator_state": self.operator_state.copy(),
            "offset": self.offset.copy(),
        }

        handle = CheckpointHandle(
            checkpoint_id=checkpoint_id,
            stage_id=self.stage_id,
            worker_id=self.worker_id,
            state_path=state_path,
            offset=self.offset.copy(),
            size_bytes=size_bytes,
        )

        self.logger.info(
            f"Created checkpoint {checkpoint_id} for worker {self.worker_id}, "
            f"size: {size_bytes} bytes"
        )

        return handle

    def restore(self, checkpoint_id: str) -> None:
        """Restore state from a checkpoint"""
        state_path = (
            f"{self.stage_id}/checkpoints/{checkpoint_id}/worker_{self.worker_id}_state.pkl"
        )

        if not self.state_backend.exists(state_path):
            self.logger.warning(
                f"Checkpoint state not found: {state_path}, starting with empty state"
            )
            return

        state = self.state_backend.load_state(state_path)

        # Handle delta checkpoints
        if state.get("is_delta", False):
            # Merge with existing state
            self.keyed_state.update(state.get("keyed_state", {}))
        else:
            # Full restore
            self.keyed_state = state.get("keyed_state", {})

        self.operator_state = state.get("operator_state", {})
        self.offset = state.get("offset", {})

        self.last_checkpoint_state = {
            "keyed_state": self.keyed_state.copy(),
            "operator_state": self.operator_state.copy(),
            "offset": self.offset.copy(),
        }

        self.logger.info(
            f"Restored state from checkpoint {checkpoint_id} for worker {self.worker_id}"
        )

    def clear(self) -> None:
        """Clear all state"""
        self.keyed_state.clear()
        self.operator_state.clear()
        self.offset.clear()
        self.last_checkpoint_state = None
