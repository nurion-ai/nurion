"""Split-scoped state management."""

import copy
import logging
import pickle
from typing import Any, Dict, Iterable, List, Optional

from solstice.core.models import CheckpointHandle
from solstice.state.backend import StateBackend


class StateManager:
    """Manage operator and keyed state at split granularity."""

    def __init__(
        self,
        stage_id: str,
        state_backend: StateBackend,
        *,
        worker_id: Optional[str] = None,
    ):
        """Create a state manager bound to a specific stage."""
        self.stage_id = stage_id
        self.state_backend = state_backend
        self.worker_id = worker_id
        self.logger = logging.getLogger(self.__class__.__name__)

        # Split-scoped state
        self._active_split_id: Optional[str] = None
        self._split_operator_state: Dict[str, Dict[str, Any]] = {}
        self._split_keyed_state: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._split_offsets: Dict[str, Dict[str, Any]] = {}
        self._split_attempts: Dict[str, int] = {}
        self._split_metadata: Dict[str, Dict[str, Any]] = {}

        # Last snapshot for delta calculations (future use)
        self._last_checkpoint_state: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Split lifecycle
    # ------------------------------------------------------------------
    def activate_split(
        self,
        split_id: str,
        *,
        attempt: int = 0,
        parents: Optional[Iterable[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Set the active split context for subsequent state operations."""
        self._active_split_id = split_id
        self._split_operator_state.setdefault(split_id, {})
        self._split_keyed_state.setdefault(split_id, {})
        self._split_offsets.setdefault(split_id, {})
        self._split_attempts.setdefault(split_id, attempt)

        if metadata:
            self._split_metadata.setdefault(split_id, {}).update(metadata)
        if parents:
            parent_list = list(parents)
            self._split_metadata.setdefault(split_id, {}).setdefault("parents", parent_list)

    def clear_split(self, split_id: str) -> None:
        """Release all state associated with a split."""
        self._split_operator_state.pop(split_id, None)
        self._split_keyed_state.pop(split_id, None)
        self._split_offsets.pop(split_id, None)
        self._split_attempts.pop(split_id, None)
        self._split_metadata.pop(split_id, None)
        self._last_checkpoint_state.pop(split_id, None)
        if self._active_split_id == split_id:
            self._active_split_id = None

    def active_splits(self) -> List[str]:
        """Return the list of splits with in-memory state."""
        splits = set(self._split_operator_state.keys())
        splits.update(self._split_keyed_state.keys())
        splits.update(self._split_offsets.keys())
        return sorted(splits)

    # ------------------------------------------------------------------
    # State accessors/mutators
    # ------------------------------------------------------------------
    def _ensure_active_split(self) -> str:
        if not self._active_split_id:
            raise RuntimeError("No active split is set for state operations")
        return self._active_split_id

    def get_keyed_state(self, key: str) -> Dict[str, Any]:
        """Get state for a specific key within the active split."""
        split_id = self._ensure_active_split()
        keyed = self._split_keyed_state.setdefault(split_id, {})
        if key not in keyed:
            keyed[key] = {}
        return keyed[key]

    def update_keyed_state(self, key: str, state: Dict[str, Any]) -> None:
        """Update state for a specific key within the active split."""
        split_id = self._ensure_active_split()
        self._split_keyed_state.setdefault(split_id, {})[key] = dict(state)

    def get_operator_state(self) -> Dict[str, Any]:
        """Retrieve operator-level state for the active split."""
        split_id = self._ensure_active_split()
        return self._split_operator_state.setdefault(split_id, {}).copy()

    def update_operator_state(self, state: Dict[str, Any]) -> None:
        """Merge operator-level state for the active split."""
        split_id = self._ensure_active_split()
        bucket = self._split_operator_state.setdefault(split_id, {})
        bucket.update(state)

    def update_offset(self, offset: Dict[str, Any]) -> None:
        """Update processing offset for the active split."""
        split_id = self._ensure_active_split()
        bucket = self._split_offsets.setdefault(split_id, {})
        bucket.update(offset)

    def get_offset(self) -> Dict[str, Any]:
        """Return a copy of offsets for the active split."""
        split_id = self._ensure_active_split()
        return self._split_offsets.setdefault(split_id, {}).copy()

    # ------------------------------------------------------------------
    # Checkpoint / Restore
    # ------------------------------------------------------------------
    def checkpoint(
        self,
        checkpoint_id: str,
        *,
        worker_id: Optional[str] = None,
    ) -> List[CheckpointHandle]:
        """Persist state for all known splits to the backend."""
        handles: List[CheckpointHandle] = []
        for split_id in self.active_splits():
            handle = self._checkpoint_split(
                split_id,
                checkpoint_id,
                worker_id=worker_id,
            )
            if handle:
                handles.append(handle)
        return handles

    def _checkpoint_split(
        self,
        split_id: str,
        checkpoint_id: str,
        *,
        worker_id: Optional[str] = None,
    ) -> Optional[CheckpointHandle]:
        operator_state = copy.deepcopy(self._split_operator_state.get(split_id, {}))
        keyed_state = copy.deepcopy(self._split_keyed_state.get(split_id, {}))
        offsets = copy.deepcopy(self._split_offsets.get(split_id, {}))
        metadata = copy.deepcopy(self._split_metadata.get(split_id, {}))

        if not operator_state and not keyed_state and not offsets and not metadata:
            # No meaningful state to persist
            return None

        state_payload = {
            "stage_id": self.stage_id,
            "split_id": split_id,
            "attempt": self._split_attempts.get(split_id, 0),
            "operator_state": operator_state,
            "keyed_state": keyed_state,
            "offset": offsets,
            "metadata": metadata,
        }

        state_path = f"{self.stage_id}/splits/{split_id}/checkpoints/{checkpoint_id}.pkl"
        self.state_backend.save_state(state_path, state_payload)

        size_bytes = len(pickle.dumps(state_payload))

        self._last_checkpoint_state[split_id] = {
            "operator_state": operator_state,
            "keyed_state": keyed_state,
            "offset": offsets,
            "metadata": metadata,
        }

        handle = CheckpointHandle(
            checkpoint_id=checkpoint_id,
            stage_id=self.stage_id,
            split_id=split_id,
            split_attempt=self._split_attempts.get(split_id, 0),
            state_path=state_path,
            offset=offsets,
            size_bytes=size_bytes,
            metadata=metadata,
            worker_id=worker_id or self.worker_id,
        )

        self.logger.info(
            "Checkpointed split %s (checkpoint=%s, size=%d bytes)",
            split_id,
            checkpoint_id,
            size_bytes,
        )

        return handle

    def restore_split(
        self,
        split_id: str,
        checkpoint_id: str,
        *,
        state_path: Optional[str] = None,
    ) -> bool:
        """Restore a specific split from the backend."""
        resolved_path = state_path or f"{self.stage_id}/splits/{split_id}/checkpoints/{checkpoint_id}.pkl"

        if not self.state_backend.exists(resolved_path):
            self.logger.warning("Split state not found: %s", resolved_path)
            return False

        state = self.state_backend.load_state(resolved_path)

        self._split_operator_state[split_id] = state.get("operator_state", {})
        self._split_keyed_state[split_id] = state.get("keyed_state", {})
        self._split_offsets[split_id] = state.get("offset", {})
        self._split_metadata[split_id] = state.get("metadata", {})
        self._split_attempts[split_id] = state.get("attempt", state.get("split_attempt", 0))

        self._last_checkpoint_state[split_id] = {
            "operator_state": copy.deepcopy(self._split_operator_state[split_id]),
            "keyed_state": copy.deepcopy(self._split_keyed_state[split_id]),
            "offset": copy.deepcopy(self._split_offsets[split_id]),
            "metadata": copy.deepcopy(self._split_metadata[split_id]),
        }

        # Make the restored split active by default for backwards compatibility.
        self._active_split_id = split_id

        self.logger.info(
            "Restored split %s from checkpoint %s",
            split_id,
            checkpoint_id,
        )
        return True

    def restore_many(self, handles: Iterable[CheckpointHandle]) -> None:
        """Restore multiple splits based on checkpoint handles."""
        for handle in handles:
            self.restore_split(handle.split_id, handle.checkpoint_id, state_path=handle.state_path)

    def clear(self) -> None:
        """Clear all managed state."""
        for split in list(self.active_splits()):
            self.clear_split(split)
