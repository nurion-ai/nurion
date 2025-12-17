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

"""Base operator interface with EasyConfig pattern"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar, TYPE_CHECKING
import logging

from solstice.core.models import SplitPayload, Split

if TYPE_CHECKING:
    from solstice.core.stage_master import StageMaster


T = TypeVar("T", bound="Operator")


@dataclass
class OperatorConfig(ABC):
    """Base configuration class for operators.

    Subclasses should define their configuration fields as dataclass fields,
    and set the `operator_class` class variable to the corresponding operator class.

    Example:
        @dataclass
        class MyOperatorConfig(OperatorConfig):
            operator_class = MyOperator

            param1: str
            param2: int = 10

        # Usage:
        config = MyOperatorConfig(param1="value")
        operator = config.setup(worker_id="worker_0")
    """

    operator_class: ClassVar[Type["Operator"]]
    master_class: ClassVar[Type["StageMaster"]]

    def setup(self, worker_id: Optional[str] = None) -> "Operator":
        """Create and return an operator instance with this configuration.

        Args:
            worker_id: Optional worker ID to pass to the operator

        Returns:
            Configured operator instance
        """
        return self.operator_class(config=self, worker_id=worker_id)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary representation."""
        result = {}
        for f in fields(self):
            value = getattr(self, f.name)
            # Handle nested configs
            if isinstance(value, OperatorConfig):
                result[f.name] = value.to_dict()
            else:
                result[f.name] = value
        return result


class Operator(ABC):
    """Base class for all operators"""

    def __init__(
        self,
        config: OperatorConfig,
        worker_id: Optional[str] = None,
    ):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.worker_id = worker_id

    @abstractmethod
    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        pass

    def close(self) -> None:
        """Clean up operator resources"""
        pass


class SourceOperator(Operator):
    """Base class for source operators that read data from external systems.

    Source operators maintain offset tracking for checkpoint/resume capability.
    Subclasses should update the offset after reading data using `update_offset()`.
    """

    def __init__(
        self,
        config: OperatorConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        # Offset tracking for checkpoint/resume
        self._current_offset: Dict[str, Any] = {}

    @abstractmethod
    def read(self, split: Split) -> Optional[SplitPayload]:
        """Read data for a specific split.

        Args:
            split: Split object containing all metadata needed to read data
                  (data_range, metadata, etc.)

        Returns:
            SplitPayload containing the data, or None if no data available

        Note:
            Implementations should call `update_offset()` after successful reads
            to enable checkpoint/resume functionality.
        """
        pass

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split for source operators.

        For source operators, payload is None and split contains all metadata.
        This method calls read() with the split.
        """
        if payload is not None:
            raise ValueError("Source operators should not receive payload, only split")

        return self.read(split)

    def update_offset(self, offset: Dict[str, Any]) -> None:
        """Update the current read offset.

        Called by subclasses after successfully reading data.
        The offset is persisted during checkpoints for resume capability.

        Args:
            offset: Dictionary containing offset information (e.g., file position,
                   partition offset, row number, etc.)
        """
        self._current_offset.update(offset)

    def get_offset(self) -> Dict[str, Any]:
        """Get the current read offset for checkpointing.

        Returns:
            Dictionary containing the current offset state
        """
        return dict(self._current_offset)

    def restore_offset(self, offset: Dict[str, Any]) -> None:
        """Restore offset from a checkpoint.

        Called during job recovery to resume from a previous position.

        Args:
            offset: Dictionary containing offset information from checkpoint
        """
        self._current_offset = dict(offset)
        self.logger.info(f"Restored offset: {offset}")


class SinkOperator(Operator):
    """Base class for sink operators with exactly-once semantics support.

    Sink operators can implement two-phase commit for exactly-once guarantees:
    1. `process_split()` - Buffer/stage writes (pre-commit)
    2. `prepare_commit()` - Prepare for commit (optional)
    3. `commit()` - Finalize writes
    4. `rollback()` - Rollback uncommitted writes on failure

    For simpler at-least-once semantics, just implement `process_split()`.
    """

    def __init__(
        self,
        config: OperatorConfig,
        worker_id: Optional[str] = None,
    ):
        super().__init__(config, worker_id)
        # Track pending writes for exactly-once
        self._pending_commit_id: Optional[str] = None
        self._commit_offset: Dict[str, Any] = {}

    def prepare_commit(self, checkpoint_id: str) -> bool:
        """Prepare for commit (phase 1 of two-phase commit).

        Called before checkpoint finalization. Implementations should
        flush any buffered data and prepare for commit.

        Args:
            checkpoint_id: The checkpoint ID this commit is associated with

        Returns:
            True if prepare succeeded, False otherwise
        """
        self._pending_commit_id = checkpoint_id
        return True

    def commit(self, checkpoint_id: str) -> bool:
        """Commit pending writes (phase 2 of two-phase commit).

        Called after checkpoint is successfully finalized.
        Implementations should finalize any staged writes.

        Args:
            checkpoint_id: The checkpoint ID to commit

        Returns:
            True if commit succeeded, False otherwise
        """
        if self._pending_commit_id == checkpoint_id:
            self._pending_commit_id = None
            return True
        return False

    def rollback(self, checkpoint_id: str) -> bool:
        """Rollback uncommitted writes.

        Called when checkpoint fails or job restarts.
        Implementations should discard any uncommitted staged writes.

        Args:
            checkpoint_id: The checkpoint ID to rollback

        Returns:
            True if rollback succeeded, False otherwise
        """
        if self._pending_commit_id == checkpoint_id:
            self._pending_commit_id = None
        return True

    def get_commit_offset(self) -> Dict[str, Any]:
        """Get the current commit offset for checkpointing.

        Returns:
            Dictionary containing commit state information
        """
        return dict(self._commit_offset)

    def restore_commit_offset(self, offset: Dict[str, Any]) -> None:
        """Restore commit offset from a checkpoint.

        Called during job recovery.

        Args:
            offset: Dictionary containing commit offset from checkpoint
        """
        self._commit_offset = dict(offset)
        self.logger.info(f"Restored commit offset: {offset}")
