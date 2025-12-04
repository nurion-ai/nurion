"""State management and checkpoint system"""

from solstice.state.store import (
    CheckpointStore,
    LocalCheckpointStore,
    S3CheckpointStore,
    SlateDBCheckpointStore,
    CheckpointManifest,
    StageCheckpointData,
    SplitCheckpointData,
    create_checkpoint_store,
)
from solstice.state.checkpoint_manager import (
    CheckpointManager,
    StageCheckpointTracker,
)

__all__ = [
    # Store abstractions
    "CheckpointStore",
    "LocalCheckpointStore",
    "S3CheckpointStore",
    "SlateDBCheckpointStore",
    "create_checkpoint_store",
    # Data structures
    "CheckpointManifest",
    "StageCheckpointData",
    "SplitCheckpointData",
    # Manager
    "CheckpointManager",
    "StageCheckpointTracker",
]
