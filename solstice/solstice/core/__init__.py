"""Core components of the streaming framework.

V2 Architecture (Recommended):
- StageMasterV2: Simplified master with queue-based output
- StageWorkerV2: Self-scheduling workers pulling from upstream
- StageConfigV2: Configuration for v2 stages

Legacy (Deprecated):
- StageMasterActor: Original master with complex scheduling
- StageWorker: Original worker with push-based flow
"""

from solstice.core.job import Job
from solstice.core.operator import Operator, OperatorConfig
from solstice.core.stage import Stage
from solstice.core.models import JobCheckpointConfig

# V2 Architecture (recommended)
from solstice.core.stage_master_v2 import (
    StageMasterV2,
    StageWorkerV2,
    StageConfigV2,
    QueueType,
    QueueMessage,
    QueueEndpoint,
)

# Legacy (deprecated - will be removed in future version)
from solstice.core.stage_master import StageMasterActor, StageMasterConfig, DefaultStageMasterConfig
from solstice.core.worker import StageWorker

__all__ = [
    # Core
    "Job",
    "Stage",
    "Operator",
    "OperatorConfig",
    "JobCheckpointConfig",
    
    # V2 Architecture (recommended)
    "StageMasterV2",
    "StageWorkerV2",
    "StageConfigV2",
    "QueueType",
    "QueueMessage",
    "QueueEndpoint",
    
    # Legacy (deprecated)
    "StageMasterActor",
    "StageMasterConfig",
    "DefaultStageMasterConfig",
    "StageWorker",
]
