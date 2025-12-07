"""Core components of the streaming framework.

Components:
- StageMaster: Queue-based master managing workers
- StageWorker: Self-scheduling workers pulling from upstream
- StageConfig: Configuration for stages
- QueueType/QueueMessage/QueueEndpoint: Queue communication types
"""

from solstice.core.job import Job
from solstice.core.operator import Operator, OperatorConfig
from solstice.core.stage import Stage
from solstice.core.models import JobCheckpointConfig

from solstice.core.stage_master import (
    StageMaster,
    StageWorker,
    StageConfig,
    QueueType,
    QueueMessage,
    QueueEndpoint,
)

__all__ = [
    # Core
    "Job",
    "Stage",
    "Operator",
    "OperatorConfig",
    "JobCheckpointConfig",
    
    # Stage Architecture
    "StageMaster",
    "StageWorker",
    "StageConfig",
    "QueueType",
    "QueueMessage",
    "QueueEndpoint",
]
