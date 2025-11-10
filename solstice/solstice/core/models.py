"""Core data models for the streaming framework"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ShardStatus(str, Enum):
    """Status of a shard"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class CheckpointStatus(str, Enum):
    """Status of a checkpoint"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Shard:
    """Represents a data shard for processing"""

    shard_id: str
    data_range: Dict[str, Any]  # Can contain offset, file path, key range, etc.
    worker_id: Optional[str] = None
    status: ShardStatus = ShardStatus.PENDING
    retry_count: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkerMetrics:
    """Metrics reported by a worker"""

    worker_id: str
    stage_id: str
    processing_rate: float  # records/sec
    backlog_size: int
    key_distribution: Dict[str, int] = field(default_factory=dict)
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    timestamp: float = field(default_factory=time.time)


@dataclass
class CheckpointHandle:
    """Handle to a checkpoint stored remotely"""

    checkpoint_id: str
    stage_id: str
    worker_id: str
    state_path: str  # S3/DFS path
    offset: Dict[str, Any]
    size_bytes: int
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Barrier:
    """Checkpoint barrier marker"""

    barrier_id: str
    checkpoint_id: str
    stage_id: str
    timestamp: float = field(default_factory=time.time)
    upstream_stages: List[str] = field(default_factory=list)
    downstream_stages: List[str] = field(default_factory=list)


@dataclass
class BackpressureSignal:
    """Signal for backpressure propagation"""

    from_stage: str
    to_stage: str
    slow_down_factor: float  # 0.0 to 1.0, where 0.0 means pause
    reason: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class Record:
    """A single record flowing through the pipeline"""

    key: Optional[str] = None
    value: Any = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Batch:
    """A batch of records"""

    records: List[Record]
    batch_id: str
    source_shard: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def __len__(self):
        return len(self.records)
