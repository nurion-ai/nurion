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

"""Data models for WebUI."""

from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Literal, Optional, TypeVar


# === Paged Response ===

T = TypeVar("T")


@dataclass
class PagedResponse(Generic[T]):
    """Standard paged response for large datasets."""

    items: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int

    @property
    def has_next(self) -> bool:
        return self.page < self.total_pages

    @property
    def has_prev(self) -> bool:
        return self.page > 1


# === Job Models ===


@dataclass
class JobArchive:
    """Complete job archive for History Server."""

    job_id: str
    status: Literal["COMPLETED", "FAILED", "CANCELLED"]
    start_time: float
    end_time: float
    duration_ms: int

    # Configuration
    config: Dict[str, Any]

    # Structure
    stages: List[Dict[str, Any]]
    dag_edges: Dict[str, List[str]]

    # Final metrics
    final_metrics: Dict[str, Any]

    # Summary counts
    total_input_records: int = 0
    total_output_records: int = 0

    def to_json(self) -> str:
        """Serialize to JSON."""
        import json

        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data: str) -> "JobArchive":
        """Deserialize from JSON."""
        import json

        return cls(**json.loads(data))


@dataclass
class JobDetail:
    """Detailed job information for WebUI."""

    job_id: str
    status: Literal["RUNNING", "COMPLETED", "FAILED", "CANCELLED"]
    start_time: float
    end_time: Optional[float]
    duration_ms: int

    # Stage DAG
    stages: List[Dict[str, Any]]
    dag_edges: Dict[str, List[str]]

    # Progress
    total_splits: int
    completed_splits: int
    progress_percent: float

    # Throughput and ETA
    input_throughput: float  # records/s
    output_throughput: float  # records/s
    eta_seconds: Optional[float]


# === Stage Models ===


@dataclass
class RateSample:
    """Time-series sample for rate metrics."""

    timestamp: float
    rate: float


@dataclass
class PartitionDetail:
    """Partition-level metrics."""

    partition_id: int
    latest_offset: int
    committed_offset: int
    lag: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "latest_offset": self.latest_offset,
            "committed_offset": self.committed_offset,
            "lag": self.lag,
        }


@dataclass
class StageDetail:
    """Detailed stage information."""

    stage_id: str
    operator_type: str

    # Parallelism
    current_parallelism: int
    min_parallelism: int
    max_parallelism: int

    # Progress
    input_records: int
    output_records: int
    selectivity: float  # output/input ratio

    # Queue status
    input_queue_lag: int
    output_queue_size: int

    # Rate history (for charts)
    produce_rate_history: List[RateSample]
    consume_rate_history: List[RateSample]

    # Partition metrics
    partition_metrics: List[PartitionDetail]
    skew_detected: bool
    skew_ratio: float

    # Backpressure
    backpressure_active: bool
    backpressure_ratio: float

    # Workers
    workers: List[Dict[str, Any]]


# === Worker Models ===


@dataclass
class WorkerDetail:
    """Detailed worker information."""

    worker_id: str
    stage_id: str
    actor_id: str
    node_id: str
    pid: int

    # Resource usage
    cpu_percent: float
    memory_mb: int
    memory_percent: float
    gpu_memory_mb: Optional[int]
    gpu_utilization: Optional[float]

    # Network IO
    network_recv_bytes: int
    network_send_bytes: int

    # Processing stats
    records_processed: int
    records_per_second: float
    avg_processing_time_ms: float

    # Status
    status: Literal["RUNNING", "IDLE", "BLOCKED", "DEAD"]
    assigned_partitions: List[int]

    # Links
    ray_dashboard_url: str
    log_url: str
    stacktrace_url: str


# === Exception Models ===


@dataclass
class ExceptionInfo:
    """Exception information."""

    exception_id: str
    timestamp: float
    exception_type: str
    message: str
    stacktrace: str

    # Source
    job_id: str
    stage_id: str
    worker_id: Optional[str]
    split_id: Optional[str]

    # Aggregation info
    occurrence_count: int
    first_seen: float
    last_seen: float

    def to_json(self) -> str:
        import json

        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data: str) -> "ExceptionInfo":
        import json

        return cls(**json.loads(data))


# === Timeline Models ===


@dataclass
class TimelineEvent:
    """Timeline event for visualization."""

    timestamp: float
    event_type: str
    stage_id: Optional[str]
    worker_id: Optional[str]
    description: str
    duration_ms: Optional[int]
    metadata: Dict[str, Any] = field(default_factory=dict)


# === Checkpoint Models ===


@dataclass
class CheckpointInfo:
    """Checkpoint information."""

    checkpoint_id: str
    trigger_time: float
    completion_time: Optional[float]
    duration_ms: Optional[int]
    status: Literal["IN_PROGRESS", "COMPLETED", "FAILED"]

    # Size
    state_size_bytes: int

    # Per-stage checkpoints
    stage_checkpoints: Dict[str, Dict[str, Any]]

    def to_json(self) -> str:
        import json

        return json.dumps(self.__dict__)


# === Backpressure Models ===


@dataclass
class BackpressureSample:
    """Backpressure sample for time-series."""

    timestamp: float
    ratio: float
    active: bool


@dataclass
class BackpressureStatus:
    """Backpressure status for a stage."""

    stage_id: str
    status: Literal["OK", "LOW", "HIGH"]
    ratio: float

    # History
    history: List[BackpressureSample]

    # Analysis
    bottleneck_stage: Optional[str]
    suggested_action: Optional[str]
