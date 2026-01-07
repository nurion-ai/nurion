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

"""State message definitions for push-based metrics.

All state changes and metrics are published as StateMessages to Tansu.
This enables event sourcing - replay messages to rebuild state.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class StateMessageType(str, Enum):
    """Types of state messages.

    Messages are categorized into:
    - Lifecycle events: Happen once (job/stage/worker start/stop)
    - Metrics: Periodic updates (throughput, counts, lag)
    - Events: Sporadic occurrences (exceptions, backpressure)
    """

    # Job lifecycle
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"

    # Stage lifecycle
    STAGE_STARTED = "stage_started"
    STAGE_COMPLETED = "stage_completed"

    # Worker lifecycle
    WORKER_STARTED = "worker_started"
    WORKER_STOPPED = "worker_stopped"

    # Metrics (periodic, rate-limited)
    STAGE_METRICS = "stage_metrics"
    WORKER_METRICS = "worker_metrics"

    # Events (sporadic)
    EXCEPTION = "exception"
    BACKPRESSURE = "backpressure"
    CHECKPOINT = "checkpoint"

    # Lineage
    SPLIT_PROCESSED = "split_processed"


@dataclass
class StateMessage:
    """Unified message for job state and metrics.

    All state changes and metrics are published as StateMessages.
    This enables event sourcing: replay messages to rebuild state.

    Attributes:
        message_type: Type of state update
        job_id: Job this message belongs to
        source_id: Entity that produced this message (job_id, stage_id, or worker_id)
        timestamp: When this message was created (Unix timestamp)
        payload: Type-specific data
        sequence: Optional sequence number for ordering (set by producer)
    """

    message_type: StateMessageType
    job_id: str
    source_id: str
    timestamp: float = field(default_factory=time.time)
    payload: Dict[str, Any] = field(default_factory=dict)
    sequence: Optional[int] = None

    def to_bytes(self) -> bytes:
        """Serialize to bytes for Tansu produce."""
        return json.dumps(
            {
                "message_type": self.message_type.value,
                "job_id": self.job_id,
                "source_id": self.source_id,
                "timestamp": self.timestamp,
                "payload": self.payload,
                "sequence": self.sequence,
            }
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> StateMessage:
        """Deserialize from Tansu consume."""
        d = json.loads(data.decode("utf-8"))
        return cls(
            message_type=StateMessageType(d["message_type"]),
            job_id=d["job_id"],
            source_id=d["source_id"],
            timestamp=d["timestamp"],
            payload=d.get("payload", {}),
            sequence=d.get("sequence"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "message_type": self.message_type.value,
            "job_id": self.job_id,
            "source_id": self.source_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
            "sequence": self.sequence,
        }


# =============================================================================
# Factory functions for common message types
# =============================================================================


def job_started_message(
    job_id: str,
    dag_edges: Dict[str, list],
    stages: list,
    config: Optional[Dict[str, Any]] = None,
) -> StateMessage:
    """Create a JOB_STARTED message.

    Args:
        job_id: Job identifier
        dag_edges: Pipeline DAG structure {stage_id: [upstream_stage_ids]}
        stages: List of stage info dicts
        config: Optional job configuration
    """
    return StateMessage(
        message_type=StateMessageType.JOB_STARTED,
        job_id=job_id,
        source_id=job_id,
        payload={
            "dag_edges": dag_edges,
            "stages": stages,
            "config": config or {},
        },
    )


def job_completed_message(
    job_id: str,
    status: str = "COMPLETED",
    duration_ms: Optional[int] = None,
    final_metrics: Optional[Dict[str, Any]] = None,
) -> StateMessage:
    """Create a JOB_COMPLETED or JOB_FAILED message."""
    msg_type = (
        StateMessageType.JOB_COMPLETED if status == "COMPLETED" else StateMessageType.JOB_FAILED
    )
    return StateMessage(
        message_type=msg_type,
        job_id=job_id,
        source_id=job_id,
        payload={
            "status": status,
            "duration_ms": duration_ms,
            "final_metrics": final_metrics or {},
        },
    )


def stage_started_message(
    job_id: str,
    stage_id: str,
    operator_type: str,
    min_parallelism: int,
    max_parallelism: int,
) -> StateMessage:
    """Create a STAGE_STARTED message."""
    return StateMessage(
        message_type=StateMessageType.STAGE_STARTED,
        job_id=job_id,
        source_id=stage_id,
        payload={
            "operator_type": operator_type,
            "min_parallelism": min_parallelism,
            "max_parallelism": max_parallelism,
        },
    )


def stage_metrics_message(
    job_id: str,
    stage_id: str,
    worker_count: int,
    input_records: int,
    output_records: int,
    input_throughput: float = 0.0,
    output_throughput: float = 0.0,
    queue_lag: int = 0,
    backpressure_active: bool = False,
    partition_metrics: Optional[Dict[int, Any]] = None,
) -> StateMessage:
    """Create a STAGE_METRICS message."""
    return StateMessage(
        message_type=StateMessageType.STAGE_METRICS,
        job_id=job_id,
        source_id=stage_id,
        payload={
            "worker_count": worker_count,
            "input_records": input_records,
            "output_records": output_records,
            "input_throughput": input_throughput,
            "output_throughput": output_throughput,
            "queue_lag": queue_lag,
            "backpressure_active": backpressure_active,
            "partition_metrics": partition_metrics or {},
        },
    )


def worker_started_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    assigned_partitions: Optional[list] = None,
) -> StateMessage:
    """Create a WORKER_STARTED message."""
    return StateMessage(
        message_type=StateMessageType.WORKER_STARTED,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "assigned_partitions": assigned_partitions or [],
        },
    )


def worker_stopped_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    reason: str = "completed",
    processed_count: int = 0,
    error_count: int = 0,
) -> StateMessage:
    """Create a WORKER_STOPPED message."""
    return StateMessage(
        message_type=StateMessageType.WORKER_STOPPED,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "reason": reason,
            "processed_count": processed_count,
            "error_count": error_count,
        },
    )


def worker_metrics_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    input_records: int,
    output_records: int,
    processing_time: float,
    processed_count: int = 0,
    assigned_partitions: Optional[list] = None,
    is_running: bool = True,
) -> StateMessage:
    """Create a WORKER_METRICS message."""
    return StateMessage(
        message_type=StateMessageType.WORKER_METRICS,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "input_records": input_records,
            "output_records": output_records,
            "processing_time": processing_time,
            "processed_count": processed_count,
            "assigned_partitions": assigned_partitions or [],
            "is_running": is_running,
        },
    )


def exception_message(
    job_id: str,
    stage_id: str,
    worker_id: Optional[str],
    exception_type: str,
    message: str,
    stacktrace: str,
    split_id: Optional[str] = None,
) -> StateMessage:
    """Create an EXCEPTION message."""
    return StateMessage(
        message_type=StateMessageType.EXCEPTION,
        job_id=job_id,
        source_id=worker_id or stage_id,
        payload={
            "stage_id": stage_id,
            "worker_id": worker_id,
            "exception_type": exception_type,
            "message": message,
            "stacktrace": stacktrace,
            "split_id": split_id,
        },
    )


def backpressure_message(
    job_id: str,
    stage_id: str,
    active: bool,
    queue_lag: int = 0,
    slow_down_factor: float = 1.0,
) -> StateMessage:
    """Create a BACKPRESSURE message."""
    return StateMessage(
        message_type=StateMessageType.BACKPRESSURE,
        job_id=job_id,
        source_id=stage_id,
        payload={
            "active": active,
            "queue_lag": queue_lag,
            "slow_down_factor": slow_down_factor,
        },
    )


def split_processed_message(
    job_id: str,
    stage_id: str,
    worker_id: str,
    split_id: str,
    parent_split_ids: list,
    input_records: int,
    output_records: int,
    processing_time: float,
) -> StateMessage:
    """Create a SPLIT_PROCESSED message for lineage tracking."""
    return StateMessage(
        message_type=StateMessageType.SPLIT_PROCESSED,
        job_id=job_id,
        source_id=worker_id,
        payload={
            "stage_id": stage_id,
            "split_id": split_id,
            "parent_split_ids": parent_split_ids,
            "input_records": input_records,
            "output_records": output_records,
            "processing_time": processing_time,
        },
    )
