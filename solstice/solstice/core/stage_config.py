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

"""Configuration and data classes for Stage Master v2.

This module contains:
- StageConfig: Configuration for stage execution
- FailurePolicy/FailureTracker: Worker fault tolerance
- QueueMessage: Inter-stage message format
- StageStatus: Stage runtime status
- QueueEndpoint: Queue connection info
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from solstice.queue import QueueType

if TYPE_CHECKING:
    pass


@dataclass
class StageConfig:
    """Configuration for Stage Master v2.

    Attributes:
        queue_type: Type of queue backend:
            - MEMORY: In-process only (single-worker testing)
            - RAY: Shared via Ray actor (distributed testing)
            - TANSU: Persistent broker (production)
        max_workers: Maximum number of workers
        min_workers: Minimum number of workers
        batch_size: Number of messages to fetch per batch
        commit_interval_ms: Interval between offset commits (ms) - legacy, not actively used
        commit_batch_size: Commit offset after every N messages processed.
            Lower values = better exactly-once guarantees but more overhead.
            Higher values = better throughput but larger duplicate window on crash.
            Default: 5 (balance between safety and performance)
        partition_count: Number of partitions for the output queue.
            If None, automatically set based on max_workers.
            For single worker, uses 1 partition. For multiple workers,
            uses min(max_workers, actual_worker_count) partitions.
        upstream_endpoint: Queue endpoint for upstream stage (None for source stages)
        upstream_topic: Topic name for upstream queue (None for source stages)
        state_endpoint: Queue endpoint for push-based state/metrics (WebUI)
        state_topic: Topic name for state messages (WebUI)
    """

    queue_type: QueueType = QueueType.TANSU  # Default to Tansu for persistence

    max_workers: int = 4
    min_workers: int = 1

    batch_size: int = 100
    commit_interval_ms: int = 5000
    commit_batch_size: int = 5  # Commit offset after every N messages for exactly-once

    # Partition configuration
    partition_count: Optional[int] = None  # None = auto based on workers

    # Backpressure thresholds
    backpressure_threshold_lag: int = 5000
    backpressure_threshold_queue_size: int = 1000

    # Worker resources
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory_mb: int = 0

    # Resource backoff configuration
    worker_ready_timeout_seconds: float = 30.0  # Max time to wait for worker to be ready
    worker_spawn_retry_delay_seconds: float = 2.0  # Delay between spawn retries

    # Upstream queue connection (set by runner for non-source stages)
    upstream_endpoint: Optional["QueueEndpoint"] = None
    upstream_topic: Optional[str] = None

    # Shared broker endpoint (set by runner, required for TANSU queue type)
    # All stages connect to this single broker instead of creating their own
    shared_broker_endpoint: Optional["QueueEndpoint"] = None

    # State push connection (for WebUI metrics)
    state_endpoint: Optional["QueueEndpoint"] = None
    state_topic: Optional[str] = None

    # Lineage tracking (for WebUI)
    lineage_sample_rate: float = 0.0  # 0=off, 1=full, 0.x=sampling

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "max_workers": self.max_workers,
            "min_workers": self.min_workers,
            "batch_size": self.batch_size,
            "commit_interval_ms": self.commit_interval_ms,
            "commit_batch_size": self.commit_batch_size,
            "partition_count": self.partition_count,
            "backpressure_threshold_lag": self.backpressure_threshold_lag,
            "backpressure_threshold_queue_size": self.backpressure_threshold_queue_size,
            "upstream_topic": self.upstream_topic,
            "state_topic": self.state_topic,
        }


@dataclass
class FailurePolicy:
    """Worker failure handling policy.

    Based on the "Circuit Breaker with Sliding Window" pattern:
    - Track failures within a time window (not cumulative)
    - Use failure rate relative to worker count
    - Apply exponential backoff for recovery attempts

    Theory:
    - Transient failures (network blips, GC pauses) should be tolerated
    - Sustained failures indicate systemic issues and should fail-fast
    - The sliding window prevents old failures from affecting current decisions
    """

    # Time window for failure rate calculation (seconds)
    # Failures older than this are forgotten
    window_seconds: float = 60.0

    # Maximum allowed failures per worker within the window
    # e.g., 2.0 means each worker can fail twice per minute on average
    max_failures_per_worker: float = 2.0

    # Minimum absolute failures before applying rate limit
    # Prevents failing too early when there are few workers
    min_failures_before_limit: int = 3

    # Base delay between recovery attempts (seconds)
    base_recovery_delay: float = 0.5

    # Maximum delay (exponential backoff cap)
    max_recovery_delay: float = 5.0


class FailureTracker:
    """Tracks worker failures and decides when to give up.

    Uses a sliding window approach to distinguish between:
    - Transient failures: Occasional failures that should be recovered
    - Sustained failures: High failure rate indicating systemic issues
    """

    def __init__(self, policy: FailurePolicy, logger: Any) -> None:
        self.policy = policy
        self.logger = logger
        self._failure_timestamps: List[float] = []
        self._recovery_attempt: int = 0
        self._peak_workers: int = 1  # Track highest worker count seen

    def record_failures(self, count: int, current_worker_count: int) -> None:
        """Record worker failures and prune old entries."""
        now = time.time()

        # Add new failures
        self._failure_timestamps.extend([now] * count)

        # Prune failures outside the window
        cutoff = now - self.policy.window_seconds
        self._failure_timestamps = [t for t in self._failure_timestamps if t > cutoff]

        self.logger.debug(
            f"Recorded {count} failures, {len(self._failure_timestamps)} in window, "
            f"{current_worker_count} workers active"
        )

    def record_success(self) -> None:
        """Record successful completion, reset backoff."""
        self._recovery_attempt = 0

    def should_give_up(self, current_worker_count: int) -> tuple[bool, str]:
        """Decide if we should stop trying to recover.

        Uses the higher of current workers or peak workers seen to avoid
        failing too early when many workers fail simultaneously.

        Returns:
            (should_give_up, reason)
        """
        failure_count = len(self._failure_timestamps)

        # Always allow some minimum failures before applying rate limit
        if failure_count < self.policy.min_failures_before_limit:
            return False, ""

        # Track peak worker count to handle simultaneous failures fairly
        # When all workers fail at once, we should still allow recovery attempts
        self._peak_workers = max(
            self._peak_workers,
            current_worker_count,
            failure_count,  # At least as many workers as failures seen
        )

        # Use peak workers for rate calculation
        effective_workers = max(1, self._peak_workers)
        max_allowed = self.policy.max_failures_per_worker * effective_workers

        if failure_count >= max_allowed:
            rate = failure_count / effective_workers
            return True, (
                f"Failure rate too high: {failure_count} failures / {effective_workers} workers "
                f"= {rate:.1f} per worker (limit: {self.policy.max_failures_per_worker})"
            )

        return False, ""

    def get_recovery_delay(self) -> float:
        """Get delay before next recovery attempt (exponential backoff)."""
        delay = self.policy.base_recovery_delay * (2**self._recovery_attempt)
        delay = float(min(delay, self.policy.max_recovery_delay))
        self._recovery_attempt += 1
        return delay

    def reset(self) -> None:
        """Reset tracker state."""
        self._failure_timestamps.clear()
        self._recovery_attempt = 0


class MessageType:
    """Message types for inter-stage communication."""

    DATA = "data"  # Normal data message
    EOF = "eof"  # End-of-stream marker - no more messages after this


@dataclass
class QueueMessage:
    """Message format for inter-stage communication.

    The actual data payload is stored in SplitPayloadStore,
    only the reference key is passed through the queue.

    Message types:
    - DATA: Normal data message with payload
    - EOF: End-of-stream marker, signals no more messages in this partition
    """

    message_id: str
    split_id: str
    payload_key: str  # Key to lookup SplitPayload in SplitPayloadStore
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    message_type: str = MessageType.DATA  # DATA or EOF

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "message_id": self.message_id,
                "split_id": self.split_id,
                "payload_key": self.payload_key,
                "metadata": self.metadata,
                "timestamp": self.timestamp,
                "message_type": self.message_type,
            }
        ).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueueMessage":
        d = json.loads(data.decode())
        # Handle backward compatibility - old messages without message_type
        if "message_type" not in d:
            d["message_type"] = MessageType.DATA
        return cls(**d)

    def is_eof(self) -> bool:
        """Check if this is an end-of-stream marker."""
        return self.message_type == MessageType.EOF

    @classmethod
    def create_eof(cls, partition: int) -> "QueueMessage":
        """Create an EOF marker message for a partition."""
        return cls(
            message_id=f"eof_partition_{partition}",
            split_id="",
            payload_key="",
            message_type=MessageType.EOF,
            metadata={"partition": partition},
        )


@dataclass
class StageStatus:
    """Status of a stage."""

    stage_id: str
    worker_count: int
    output_queue_size: int  # Real-time progress indicator (records in output queue)
    is_running: bool
    is_finished: bool
    failed: bool = False
    failure_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    backpressure_active: bool = False  # Backpressure status


@dataclass
class QueueEndpoint:
    """Queue connection info that can be serialized to workers.

    Workers use this to create their own queue connections.
    """

    queue_type: QueueType
    host: str = "localhost"
    port: int = 9092
    storage_url: str = "memory://"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_type": self.queue_type.value,
            "host": self.host,
            "port": self.port,
            "storage_url": self.storage_url,
        }


def create_queue_endpoint(
    queue_type: QueueType,
    host: str | None = None,
    port: int | None = None,
    storage_url: str | None = None,
) -> QueueEndpoint:
    """Factory to build a queue endpoint without scattering conditionals."""
    return QueueEndpoint(
        queue_type=queue_type,
        host=host or "localhost",
        port=port if port is not None else 9092,
        storage_url=storage_url or "memory://",
    )
