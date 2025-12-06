"""Output buffer for Pull-based data flow between stages.

.. deprecated::
    This module is deprecated. Use :mod:`solstice.queue` backends instead.
    
    The queue module provides:
    - QueueBackend: Abstract interface for message queues
    - MemoryBackend: Fast in-memory queue
    - RayBackend: Distributed queue via Ray actor
    - TansuBackend: Persistent queue with Tansu broker
    
    Migration:
        # Old (deprecated)
        from solstice.core.output_buffer import OutputBuffer
        
        # New (recommended)
        from solstice.queue import MemoryBackend, RayBackend
"""

import warnings

warnings.warn(
    "solstice.core.output_buffer is deprecated. Use solstice.queue backends instead.",
    DeprecationWarning,
    stacklevel=2,
)

import threading
from typing import Dict, List, Tuple

from solstice.core.models import Split
from solstice.utils.logging import create_ray_logger


class OutputBuffer:
    """Thread-safe output buffer for cursor-based reads by downstream consumers.

    Thread-safety is required because Ray actors with max_concurrency > 1
    can have concurrent fetch_splits calls from multiple downstream stages.
    """

    def __init__(self, stage_id: str, max_size: int = 1000, max_consumer_lag: int = 500):
        self.stage_id = stage_id
        self.max_size = max_size
        self.max_consumer_lag = max_consumer_lag
        self.logger = create_ray_logger(f"OutputBuffer-{stage_id}")

        self._lock = threading.Lock()
        self._buffer: List[Tuple[int, Split]] = []  # (sequence_num, split)
        self._sequence_counter = 0
        self._consumer_cursors: Dict[str, int] = {}  # consumer_id -> cursor
        self._closed = False
        self._upstream_finished = False

    def append(self, split: Split) -> bool:
        """Append a split to the buffer. Returns False if buffer is full."""
        with self._lock:
            if self._closed or len(self._buffer) >= self.max_size:
                return False

            self._sequence_counter += 1
            self._buffer.append((self._sequence_counter, split))
            return True

    def fetch(
        self, consumer_id: str, cursor: int = 0, max_splits: int = 100
    ) -> Tuple[List[Split], int]:
        """Fetch splits after the given cursor. Returns (splits, new_cursor)."""
        with self._lock:
            if consumer_id not in self._consumer_cursors:
                self._consumer_cursors[consumer_id] = cursor
                self.logger.debug(f"Registered consumer: {consumer_id}")

            result: List[Split] = []
            new_cursor = cursor

            for seq, split in self._buffer:
                if seq <= cursor:
                    continue
                if len(result) >= max_splits:
                    break
                result.append(split)
                new_cursor = seq

            self._consumer_cursors[consumer_id] = new_cursor
            return result, new_cursor

    def is_empty_after(self, cursor: int) -> bool:
        """Check if there are no splits after the given cursor."""
        with self._lock:
            return not any(seq > cursor for seq, _ in self._buffer)

    def gc(self) -> int:
        """Remove splits consumed by all consumers. Returns count removed."""
        with self._lock:
            if not self._consumer_cursors or not self._buffer:
                return 0

            min_cursor = min(self._consumer_cursors.values())
            original_len = len(self._buffer)
            self._buffer = [(seq, s) for seq, s in self._buffer if seq > min_cursor]
            return original_len - len(self._buffer)

    def mark_upstream_finished(self) -> None:
        """Mark upstream as finished producing."""
        self._upstream_finished = True

    def is_upstream_finished(self) -> bool:
        return self._upstream_finished

    def close(self) -> None:
        """Close the buffer."""
        with self._lock:
            self._closed = True
            self._upstream_finished = True

    def get_buffer_stats(self) -> Dict:
        """Get buffer statistics for monitoring."""
        with self._lock:
            return {
                "stage_id": self.stage_id,
                "buffer_size": len(self._buffer),
                "max_size": self.max_size,
                "sequence_counter": self._sequence_counter,
                "upstream_finished": self._upstream_finished,
                "consumer_count": len(self._consumer_cursors),
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)
