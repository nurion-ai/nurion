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

"""Common data structures for queue backends."""

from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class Record:
    """A record fetched from the queue.

    Attributes:
        offset: Monotonically increasing sequence number assigned by the queue.
                This is the primary identifier for exactly-once semantics.
        key: Optional key for partitioning (not used in single-partition mode).
        value: The message payload as bytes.
        timestamp: Unix timestamp in milliseconds when the record was produced.
    """

    offset: int
    value: bytes
    key: Optional[bytes] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def __repr__(self) -> str:
        value_preview = self.value[:50] if len(self.value) <= 50 else self.value[:50] + b"..."
        return f"Record(offset={self.offset}, value={value_preview!r})"
