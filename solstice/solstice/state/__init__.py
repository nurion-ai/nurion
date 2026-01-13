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

"""State management for Solstice operators.

This module provides partition-scoped state storage for stateful operators
like Dedup and Connected Components. Key features:

- **Partition-scoped**: Each partition has its own SlateDB instance
- **Worker-agnostic**: State is tied to partitions, not workers
- **Fencing**: SlateDB's built-in fencing prevents split-brain scenarios

Architecture:
    Each partition's state is stored in a separate SlateDB instance:

    {base_path}/{job_id}/{stage_id}/partition_{id}/

    This ensures:
    1. Single-writer per partition (enforced by SlateDB fencing)
    2. Elastic scaling without state migration
"""

from solstice.state.protocols import PartitionStateStore
from solstice.state.slatedb_store import SlateDBPartitionStateStore

__all__ = [
    "PartitionStateStore",
    "SlateDBPartitionStateStore",
]
