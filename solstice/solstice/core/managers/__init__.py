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

"""Stage Master component managers.

These managers handle specific concerns within a StageMaster:
- PartitionManager: Partition assignment and rebalancing
- WorkerManager: Worker lifecycle (spawn, stop, status)
- RecoveryManager: Failure tracking and worker recovery
- BackpressureMonitor: Backpressure detection and scaling
"""

from solstice.core.managers.partition_manager import PartitionManager
from solstice.core.managers.worker_manager import WorkerManager
from solstice.core.managers.recovery_manager import RecoveryManager
from solstice.core.managers.backpressure_monitor import BackpressureMonitor

__all__ = [
    "PartitionManager",
    "WorkerManager",
    "RecoveryManager",
    "BackpressureMonitor",
]
