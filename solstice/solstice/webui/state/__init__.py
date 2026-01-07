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

"""Push-based state management for WebUI.

This module provides event-driven state management using Tansu message queue,
replacing the pull-based ray.get() polling approach.

Key components:
- StateMessage: Unified message format for all state updates
- JobStateManager: Consumes and aggregates state from Tansu topic
- StateProducer: Helper for producing state messages (used by workers)

Benefits over pull-based approach:
- No Ray GCS pressure from frequent ray.get() calls
- Workers are not blocked by metrics collection
- Predictable latency with time-window aggregation
- Event sourcing enables state replay for debugging
"""

from solstice.webui.state.messages import (
    StateMessage,
    StateMessageType,
)
from solstice.webui.state.manager import JobStateManager
from solstice.webui.state.producer import StateProducer

__all__ = [
    "StateMessage",
    "StateMessageType",
    "JobStateManager",
    "StateProducer",
]
