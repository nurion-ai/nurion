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

"""Event collector for Ray Event Export integration.

Ray Event Export allows Ray to push events to an HTTP endpoint.
This collector receives events via HTTP POST and stores them in SlateDB.

Reference: https://docs.ray.io/en/latest/ray-observability/user-guides/ray-event-export.html
"""

import time
from typing import Any, Dict, List, Optional

from solstice.webui.storage import JobStorage
from solstice.utils.logging import create_ray_logger


class EventCollector:
    """Collect and store Ray events from Event Export HTTP endpoint.

    Ray's Event Export is CLUSTER-LEVEL:
    - Configured once per Ray cluster (not per job)
    - All events from all jobs are sent to the same HTTP endpoint
    - Events are tagged with job_id and stored separately

    This collector:
    - Receives events from Ray via HTTP POST (handled by API)
    - Extracts job_id from event data
    - Tags events for filtering
    - Stores in SlateDB for later analysis

    Configure Ray cluster to export events:
        RAY_EVENT_EXPORT_ENABLED=1
        RAY_EVENT_EXPORT_HTTP_URL=http://<host>:<port>/solstice/api/events/ingest

    Note: Multiple Solstice jobs in the same cluster will share this event stream.
    Each job's events are stored separately based on job_id.

    Usage:
        # In API endpoint
        collector = EventCollector(job_id, storage)
        collector.ingest_event(event)  # Called by HTTP POST handler

        # Query events
        events = collector.get_events(limit=100)
    """

    def __init__(self, job_id: str, storage: JobStorage):
        """Initialize event collector.

        Args:
            job_id: Job identifier for tagging events
            storage: SlateDB storage instance
        """
        self.job_id = job_id
        self.storage = storage
        self.logger = create_ray_logger(f"EventCollector-{job_id}")

        self._event_count = 0

    def ingest_event(self, event: Dict[str, Any]) -> None:
        """Ingest a single event from Ray Event Export.

        This method is called by the HTTP endpoint when Ray pushes an event.

        Args:
            event: Event data from Ray (JSON format)
                - eventId: Unique event identifier
                - sourceType: GCS, CORE_WORKER, etc.
                - eventType: TASK_DEFINITION_EVENT, ACTOR_LIFECYCLE_EVENT, etc.
                - timestamp: ISO 8601 timestamp
                - severity: INFO, WARNING, ERROR
                - sessionName: Ray session identifier
                - [eventType]Event: Type-specific event data
        """
        event_id = event.get("eventId", f"unknown_{time.time()}")
        event_type = event.get("eventType", "UNKNOWN")

        # Tag with job_id for filtering
        tagged_event = {
            **event,
            "solstice_job_id": self.job_id,
            "ingested_at": time.time(),
        }

        # Store in SlateDB
        self.storage.store_ray_event(event_id, tagged_event)

        self._event_count += 1

        if self._event_count % 100 == 0:
            self.logger.info(f"Ingested {self._event_count} Ray events")
        else:
            self.logger.debug(f"Ingested event: {event_type} from {event.get('sourceType')}")

    def get_event_count(self) -> int:
        """Get total number of events ingested.

        Returns:
            Event count
        """
        return self._event_count

    def get_events(
        self,
        event_types: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query collected events from storage.

        Args:
            event_types: Optional filter by event type
            limit: Maximum number of events to return
            offset: Number of events to skip

        Returns:
            List of event data
        """
        return self.storage.list_ray_events(
            self.job_id,
            event_types=event_types,
            limit=limit,
            offset=offset,
        )
