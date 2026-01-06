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

"""Events API - Ray Event Export endpoint and query."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from solstice.webui.collectors.events import EventCollector

router = APIRouter(tags=["events"])


def _get_event_collector(request: Request) -> EventCollector:
    """Get or create EventCollector for the current request.

    EventCollector is stateless for ingestion, so we create ephemeral instances.
    For running jobs, we could cache the collector in app.state.
    """
    # Extract job_id from event (will be passed by caller)
    # For now, use "global" collector for all events
    if not hasattr(request.app.state, "event_collector"):
        request.app.state.event_collector = EventCollector(
            job_id="global",  # Will be overridden per event
            storage=request.app.state.storage,
        )
    return request.app.state.event_collector


@router.post("/events/ingest")
async def ingest_ray_event(event: Dict[str, Any], request: Request) -> Dict[str, str]:
    """Ingest Ray Event Export events (CLUSTER-LEVEL endpoint).

    This endpoint receives ALL events from the Ray cluster, for ALL jobs.
    Ray Event Export is configured once per cluster, not per job.

    **No Conflicts**: Multiple Solstice jobs in the same cluster share this endpoint.
    Events are tagged with job_id and stored separately in SlateDB.

    Configure Ray cluster (ONCE) to export events:
        RAY_EVENT_EXPORT_ENABLED=1
        RAY_EVENT_EXPORT_HTTP_URL=http://<host>:<port>/solstice/api/events/ingest

    Event format: https://docs.ray.io/en/latest/ray-observability/user-guides/ray-event-export.html

    Args:
        event: Ray event data (JSON)
            - eventId: Unique event ID
            - sourceType: GCS, CORE_WORKER
            - eventType: TASK_DEFINITION_EVENT, ACTOR_LIFECYCLE_EVENT, etc.
            - timestamp: ISO 8601 timestamp
            - severity: INFO, WARNING, ERROR
            - sessionName: Ray session ID

    Returns:
        Success status
    """
    if not request.app.state.storage:
        return {"status": "error", "message": "Storage not configured"}

    collector = _get_event_collector(request)
    collector.ingest_event(event)

    return {"status": "ok", "event_id": event.get("eventId")}


@router.get("/jobs/{job_id}/events")
async def list_job_events(
    job_id: str,
    request: Request,
    event_types: Optional[List[str]] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    """List Ray events for a job.

    Args:
        job_id: Job identifier
        event_types: Optional filter by event type
        limit: Maximum number of events to return
        offset: Number of events to skip

    Returns:
        List of Ray events
    """
    if not request.app.state.storage:
        return []

    # Use EventCollector for consistent API
    collector = EventCollector(job_id, request.app.state.storage)
    return collector.get_events(
        event_types=event_types,
        limit=limit,
        offset=offset,
    )
