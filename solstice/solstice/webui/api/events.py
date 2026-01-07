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

"""Events API - Ray cluster events via State API.

This module provides events from Ray State API instead of Event Export.
Ray State API is available without cluster-level configuration.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["events"])


@router.get("/jobs/{job_id}/events")
async def list_job_events(
    job_id: str,
    request: Request,
    event_type: Optional[str] = Query(None, description="Filter by type: actors, tasks"),
    limit: int = Query(100, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    """List events for a job using Ray State API.

    This endpoint queries Ray State API to get events related to the job.
    Available event types:
    - actors: Actor lifecycle events (created, running, dead)
    - tasks: Task execution events

    Args:
        job_id: Job identifier (used to filter by actor/task name prefix)
        event_type: Filter by event type
        limit: Maximum number of events to return

    Returns:
        List of events with type, timestamp, and details
    """
    import ray

    if not ray.is_initialized():
        return []

    events = []

    try:
        from ray.util.state import list_actors, list_tasks

        # Get actor events
        if event_type is None or event_type == "actors":
            try:
                actors = list_actors(limit=limit)
                for actor in actors:
                    actor_name = actor.get("name", "")
                    # Filter by job_id in name (workers are named with job_id prefix)
                    if job_id in actor_name or not job_id:
                        events.append(
                            {
                                "event_type": "ACTOR",
                                "name": actor_name,
                                "actor_id": actor.get("actor_id"),
                                "state": actor.get("state"),
                                "node_id": actor.get("node_id"),
                                "pid": actor.get("pid"),
                                "timestamp": None,  # Ray State API doesn't provide creation time
                                "details": {
                                    "class_name": actor.get("class_name"),
                                    "resources": actor.get("required_resources"),
                                },
                            }
                        )
            except Exception as e:
                events.append(
                    {
                        "event_type": "ERROR",
                        "name": "list_actors",
                        "details": str(e),
                    }
                )

        # Get task events
        if event_type is None or event_type == "tasks":
            try:
                tasks = list_tasks(limit=limit)
                for task in tasks:
                    task_name = task.get("name", "")
                    func_name = task.get("func_or_class_name", "")
                    # Filter by job_id
                    if job_id in task_name or job_id in func_name or not job_id:
                        events.append(
                            {
                                "event_type": "TASK",
                                "name": task_name or func_name,
                                "task_id": task.get("task_id"),
                                "state": task.get("state"),
                                "node_id": task.get("node_id"),
                                "timestamp": None,
                                "details": {
                                    "func_name": func_name,
                                    "actor_id": task.get("actor_id"),
                                },
                            }
                        )
            except Exception as e:
                events.append(
                    {
                        "event_type": "ERROR",
                        "name": "list_tasks",
                        "details": str(e),
                    }
                )

    except ImportError:
        return [{"event_type": "ERROR", "details": "ray.util.state not available"}]

    return events[:limit]


@router.get("/cluster/events")
async def list_cluster_events(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
) -> Dict[str, Any]:
    """Get cluster-wide event summary.

    Returns:
        Summary of actors and tasks across the cluster
    """
    import ray

    if not ray.is_initialized():
        return {"error": "Ray not initialized"}

    try:
        from ray.util.state import list_actors, list_tasks, list_nodes

        actors = list_actors(limit=limit)
        tasks = list_tasks(limit=limit)

        # Count by state
        actor_states = {}
        for a in actors:
            state = a.get("state", "UNKNOWN")
            actor_states[state] = actor_states.get(state, 0) + 1

        task_states = {}
        for t in tasks:
            state = t.get("state", "UNKNOWN")
            task_states[state] = task_states.get(state, 0) + 1

        # Get nodes
        nodes = []
        try:
            node_list = list_nodes()
            for n in node_list:
                nodes.append(
                    {
                        "node_id": n.get("node_id"),
                        "state": n.get("state"),
                        "node_ip": n.get("node_ip"),
                        "resources": n.get("resources_total"),
                    }
                )
        except Exception:
            pass

        return {
            "actors": {
                "total": len(actors),
                "by_state": actor_states,
            },
            "tasks": {
                "total": len(tasks),
                "by_state": task_states,
            },
            "nodes": nodes,
        }

    except ImportError:
        return {"error": "ray.util.state not available"}
    except Exception as e:
        return {"error": str(e)}


# Legacy endpoint for Event Export compatibility
@router.post("/events/ingest")
async def ingest_ray_event(event: Dict[str, Any], request: Request) -> Dict[str, str]:
    """Ingest Ray Event Export events (legacy endpoint).

    This endpoint is kept for backwards compatibility with Ray Event Export.
    New deployments should use the /jobs/{job_id}/events endpoint instead.
    """
    if not request.app.state.storage:
        return {"status": "error", "message": "Storage not configured"}

    from solstice.webui.collectors.events import EventCollector

    # Create ephemeral collector
    collector = EventCollector(
        job_id=event.get("solstice_job_id", "global"),
        storage=request.app.state.storage,
    )
    collector.ingest_event(event)

    return {"status": "ok", "event_id": event.get("eventId")}
