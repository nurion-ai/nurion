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

"""Stages API - stage metrics and details.

Architecture:
- JobRunner writes to JobStorage (SlateDB) via JobStateManager
- Portal/History Server reads from JobStorage (read-only)

Note: storage is guaranteed to exist (app won't start without it).
"""

import time
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["stages"])


@router.get("/jobs/{job_id}/stages")
async def list_stages(job_id: str, request: Request) -> Dict[str, Any]:
    """List all stages for a job."""
    storage = request.app.state.storage
    job_data = storage.get_job(job_id)
    if job_data:
        return {
            "job_id": job_id,
            "stages": job_data.get("stages", []),
            "dag_edges": job_data.get("dag_edges", {}),
        }
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")


@router.get("/jobs/{job_id}/stages/{stage_id}")
async def get_stage_detail(
    job_id: str,
    stage_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get detailed stage information."""
    storage = request.app.state.storage
    job_data = storage.get_job(job_id)
    if job_data:
        stages = job_data.get("stages", [])
        for stage in stages:
            if stage.get("stage_id") == stage_id:
                return stage
        raise HTTPException(status_code=404, detail=f"Stage {stage_id} not found")
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")


@router.get("/jobs/{job_id}/stages/{stage_id}/metrics")
async def get_stage_metrics_history(
    job_id: str,
    stage_id: str,
    request: Request,
    start_time: float = Query(0),
    end_time: float = Query(0),
) -> List[Dict[str, Any]]:
    """Get metrics history for a stage.

    Args:
        job_id: Job identifier
        stage_id: Stage identifier
        start_time: Start timestamp (Unix seconds)
        end_time: End timestamp (Unix seconds)

    Returns:
        List of metrics snapshots
    """
    # Default to last 5 minutes
    if end_time == 0:
        end_time = time.time()
    if start_time == 0:
        start_time = end_time - 300

    storage = request.app.state.storage
    return storage.get_metrics_history(job_id, stage_id, start_time, end_time)


@router.get("/jobs/{job_id}/stages/{stage_id}/workers")
async def list_stage_workers(
    job_id: str,
    stage_id: str,
    request: Request,
) -> List[Dict[str, Any]]:
    """List workers for a specific stage.

    Args:
        job_id: Job identifier
        stage_id: Stage identifier

    Returns:
        List of worker info
    """
    storage = request.app.state.storage
    worker_events = storage.list_worker_events(job_id, stage_id=stage_id, limit=500)
    # Deduplicate by worker_id, keeping latest
    workers_dict: Dict[str, Any] = {}
    for event in worker_events:
        worker_id = event.get("worker_id")
        if worker_id not in workers_dict:
            workers_dict[worker_id] = event
    return list(workers_dict.values())
