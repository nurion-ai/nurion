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

"""Jobs API - list and retrieve job information."""

import time
from typing import Any, Dict, Optional

import ray
from fastapi import APIRouter, HTTPException, Query, Request

from solstice.webui.ray_state import get_running_job_info, get_running_jobs_from_ray

router = APIRouter(tags=["jobs"])


@router.get("/jobs")
async def list_all_jobs(
    request: Request,
    status: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """List all jobs (running and historical).

    Args:
        status: Optional status filter (RUNNING, COMPLETED, FAILED)
        limit: Maximum number of jobs to return
        offset: Number of jobs to skip

    Returns:
        Dictionary with 'running' and 'completed' job lists
    """
    running_jobs = []
    completed_jobs = []

    # Get running jobs from Ray State API (no registry needed)
    if ray.is_initialized() and status != "COMPLETED" and status != "FAILED":
        running_jobs = get_running_jobs_from_ray()

        # Filter by status if specified
        if status == "RUNNING":
            running_jobs = [j for j in running_jobs if j["status"] == "RUNNING"]

    # Get completed jobs from storage
    if request.app.state.storage and status != "RUNNING":
        completed_jobs = request.app.state.storage.list_jobs(
            status=status if status in ["COMPLETED", "FAILED"] else None,
            limit=limit,
            offset=offset,
        )

    return {
        "running": running_jobs,
        "completed": completed_jobs,
        "total": len(running_jobs) + len(completed_jobs),
    }


@router.get("/jobs/{job_id}")
async def get_job_detail(job_id: str, request: Request) -> Dict[str, Any]:
    """Get detailed job information.

    Args:
        job_id: Job identifier

    Returns:
        Detailed job information

    Raises:
        HTTPException: If job not found
    """
    # Check if it's a running job (query payload_store actor directly)
    if ray.is_initialized():
        job_info = get_running_job_info(job_id)

        if job_info:
            return {
                "job_id": job_id,
                "status": "RUNNING",
                "start_time": job_info.get("start_time", time.time()),
                "end_time": None,
                "duration_ms": int((time.time() - job_info.get("start_time", time.time())) * 1000),
                "stages": job_info.get("stages", []),
                "dag_edges": job_info.get("dag_edges", {}),
                "stage_count": job_info.get("stage_count", 0),
                "worker_count": job_info.get("worker_count", 0),
                "error": None,
            }

    # Check historical data
    if request.app.state.storage:
        job_data = request.app.state.storage.get_job_archive(job_id)
        if job_data:
            return job_data

    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
