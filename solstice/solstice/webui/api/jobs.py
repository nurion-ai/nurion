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

from solstice.webui.registry import get_or_create_registry

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

    # Get running jobs from registry
    if ray.is_initialized():
        registry = get_or_create_registry()
        jobs_dict = ray.get(registry.list_jobs.remote())
        running_jobs = [j.to_dict() for j in jobs_dict.values()]

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
    # Check if it's a running job
    if ray.is_initialized():
        registry = get_or_create_registry()
        job_info = ray.get(registry.get_job.remote(job_id))

        if job_info and request.app.state.mode == "embedded":
            # Get real-time data from runner
            runner = request.app.state.job_runner
            if runner and runner.job.job_id == job_id:
                status = runner.get_status()

                # Build stage info
                stages = []
                for stage_id, stage_status in status.stages.items():
                    stages.append(
                        {
                            "stage_id": stage_id,
                            "worker_count": stage_status["worker_count"],
                            "output_queue_size": stage_status["output_queue_size"],
                            "is_running": stage_status["is_running"],
                            "is_finished": stage_status["is_finished"],
                            "failed": stage_status["failed"],
                        }
                    )

                return {
                    "job_id": job_id,
                    "job_name": runner.job.job_id,
                    "status": "RUNNING" if status.is_running else "COMPLETED",
                    "start_time": status.start_time or time.time(),
                    "end_time": None,
                    "duration_ms": int(status.elapsed_time * 1000),
                    "stages": stages,
                    "dag_edges": runner.job.dag_edges,
                    "error": status.error,
                }

    # Check historical data
    if request.app.state.storage:
        job_data = request.app.state.storage.get_job_archive(job_id)
        if job_data:
            return job_data

    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
