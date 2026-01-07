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

"""Jobs API - list and retrieve job information.

Architecture:
- JobRunner writes to JobStorage (SlateDB) via JobStateManager
- Portal/History Server reads from JobStorage (read-only)
- Both running and completed jobs use the same code path

Note: storage is guaranteed to exist (app won't start without it).
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["jobs"])


@router.get("/jobs")
async def list_all_jobs(
    request: Request,
    status: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """List all jobs.

    Args:
        status: Optional status filter (RUNNING, COMPLETED, FAILED)
        limit: Maximum number of jobs to return
        offset: Number of jobs to skip

    Returns:
        Dictionary with jobs list and total count
    """
    storage = request.app.state.storage
    jobs = storage.list_jobs(status=status, limit=limit, offset=offset)
    return {
        "jobs": jobs,
        "total": len(jobs),
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
    storage = request.app.state.storage
    job_data = storage.get_job(job_id)
    if job_data:
        return job_data
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
