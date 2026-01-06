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

"""Workers API - worker status, logs, and debugging."""

import subprocess
from typing import Any, Dict, List

import ray
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

router = APIRouter(tags=["workers"])


@router.get("/jobs/{job_id}/workers")
async def list_workers(job_id: str, request: Request) -> List[Dict[str, Any]]:
    """List all workers for a job."""

    workers = []

    # Embedded mode: get from runner
    if request.app.state.mode == "embedded":
        runner = request.app.state.job_runner
        if runner and runner.job.job_id == job_id:
            for stage_id, master in runner._masters.items():
                for worker_id, worker_handle in master._workers.items():
                    worker_status = ray.get(worker_handle.get_status.remote(), timeout=1)
                    workers.append(
                        {
                            "worker_id": worker_id,
                            "stage_id": stage_id,
                            "status": "RUNNING" if worker_status.get("running") else "IDLE",
                            "processed_count": worker_status.get("processed_count", 0),
                            "assigned_partitions": worker_status.get("assigned_partitions", []),
                        }
                    )

    # History mode: get from storage
    elif request.app.state.storage:
        worker_events = request.app.state.storage.list_worker_events(job_id, limit=1000)
        # Group by worker_id and return latest status
        workers_dict = {}
        for event in worker_events:
            worker_id = event.get("worker_id")
            if worker_id not in workers_dict:
                workers_dict[worker_id] = event
        workers = list(workers_dict.values())

    return workers


@router.get("/jobs/{job_id}/workers/{worker_id}")
async def get_worker_detail(
    job_id: str,
    worker_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get detailed worker information."""

    # Embedded mode: get from runner
    if request.app.state.mode == "embedded":
        runner = request.app.state.job_runner
        if runner and runner.job.job_id == job_id:
            # Find worker across all stages
            for stage_id, master in runner._masters.items():
                worker_handle = master._workers.get(worker_id)
                if worker_handle:
                    worker_status = ray.get(worker_handle.get_status.remote(), timeout=1)

                    # Get actor info
                    actor_id = None
                    node_id = None
                    pid = None
                    actor_info = ray.util.state.get_actor(worker_id)
                    if actor_info:
                        actor_id = actor_info.get("actor_id")
                        node_id = actor_info.get("node_id")
                        pid = actor_info.get("pid")

                    return {
                        "worker_id": worker_id,
                        "stage_id": stage_id,
                        "actor_id": actor_id,
                        "node_id": node_id,
                        "pid": pid,
                        "status": "RUNNING" if worker_status.get("running") else "IDLE",
                        "processed_count": worker_status.get("processed_count", 0),
                        "error_count": worker_status.get("error_count", 0),
                        "assigned_partitions": worker_status.get("assigned_partitions", []),
                    }

    raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")


@router.get("/jobs/{job_id}/workers/{worker_id}/logs")
async def get_worker_logs(
    job_id: str,
    worker_id: str,
    request: Request,
    tail: int = Query(100, ge=1, le=10000),
) -> PlainTextResponse:
    """Get worker logs.

    Args:
        job_id: Job identifier
        worker_id: Worker identifier
        tail: Number of lines to return from the end

    Returns:
        Plain text log content
    """
    # Only available in embedded mode
    if request.app.state.mode != "embedded":
        return PlainTextResponse("Logs only available for running jobs")

    # Get logs from Ray
    logs = ray.util.state.get_log(
        actor_id=worker_id,
        tail=tail,
    )

    if logs:
        return PlainTextResponse(logs)
    else:
        return PlainTextResponse("No logs available")


@router.get("/jobs/{job_id}/workers/{worker_id}/stacktrace")
async def get_worker_stacktrace(
    job_id: str,
    worker_id: str,
    request: Request,
) -> PlainTextResponse:
    """Get worker stacktrace using py-spy.

    Args:
        job_id: Job identifier
        worker_id: Worker identifier

    Returns:
        Plain text stacktrace
    """
    # Only available in embedded mode
    if request.app.state.mode != "embedded":
        return PlainTextResponse("Stacktrace only available for running jobs")

    runner = request.app.state.job_runner
    if not runner or runner.job.job_id != job_id:
        raise HTTPException(status_code=404, detail="Job not found")

    # Find worker and get PID
    for stage_id, master in runner._masters.items():
        worker_handle = master._workers.get(worker_id)
        if worker_handle:
            # Get actor info to find PID
            actor_info = ray.util.state.get_actor(worker_id)
            if not actor_info or "pid" not in actor_info:
                return PlainTextResponse("Could not determine worker PID")

            pid = actor_info["pid"]

            # Use py-spy to dump stacktrace
            try:
                result = subprocess.run(
                    ["py-spy", "dump", "--pid", str(pid)],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if result.returncode == 0:
                    return PlainTextResponse(result.stdout)
                else:
                    return PlainTextResponse(
                        f"py-spy failed: {result.stderr}\n\n"
                        f"Make sure py-spy is installed: pip install py-spy"
                    )

            except subprocess.TimeoutExpired:
                raise HTTPException(status_code=504, detail="py-spy timeout")

    raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")
