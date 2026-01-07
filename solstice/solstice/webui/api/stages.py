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

"""Stages API - stage metrics and details."""

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["stages"])


@router.get("/jobs/{job_id}/stages")
async def list_stages(job_id: str, request: Request) -> Dict[str, Any]:
    """List all stages for a job."""

    # Embedded mode: get from runner
    if request.app.state.mode == "embedded":
        runner = request.app.state.job_runner
        if runner and runner.job.job_id == job_id:
            stages = []
            for stage_id, master in runner._masters.items():
                status = master.get_status()
                stages.append(
                    {
                        "stage_id": stage_id,
                        "worker_count": status.worker_count,
                        "output_queue_size": status.output_queue_size,
                        "is_running": status.is_running,
                        "is_finished": status.is_finished,
                        "failed": status.failed,
                        "backpressure_active": status.backpressure_active,
                    }
                )

            return {
                "job_id": job_id,
                "stages": stages,
                "dag_edges": runner.job.dag_edges,
            }

    # History mode: get from storage
    if request.app.state.storage:
        job_data = request.app.state.storage.get_job_archive(job_id)
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

    # Embedded mode: get from runner
    if request.app.state.mode == "embedded":
        runner = request.app.state.job_runner
        if runner and runner.job.job_id == job_id:
            master = runner._masters.get(stage_id)
            if not master:
                raise HTTPException(status_code=404, detail=f"Stage {stage_id} not found")

            status = master.get_status()

            # Get partition metrics if available
            partition_metrics = []
            pm_dict = await master.get_partition_metrics()
            partition_metrics = [pm.to_dict() for pm in pm_dict.values()]

            return {
                "stage_id": stage_id,
                "operator_type": type(master.stage.operator_config).__name__,
                "worker_count": status.worker_count,
                "min_parallelism": master.config.min_workers,
                "max_parallelism": master.config.max_workers,
                "output_queue_size": status.output_queue_size,
                "is_running": status.is_running,
                "is_finished": status.is_finished,
                "failed": status.failed,
                "backpressure_active": status.backpressure_active,
                "partition_metrics": partition_metrics,
            }

    # History mode: get from storage
    if request.app.state.storage:
        job_data = request.app.state.storage.get_job_archive(job_id)
        if job_data:
            stages = job_data.get("stages", [])
            for stage in stages:
                if stage.get("stage_id") == stage_id:
                    return stage

    raise HTTPException(status_code=404, detail=f"Stage {stage_id} not found")


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
    import time

    # Default to last 5 minutes
    if end_time == 0:
        end_time = time.time()
    if start_time == 0:
        start_time = end_time - 300

    # Get from storage
    if request.app.state.storage:
        return request.app.state.storage.get_metrics_history(
            job_id, stage_id, start_time, end_time
        )

    return []


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
    import ray

    workers = []

    # Embedded mode: get from runner
    if request.app.state.mode == "embedded":
        runner = request.app.state.job_runner
        if runner and runner.job.job_id == job_id:
            master = runner._masters.get(stage_id)
            if master:
                for worker_id, worker_handle in master._workers.items():
                    try:
                        worker_status = ray.get(worker_handle.get_status.remote(), timeout=1)

                        # Try to get actor info
                        actor_id = None
                        try:
                            actor_info = ray.util.state.get_actor(worker_id)
                            if actor_info:
                                actor_id = str(actor_info.get("actor_id", ""))
                        except Exception:
                            pass

                        workers.append({
                            "worker_id": worker_id,
                            "stage_id": stage_id,
                            "actor_id": actor_id,
                            "status": "RUNNING" if worker_status.get("running") else "IDLE",
                            "processed_count": worker_status.get("processed_count", 0),
                            "assigned_partitions": worker_status.get("assigned_partitions", []),
                        })
                    except Exception:
                        workers.append({
                            "worker_id": worker_id,
                            "stage_id": stage_id,
                            "status": "UNKNOWN",
                        })

    return workers
