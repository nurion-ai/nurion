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

"""Ray State API utilities for querying running Solstice jobs.

This module provides direct Ray State API queries for discovering jobs
by inspecting Ray actors. Used by the Portal for job discovery.

Note: For running jobs with push-based metrics enabled, prefer using
the registry functions in solstice.webui.state.registry instead.
"""

import time
from typing import Any, Dict, List, Optional

import ray


def get_running_jobs_from_ray() -> List[Dict[str, Any]]:
    """Get running Solstice jobs using Ray State API.

    Queries Ray actors to find running jobs by looking for:
    - _RaySplitPayloadStoreActor: Identifies job_id (name: payload_store_{job_id})
    - StageWorker: Counts workers per stage (name: {stage_id}:{worker_id})

    Returns:
        List of job info dictionaries with keys:
        - job_id: Job identifier
        - status: Always "RUNNING"
        - start_time: From job metadata or approximate
        - stage_count: Number of stages
        - worker_count: Total workers
        - stages: List of stage info dicts
        - dag_edges: Pipeline structure
    """
    try:
        from ray.util.state import list_actors

        # Map job_id -> job info
        jobs_map: Dict[str, Dict[str, Any]] = {}
        actors = list_actors(filters=[("state", "=", "ALIVE")])

        # First pass: find jobs by payload_store actors
        now = time.time()
        for actor in actors:
            if actor.class_name == "_RaySplitPayloadStoreActor" and actor.name:
                parts = actor.name.split("_", 2)  # [payload, store, job_id]
                if len(parts) >= 3:
                    job_id = parts[2]
                    if job_id not in jobs_map:
                        jobs_map[job_id] = {
                            "job_id": job_id,
                            "status": "RUNNING",
                            "start_time": now,  # Approximate; use JobStateManager for accurate time
                            "last_update": now,
                            "stage_count": 0,
                            "worker_count": 0,
                            "stages": [],
                            "dag_edges": {},
                        }

        # Second pass: count workers and stages
        stages_seen: Dict[str, set] = {job_id: set() for job_id in jobs_map}
        for actor in actors:
            if actor.class_name == "StageWorker" and actor.name:
                if ":" in actor.name:
                    parts = actor.name.split(":", 1)
                    stage_id = parts[0]

                    for job_id in jobs_map:
                        if job_id not in stages_seen:
                            stages_seen[job_id] = set()

                        if stage_id not in stages_seen[job_id]:
                            stages_seen[job_id].add(stage_id)
                            jobs_map[job_id]["stages"].append(
                                {
                                    "stage_id": stage_id,
                                    "worker_count": 1,
                                    "is_running": True,
                                }
                            )
                            jobs_map[job_id]["stage_count"] += 1
                        else:
                            for s in jobs_map[job_id]["stages"]:
                                if s["stage_id"] == stage_id:
                                    s["worker_count"] += 1
                                    break

                        jobs_map[job_id]["worker_count"] += 1
                        break

        # Note: Metadata (dag_edges, start_time) is now managed by JobStateManager
        # via push-based events. Legacy payload_store no longer stores metadata.

        return list(jobs_map.values())
    except Exception:
        return []


def get_running_job_info(job_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed info for a specific running job.

    Checks if the job's payload_store actor exists to confirm running status,
    then enriches with real-time worker metrics from StageWorker actors.

    Note: Detailed metadata (dag_edges, start_time) is now managed by
    JobStateManager via push-based events. Use get_running_job_info_from_state_manager()
    for complete job info when available.

    Args:
        job_id: The job identifier

    Returns:
        Job info dict with stages and metrics, or None if not found
    """
    try:
        from ray.util.state import list_actors

        # Get all ALIVE actors first (works across namespaces)
        actors = list_actors(filters=[("state", "=", "ALIVE")])

        # Check if payload_store actor exists to confirm job is running
        actor_name = f"payload_store_{job_id}"
        payload_store_exists = any(
            a.class_name == "_RaySplitPayloadStoreActor" and a.name == actor_name for a in actors
        )
        if not payload_store_exists:
            return None

        now = time.time()
        result = {
            "job_id": job_id,
            "status": "RUNNING",
            "start_time": now,  # Approximate; use JobStateManager for accurate time
            "last_update": now,
            "dag_edges": {},  # Use JobStateManager for dag_edges
            "stages": [],
            "stage_count": 0,
            "worker_count": 0,
        }
        stage_workers: Dict[str, List[str]] = {}  # stage_id -> [actor_names]

        for a in actors:
            if a.class_name == "StageWorker" and a.name and ":" in a.name:
                stage_id = a.name.split(":", 1)[0]
                if stage_id not in stage_workers:
                    stage_workers[stage_id] = []
                stage_workers[stage_id].append(a.name)

        # Build stages info with real-time metrics
        for stage_id, worker_names in stage_workers.items():
            stage_info = {
                "stage_id": stage_id,
                "worker_count": len(worker_names),
                "is_running": True,
                "input_count": 0,
                "output_count": 0,
            }

            # Aggregate metrics from workers
            for worker_name in worker_names:
                try:
                    worker = ray.get_actor(worker_name)
                    metrics = ray.get(worker.get_metrics.remote(), timeout=1)
                    if metrics:
                        stage_info["input_count"] += metrics.get("input_records", 0)
                        stage_info["output_count"] += metrics.get("output_records", 0)
                except Exception:
                    pass

            result["stages"].append(stage_info)
            result["worker_count"] += len(worker_names)

        result["stage_count"] = len(result["stages"])
        return result

    except Exception:
        return None


def is_job_running(job_id: str) -> bool:
    """Check if a job is currently running.

    Args:
        job_id: The job identifier

    Returns:
        True if job is running, False otherwise
    """
    try:
        ray.get_actor(f"payload_store_{job_id}")
        return True
    except ValueError:
        return False
