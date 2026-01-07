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

This module encapsulates all Ray State API interactions for the WebUI,
providing a clean interface for discovering and querying running jobs.

Key design: No centralized state (JobRegistry). All information is obtained
by querying Ray actors directly:
- _RaySplitPayloadStoreActor: job metadata (dag_edges, start_time, stages config)
- StageWorker: real-time metrics (input/output counts)
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
        payload_store_actors: Dict[str, str] = {}  # job_id -> actor_name
        actors = list_actors(filters=[("state", "=", "ALIVE")])

        # First pass: find jobs by payload_store actors
        now = time.time()
        for actor in actors:
            if actor.class_name == "_RaySplitPayloadStoreActor" and actor.name:
                parts = actor.name.split("_", 2)  # [payload, store, job_id]
                if len(parts) >= 3:
                    job_id = parts[2]
                    if job_id not in jobs_map:
                        payload_store_actors[job_id] = actor.name
                        jobs_map[job_id] = {
                            "job_id": job_id,
                            "status": "RUNNING",
                            "start_time": now,  # Will be updated from metadata
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
                            jobs_map[job_id]["stages"].append({
                                "stage_id": stage_id,
                                "worker_count": 1,
                                "is_running": True,
                            })
                            jobs_map[job_id]["stage_count"] += 1
                        else:
                            for s in jobs_map[job_id]["stages"]:
                                if s["stage_id"] == stage_id:
                                    s["worker_count"] += 1
                                    break

                        jobs_map[job_id]["worker_count"] += 1
                        break

        # Third pass: get metadata from payload_store actors
        for job_id, actor_name in payload_store_actors.items():
            try:
                actor = ray.get_actor(actor_name)
                metadata = ray.get(actor.get_job_metadata.remote(), timeout=2)
                if metadata:
                    jobs_map[job_id]["dag_edges"] = metadata.get("dag_edges", {})
                    jobs_map[job_id]["start_time"] = metadata.get("start_time", now)
            except Exception:
                pass  # Keep defaults

        return list(jobs_map.values())
    except Exception:
        return []


def get_running_job_info(job_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed info for a specific running job.

    Queries the job's payload_store actor for metadata, then enriches
    with real-time worker metrics from StageWorker actors.

    Args:
        job_id: The job identifier

    Returns:
        Job info dict with dag_edges, stages, metrics, or None if not found
    """
    try:
        # Get payload_store actor for this job
        actor_name = f"payload_store_{job_id}"
        try:
            actor = ray.get_actor(actor_name)
        except ValueError:
            # Job not running
            return None

        # Get base metadata
        metadata = ray.get(actor.get_job_metadata.remote(), timeout=2)
        if not metadata:
            metadata = {}

        now = time.time()
        result = {
            "job_id": job_id,
            "status": "RUNNING",
            "start_time": metadata.get("start_time", now),
            "last_update": now,
            "dag_edges": metadata.get("dag_edges", {}),
            "stages": [],
            "stage_count": 0,
            "worker_count": 0,
        }

        # Get stage and worker info from Ray State API
        from ray.util.state import list_actors

        actors = list_actors(filters=[("state", "=", "ALIVE")])
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
