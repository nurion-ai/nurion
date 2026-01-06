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

"""Configuration API - job and environment configuration."""

import os
from typing import Any, Dict

import ray
from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["configuration"])


@router.get("/jobs/{job_id}/configuration")
async def get_configuration(job_id: str, request: Request) -> Dict[str, Any]:
    """Get job and environment configuration."""

    # Embedded mode: get from runner
    if request.app.state.mode == "embedded":
        runner = request.app.state.job_runner
        if runner and runner.job.job_id == job_id:
            # Collect stage configs
            stage_configs = {}
            for stage_id, master in runner._masters.items():
                stage_configs[stage_id] = {
                    "operator_type": type(master.stage.operator_config).__name__,
                    "min_parallelism": master.config.min_workers,
                    "max_parallelism": master.config.max_workers,
                    "num_cpus": master.config.num_cpus,
                    "num_gpus": master.config.num_gpus,
                    "memory_mb": master.config.memory_mb,
                }

            # Ray cluster resources
            ray_config = {}
            if ray.is_initialized():
                ray_config = {
                    "cluster_resources": ray.cluster_resources(),
                    "available_resources": ray.available_resources(),
                }

            # Environment variables
            environment = {
                "SOLSTICE_LOG_LEVEL": os.getenv("SOLSTICE_LOG_LEVEL", "INFO"),
                "RAY_PROMETHEUS_HOST": os.getenv("RAY_PROMETHEUS_HOST"),
                "SOLSTICE_GRAFANA_URL": os.getenv("SOLSTICE_GRAFANA_URL"),
            }

            return {
                "job_config": {
                    "job_id": runner.job.job_id,
                    "queue_type": runner.queue_type.value,
                    "tansu_storage_url": runner.tansu_storage_url,
                },
                "stage_configs": stage_configs,
                "ray_config": ray_config,
                "environment": environment,
            }

    # History mode: get from storage
    if request.app.state.storage:
        job_data = request.app.state.storage.get_job_archive(job_id)
        if job_data:
            return job_data.get("config", {})

    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
