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

"""Overview API - cluster and job statistics."""

from typing import Any, Dict, Optional

import ray
from fastapi import APIRouter, Request

from solstice.webui.app import get_ray_dashboard_url
from solstice.webui.storage.prometheus_exporter import (
    get_grafana_url,
    get_ray_prometheus_url,
)

router = APIRouter(tags=["overview"])


@router.get("/overview")
async def get_overview(request: Request) -> Dict[str, Any]:
    """Get cluster and job overview."""

    # Get Ray cluster resources
    cluster_resources = {}
    if ray.is_initialized():
        cluster_resources = ray.cluster_resources()

    # Calculate usage
    total_cpus = cluster_resources.get("CPU", 0)
    used_cpus = total_cpus - ray.available_resources().get("CPU", 0)

    total_memory = cluster_resources.get("memory", 0)
    used_memory = total_memory - ray.available_resources().get("memory", 0)

    total_gpus = cluster_resources.get("GPU", 0)
    used_gpus = total_gpus - ray.available_resources().get("GPU", 0) if total_gpus > 0 else 0

    # Get job statistics
    job_stats = {"running": 0, "completed": 0, "failed": 0}

    if request.app.state.mode == "embedded" and request.app.state.job_runner:
        # Embedded mode: current job
        runner = request.app.state.job_runner
        status = runner.get_status()

        if status.is_running:
            job_stats["running"] = 1
        elif status.error:
            job_stats["failed"] = 1
        else:
            job_stats["completed"] = 1

    # TODO: Query storage for historical job stats

    return {
        "cluster": {
            "total_cpus": total_cpus,
            "used_cpus": used_cpus,
            "total_memory_gb": total_memory / (1024**3) if total_memory > 0 else 0,
            "used_memory_gb": used_memory / (1024**3) if used_memory > 0 else 0,
            "total_gpus": total_gpus,
            "used_gpus": used_gpus,
        },
        "jobs": job_stats,
        "mode": request.app.state.mode,
    }


@router.get("/external-links")
async def get_external_links() -> Dict[str, Optional[str]]:
    """Get external tool links."""
    return {
        "ray_dashboard": get_ray_dashboard_url(),
        "grafana": get_grafana_url(),
        "prometheus": get_ray_prometheus_url(),
    }
