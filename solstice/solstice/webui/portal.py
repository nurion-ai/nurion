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

"""Portal service - global entry point for all Solstice jobs."""

from pathlib import Path

import ray
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from ray import serve

from solstice.webui.app import get_ray_dashboard_url, setup_template_filters
from solstice.webui.ray_state import get_running_job_info, get_running_jobs_from_ray
from solstice.webui.storage.portal_storage import PortalStorage
from solstice.utils.logging import create_ray_logger

WEBUI_DIR = Path(__file__).parent
TEMPLATES_DIR = WEBUI_DIR / "templates"
STATIC_DIR = WEBUI_DIR / "static"


def create_portal_app(storage_path: str) -> FastAPI:
    """Create the Portal FastAPI application.

    This function creates and configures the FastAPI app with all routes.
    It's called once when the Portal deployment is created.

    Note: Portal uses PortalStorage (read-only, scans job directories)
    instead of JobStorage (single job, write-enabled).
    """
    storage = PortalStorage(storage_path)
    logger = create_ray_logger("SolsticePortal")

    # Templates (required)
    if not TEMPLATES_DIR.exists():
        raise RuntimeError(f"Templates directory not found: {TEMPLATES_DIR}")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    setup_template_filters(templates)

    # Create app
    app = FastAPI(title="Solstice Portal")

    # Mount static files (required)
    if not STATIC_DIR.exists():
        raise RuntimeError(f"Static directory not found: {STATIC_DIR}")
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Store references in app state for route handlers
    app.state.storage = storage
    app.state.templates = templates
    app.state.logger = logger

    # === Routes ===

    @app.get("/", response_class=HTMLResponse)
    async def portal_home(request: Request):
        """Portal home page - list all jobs."""
        # Get running jobs directly from Ray State API (no registry needed)
        running_jobs = get_running_jobs_from_ray()

        completed_jobs = []
        try:
            completed_jobs = storage.list_jobs(status="COMPLETED", limit=20)
        except Exception:
            pass

        return templates.TemplateResponse(
            "portal.html",
            {
                "request": request,
                "running_jobs": running_jobs,
                "completed_jobs": completed_jobs,
                "ray_dashboard_url": get_ray_dashboard_url(),
            },
        )

    @app.get("/running", response_class=HTMLResponse)
    async def running_jobs_page(request: Request):
        """Running jobs list page."""
        # Get running jobs directly from Ray State API (no registry needed)
        running_jobs = get_running_jobs_from_ray()

        return templates.TemplateResponse(
            "running_jobs.html",
            {
                "request": request,
                "jobs": running_jobs,
            },
        )

    @app.get("/completed", response_class=HTMLResponse)
    async def completed_jobs_page(request: Request):
        """Completed jobs list page."""
        completed_jobs = []
        try:
            completed_jobs = storage.list_jobs(limit=100)
        except Exception:
            pass

        return templates.TemplateResponse(
            "completed_jobs.html",
            {
                "request": request,
                "jobs": completed_jobs,
            },
        )

    @app.get("/jobs/{job_id}/", response_class=HTMLResponse)
    async def job_detail_page(job_id: str, request: Request):
        """Job detail page."""
        # Check if job is running (using Ray State API, no registry needed)
        job_info = get_running_job_info(job_id)
        stages = job_info.get("stages", []) if job_info else []

        if job_info:
            return templates.TemplateResponse(
                "job_detail.html",
                {
                    "request": request,
                    "job": job_info,
                    "stages": stages,
                    "dag_edges": {},
                },
            )

        # Check historical data
        try:
            job_data = storage.get_job_archive(job_id)
            if job_data:
                return templates.TemplateResponse(
                    "job_detail.html",
                    {
                        "request": request,
                        "job": job_data,
                        "stages": job_data.get("stages", []),
                        "dag_edges": job_data.get("dag_edges", {}),
                    },
                )
        except Exception:
            pass

        return templates.TemplateResponse(
            "job_detail.html",
            {
                "request": request,
                "job": {"job_id": job_id, "status": "NOT_FOUND"},
                "stages": [],
                "dag_edges": {},
            },
        )

    @app.get("/jobs/{job_id}/stages/{stage_id}", response_class=HTMLResponse)
    async def stage_detail_page(job_id: str, stage_id: str, request: Request):
        """Stage detail page with workers and partition metrics."""
        stage_data = None
        workers = []
        partition_metrics = []

        # Try running job (get basic info from Ray State API)
        job_info = get_running_job_info(job_id)
        if job_info:
            for s in job_info.get("stages", []):
                if s.get("stage_id") == stage_id:
                    stage_data = s
                    workers = s.get("workers", [])
                    partition_metrics = s.get("partition_metrics", [])
                    break

        # Try historical data
        if not stage_data:
            try:
                job_archive = storage.get_job_archive(job_id)
                if job_archive:
                    for s in job_archive.get("stages", []):
                        if s.get("stage_id") == stage_id:
                            stage_data = s
                            break
            except Exception:
                pass

        if not stage_data:
            stage_data = {"stage_id": stage_id, "status": "NOT_FOUND"}

        return templates.TemplateResponse(
            "stage_detail.html",
            {
                "request": request,
                "job_id": job_id,
                "stage": stage_data,
                "workers": workers,
                "partition_metrics": partition_metrics,
            },
        )

    @app.get("/jobs/{job_id}/workers", response_class=HTMLResponse)
    async def workers_list_page(job_id: str, request: Request):
        """Workers list page - shows all workers for a job (running + historical)."""
        job_data = {"job_id": job_id, "status": "UNKNOWN"}
        stages = []
        all_workers = []
        workers_from_history = {}  # worker_id -> worker_data

        # First, get historical workers from storage (includes completed workers)
        try:
            historical_workers = storage.list_workers(job_id, limit=500)
            for w in historical_workers:
                workers_from_history[w.get("worker_id")] = w
        except Exception:
            pass

        # Then get running workers from Ray State API
        job_info = get_running_job_info(job_id)
        if job_info:
            job_data = job_info
            stages = job_info.get("stages", [])
            for s in stages:
                for w in s.get("workers", []):
                    worker_id = w.get("worker_id")
                    # Merge with historical data if available
                    if worker_id in workers_from_history:
                        merged = workers_from_history[worker_id].copy()
                        merged.update(w)  # Running data overrides
                        merged["stage_id"] = s.get("stage_id", "unknown")
                        workers_from_history[worker_id] = merged
                    else:
                        worker = dict(w)
                        worker["stage_id"] = s.get("stage_id", "unknown")
                        workers_from_history[worker_id] = worker

        # Fallback to archived job data for stages
        if not stages:
            try:
                job_archive = storage.get_job_archive(job_id)
                if job_archive:
                    job_data = job_archive
                    stages = job_archive.get("stages", [])
            except Exception:
                pass

        # Convert workers dict to list
        all_workers = list(workers_from_history.values())

        return templates.TemplateResponse(
            "workers.html",
            {
                "request": request,
                "job": job_data,
                "stages": stages,
                "workers": all_workers,
            },
        )

    @app.get("/jobs/{job_id}/workers/{worker_id}", response_class=HTMLResponse)
    async def worker_detail_page(job_id: str, worker_id: str, request: Request):
        """Worker detail page with full history."""
        import time as time_module

        worker_data = {"worker_id": worker_id, "stage_id": "", "status": "UNKNOWN"}
        worker_events = []

        # First try to get historical worker data from storage
        try:
            historical_data = storage.get_worker_history(job_id, worker_id)
            if historical_data:
                worker_data = historical_data
        except Exception:
            pass

        # Get worker events
        try:
            worker_events = storage.list_worker_events(job_id, worker_id=worker_id, limit=50)
        except Exception:
            pass

        # Try to get live worker data from running job via Ray State API
        job_info = get_running_job_info(job_id)
        if job_info:
            for stage in job_info.get("stages", []):
                for w in stage.get("workers", []):
                    if w.get("worker_id") == worker_id:
                        # Merge live data with historical data
                        worker_data.update(w)
                        worker_data["stage_id"] = stage.get("stage_id", "")
                        break
                if worker_data.get("stage_id"):
                    break

        # Try to get Ray actor info for additional details
        try:
            from ray.util.state import list_actors

            actors = list_actors(
                filters=[("class_name", "=", "StageWorker"), ("state", "=", "ALIVE")]
            )
            for actor in actors:
                if worker_id in actor.get("name", ""):
                    worker_data["actor_id"] = actor.get("actor_id")
                    worker_data["node_id"] = actor.get("node_id")
                    worker_data["pid"] = actor.get("pid")
                    break
        except Exception:
            pass

        return templates.TemplateResponse(
            "worker_detail.html",
            {
                "request": request,
                "job_id": job_id,
                "worker": worker_data,
                "worker_events": worker_events,
                "now": time_module.time(),
            },
        )

    @app.get("/jobs/{job_id}/exceptions", response_class=HTMLResponse)
    async def exceptions_page(job_id: str, request: Request):
        """Exceptions page."""
        exceptions = []
        try:
            exceptions = storage.list_exceptions(job_id, limit=100)
        except Exception:
            pass

        return templates.TemplateResponse(
            "exceptions.html",
            {
                "request": request,
                "job_id": job_id,
                "exceptions": exceptions,
            },
        )

    @app.get("/jobs/{job_id}/checkpoints", response_class=HTMLResponse)
    async def checkpoints_page(job_id: str, request: Request):
        """Checkpoints page."""
        return templates.TemplateResponse(
            "checkpoints.html",
            {
                "request": request,
                "job_id": job_id,
                "checkpoints": [],
            },
        )

    @app.get("/jobs/{job_id}/lineage", response_class=HTMLResponse)
    async def lineage_page(job_id: str, request: Request):
        """Lineage page."""
        return templates.TemplateResponse(
            "lineage.html",
            {
                "request": request,
                "job_id": job_id,
                "lineage": [],
            },
        )

    @app.get("/jobs/{job_id}/configuration", response_class=HTMLResponse)
    async def configuration_page(job_id: str, request: Request):
        """Configuration page showing job, stage, and environment settings."""
        import os

        config_data = {
            "job_config": {},
            "stage_configs": {},
            "ray_config": {},
            "environment": {},
        }

        # Try to get from running job (using Ray State API, no registry needed)
        job_info = get_running_job_info(job_id)
        if job_info:
            config_data["job_config"] = {
                "job_id": job_info.get("job_id"),
                "status": job_info.get("status"),
            }

            # Get stage configs
            for stage in job_info.get("stages", []):
                stage_id = stage.get("stage_id", "")
                if stage_id:
                    config_data["stage_configs"][stage_id] = {
                        "worker_count": stage.get("worker_count"),
                        "is_running": stage.get("is_running"),
                    }

            # Ray resources
            if ray.is_initialized():
                config_data["ray_config"] = {
                    "cluster_resources": ray.cluster_resources(),
                    "available_resources": ray.available_resources(),
                }

            # Environment
            config_data["environment"] = {
                "SOLSTICE_LOG_LEVEL": os.getenv("SOLSTICE_LOG_LEVEL", "INFO"),
                "RAY_PROMETHEUS_HOST": os.getenv("RAY_PROMETHEUS_HOST"),
                "SOLSTICE_GRAFANA_URL": os.getenv("SOLSTICE_GRAFANA_URL"),
            }

        # Fallback to storage
        if not config_data["job_config"]:
            try:
                job_archive = storage.get_job_archive(job_id)
                if job_archive:
                    config_data = job_archive.get("config", config_data)
            except Exception:
                pass

        return templates.TemplateResponse(
            "configuration.html",
            {
                "request": request,
                "job_id": job_id,
                "config": config_data,
            },
        )

    @app.get("/api/jobs")
    async def api_list_jobs():
        """API endpoint for listing jobs."""
        # Get running jobs from Ray State API (no registry needed)
        running = get_running_jobs_from_ray()

        completed = []
        try:
            completed = storage.list_jobs(limit=100)
        except Exception:
            pass

        return {
            "running": running,
            "completed": completed,
        }

    @app.get("/api/jobs/{job_id}/stages")
    async def api_list_stages(job_id: str):
        """API endpoint for listing stages of a job.

        For running jobs, gets data from Ray State API.
        For completed jobs, gets data from storage.
        """

        def transform_stages_for_ui(stages: list) -> list:
            """Transform stage data to have consistent field names for the UI."""
            transformed = []
            for stage in stages:
                # Extract metrics from final_metrics or direct fields
                metrics = stage.get("final_metrics", {})
                transformed.append(
                    {
                        "stage_id": stage.get("stage_id", ""),
                        "operator_type": stage.get("operator_type", ""),
                        "worker_count": stage.get("final_worker_count", 0)
                        or metrics.get("worker_count", 0),
                        "is_running": not stage.get("is_finished", True),
                        "is_finished": stage.get("is_finished", False),
                        "failed": stage.get("failed", False),
                        "input_count": metrics.get("input_records", 0),
                        "output_count": metrics.get("output_records", 0),
                        "output_queue_size": metrics.get("output_buffer_size", 0),
                    }
                )
            return transformed

        # First check storage for completed jobs (more detailed data)
        try:
            job_data = storage.get_job_archive(job_id)
            if job_data:
                stages = job_data.get("stages", [])
                return {
                    "job_id": job_id,
                    "stages": transform_stages_for_ui(stages),
                    "dag_edges": job_data.get("dag_edges", {}),
                }
        except Exception:
            pass

        # For running jobs, get from JobRegistry (updated by MetricsCollector)
        job_info = get_running_job_info(job_id)
        if job_info:
            # Registry stages already have input_count and output_count
            # DAG edges are stored in the registry since registration
            return {
                "job_id": job_id,
                "stages": job_info.get("stages", []),
                "dag_edges": job_info.get("dag_edges", {}),
            }

        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    @app.get("/health")
    async def health():
        """Health check."""
        return {"status": "ok", "service": "portal"}

    logger.info(f"Portal app created with storage: {storage_path}")
    return app


# Global app instance - will be set when Portal is started
_portal_app = None


@serve.deployment(
    name="solstice-portal",
    ray_actor_options={"num_cpus": 0.1, "num_gpus": 0},
)
class SolsticePortal:
    """Global WebUI portal for all Solstice jobs.

    Uses FastAPI app created by create_portal_app().
    Ray Serve handles ASGI forwarding via @serve.ingress pattern.
    """

    def __init__(self, storage_path: str):
        """Initialize portal with FastAPI app."""
        self.app = create_portal_app(storage_path)

    async def __call__(self, request: Request):
        """Handle HTTP request by forwarding to FastAPI app."""
        # Build ASGI scope from Starlette Request
        scope = request.scope
        receive = request.receive

        # Create a response collector
        response_started = False
        status_code = 200
        response_headers = []
        body_parts = []

        async def send(message):
            nonlocal response_started, status_code, response_headers
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                response_headers = message.get("headers", [])
            elif message["type"] == "http.response.body":
                body_parts.append(message.get("body", b""))

        await self.app(scope, receive, send)

        # Build response - decode headers from bytes to strings
        from starlette.responses import Response

        body = b"".join(body_parts)
        # ASGI headers are [(bytes, bytes), ...], convert to {str: str}
        headers = {
            k.decode("latin-1") if isinstance(k, bytes) else k: v.decode("latin-1")
            if isinstance(v, bytes)
            else v
            for k, v in response_headers
        }
        return Response(content=body, status_code=status_code, headers=headers)


def start_portal(storage_path: str, port: int = 8000) -> str:
    """Start the global Solstice Portal service.

    This function:
    1. Starts Ray Serve if not already running
    2. Deploys the SolsticePortal deployment
    3. Returns the portal path (relative)

    Args:
        storage_path: SlateDB storage path for historical data
        port: HTTP port for Ray Serve

    Returns:
        Portal path (e.g., "/solstice")
    """
    logger = create_ray_logger("PortalStarter")

    # Start Ray Serve if not running
    try:
        serve.start(
            detached=True,
            http_options={"host": "0.0.0.0", "port": port},
        )
        logger.info(f"Started Ray Serve on port {port}")
    except Exception as e:
        # Already running is OK
        logger.info(f"Ray Serve already running: {e}")

    # Deploy portal
    try:
        handle = SolsticePortal.bind(storage_path)
        serve.run(handle, name="solstice-portal", route_prefix="/solstice")
        logger.info(f"Deployed Solstice Portal with storage: {storage_path}")
    except Exception as e:
        logger.warning(f"Failed to deploy portal: {e}")
        raise

    path = "/solstice"
    logger.info(f"Portal deployed at Ray Serve port {port}, path: {path}")
    return path


def portal_exists() -> bool:
    """Check if portal is already deployed.

    Returns:
        True if portal deployment exists, False otherwise
    """
    try:
        # Check if application exists
        status = serve.status()
        return "solstice-portal" in status.applications
    except Exception:
        return False
