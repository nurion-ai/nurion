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
from solstice.webui.registry import get_or_create_registry
from solstice.webui.storage import SlateDBStorage
from solstice.utils.logging import create_ray_logger

WEBUI_DIR = Path(__file__).parent
TEMPLATES_DIR = WEBUI_DIR / "templates"
STATIC_DIR = WEBUI_DIR / "static"


def create_portal_app(storage_path: str) -> FastAPI:
    """Create the Portal FastAPI application.

    This function creates and configures the FastAPI app with all routes.
    It's called once when the Portal deployment is created.
    """
    storage = SlateDBStorage(storage_path)
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
        running_jobs = []
        try:
            registry = get_or_create_registry()
            jobs_dict = ray.get(registry.list_jobs.remote(), timeout=2)
            running_jobs = list(jobs_dict.values())
        except Exception:
            pass

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
        running_jobs = []
        try:
            registry = get_or_create_registry()
            jobs_dict = ray.get(registry.list_jobs.remote(), timeout=2)
            running_jobs = list(jobs_dict.values())
        except Exception:
            pass

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
        job_info = None
        stages = []

        # Check if job is running
        try:
            registry = get_or_create_registry()
            job_info = ray.get(registry.get_job.remote(job_id), timeout=1)
            if job_info:
                stages = job_info.stages if hasattr(job_info, "stages") else []
        except Exception:
            pass

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
        """Stage detail page."""
        stage_data = None

        # Try running job
        try:
            registry = get_or_create_registry()
            job_reg = ray.get(registry.get_job.remote(job_id), timeout=1)
            if job_reg and hasattr(job_reg, "stages"):
                for s in job_reg.stages:
                    if hasattr(s, "stage_id") and s.stage_id == stage_id:
                        stage_data = s
                        break
                    elif isinstance(s, dict) and s.get("stage_id") == stage_id:
                        stage_data = s
                        break
        except Exception:
            pass

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
            },
        )

    @app.get("/jobs/{job_id}/workers/{worker_id}", response_class=HTMLResponse)
    async def worker_detail_page(job_id: str, worker_id: str, request: Request):
        """Worker detail page."""
        worker_data = {"worker_id": worker_id, "stage_id": "", "status": "UNKNOWN"}

        try:
            events = storage.list_worker_events(job_id, worker_id=worker_id, limit=1)
            if events:
                worker_data.update(events[0])
        except Exception:
            pass

        return templates.TemplateResponse(
            "worker_detail.html",
            {
                "request": request,
                "job_id": job_id,
                "worker": worker_data,
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

    @app.get("/api/jobs")
    async def api_list_jobs():
        """API endpoint for listing jobs."""
        running = []
        try:
            registry = get_or_create_registry()
            jobs_dict = ray.get(registry.list_jobs.remote(), timeout=2)
            running = [j.to_dict() for j in jobs_dict.values()]
        except Exception:
            pass

        completed = []
        try:
            completed = storage.list_jobs(limit=100)
        except Exception:
            pass

        return {
            "running": running,
            "completed": completed,
        }

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
