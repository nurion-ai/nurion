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

"""FastAPI application factory for Solstice WebUI."""

import os
from pathlib import Path
from typing import Literal, Optional, TYPE_CHECKING

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from solstice.webui.storage import SlateDBStorage
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner


# Paths
WEBUI_DIR = Path(__file__).parent
TEMPLATES_DIR = WEBUI_DIR / "templates"
STATIC_DIR = WEBUI_DIR / "static"


def create_app(
    mode: Literal["embedded", "history"] = "embedded",
    storage: Optional[SlateDBStorage] = None,
    job_runner: Optional["RayJobRunner"] = None,
) -> FastAPI:
    """Create WebUI FastAPI application.

    Args:
        mode: Running mode
            - "embedded": Embedded mode, runs with job, can access real-time data
            - "history": History Server mode, read-only historical data
        storage: SlateDB storage instance (required for history mode)
        job_runner: RayJobRunner instance (required for embedded mode)

    Returns:
        FastAPI application

    Raises:
        ValueError: If required dependencies are missing for the mode
    """
    logger = create_ray_logger("WebUIApp")

    # Validate mode-specific requirements
    if mode == "history" and storage is None:
        raise ValueError("History mode requires storage parameter")
    if mode == "embedded" and job_runner is None:
        raise ValueError("Embedded mode requires job_runner parameter")

    app = FastAPI(
        title="Solstice Debug UI",
        description="Solstice streaming job debugging interface",
        version="0.1.0",
    )

    # Inject state
    app.state.mode = mode
    app.state.storage = storage
    app.state.job_runner = job_runner

    logger.info(f"Creating WebUI app in {mode} mode")

    # Mount static files (required)
    if not STATIC_DIR.exists():
        raise RuntimeError(f"Static directory not found: {STATIC_DIR}")
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Set up templates (required)
    if not TEMPLATES_DIR.exists():
        raise RuntimeError(f"Templates directory not found: {TEMPLATES_DIR}")
    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    setup_template_filters(app.state.templates)

    # Register routes based on mode
    if mode == "embedded":
        from solstice.webui.api import realtime

        app.include_router(realtime.router, prefix="/api")
        logger.info("Registered real-time API routes")

    # Shared routes (both modes)
    from solstice.webui.api import (
        overview,
        jobs,
        stages,
        workers,
        exceptions,
        lineage,
        configuration,
        events,
    )

    app.include_router(overview.router, prefix="/api")
    app.include_router(jobs.router, prefix="/api")
    app.include_router(stages.router, prefix="/api")
    app.include_router(workers.router, prefix="/api")
    app.include_router(exceptions.router, prefix="/api")
    app.include_router(lineage.router, prefix="/api")
    app.include_router(configuration.router, prefix="/api")
    app.include_router(events.router, prefix="/api")

    logger.info("Registered API routes")

    # Health check endpoint
    @app.get("/health")
    async def health_check():
        return {"status": "ok", "mode": mode}

    # Root redirect
    @app.get("/")
    async def root():
        from fastapi.responses import RedirectResponse

        return RedirectResponse(url="/overview")

    return app


def get_ray_dashboard_url() -> str:
    """Get Ray Dashboard URL for external links.

    Returns:
        Ray Dashboard URL (defaults to http://localhost:8265)
    """
    import ray

    # Get from environment or use default
    if ray.is_initialized():
        # Ray Dashboard typically runs on port 8265
        return os.getenv("RAY_DASHBOARD_URL", "http://localhost:8265")

    return os.getenv("RAY_DASHBOARD_URL", "http://localhost:8265")


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2h 15m", "45s", "1.2s")
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_bytes(bytes_value: int) -> str:
    """Format bytes in human-readable form.

    Args:
        bytes_value: Size in bytes

    Returns:
        Formatted string (e.g., "1.2GB", "45MB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f}{unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f}PB"


def format_number(num: int) -> str:
    """Format large numbers with abbreviations.

    Args:
        num: Number to format

    Returns:
        Formatted string (e.g., "1.2M", "45K")
    """
    if num >= 1e9:
        return f"{num / 1e9:.1f}B"
    elif num >= 1e6:
        return f"{num / 1e6:.1f}M"
    elif num >= 1e3:
        return f"{num / 1e3:.1f}K"
    else:
        return str(num)


def format_datetime(timestamp: float) -> str:
    """Format Unix timestamp as human-readable datetime.

    Args:
        timestamp: Unix timestamp in seconds

    Returns:
        Formatted string (e.g., "2026-01-06 12:53:23")
    """
    from datetime import datetime

    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError, TypeError):
        return "Unknown"


# Register template filters
def setup_template_filters(templates: Jinja2Templates):
    """Register custom template filters."""
    templates.env.filters["format_duration"] = format_duration
    templates.env.filters["format_bytes"] = format_bytes
    templates.env.filters["format_number"] = format_number
    templates.env.filters["format_datetime"] = format_datetime
