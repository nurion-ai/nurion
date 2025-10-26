"""Application factory for the Aether FastAPI service."""

from fastapi import FastAPI

from .api.routes import register_routes
from .core.settings import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """Construct a configured FastAPI application instance."""

    app = FastAPI(  # noqa: FBT003 - explicit bool for clarity
        title="Aether Data Platform",
        description=(
            "Task orchestration, Kubernetes management, and data lake catalog services."
        ),
        version="0.1.0",
    )

    app.state.settings = settings or get_settings()

    register_routes(app)

    return app

