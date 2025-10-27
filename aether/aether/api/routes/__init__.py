"""API route registration for the platform service."""

from fastapi import APIRouter, FastAPI

from . import health, lance_namespace


def register_routes(app: FastAPI) -> None:
    """Attach all route groups to the FastAPI application."""

    api_router = APIRouter(prefix="/api")
    api_router.include_router(health.router, tags=["health"])
    api_router.include_router(lance_namespace.router)

    app.include_router(api_router)


__all__ = ["register_routes"]
