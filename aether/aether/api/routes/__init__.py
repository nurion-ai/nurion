"""API route registration for the platform service."""

from fastapi import APIRouter, FastAPI

from . import health, iceberg_catalog, k8s, lance_namespace


def register_routes(app: FastAPI) -> None:
    """Attach all route groups to the FastAPI application."""

    api_router = APIRouter(prefix="/api")
    api_router.include_router(health.router, tags=["health"])
    api_router.include_router(lance_namespace.router)
    api_router.include_router(iceberg_catalog.router)
    api_router.include_router(k8s.router)

    app.include_router(api_router)


__all__ = ["register_routes"]
