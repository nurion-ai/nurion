"""Application factory for the Aether FastAPI service."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.exc import OperationalError

from .api.routes import register_routes
from .core.settings import Settings, get_settings
from .db.session import async_engine, async_session_factory
from .models.base import BaseModel
from .services import lance_table_service
from .services.iceberg_catalog_service import IcebergCatalogService
from .services.rayjob_sync_service import start_sync_service, stop_sync_service

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None, *, skip_lifespan: bool = False) -> FastAPI:
    """Construct a configured FastAPI application instance.

    Args:
        settings: Application settings. If None, uses default settings.
        skip_lifespan: If True, skip the startup/shutdown lifecycle hooks.
            Useful for testing when the database is already initialized.
    """

    app = FastAPI(  # noqa: FBT003 - explicit bool for clarity
        title="Aether Data Platform",
        description=("Task orchestration, Kubernetes management, and data lake catalog services."),
        version="0.1.0",
    )

    app.state.settings = settings or get_settings()

    if not skip_lifespan:

        @asynccontextmanager
        async def lifespan(_: FastAPI):
            await on_startup()
            try:
                yield
            finally:
                await on_shutdown()

        app.router.lifespan_context = lifespan

    register_routes(app)

    return app


async def on_startup() -> None:
    """Startup hook registered via lifespan."""
    logger.info("Running database migrations...")
    try:
        async with async_engine.begin() as conn:
            await conn.run_sync(BaseModel.metadata.create_all)
    except OperationalError as exc:  # pragma: no cover - defensive logging
        logger.error("Database migration failed: %s", exc)
        raise

    async with async_session_factory() as session:
        await lance_table_service.ensure_default_namespace(session)

    # Create default Iceberg namespace using SqlCatalog
    try:
        import asyncio

        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        iceberg_service = IcebergCatalogService()
        await asyncio.to_thread(
            iceberg_service.catalog.create_namespace,
            ("default",),
        )
        logger.info("Created default Iceberg namespace")
    except NamespaceAlreadyExistsError:
        logger.info("Default Iceberg namespace already exists")
    except Exception as exc:
        logger.warning("Failed to create default Iceberg namespace: %s", exc)

    # Start RayJob sync service
    logger.info("Starting RayJob sync service...")
    await start_sync_service()


async def on_shutdown() -> None:
    """Shutdown hook registered via lifespan."""
    logger.info("Stopping RayJob sync service...")
    await stop_sync_service()
