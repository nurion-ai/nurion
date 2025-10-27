"""Application factory for the Aether FastAPI service."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from sqlalchemy.exc import OperationalError

from .api.routes import register_routes
from .core.settings import Settings, get_settings
from .db.session import async_engine, async_session_factory
from .models.base import BaseModel
from .services import catalog_service

logger = logging.getLogger(__name__)


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

    @app.on_event("startup")
    async def on_startup() -> None:  # pragma: no cover - startup hook
        logger.info("Running database migrations...")
        try:
            async with async_engine.begin() as conn:
                await conn.run_sync(BaseModel.metadata.create_all)
        except OperationalError as exc:  # pragma: no cover - defensive logging
            logger.error("Database migration failed: %s", exc)
            raise

        async with async_session_factory() as session:
            await catalog_service.ensure_default_namespace(session)

    register_routes(app)

    return app

