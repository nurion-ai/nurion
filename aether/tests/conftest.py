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

"""Shared test fixtures for aether tests.

Provides testcontainer-based fixtures for:
- PostgreSQL database
- MinIO object storage
- FastAPI test application with real HTTP server for SDK testing
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import AsyncGenerator, Generator
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
import requests
import uvicorn
from minio import Minio
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from aether.core.settings import IcebergCatalogSettings, Settings

# Import all models to ensure they're registered with the metadata
from aether.models import iceberg, k8s, lance  # noqa: F401
from aether.models.base import BaseModel

if TYPE_CHECKING:
    pass


def _find_free_port() -> int:
    """Find an available port on localhost."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# ============================================================================
# Module-scoped container fixtures (shared across tests in a module)
# ============================================================================


@pytest.fixture(scope="module")
def postgres_container():
    """Start PostgreSQL container for the test module."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def minio_container():
    """Start MinIO container for S3-compatible object storage."""
    with MinioContainer() as minio:
        # Create the warehouse bucket using the new minio client API (keyword-only args)
        host_ip = minio.get_container_host_ip()
        exposed_port = minio.get_exposed_port(9000)
        minio_client = Minio(
            endpoint=f"{host_ip}:{exposed_port}",
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            secure=False,
        )
        bucket_name = "warehouse"
        if not minio_client.bucket_exists(bucket_name=bucket_name):
            minio_client.make_bucket(bucket_name=bucket_name)
        yield minio


@pytest.fixture(scope="module")
def database_url(postgres_container) -> str:
    """Get async database URL from PostgreSQL container."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"


@pytest.fixture(scope="module")
def sync_database_url(postgres_container) -> str:
    """Get sync database URL from PostgreSQL container for setup."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname
    # Use psycopg (v3) driver
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"


@pytest.fixture(scope="module")
def minio_endpoint(minio_container) -> str:
    """Get MinIO endpoint URL."""
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="module")
def test_settings(database_url: str, minio_endpoint: str) -> Settings:
    """Create test settings with container endpoints."""
    iceberg_settings = IcebergCatalogSettings(
        storage_backend="s3",
        warehouse="s3://warehouse",
        s3_endpoint=minio_endpoint,
        s3_external_endpoint=minio_endpoint,
        s3_access_key_id="minioadmin",
        s3_secret_access_key="minioadmin",
        s3_region="us-east-1",
    )
    return Settings(
        app_name="Aether Test",
        environment="test",
        database_url=database_url,
        iceberg=iceberg_settings,
    )


@pytest.fixture(scope="module")
def db_engine(database_url: str):
    """Create async database engine."""
    engine = create_async_engine(database_url, echo=False)
    yield engine


@pytest.fixture(scope="module")
def db_session_factory(db_engine):
    """Create session factory."""
    return async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)


# ============================================================================
# Module-scoped fixture for running the app server
# ============================================================================


@pytest.fixture(scope="module")
def app_server(
    test_settings: Settings,
    sync_database_url: str,
    database_url: str,
    minio_endpoint: str,
) -> Generator[str]:
    """Run FastAPI app in a background thread and return the base URL.

    This fixture runs the complete app with a real HTTP server, suitable for
    testing with external clients like pyiceberg and lance-namespace SDKs.
    """
    import asyncio
    import os

    from aether.core.settings import get_settings
    from aether.db import session as db_session_module
    from aether.services.iceberg_catalog_service import clear_catalog_cache

    # Clear caches and set environment variables so get_settings() and SqlCatalog pick them up
    get_settings.cache_clear()
    clear_catalog_cache()

    # Use local storage for tests to avoid pyarrow/MinIO multipart upload issues
    import tempfile

    test_warehouse_dir = tempfile.mkdtemp(prefix="iceberg_test_")

    # Set environment variables for settings
    os.environ["DATABASE_URL"] = database_url
    os.environ["ICEBERG__STORAGE_BACKEND"] = "local"
    os.environ["ICEBERG__LOCAL_ROOT_PATH"] = test_warehouse_dir
    os.environ["ICEBERG__WAREHOUSE"] = f"file://{test_warehouse_dir}"

    # Initialize database tables using sync engine (to avoid event loop issues)
    sync_engine = create_engine(sync_database_url, echo=False)
    BaseModel.metadata.drop_all(sync_engine)
    BaseModel.metadata.create_all(sync_engine)

    # Insert default lance namespace using raw SQL
    with sync_engine.connect() as conn:
        conn.execute(
            text("""
            INSERT INTO catalog_namespaces
                (name, description, delimiter, properties, created_at, updated_at)
            VALUES ('default', 'Default namespace', '.', '{}', NOW(), NOW())
            ON CONFLICT (name) DO NOTHING
        """)
        )
        conn.commit()
    sync_engine.dispose()

    # Create the Iceberg default namespace using pyiceberg's SqlCatalog
    # This will create SqlCatalog's own tables and the default namespace
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    iceberg_catalog = SqlCatalog(
        "aether_catalog",  # Must match the catalog name in iceberg_catalog_service.py
        **{
            "uri": sync_database_url,
            "warehouse": f"file://{test_warehouse_dir}",
        },
    )
    try:
        iceberg_catalog.create_namespace(("default",))
    except NamespaceAlreadyExistsError:
        pass  # Already exists

    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"

    # Save original values
    original_engine = db_session_module.async_engine
    original_factory = db_session_module.async_session_factory

    # Create new engine and factory for the test database
    test_engine = create_async_engine(database_url, echo=False)
    test_factory = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

    # Patch the module globals
    db_session_module.async_engine = test_engine
    db_session_module.async_session_factory = test_factory

    # Import app after patching
    from aether.app import create_app

    # Create app without lifespan (we already initialized the database)
    app = create_app(settings=test_settings, skip_lifespan=True)

    # Create a uvicorn server
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)

    # Event to signal server is ready
    server_started = threading.Event()
    server_error = None

    def run_server():
        nonlocal server_error
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Mark server as started once the event loop begins
            async def serve_with_signal():
                server_started.set()
                await server.serve()

            loop.run_until_complete(serve_with_signal())
        except Exception as e:
            server_error = e
            server_started.set()

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

    # Wait for the server to start
    server_started.wait(timeout=5)

    if server_error:
        raise RuntimeError(f"Server failed to start: {server_error}")

    # Poll until the server is ready
    for _ in range(100):
        try:
            resp = requests.get(f"{base_url}/api/health", timeout=1)
            if resp.status_code == 200:
                break
        except Exception:
            time.sleep(0.1)
    else:
        raise RuntimeError("Server failed to respond within 10 seconds")

    try:
        yield base_url
    finally:
        # Restore original values
        db_session_module.async_engine = original_engine
        db_session_module.async_session_factory = original_factory
        # Signal server to stop
        server.should_exit = True


# ============================================================================
# Function-scoped fixtures (fresh for each test)
# ============================================================================


@pytest.fixture
async def db_session(db_engine, db_session_factory) -> AsyncGenerator[AsyncSession]:
    """Create a fresh database session with clean tables for each test."""
    from aether.services import lance_table_service

    # Recreate tables for each test
    async with db_engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)

    async with db_session_factory() as session:
        # Ensure default lance namespace exists
        await lance_table_service.ensure_default_namespace(session)
        yield session
        await session.rollback()


# ============================================================================
# Markers
# ============================================================================


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
