from __future__ import annotations

"""Shared test fixtures."""

import hashlib
import random
import uuid

import pytest_asyncio

from solstice.core.split_payload_store import RaySplitPayloadStore
from solstice.queue import TansuBackend


@pytest_asyncio.fixture
async def tansu_backend():
    """Start a real TansuBackend backed by in-memory storage."""
    port = 10000 + random.randint(0, 9999)
    backend = TansuBackend(storage_url="memory://tansu/", port=port)
    await backend.start()
    try:
        yield backend
    finally:
        await backend.stop()
"""Pytest configuration and fixtures for Solstice tests.

Provides testcontainer-based fixtures for integration tests:
- PostgreSQL database
- MinIO object storage
- Aether REST catalog server
- Ray cluster fixtures
"""

import os
import socket
import sys
import tempfile
import threading
import time
from collections.abc import Generator
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
import ray

if TYPE_CHECKING:
    pass


# ============================================================================
# Common excludes for Ray runtime environment
# ============================================================================

RAY_RUNTIME_EXCLUDES = [
    # Exclude virtual environments to prevent Python version conflicts
    "**/.venv/**",
    ".venv/**",
    # Exclude uv/pip config to prevent auto-creating venvs on workers
    # .python-version specifies 3.12 but we run 3.13, causing version mismatch
    "**/.python-version",
    "**/pyproject.toml",
    "**/uv.lock",
    "**/poetry.lock",
    "**/requirements.txt",
    # Cache and build directories
    "**/__pycache__/**",
    "**/.git/**",
    "**/.pytest_cache/**",
    # Large files
    "**/java/**",
    "**/raydp/jars/**",
    "**/tests/testdata/resources/videos/**",
    "**/tests/testdata/resources/tmp/**",
    "**/*.jar",
    "**/*.mp4",
    "**/*.tar.gz",
    "**/*.lance",
    "**/*.pyc",
]


def _find_free_port() -> int:
    """Find an available port on localhost."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def ensure_spark_testdata():
    """Ensure Spark test data files exist before any tests run."""
    try:
        from tests.testdata.generate_spark_testdata import ensure_spark_testdata as generate

        generate()
    except ImportError:
        # Dependencies not installed, skip testdata generation
        pass


# ============================================================================
# Testcontainer fixtures for integration tests
# ============================================================================


@pytest.fixture(scope="module")
def postgres_container():
    """Start PostgreSQL container for the test module."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def minio_container():
    """Start MinIO container for S3-compatible object storage."""
    from testcontainers.minio import MinioContainer
    from minio import Minio

    with MinioContainer() as minio:
        # Create the warehouse bucket
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
def minio_endpoint(minio_container) -> str:
    """Get MinIO endpoint URL."""
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="module")
def minio_credentials(minio_container) -> dict:
    """Get MinIO credentials."""
    return {
        "access_key": minio_container.access_key,
        "secret_key": minio_container.secret_key,
    }


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
    """Get sync database URL from PostgreSQL container."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"


@pytest.fixture(scope="module")
def aether_server(
    postgres_container,
    minio_container,
    database_url: str,
    sync_database_url: str,
    minio_endpoint: str,
    minio_credentials: dict,
) -> Generator[str, None, None]:
    """Start Aether REST catalog server and return base URL.

    This fixture starts the aether app with testcontainer backends,
    suitable for testing Iceberg and Lance catalog operations.
    """
    import requests
    import uvicorn
    from sqlalchemy import create_engine, text
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    # Add aether to path
    aether_path = os.path.join(os.path.dirname(__file__), "..", "..", "aether")
    if aether_path not in sys.path:
        sys.path.insert(0, aether_path)

    from aether.app import create_app
    from aether.core.settings import IcebergCatalogSettings, Settings, get_settings
    from aether.db import session as db_session_module
    from aether.models.base import BaseModel
    from aether.services.iceberg_catalog_service import clear_catalog_cache

    # Clear caches
    get_settings.cache_clear()
    clear_catalog_cache()

    # Use local storage for Iceberg to avoid multipart upload issues
    test_warehouse_dir = tempfile.mkdtemp(prefix="iceberg_test_")

    # Set environment variables
    os.environ["DATABASE_URL"] = database_url
    os.environ["ICEBERG__STORAGE_BACKEND"] = "local"
    os.environ["ICEBERG__LOCAL_ROOT_PATH"] = test_warehouse_dir
    os.environ["ICEBERG__WAREHOUSE"] = f"file://{test_warehouse_dir}"

    # Create settings
    iceberg_settings = IcebergCatalogSettings(
        storage_backend="local",
        warehouse=f"file://{test_warehouse_dir}",
        local_root_path=test_warehouse_dir,
    )
    test_settings = Settings(
        app_name="Aether Test",
        environment="test",
        database_url=database_url,
        iceberg=iceberg_settings,
    )

    # Initialize database
    sync_engine = create_engine(sync_database_url, echo=False)
    BaseModel.metadata.drop_all(sync_engine)
    BaseModel.metadata.create_all(sync_engine)

    # Insert default lance namespace
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

    # Create Iceberg default namespace
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    iceberg_catalog = SqlCatalog(
        "aether_catalog",
        **{
            "uri": sync_database_url,
            "warehouse": f"file://{test_warehouse_dir}",
        },
    )
    try:
        iceberg_catalog.create_namespace(("default",))
    except NamespaceAlreadyExistsError:
        pass

    # Find free port
    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"

    # Patch database module
    original_engine = db_session_module.async_engine
    original_factory = db_session_module.async_session_factory

    test_engine = create_async_engine(database_url, echo=False)
    test_factory = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

    db_session_module.async_engine = test_engine
    db_session_module.async_session_factory = test_factory

    # Create and run app
    app = create_app(settings=test_settings, skip_lifespan=True)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)

    server_started = threading.Event()
    server_error = None

    def run_server():
        nonlocal server_error
        try:
            import asyncio

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def serve_with_signal():
                server_started.set()
                await server.serve()

            loop.run_until_complete(serve_with_signal())
        except Exception as e:
            server_error = e
            server_started.set()

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

    server_started.wait(timeout=10)

    if server_error:
        raise RuntimeError(f"Aether server failed to start: {server_error}")

    # Wait for server to be ready
    for _ in range(100):
        try:
            resp = requests.get(f"{base_url}/api/health", timeout=1)
            if resp.status_code == 200:
                break
        except Exception:
            time.sleep(0.1)
    else:
        raise RuntimeError("Aether server failed to respond within 10 seconds")

    try:
        yield base_url
    finally:
        db_session_module.async_engine = original_engine
        db_session_module.async_session_factory = original_factory
        server.should_exit = True


@pytest.fixture(scope="module")
def iceberg_catalog_uri(aether_server: str) -> str:
    """Get Iceberg REST catalog URI."""
    return f"{aether_server}/api/iceberg-catalog"


@pytest.fixture(scope="module")
def lance_namespace_uri(aether_server: str) -> str:
    """Get Lance namespace API URI."""
    return f"{aether_server}/api/lance-namespace"


# ============================================================================
# S3 storage options fixture
# ============================================================================


@pytest.fixture(scope="module")
def s3_storage_options(minio_endpoint: str, minio_credentials: dict) -> dict:
    """Get S3 storage options for Lance/PyArrow."""
    return {
        "aws_access_key_id": minio_credentials["access_key"],
        "aws_secret_access_key": minio_credentials["secret_key"],
        "aws_endpoint": minio_endpoint,
        "aws_region": "us-east-1",
        "allow_http": "true",
    }


# ============================================================================
# Ray cluster fixture
# ============================================================================


@pytest.fixture(scope="module")
def ray_cluster():
    """Initialize Ray cluster with unified configuration.

    - num_cpus=4
    - Includes raydp JARs if available
    - Excludes large files from runtime environment
    """
    from ray.job_config import JobConfig

    if ray.is_initialized():
        ray.shutdown()

    # Try to get raydp jars if available
    jars_paths = []
    try:
        from raydp.utils import code_search_path
        jars_paths = code_search_path()
    except ImportError:
        pass

    job_config = JobConfig(code_search_path=jars_paths) if jars_paths else None

    ray.init(
        num_cpus=4,
        job_config=job_config,
        runtime_env={"excludes": RAY_RUNTIME_EXCLUDES},
        ignore_reinit_error=True,
    )

    yield

    # Cleanup Spark if running
    try:
        import raydp
        raydp.stop_spark()
    except Exception:
        pass
    ray.shutdown()


@pytest_asyncio.fixture
async def payload_store(ray_cluster, request):
    """Create a unique RaySplitPayloadStore for each test to avoid name collisions."""
    test_name = request.node.name.replace("[", "_").replace("]", "_")
    unique = hashlib.md5(test_name.encode()).hexdigest()[:8] if test_name else str(
        uuid.uuid4()
    )[:8]
    store = RaySplitPayloadStore(name=f"test_store_{unique}")
    yield store
    # Ray handles cleanup
