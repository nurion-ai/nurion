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

"""Integration tests for K8s cluster services using testcontainers.

Tests:
- k8s_cluster_service: Cluster CRUD operations
- K8s database models
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from aether.models.base import BaseModel
from aether.models.k8s import K8sCluster, RayJob
from aether.schemas.k8s import ClusterConfigCreate, ClusterConfigUpdate
from aether.services import k8s_cluster_service
from aether.services.k8s_connection import clear_client_cache

pytestmark = pytest.mark.integration


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="module")
def postgres_container():
    """Start PostgreSQL container."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
async def db_session(postgres_container):
    """Create database session with clean tables for each test."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname

    async_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_async_engine(async_url, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()

    await engine.dispose()


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear K8s client caches."""
    clear_client_cache()
    yield
    clear_client_cache()


@pytest.fixture
def mock_kubeconfig() -> str:
    """Return a mock kubeconfig for DB-only tests."""
    return """
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://mock-server:6443
  name: mock-cluster
contexts:
- context:
    cluster: mock-cluster
    user: mock-user
  name: mock-context
current-context: mock-context
users:
- name: mock-user
  user:
    token: mock-token
"""


# ============================================================================
# k8s_cluster_service Tests
# ============================================================================


class TestK8sClusterService:
    """Tests for k8s_cluster_service."""

    @pytest.mark.asyncio
    async def test_create_cluster(self, db_session, mock_kubeconfig):
        """Test creating a cluster."""
        request = ClusterConfigCreate(
            name="test-create",
            kubeconfig=mock_kubeconfig,
            default_queue="default-queue",
            default_ray_image="rayproject/ray:2.50.0",
            is_default=True,
        )

        result = await k8s_cluster_service.create_cluster(request, db_session)

        assert result.cluster.name == "test-create"
        assert result.cluster.default_queue == "default-queue"
        assert result.cluster.is_default is True
        assert result.cluster.kubeconfig is not None

    @pytest.mark.asyncio
    async def test_list_clusters(self, db_session, mock_kubeconfig):
        """Test listing multiple clusters."""
        for i in range(3):
            request = ClusterConfigCreate(
                name=f"test-list-{i}",
                kubeconfig=mock_kubeconfig,
                default_queue=f"queue-{i}",
                default_ray_image="rayproject/ray:2.50.0",
            )
            await k8s_cluster_service.create_cluster(request, db_session)

        result = await k8s_cluster_service.list_clusters(db_session)

        assert len(result.clusters) >= 3
        names = [c.name for c in result.clusters]
        assert all(f"test-list-{i}" in names for i in range(3))

    @pytest.mark.asyncio
    async def test_get_cluster_by_name(self, db_session, mock_kubeconfig):
        """Test getting cluster by name."""
        request = ClusterConfigCreate(
            name="test-get-by-name",
            kubeconfig=mock_kubeconfig,
            default_queue="test-queue",
            default_ray_image="rayproject/ray:2.50.0",
        )
        await k8s_cluster_service.create_cluster(request, db_session)

        result = await k8s_cluster_service.get_cluster("test-get-by-name", db_session)

        assert result.cluster.name == "test-get-by-name"

    @pytest.mark.asyncio
    async def test_get_cluster_not_found(self, db_session):
        """Test getting non-existent cluster raises error."""
        with pytest.raises(ValueError, match="Cluster not found"):
            await k8s_cluster_service.get_cluster("non-existent", db_session)

    @pytest.mark.asyncio
    async def test_update_cluster(self, db_session, mock_kubeconfig):
        """Test updating cluster configuration."""
        request = ClusterConfigCreate(
            name="test-update",
            kubeconfig=mock_kubeconfig,
            default_queue="original-queue",
            default_ray_image="rayproject/ray:2.50.0",
        )
        await k8s_cluster_service.create_cluster(request, db_session)

        update = ClusterConfigUpdate(
            default_queue="updated-queue",
            default_ray_image="rayproject/ray:2.51.0",
        )
        result = await k8s_cluster_service.update_cluster("test-update", update, db_session)

        assert result.cluster.default_queue == "updated-queue"
        assert result.cluster.default_ray_image == "rayproject/ray:2.51.0"

    @pytest.mark.asyncio
    async def test_delete_cluster(self, db_session, mock_kubeconfig):
        """Test deleting cluster."""
        request = ClusterConfigCreate(
            name="test-delete",
            kubeconfig=mock_kubeconfig,
            default_queue="queue",
            default_ray_image="rayproject/ray:2.50.0",
        )
        await k8s_cluster_service.create_cluster(request, db_session)

        result = await k8s_cluster_service.delete_cluster("test-delete", db_session)
        assert result is True

        with pytest.raises(ValueError, match="Cluster not found"):
            await k8s_cluster_service.get_cluster("test-delete", db_session)

    @pytest.mark.asyncio
    async def test_set_default_cluster(self, db_session, mock_kubeconfig):
        """Test setting default cluster unsets other defaults."""
        req1 = ClusterConfigCreate(
            name="test-default-1",
            kubeconfig=mock_kubeconfig,
            default_queue="queue",
            default_ray_image="rayproject/ray:2.50.0",
            is_default=True,
        )
        await k8s_cluster_service.create_cluster(req1, db_session)

        req2 = ClusterConfigCreate(
            name="test-default-2",
            kubeconfig=mock_kubeconfig,
            default_queue="queue",
            default_ray_image="rayproject/ray:2.50.0",
            is_default=False,
        )
        await k8s_cluster_service.create_cluster(req2, db_session)

        result = await k8s_cluster_service.set_default_cluster("test-default-2", db_session)
        assert result.cluster.is_default is True

        first = await k8s_cluster_service.get_cluster("test-default-1", db_session)
        assert first.cluster.is_default is False

    @pytest.mark.asyncio
    async def test_get_default_cluster(self, db_session, mock_kubeconfig):
        """Test getting default cluster."""
        request = ClusterConfigCreate(
            name="test-get-default",
            kubeconfig=mock_kubeconfig,
            default_queue="queue",
            default_ray_image="rayproject/ray:2.50.0",
            is_default=True,
        )
        await k8s_cluster_service.create_cluster(request, db_session)

        cluster = await k8s_cluster_service.get_default_cluster(db_session)

        assert cluster is not None
        assert cluster.is_default is True


# ============================================================================
# Database Model Tests
# ============================================================================


class TestK8sModels:
    """Tests for K8s database models."""

    @pytest.mark.asyncio
    async def test_rayjob_cluster_relationship(self, db_session, mock_kubeconfig):
        """Test RayJob -> K8sCluster relationship."""
        cluster = K8sCluster(
            name="test-rel-cluster",
            kubeconfig=mock_kubeconfig,
            default_queue="queue",
            default_ray_image="rayproject/ray:2.50.0",
        )
        db_session.add(cluster)
        await db_session.flush()

        job = RayJob(
            job_name="test-rel-job",
            namespace="default",
            queue_name="test-queue",
            entrypoint="python main.py",
            cluster_id=cluster.id,
        )
        db_session.add(job)
        await db_session.flush()

        assert job.cluster_id == cluster.id

    @pytest.mark.asyncio
    async def test_rayjob_status_tracking(self, db_session, mock_kubeconfig):
        """Test RayJob status fields."""
        cluster = K8sCluster(
            name="test-status-cluster",
            kubeconfig=mock_kubeconfig,
            default_queue="queue",
            default_ray_image="rayproject/ray:2.50.0",
        )
        db_session.add(cluster)
        await db_session.flush()

        job = RayJob(
            job_name="test-status-job",
            namespace="default",
            queue_name="test-queue",
            entrypoint="python main.py",
            status="PENDING",
            cluster_id=cluster.id,
        )
        db_session.add(job)
        await db_session.flush()

        job.status = "RUNNING"
        job.started_at = datetime.now(UTC)
        await db_session.flush()

        assert job.status == "RUNNING"
        assert job.started_at is not None

        job.status = "SUCCEEDED"
        job.finished_at = datetime.now(UTC)
        await db_session.flush()

        assert job.status == "SUCCEEDED"
        assert job.finished_at is not None

    @pytest.mark.asyncio
    async def test_cascade_delete_cluster(self, db_session, mock_kubeconfig):
        """Test deleting cluster cascades to jobs."""
        cluster = K8sCluster(
            name="test-cascade-cluster",
            kubeconfig=mock_kubeconfig,
            default_queue="queue",
            default_ray_image="rayproject/ray:2.50.0",
        )
        db_session.add(cluster)
        await db_session.flush()

        for i in range(3):
            job = RayJob(
                job_name=f"test-cascade-job-{i}",
                namespace="default",
                queue_name="queue",
                entrypoint="python main.py",
                cluster_id=cluster.id,
            )
            db_session.add(job)
        await db_session.flush()

        await db_session.delete(cluster)
        await db_session.flush()

        result = await db_session.execute(
            select(RayJob).where(RayJob.job_name.like("test-cascade-job-%"))
        )
        remaining_jobs = result.scalars().all()
        assert len(remaining_jobs) == 0
