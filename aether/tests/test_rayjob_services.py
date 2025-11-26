"""Integration tests for RayJob services using testcontainers.

Tests:
- rayjob_service: RayJob submission, listing, deletion
- rayjob_sync_service: Background sync service
- localqueue_service: LocalQueue operations
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pytest
import yaml
from kubernetes import client
from kubernetes import config as k8s_config
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.k3s import K3SContainer
from testcontainers.postgres import PostgresContainer

from aether.models.base import BaseModel
from aether.models.k8s import K8sCluster, RayJob
from aether.schemas.k8s import ClusterConfigCreate, RayJobSubmitRequest
from aether.services import k8s_cluster_service, rayjob_service
from aether.services.k8s_connection import clear_client_cache, get_custom_objects_api
from aether.services.localqueue_service import clear_namespace_cache
from aether.services.rayjob_sync_service import RayJobSyncService

pytestmark = pytest.mark.integration

# Ray CRD definition (minimal for testing)
RAY_CRD = """
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: rayjobs.ray.io
spec:
  group: ray.io
  names:
    kind: RayJob
    listKind: RayJobList
    plural: rayjobs
    singular: rayjob
  scope: Namespaced
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              x-kubernetes-preserve-unknown-fields: true
            status:
              type: object
              x-kubernetes-preserve-unknown-fields: true
      subresources:
        status: {}
"""

# Kueue LocalQueue CRD definition (minimal for testing)
KUEUE_LOCALQUEUE_CRD = """
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: localqueues.kueue.x-k8s.io
spec:
  group: kueue.x-k8s.io
  names:
    kind: LocalQueue
    listKind: LocalQueueList
    plural: localqueues
    singular: localqueue
  scope: Namespaced
  versions:
    - name: v1beta1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                clusterQueue:
                  type: string
            status:
              type: object
              x-kubernetes-preserve-unknown-fields: true
      subresources:
        status: {}
"""


# ============================================================================
# Fixtures
# ============================================================================


def _install_crds(k3s_container):
    """Install Ray and Kueue CRDs in K3S."""
    kubeconfig = k3s_container.config_yaml()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(kubeconfig)
        config_path = f.name

    try:
        k8s_config.load_kube_config(config_file=config_path)
        api_ext = client.ApiextensionsV1Api()

        # Install Ray CRD
        ray_crd = yaml.safe_load(RAY_CRD)
        try:
            api_ext.create_custom_resource_definition(body=ray_crd)
        except client.ApiException as e:
            if e.status != 409:  # Already exists
                raise

        # Install Kueue LocalQueue CRD
        kueue_crd = yaml.safe_load(KUEUE_LOCALQUEUE_CRD)
        try:
            api_ext.create_custom_resource_definition(body=kueue_crd)
        except client.ApiException as e:
            if e.status != 409:
                raise

        # Wait for CRDs to be ready
        time.sleep(2)

    finally:
        Path(config_path).unlink(missing_ok=True)


@pytest.fixture(scope="module")
def postgres_container():
    """Start PostgreSQL container."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def k3s_container():
    """Start K3S container and install CRDs."""
    with K3SContainer() as k3s:
        _install_crds(k3s)
        yield k3s


@pytest.fixture(scope="module")
def k8s_kubeconfig(k3s_container) -> str:
    """Get kubeconfig from K3S."""
    return k3s_container.config_yaml()


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
    """Clear K8s client and namespace caches."""
    clear_client_cache()
    clear_namespace_cache()
    yield
    clear_client_cache()
    clear_namespace_cache()


@pytest.fixture
async def cluster_with_queue(db_session, k8s_kubeconfig):
    """Create a cluster and LocalQueue for testing RayJobs."""
    cluster_req = ClusterConfigCreate(
        name="test-cluster",
        kubeconfig=k8s_kubeconfig,
        default_queue="test-queue",
        default_ray_image="rayproject/ray:2.50.0",
        is_default=True,
    )
    await k8s_cluster_service.create_cluster(cluster_req, db_session)

    result = await db_session.execute(select(K8sCluster).where(K8sCluster.name == "test-cluster"))
    cluster = result.scalar_one()

    # Create LocalQueue in K8s
    custom_api = get_custom_objects_api(cluster)
    localqueue = {
        "apiVersion": "kueue.x-k8s.io/v1beta1",
        "kind": "LocalQueue",
        "metadata": {"name": "test-queue", "namespace": "default"},
        "spec": {"clusterQueue": "test-cluster-queue"},
    }
    try:
        custom_api.create_namespaced_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            namespace="default",
            plural="localqueues",
            body=localqueue,
        )
    except client.ApiException as e:
        if e.status != 409:
            raise

    return cluster


# ============================================================================
# rayjob_service Tests
# ============================================================================


class TestRayJobService:
    """Tests for rayjob_service with real K8s operations."""

    @pytest.mark.asyncio
    async def test_submit_job_no_cluster(self, db_session):
        """Test submitting job fails when no cluster configured."""
        request = RayJobSubmitRequest(
            entrypoint="python main.py",
            job_name="test-job",
        )

        with pytest.raises(ValueError, match="No cluster configured"):
            await rayjob_service.submit_job(request, db_session)

    @pytest.mark.asyncio
    async def test_submit_job_success(self, db_session, cluster_with_queue):
        """Test submitting a RayJob successfully."""
        request = RayJobSubmitRequest(
            entrypoint="python -c 'print(1)'",
            job_name="test-submit-job",
            queue_name="test-queue",
            num_cpus=2,
            memory_gb=4,
            worker_replicas=1,
        )

        result = await rayjob_service.submit_job(request, db_session)

        assert result.job_name == "test-submit-job"
        assert result.queue_name == "test-queue"
        assert result.namespace == "default"
        assert result.cluster_name == "test-cluster"

        # Verify job is saved in database
        db_job = await db_session.execute(
            select(RayJob).where(RayJob.job_name == "test-submit-job")
        )
        job = db_job.scalar_one()
        assert job.entrypoint == "python -c 'print(1)'"
        assert job.num_cpus == 2
        assert job.memory_gb == 4
        assert job.status == "PENDING"

    @pytest.mark.asyncio
    async def test_submit_job_auto_name(self, db_session, cluster_with_queue):
        """Test submitting a RayJob with auto-generated name."""
        request = RayJobSubmitRequest(
            entrypoint="python main.py",
            queue_name="test-queue",
        )

        result = await rayjob_service.submit_job(request, db_session)

        assert result.job_name.startswith("rayjob-")
        assert result.namespace == "default"

    @pytest.mark.asyncio
    async def test_list_jobs_empty(self, db_session):
        """Test listing jobs returns empty when no cluster."""
        result = await rayjob_service.list_jobs(db=db_session)

        assert result.jobs == []
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_list_jobs_with_data(self, db_session, cluster_with_queue):
        """Test listing jobs after submitting."""
        for i in range(3):
            request = RayJobSubmitRequest(
                entrypoint=f"python job{i}.py",
                job_name=f"test-list-job-{i}",
                queue_name="test-queue",
            )
            await rayjob_service.submit_job(request, db_session)

        result = await rayjob_service.list_jobs(
            queue_name="test-queue",
            include_completed=True,
            db=db_session,
        )

        assert result.total >= 3
        job_names = [j.job_name for j in result.jobs]
        assert all(f"test-list-job-{i}" in job_names for i in range(3))

    @pytest.mark.asyncio
    async def test_get_job_status(self, db_session, cluster_with_queue):
        """Test getting job status."""
        request = RayJobSubmitRequest(
            entrypoint="python main.py",
            job_name="test-status-job",
            queue_name="test-queue",
        )
        await rayjob_service.submit_job(request, db_session)

        result = await rayjob_service.get_job_status(
            job_name="test-status-job",
            queue_name="test-queue",
            db=db_session,
        )

        assert result.status.job_name == "test-status-job"
        assert result.status.namespace == "default"
        assert result.status.job_status is not None

    @pytest.mark.asyncio
    async def test_delete_job(self, db_session, cluster_with_queue):
        """Test deleting a RayJob."""
        request = RayJobSubmitRequest(
            entrypoint="python main.py",
            job_name="test-delete-job",
            queue_name="test-queue",
        )
        await rayjob_service.submit_job(request, db_session)

        result = await rayjob_service.delete_jobs(
            job_names=["test-delete-job"],
            queue_name="test-queue",
            db=db_session,
        )

        assert result.results["test-delete-job"] is True

        # Verify job is deleted from K8s
        from kubernetes.client.rest import ApiException

        with pytest.raises(ApiException) as exc_info:
            await rayjob_service.get_job_status(
                job_name="test-delete-job",
                queue_name="test-queue",
                db=db_session,
            )
        assert exc_info.value.status == 404

    @pytest.mark.asyncio
    async def test_submit_job_with_env_vars(self, db_session, cluster_with_queue):
        """Test submitting a RayJob with environment variables."""
        request = RayJobSubmitRequest(
            entrypoint="python main.py",
            job_name="test-env-job",
            queue_name="test-queue",
            env_vars={"MY_VAR": "value1", "ANOTHER_VAR": "value2"},
        )

        result = await rayjob_service.submit_job(request, db_session)

        assert result.job_name == "test-env-job"

        db_job = await db_session.execute(select(RayJob).where(RayJob.job_name == "test-env-job"))
        job = db_job.scalar_one()
        assert job.env_vars == {"MY_VAR": "value1", "ANOTHER_VAR": "value2"}

    @pytest.mark.asyncio
    async def test_submit_job_with_runtime_env(self, db_session, cluster_with_queue):
        """Test submitting a RayJob with runtime environment."""
        runtime_env = {
            "pip": ["numpy", "pandas"],
            "working_dir": "/app",
        }
        request = RayJobSubmitRequest(
            entrypoint="python main.py",
            job_name="test-runtime-job",
            queue_name="test-queue",
            runtime_env=runtime_env,
        )

        result = await rayjob_service.submit_job(request, db_session)

        assert result.job_name == "test-runtime-job"

        db_job = await db_session.execute(
            select(RayJob).where(RayJob.job_name == "test-runtime-job")
        )
        job = db_job.scalar_one()
        assert job.runtime_env == runtime_env


# ============================================================================
# RayJobSyncService Tests
# ============================================================================


class TestRayJobSyncService:
    """Tests for RayJobSyncService."""

    @pytest.mark.asyncio
    async def test_sync_service_lifecycle(self):
        """Test sync service start/stop."""
        service = RayJobSyncService(sync_interval_seconds=60)

        assert service._running is False
        assert service._task is None

        await service.start()
        assert service._running is True
        assert service._task is not None

        await service.stop()
        assert service._running is False
        assert service._task is None

    @pytest.mark.asyncio
    async def test_sync_service_double_start(self):
        """Test starting already running service is safe."""
        service = RayJobSyncService(sync_interval_seconds=60)

        await service.start()
        await service.start()  # Should not raise

        assert service._running is True

        await service.stop()

    @pytest.mark.asyncio
    async def test_sync_cluster_updates_job_status(self, db_session, cluster_with_queue):
        """Test sync service updates job status from K8s."""
        request = RayJobSubmitRequest(
            entrypoint="python main.py",
            job_name="test-sync-job",
            queue_name="test-queue",
        )
        await rayjob_service.submit_job(request, db_session)

        result = await db_session.execute(
            select(K8sCluster).where(K8sCluster.name == "test-cluster")
        )
        cluster = result.scalar_one()

        service = RayJobSyncService(sync_interval_seconds=60)
        await service._sync_cluster(cluster, db_session)

        db_job = await db_session.execute(select(RayJob).where(RayJob.job_name == "test-sync-job"))
        job = db_job.scalar_one()
        assert job.status is not None


# ============================================================================
# LocalQueue Tests
# ============================================================================


class TestLocalQueueService:
    """Tests for localqueue_service."""

    @pytest.mark.asyncio
    async def test_list_queues(self, db_session, cluster_with_queue):
        """Test listing LocalQueues."""
        from aether.services.localqueue_service import list_queues

        result = list_queues(cluster_with_queue)

        assert len(result.queues) >= 1
        queue_names = [q.name for q in result.queues]
        assert "test-queue" in queue_names

    @pytest.mark.asyncio
    async def test_get_namespace_for_queue(self, db_session, cluster_with_queue):
        """Test getting namespace for a queue."""
        from aether.services.localqueue_service import get_namespace_for_queue

        namespace = get_namespace_for_queue("test-queue", cluster_with_queue)

        assert namespace == "default"

    @pytest.mark.asyncio
    async def test_get_namespace_for_queue_not_found(self, db_session, cluster_with_queue):
        """Test getting namespace for non-existent queue."""
        from aether.services.localqueue_service import get_namespace_for_queue

        with pytest.raises(RuntimeError, match="not found"):
            get_namespace_for_queue("non-existent-queue", cluster_with_queue)
