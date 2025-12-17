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

"""API routes for Kubernetes and RayJob management."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...db.session import get_session
from ...models.k8s import K8sCluster, RayJob
from ...schemas.k8s import (
    ClusterConfigCreate,
    ClusterConfigResponse,
    ClusterConfigUpdate,
    ClusterStatusResponse,
    DeleteRayJobResponse,
    ListClustersResponse,
    ListLocalQueuesResponse,
    ListRayJobsResponse,
    LocalQueueInfo,
    RayJobDashboardResponse,
    RayJobInfo,
    RayJobLogsResponse,
    RayJobStatusResponse,
    RayJobSubmitRequest,
    RayJobSubmitResponse,
)
from ...services import k8s_cluster_service, localqueue_service, rayjob_service
from ...services.rayjob_sync_service import get_sync_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/k8s", tags=["kubernetes-management"])


async def get_db_session() -> AsyncIterator[AsyncSession]:
    async for session in get_session():
        yield session


# ============================================================================
# Cluster Configuration Endpoints
# ============================================================================


@router.get("/clusters", response_model=ListClustersResponse)
async def list_clusters(
    db: AsyncSession = Depends(get_db_session),
) -> ListClustersResponse:
    """List all registered Kubernetes cluster configurations."""
    return await k8s_cluster_service.list_clusters(db)


@router.post("/clusters", response_model=ClusterConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_cluster(
    request: ClusterConfigCreate = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> ClusterConfigResponse:
    """Register a new Kubernetes cluster configuration."""
    try:
        return await k8s_cluster_service.create_cluster(request, db)
    except Exception as e:
        logger.exception("Failed to create cluster configuration")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.get("/clusters/{name}", response_model=ClusterConfigResponse)
async def get_cluster(
    name: Annotated[str, Path(description="Cluster name")],
    db: AsyncSession = Depends(get_db_session),
) -> ClusterConfigResponse:
    """Get a cluster configuration by name."""
    try:
        return await k8s_cluster_service.get_cluster(name, db)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e


@router.patch("/clusters/{name}", response_model=ClusterConfigResponse)
async def update_cluster(
    name: Annotated[str, Path(description="Cluster name")],
    request: ClusterConfigUpdate = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> ClusterConfigResponse:
    """Update a cluster configuration."""
    try:
        return await k8s_cluster_service.update_cluster(name, request, db)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e


@router.delete("/clusters/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cluster(
    name: Annotated[str, Path(description="Cluster name")],
    db: AsyncSession = Depends(get_db_session),
) -> None:
    """Delete a cluster configuration."""
    try:
        await k8s_cluster_service.delete_cluster(name, db)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e


@router.post("/clusters/{name}/default", response_model=ClusterConfigResponse)
async def set_default_cluster(
    name: Annotated[str, Path(description="Cluster name")],
    db: AsyncSession = Depends(get_db_session),
) -> ClusterConfigResponse:
    """Set a cluster as the default."""
    try:
        return await k8s_cluster_service.set_default_cluster(name, db)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e


@router.get("/clusters/{name}/status", response_model=ClusterStatusResponse)
async def get_cluster_status(
    name: Annotated[str, Path(description="Cluster name")],
    db: AsyncSession = Depends(get_db_session),
) -> ClusterStatusResponse:
    """Get the connection status of a cluster."""
    return await k8s_cluster_service.get_cluster_status(name, db)


@router.post("/clusters/{name}/test", response_model=ClusterStatusResponse)
async def test_cluster_connection(
    name: Annotated[str, Path(description="Cluster name")],
    db: AsyncSession = Depends(get_db_session),
) -> ClusterStatusResponse:
    """Test connection to a cluster."""
    return await k8s_cluster_service.test_connection(name, db)


# ============================================================================
# LocalQueue Endpoints
# ============================================================================


@router.get("/queues", response_model=ListLocalQueuesResponse)
async def list_localqueues(
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> ListLocalQueuesResponse:
    """List all Kueue LocalQueues."""
    try:
        cluster_obj = await k8s_cluster_service.get_cluster_or_default(cluster, db)
        if not cluster_obj:
            raise ValueError("No cluster configured")
        return localqueue_service.list_queues(cluster_obj)
    except Exception as e:
        logger.exception("Failed to list LocalQueues")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.get("/queues/{queue_name}", response_model=LocalQueueInfo)
async def get_localqueue(
    queue_name: Annotated[str, Path(description="Queue name")],
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> LocalQueueInfo:
    """Get information about a specific LocalQueue."""
    try:
        cluster_obj = await k8s_cluster_service.get_cluster_or_default(cluster, db)
        if not cluster_obj:
            raise ValueError("No cluster configured")
        return localqueue_service.get_queue_info(queue_name, cluster_obj)
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Failed to get LocalQueue info")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


# ============================================================================
# RayJob Endpoints
# ============================================================================


@router.post("/jobs", response_model=RayJobSubmitResponse, status_code=status.HTTP_201_CREATED)
async def submit_rayjob(
    request: RayJobSubmitRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> RayJobSubmitResponse:
    """Submit a new RayJob."""
    try:
        return await rayjob_service.submit_job(request, db)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Failed to submit RayJob")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.get("/jobs", response_model=ListRayJobsResponse)
async def list_rayjobs(
    queue: Annotated[str | None, Query(description="Filter by queue name")] = None,
    user: Annotated[str | None, Query(description="Filter by user")] = None,
    include_completed: Annotated[bool, Query(description="Include completed jobs")] = False,
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> ListRayJobsResponse:
    """List RayJobs with optional filtering."""
    try:
        return await rayjob_service.list_jobs(
            queue_name=queue,
            user_filter=user,
            include_completed=include_completed,
            cluster_name=cluster,
            db=db,
        )
    except Exception as e:
        logger.exception("Failed to list RayJobs")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.get("/jobs/{job_name}", response_model=RayJobStatusResponse)
async def get_rayjob_status(
    job_name: Annotated[str, Path(description="Job name")],
    queue: Annotated[str | None, Query(description="Queue name")] = None,
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> RayJobStatusResponse:
    """Get the status of a RayJob."""
    try:
        return await rayjob_service.get_job_status(
            job_name=job_name,
            queue_name=queue,
            cluster_name=cluster,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Failed to get RayJob status")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.delete("/jobs/{job_name}", response_model=DeleteRayJobResponse)
async def delete_rayjob(
    job_name: Annotated[str, Path(description="Job name")],
    queue: Annotated[str | None, Query(description="Queue name")] = None,
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> DeleteRayJobResponse:
    """Delete a RayJob."""
    try:
        return await rayjob_service.delete_jobs(
            job_names=[job_name],
            queue_name=queue,
            cluster_name=cluster,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Failed to delete RayJob")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.post("/jobs/batch-delete", response_model=DeleteRayJobResponse)
async def batch_delete_rayjobs(
    job_names: Annotated[list[str], Body(description="List of job names to delete")],
    queue: Annotated[str | None, Query(description="Queue name")] = None,
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> DeleteRayJobResponse:
    """Delete multiple RayJobs."""
    try:
        return await rayjob_service.delete_jobs(
            job_names=job_names,
            queue_name=queue,
            cluster_name=cluster,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Failed to batch delete RayJobs")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.get("/jobs/{job_name}/logs", response_model=RayJobLogsResponse)
async def get_rayjob_logs(
    job_name: Annotated[str, Path(description="Job name")],
    log_type: Annotated[str, Query(description="Log type: 'submitter' or 'head'")] = "submitter",
    tail_lines: Annotated[int | None, Query(description="Number of lines from end")] = None,
    queue: Annotated[str | None, Query(description="Queue name")] = None,
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> RayJobLogsResponse:
    """Get logs from a RayJob."""
    try:
        return await rayjob_service.get_job_logs(
            job_name=job_name,
            log_type=log_type,
            tail_lines=tail_lines,
            queue_name=queue,
            cluster_name=cluster,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Failed to get RayJob logs")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.get("/jobs/{job_name}/dashboard", response_model=RayJobDashboardResponse)
async def get_rayjob_dashboard(
    job_name: Annotated[str, Path(description="Job name")],
    queue: Annotated[str | None, Query(description="Queue name")] = None,
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    db: AsyncSession = Depends(get_db_session),
) -> RayJobDashboardResponse:
    """Get Ray dashboard connection information for a job."""
    try:
        return await rayjob_service.get_dashboard_info(
            job_name=job_name,
            queue_name=queue,
            cluster_name=cluster,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.exception("Failed to get RayJob dashboard info")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


# ============================================================================
# Sync & History Endpoints
# ============================================================================


@router.post("/jobs/sync", status_code=status.HTTP_200_OK)
async def sync_rayjobs() -> dict[str, str]:
    """Manually trigger synchronization of RayJob states from Kubernetes to database."""
    try:
        sync_service = get_sync_service()
        await sync_service.sync_all_clusters()
        return {"message": "Sync completed successfully"}
    except Exception as e:
        logger.exception("Failed to sync RayJobs")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e


@router.get("/jobs/history", response_model=ListRayJobsResponse)
async def list_rayjobs_history(
    queue: Annotated[str | None, Query(description="Filter by queue name")] = None,
    user: Annotated[str | None, Query(description="Filter by user")] = None,
    status_filter: Annotated[str | None, Query(description="Filter by status")] = None,
    cluster: Annotated[str | None, Query(description="Target cluster")] = None,
    limit: Annotated[int, Query(description="Maximum number of jobs to return")] = 100,
    db: AsyncSession = Depends(get_db_session),
) -> ListRayJobsResponse:
    """List RayJobs from database history (includes completed/deleted jobs).

    This endpoint queries the database cache, which may not reflect real-time K8s state.
    Use GET /jobs for real-time status from Kubernetes.
    """
    try:
        # Build query
        query = select(RayJob).join(K8sCluster, RayJob.cluster_id == K8sCluster.id, isouter=True)

        if cluster:
            query = query.where(K8sCluster.name == cluster)

        if queue:
            query = query.where(RayJob.queue_name == queue)

        if user:
            query = query.where(RayJob.user.ilike(f"%{user}%"))

        if status_filter:
            query = query.where(RayJob.status == status_filter)

        query = query.order_by(RayJob.created_at.desc()).limit(limit)

        result = await db.execute(query)
        job_records = result.scalars().all()

        jobs = []
        for job in job_records:
            cluster_obj = await db.get(K8sCluster, job.cluster_id) if job.cluster_id else None
            jobs.append(
                RayJobInfo(
                    id=job.id,
                    job_name=job.job_name,
                    namespace=job.namespace,
                    job_status=job.status,
                    queue_name=job.queue_name,
                    user=job.user,
                    cluster_name=cluster_obj.name if cluster_obj else None,
                    created_at=job.created_at,
                    start_time=job.started_at,
                )
            )

        return ListRayJobsResponse(jobs=jobs, total=len(jobs))

    except Exception as e:
        logger.exception("Failed to list RayJobs history")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        ) from e
