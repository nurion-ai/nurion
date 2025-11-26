"""Pydantic schemas for Kueue and RayJob management APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


# ============================================================================
# K8s Cluster Configuration Schemas
# ============================================================================


class ClusterConfig(ApiModel):
    """Kubernetes cluster configuration."""

    name: str = Field(..., description="Cluster context name")
    kubeconfig: str | None = Field(None, description="Kubeconfig YAML content")
    default_queue: str = Field(..., description="Default Kueue queue name")
    default_ray_image: str = Field(..., description="Default Ray image for jobs")
    image_pull_secret: str | None = Field(None, description="Image pull secret name")
    is_default: bool = Field(False, description="Whether this is the default cluster")


class ClusterConfigCreate(ApiModel):
    """Request to create a cluster configuration."""

    name: str = Field(..., description="Cluster context name")
    kubeconfig: str | None = Field(None, description="Kubeconfig YAML content")
    default_queue: str = Field(..., description="Default Kueue queue name")
    default_ray_image: str = Field(..., description="Default Ray image for jobs")
    image_pull_secret: str | None = Field(None, description="Image pull secret name")
    is_default: bool = Field(False, description="Whether this is the default cluster")


class ClusterConfigUpdate(ApiModel):
    """Request to update a cluster configuration."""

    kubeconfig: str | None = None
    default_queue: str | None = None
    default_ray_image: str | None = None
    image_pull_secret: str | None = None
    is_default: bool | None = None


class ClusterConfigResponse(ApiModel):
    """Cluster configuration response."""

    cluster: ClusterConfig


class ListClustersResponse(ApiModel):
    """Response listing all cluster configurations."""

    clusters: list[ClusterConfig]


class ClusterStatus(ApiModel):
    """Kubernetes cluster status information."""

    name: str
    connected: bool
    server_version: str | None = None
    node_count: int | None = None
    error: str | None = None


class ClusterStatusResponse(ApiModel):
    """Cluster status response."""

    status: ClusterStatus


# ============================================================================
# LocalQueue Schemas
# ============================================================================


class LocalQueueInfo(ApiModel):
    """LocalQueue information."""

    name: str = Field(..., description="Queue name")
    namespace: str = Field(..., description="Namespace the queue belongs to")
    cluster_queue: str | None = Field(None, description="Associated ClusterQueue")
    pending_workloads: int = Field(0, description="Number of pending workloads")
    admitted_workloads: int = Field(0, description="Number of admitted workloads")


class ListLocalQueuesResponse(ApiModel):
    """Response listing all LocalQueues."""

    queues: list[LocalQueueInfo]


# ============================================================================
# RayJob Schemas
# ============================================================================


class RayJobSubmitRequest(ApiModel):
    """Request to submit a RayJob."""

    # Required
    entrypoint: str = Field(..., description="Entrypoint command for the job")

    # Optional job metadata
    job_name: str | None = Field(None, description="Job name (auto-generated if not provided)")
    user: str | None = Field(None, description="User who submitted the job")

    # Resource requirements
    num_cpus: int = Field(16, description="Number of CPUs per worker")
    num_gpus: int = Field(0, description="Number of GPUs per worker")
    memory_gb: int = Field(128, description="Memory in GB per worker")
    worker_replicas: int = Field(3, description="Number of worker replicas")

    # Kueue configuration
    queue_name: str | None = Field(None, description="Kueue queue name")
    cluster_name: str | None = Field(None, description="Target cluster name")

    # Runtime configuration
    runtime_env: dict[str, Any] | None = Field(None, description="Ray runtime environment")
    env_vars: dict[str, str] | None = Field(None, description="Environment variables")

    # Job lifecycle
    ttl_seconds_after_finished: int = Field(3600, description="TTL after job completion")
    shutdown_after_job_finishes: bool = Field(True, description="Shutdown cluster after job")

    # Advanced options
    ray_image: str | None = Field(None, description="Ray image (uses cluster default if not set)")
    ray_version: str = Field("2.50.0", description="Ray version")
    enable_autoscaling: bool = Field(False, description="Enable autoscaling")
    min_worker_replicas: int = Field(1, description="Minimum worker replicas (autoscaling)")
    max_worker_replicas: int = Field(10, description="Maximum worker replicas (autoscaling)")


class RayJobSubmitResponse(ApiModel):
    """Response after submitting a RayJob."""

    job_name: str
    queue_name: str
    namespace: str
    cluster_name: str


class RayJobStatus(ApiModel):
    """RayJob status information."""

    job_name: str = Field(..., description="Job name")
    namespace: str = Field(..., description="Namespace")
    job_status: str = Field(..., description="Job status (PENDING, RUNNING, SUCCEEDED, etc.)")
    ray_cluster_status: str | None = Field(None, description="Ray cluster status")
    start_time: datetime | None = Field(None, description="Job start time")
    end_time: datetime | None = Field(None, description="Job end time")
    message: str | None = Field(None, description="Status message")
    dashboard_url: str | None = Field(None, description="Ray dashboard URL")


class RayJobStatusResponse(ApiModel):
    """RayJob status response."""

    status: RayJobStatus


class RayJobInfo(ApiModel):
    """Summary information for a RayJob."""

    id: int | None = None
    job_name: str
    namespace: str
    job_status: str
    queue_name: str | None = None
    user: str | None = None
    cluster_name: str | None = None
    created_at: datetime | None = None
    start_time: datetime | None = None


class RayJobDetail(ApiModel):
    """Detailed information for a RayJob from database."""

    id: int
    job_name: str
    namespace: str
    queue_name: str
    entrypoint: str
    ray_image: str | None = None
    num_cpus: int = 16
    num_gpus: int = 0
    memory_gb: int = 128
    worker_replicas: int = 3
    runtime_env: dict[str, Any] | None = None
    env_vars: dict[str, str] | None = None
    status: str = "PENDING"
    message: str | None = None
    dashboard_url: str | None = None
    user: str | None = None
    cluster_name: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RayJobDetailResponse(ApiModel):
    """Response with detailed RayJob information."""

    job: RayJobDetail


class ListRayJobsResponse(ApiModel):
    """Response listing RayJobs."""

    jobs: list[RayJobInfo]
    total: int


class RayJobLogsRequest(ApiModel):
    """Request for RayJob logs."""

    log_type: str = Field("submitter", description="Log type: 'submitter' or 'head'")
    tail_lines: int | None = Field(None, description="Number of lines to return from the end")


class RayJobLogsResponse(ApiModel):
    """Response containing RayJob logs."""

    job_name: str
    log_type: str
    logs: str


class DeleteRayJobResponse(ApiModel):
    """Response after deleting RayJob(s)."""

    results: dict[str, bool] = Field(..., description="Mapping of job names to deletion success")


class RayJobDashboardResponse(ApiModel):
    """Response with Ray dashboard connection info."""

    job_name: str
    dashboard_url: str
    head_pod_ip: str | None = None


# ============================================================================
# Error Response
# ============================================================================


class ErrorResponse(ApiModel):
    """Standard error response."""

    error: str
    detail: str | None = None
    code: str | None = None

