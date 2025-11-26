"""Service for managing RayJobs with Kueue scheduling."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any

import yaml
from kubernetes.client.rest import ApiException
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.session import get_session
from ..models.k8s import K8sCluster, RayJob
from ..schemas.k8s import (
    DeleteRayJobResponse,
    ListRayJobsResponse,
    RayJobDashboardResponse,
    RayJobInfo,
    RayJobLogsResponse,
    RayJobStatus,
    RayJobStatusResponse,
    RayJobSubmitRequest,
    RayJobSubmitResponse,
)
from .k8s_cluster_service import get_cluster_or_default
from .k8s_connection import get_core_api, get_custom_objects_api
from .localqueue_service import get_namespace_for_queue

logger = logging.getLogger(__name__)

# Constants
RAY_GROUP = "ray.io"
RAY_VERSION = "v1"
RAY_JOBS_PLURAL = "rayjobs"


async def _resolve_session(db: AsyncSession | None) -> AsyncSession:
    if db is None:
        async for session in get_session():
            return session
    return db


async def _get_cluster_and_defaults(
    cluster_name: str | None, db: AsyncSession
) -> tuple[K8sCluster, dict[str, Any]]:
    """Get cluster and its default configuration."""
    cluster = await get_cluster_or_default(cluster_name, db)
    if not cluster:
        raise ValueError("No cluster configured. Please add a cluster configuration first.")

    defaults = {
        "queue": cluster.default_queue,
        "ray_image": cluster.default_ray_image,
        "image_secret": cluster.image_pull_secret,
    }
    return cluster, defaults


async def submit_job(
    request: RayJobSubmitRequest, db: AsyncSession | None = None
) -> RayJobSubmitResponse:
    """Submit a new RayJob.

    Args:
        request: Job submission configuration
        db: Database session

    Returns:
        Submission result with job name and metadata
    """
    session = await _resolve_session(db)
    cluster, defaults = await _get_cluster_and_defaults(request.cluster_name, session)

    # Generate job name if not provided
    job_name = request.job_name
    if not job_name:
        timestamp = int(time.time())
        job_name = f"rayjob-{timestamp}"

    # Determine queue and namespace
    queue_name = request.queue_name or defaults.get("queue")
    if not queue_name:
        raise ValueError("Queue name must be specified or a default queue must be configured")

    namespace = get_namespace_for_queue(queue_name, cluster)

    # Build and submit the job spec
    ray_image = request.ray_image or defaults.get("ray_image")
    job_spec = _create_rayjob_spec(
        job_name=job_name,
        request=request,
        namespace=namespace,
        defaults=defaults,
    )

    try:
        # Submit to Kubernetes
        custom_api = get_custom_objects_api(cluster)
        custom_api.create_namespaced_custom_object(
            group=RAY_GROUP,
            version=RAY_VERSION,
            namespace=namespace,
            plural=RAY_JOBS_PLURAL,
            body=job_spec,
        )

        # Save job record to database
        rayjob_record = RayJob(
            job_name=job_name,
            namespace=namespace,
            queue_name=queue_name,
            entrypoint=request.entrypoint,
            ray_image=ray_image,
            num_cpus=request.num_cpus,
            num_gpus=request.num_gpus,
            memory_gb=request.memory_gb,
            worker_replicas=request.worker_replicas,
            runtime_env=request.runtime_env,
            env_vars=request.env_vars,
            status="PENDING",
            user=request.user,
            cluster_id=cluster.id,
        )
        session.add(rayjob_record)
        await session.commit()

        return RayJobSubmitResponse(
            job_name=job_name,
            queue_name=queue_name,
            namespace=namespace,
            cluster_name=cluster.name,
        )

    except ApiException as e:
        logger.error("Failed to submit RayJob %s: %s", job_name, e)
        raise


async def get_job_status(
    job_name: str,
    queue_name: str | None = None,
    cluster_name: str | None = None,
    db: AsyncSession | None = None,
) -> RayJobStatusResponse:
    """Get the status of a RayJob directly from Kubernetes.

    Args:
        job_name: Name of the job
        queue_name: Queue name to find namespace
        cluster_name: Target cluster
        db: Database session

    Returns:
        Job status information
    """
    session = await _resolve_session(db)
    cluster, defaults = await _get_cluster_and_defaults(cluster_name, session)

    queue = queue_name or defaults.get("queue")
    if not queue:
        raise ValueError("Queue name must be specified")

    namespace = get_namespace_for_queue(queue, cluster)

    try:
        custom_api = get_custom_objects_api(cluster)
        response = custom_api.get_namespaced_custom_object(
            group=RAY_GROUP,
            version=RAY_VERSION,
            namespace=namespace,
            plural=RAY_JOBS_PLURAL,
            name=job_name,
        )

        status_obj = response.get("status", {})

        # Parse timestamps
        start_time = None
        if start_str := status_obj.get("startTime"):
            start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

        end_time = None
        if end_str := status_obj.get("endTime"):
            end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))

        # Get dashboard URL
        ray_cluster_status = status_obj.get("rayClusterStatus", {})
        dashboard_url = None
        head_info = ray_cluster_status.get("head", {})
        if head_info and (pod_ip := head_info.get("podIP")):
            dashboard_url = f"http://{pod_ip}:8265"

        job_status = status_obj.get("jobStatus", "UNKNOWN")

        # Note: Database sync is handled by RayJobSyncService background task

        return RayJobStatusResponse(
            status=RayJobStatus(
                job_name=job_name,
                namespace=namespace,
                job_status=job_status,
                ray_cluster_status=ray_cluster_status.get("state"),
                start_time=start_time,
                end_time=end_time,
                message=status_obj.get("message"),
                dashboard_url=dashboard_url,
            )
        )

    except ApiException as e:
        logger.error("Failed to get job status for %s: %s", job_name, e)
        raise


async def list_jobs(
    queue_name: str | None = None,
    user_filter: str | None = None,
    include_completed: bool = False,
    cluster_name: str | None = None,
    db: AsyncSession | None = None,
) -> ListRayJobsResponse:
    """List RayJobs directly from Kubernetes.

    Args:
        queue_name: Filter by queue (also determines namespace)
        user_filter: Filter by user label
        include_completed: Include completed jobs
        cluster_name: Target cluster
        db: Database session

    Returns:
        List of job information
    """
    session = await _resolve_session(db)
    cluster = await get_cluster_or_default(cluster_name, session)
    if not cluster:
        # Return empty if no cluster configured
        return ListRayJobsResponse(jobs=[], total=0)

    try:
        custom_api = get_custom_objects_api(cluster)

        # Query K8s directly
        if queue_name:
            namespace = get_namespace_for_queue(queue_name, cluster)
            response = custom_api.list_namespaced_custom_object(
                group=RAY_GROUP,
                version=RAY_VERSION,
                namespace=namespace,
                plural=RAY_JOBS_PLURAL,
            )
        else:
            response = custom_api.list_cluster_custom_object(
                group=RAY_GROUP,
                version=RAY_VERSION,
                plural=RAY_JOBS_PLURAL,
            )

        jobs = []
        for item in response.get("items", []):
            metadata = item.get("metadata", {})
            status_obj = item.get("status", {})
            labels = metadata.get("labels", {})
            annotations = metadata.get("annotations", {})

            job_status = status_obj.get("jobStatus", "UNKNOWN")
            user = labels.get("user", "")
            queue = labels.get("kueue.x-k8s.io/queue-name", "") or annotations.get(
                "kueue.x-k8s.io/queue-name", ""
            )

            # Apply filters
            if user_filter and user and user_filter.lower() not in user.lower():
                continue

            if queue_name and queue and queue_name not in queue:
                continue

            if not include_completed and job_status in ["SUCCEEDED", "FAILED", "STOPPED"]:
                continue

            # Parse timestamps
            created_at = None
            if creation_str := metadata.get("creationTimestamp"):
                try:
                    created_at = datetime.fromisoformat(creation_str.replace("Z", "+00:00"))
                except ValueError:
                    pass

            start_time = None
            if start_str := status_obj.get("startTime"):
                try:
                    start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                except ValueError:
                    pass

            jobs.append(
                RayJobInfo(
                    job_name=metadata.get("name", ""),
                    namespace=metadata.get("namespace", ""),
                    job_status=job_status,
                    queue_name=queue,
                    user=user,
                    cluster_name=cluster.name,
                    created_at=created_at,
                    start_time=start_time,
                )
            )

        return ListRayJobsResponse(jobs=jobs, total=len(jobs))

    except ApiException as e:
        logger.error("Failed to list jobs from K8s: %s", e)
        raise


async def delete_jobs(
    job_names: list[str],
    queue_name: str | None = None,
    cluster_name: str | None = None,
    db: AsyncSession | None = None,
) -> DeleteRayJobResponse:
    """Delete one or more RayJobs.

    Args:
        job_names: List of job names to delete
        queue_name: Queue name to find namespace
        cluster_name: Target cluster
        db: Database session

    Returns:
        Deletion results
    """
    session = await _resolve_session(db)
    cluster, defaults = await _get_cluster_and_defaults(cluster_name, session)

    queue = queue_name or defaults.get("queue")
    if not queue:
        raise ValueError("Queue name must be specified")

    namespace = get_namespace_for_queue(queue, cluster)
    results: dict[str, bool] = {}
    custom_api = get_custom_objects_api(cluster)

    for job_name in job_names:
        try:
            custom_api.delete_namespaced_custom_object(
                group=RAY_GROUP,
                version=RAY_VERSION,
                namespace=namespace,
                plural=RAY_JOBS_PLURAL,
                name=job_name,
            )

            # Note: Database sync is handled by RayJobSyncService background task

            logger.info("Successfully deleted RayJob: %s", job_name)
            results[job_name] = True

        except ApiException as e:
            logger.error("Failed to delete job %s: %s", job_name, e)
            results[job_name] = False

    return DeleteRayJobResponse(results=results)


async def get_job_logs(
    job_name: str,
    log_type: str = "submitter",
    tail_lines: int | None = None,
    queue_name: str | None = None,
    cluster_name: str | None = None,
    db: AsyncSession | None = None,
) -> RayJobLogsResponse:
    """Get logs from a RayJob.

    Args:
        job_name: Name of the job
        log_type: Type of logs ('submitter' or 'head')
        tail_lines: Number of lines from end
        queue_name: Queue name to find namespace
        cluster_name: Target cluster
        db: Database session

    Returns:
        Job logs
    """
    session = await _resolve_session(db)
    cluster, defaults = await _get_cluster_and_defaults(cluster_name, session)

    queue = queue_name or defaults.get("queue")
    if not queue:
        raise ValueError("Queue name must be specified")

    namespace = get_namespace_for_queue(queue, cluster)

    try:
        custom_api = get_custom_objects_api(cluster)
        core_api = get_core_api(cluster)

        # Get the RayJob to find the pod
        rayjob = custom_api.get_namespaced_custom_object(
            group=RAY_GROUP,
            version=RAY_VERSION,
            namespace=namespace,
            plural=RAY_JOBS_PLURAL,
            name=job_name,
        )

        pod_name = None
        container_name = None

        if log_type == "submitter":
            pod_name = _get_driver_pod_name(job_name, rayjob, namespace, core_api)
            container_name = "ray-job-submitter"
        else:
            pod_name, _ = _get_head_pod_info(job_name, rayjob, namespace, core_api)
            container_name = "ray-head"

        if not pod_name:
            raise RuntimeError(f"Could not find Ray {log_type} pod for job: {job_name}")

        # Get logs
        logs = core_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container_name,
            tail_lines=tail_lines,
        )

        return RayJobLogsResponse(
            job_name=job_name,
            log_type=log_type,
            logs=logs,
        )

    except ApiException as e:
        logger.error("Failed to get logs for job %s: %s", job_name, e)
        raise


async def get_dashboard_info(
    job_name: str,
    queue_name: str | None = None,
    cluster_name: str | None = None,
    db: AsyncSession | None = None,
) -> RayJobDashboardResponse:
    """Get Ray dashboard connection information.

    Args:
        job_name: Name of the job
        queue_name: Queue name to find namespace
        cluster_name: Target cluster
        db: Database session

    Returns:
        Dashboard connection info
    """
    session = await _resolve_session(db)
    cluster, defaults = await _get_cluster_and_defaults(cluster_name, session)

    queue = queue_name or defaults.get("queue")
    if not queue:
        raise ValueError("Queue name must be specified")

    namespace = get_namespace_for_queue(queue, cluster)

    try:
        custom_api = get_custom_objects_api(cluster)
        core_api = get_core_api(cluster)

        rayjob = custom_api.get_namespaced_custom_object(
            group=RAY_GROUP,
            version=RAY_VERSION,
            namespace=namespace,
            plural=RAY_JOBS_PLURAL,
            name=job_name,
        )

        _, head_pod_ip = _get_head_pod_info(job_name, rayjob, namespace, core_api)
        if not head_pod_ip:
            raise RuntimeError(f"Could not find Ray head pod IP for job: {job_name}")

        return RayJobDashboardResponse(
            job_name=job_name,
            dashboard_url=f"http://{head_pod_ip}:8265",
            head_pod_ip=head_pod_ip,
        )

    except ApiException as e:
        logger.error("Failed to get dashboard info for job %s: %s", job_name, e)
        raise


# ============================================================================
# Helper functions
# ============================================================================


def _get_head_pod_info(
    job_name: str,
    rayjob: dict[str, Any],
    namespace: str,
    core_api: Any,
) -> tuple[str | None, str | None]:
    """Get the Ray head pod name and IP for a RayJob."""
    ray_cluster_status = rayjob.get("status", {}).get("rayClusterStatus", {})
    if not ray_cluster_status:
        return None, None

    head_pod_name = None
    head_pod_ip = None

    # Try to get from head info
    head_info = ray_cluster_status.get("head")
    if head_info and isinstance(head_info, dict):
        head_pod_name = head_info.get("podName")
        head_pod_ip = head_info.get("podIP")

    # Fallback: find by cluster name
    if not head_pod_name:
        cluster_name = ray_cluster_status.get("clusterName")
        if cluster_name:
            pods = core_api.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"ray.io/node-type=head,ray.io/cluster={cluster_name}",
            )
            if pods.items:
                head_pod_name = pods.items[0].metadata.name
                head_pod_ip = pods.items[0].status.pod_ip

    # Final fallback: find by job name pattern
    if not head_pod_name:
        pods = core_api.list_namespaced_pod(
            namespace=namespace, label_selector="ray.io/node-type=head"
        )
        for pod in pods.items:
            pod_labels = pod.metadata.labels or {}
            if (
                job_name in pod.metadata.name
                or pod_labels.get("job-name") == job_name
                or pod_labels.get("ray.io/cluster") == job_name
            ):
                head_pod_name = pod.metadata.name
                head_pod_ip = pod.status.pod_ip
                break

    return head_pod_name, head_pod_ip


def _get_driver_pod_name(
    job_name: str,
    rayjob: dict[str, Any],
    namespace: str,
    core_api: Any,
) -> str | None:
    """Get the Ray driver pod name for a RayJob."""
    ray_cluster_status = rayjob.get("status", {}).get("rayClusterStatus", {})
    if not ray_cluster_status:
        return None

    pods = core_api.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"job-name={job_name}",
    )
    if pods.items:
        return pods.items[0].metadata.name
    return None


def _create_rayjob_spec(
    job_name: str,
    request: RayJobSubmitRequest,
    namespace: str,
    defaults: dict[str, Any],
) -> dict[str, Any]:
    """Create the RayJob Kubernetes resource specification."""
    queue_name = request.queue_name or defaults.get("queue", "")
    ray_image = request.ray_image or defaults.get("ray_image", "rayproject/ray:2.50.0")
    image_secret = defaults.get("image_secret")

    # Base environment variables
    base_env = [
        {"name": "RAY_SCHEDULER_EVENTS", "value": "0"},
        {"name": "RAY_DEDUP_LOGS", "value": "0"},
        {"name": "RAY_worker_heartbeat_timeout_milliseconds", "value": "60000"},
        {"name": "RAY_raylet_heartbeat_period_milliseconds", "value": "300000"},
    ]

    # Add custom env vars
    if request.env_vars:
        for name, value in request.env_vars.items():
            base_env.append({"name": name, "value": str(value)})

    # Runtime environment
    runtime_env = request.runtime_env or {}

    # Resource calculations
    worker_cpu_request = max(1, request.num_cpus)
    worker_cpu_limit = max(2, worker_cpu_request)
    worker_memory_request = max(1, request.memory_gb)
    worker_memory_limit = max(2, worker_memory_request)

    # Common labels and annotations
    common_labels = {
        "app": "aether",
        "job-name": job_name,
        "user": request.user or os.environ.get("USER", "unknown"),
    }
    common_annotations = {
        "kueue.x-k8s.io/queue-name": queue_name,
        "karpenter.sh/do-not-disrupt": "true",
        "ray.io/overwrite-container-cmd": "true",
    }

    # Image pull secrets
    image_pull_secrets = [{"name": image_secret}] if image_secret else []

    rayjob_spec: dict[str, Any] = {
        "apiVersion": "ray.io/v1",
        "kind": "RayJob",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {
                **common_labels,
                "component": "rayjob",
                "queue": queue_name,
            },
            "annotations": common_annotations,
        },
        "spec": {
            "entrypoint": request.entrypoint,
            "runtimeEnvYAML": yaml.dump(runtime_env, default_flow_style=False)
            if runtime_env
            else "",
            "jobId": job_name,
            "shutdownAfterJobFinishes": request.shutdown_after_job_finishes,
            "ttlSecondsAfterFinished": request.ttl_seconds_after_finished,
            "suspend": False,
            "submitterPodTemplate": {
                "spec": {
                    **({"imagePullSecrets": image_pull_secrets} if image_pull_secrets else {}),
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "ray-job-submitter",
                            "image": ray_image,
                            "imagePullPolicy": "IfNotPresent",
                        }
                    ],
                }
            },
            "rayClusterSpec": {
                "rayVersion": request.ray_version,
                "enableInTreeAutoscaling": request.enable_autoscaling,
                "headGroupSpec": {
                    "rayStartParams": {
                        "dashboard-host": "0.0.0.0",
                        "dashboard-port": "8265",
                        "block": "true",
                        "num-cpus": "0",
                    },
                    "template": {
                        "metadata": {
                            "labels": {**common_labels, "component": "ray-head"},
                            "annotations": common_annotations,
                        },
                        "spec": {
                            **(
                                {"imagePullSecrets": image_pull_secrets}
                                if image_pull_secrets
                                else {}
                            ),
                            "containers": [
                                {
                                    "name": "ray-head",
                                    "image": ray_image,
                                    "imagePullPolicy": "IfNotPresent",
                                    "command": ["/bin/bash", "-c", "--"],
                                    "args": [
                                        "ulimit -n 1048576 && $KUBERAY_GEN_RAY_START_CMD"
                                    ],
                                    "env": base_env,
                                    "resources": {
                                        "requests": {"cpu": "8", "memory": "32Gi"},
                                        "limits": {"cpu": "16", "memory": "64Gi"},
                                    },
                                    "ports": [
                                        {"containerPort": 6379, "name": "gcs-server"},
                                        {"containerPort": 8265, "name": "dashboard"},
                                        {"containerPort": 10001, "name": "client"},
                                    ],
                                }
                            ],
                        },
                    },
                },
                "workerGroupSpecs": [
                    {
                        "replicas": request.worker_replicas,
                        "minReplicas": request.min_worker_replicas,
                        "maxReplicas": max(
                            request.max_worker_replicas,
                            request.worker_replicas,
                        ),
                        "groupName": "aether-worker-group",
                        "rayStartParams": {
                            "block": "true",
                            "num-cpus": str(worker_cpu_request),
                        },
                        "template": {
                            "metadata": {
                                "labels": {**common_labels, "component": "ray-worker"},
                                "annotations": common_annotations,
                            },
                            "spec": {
                                **(
                                    {"imagePullSecrets": image_pull_secrets}
                                    if image_pull_secrets
                                    else {}
                                ),
                                "containers": [
                                    {
                                        "name": "ray-worker",
                                        "image": ray_image,
                                        "imagePullPolicy": "IfNotPresent",
                                        "command": ["/bin/bash", "-c", "--"],
                                        "args": [
                                            "ulimit -n 1048576 && $KUBERAY_GEN_RAY_START_CMD"
                                        ],
                                        "env": base_env,
                                        "resources": {
                                            "requests": {
                                                "cpu": str(worker_cpu_request),
                                                "memory": f"{worker_memory_request}Gi",
                                                **(
                                                    {}
                                                    if request.num_gpus == 0
                                                    else {
                                                        "nvidia.com/gpu": str(request.num_gpus)
                                                    }
                                                ),
                                            },
                                            "limits": {
                                                "cpu": str(worker_cpu_limit),
                                                "memory": f"{worker_memory_limit}Gi",
                                                **(
                                                    {}
                                                    if request.num_gpus == 0
                                                    else {
                                                        "nvidia.com/gpu": str(request.num_gpus)
                                                    }
                                                ),
                                            },
                                        },
                                    }
                                ],
                            },
                        },
                    }
                ],
            },
        },
    }

    return rayjob_spec
