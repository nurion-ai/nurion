"""Service for managing Kueue LocalQueues."""

from __future__ import annotations

import logging

from kubernetes.client.rest import ApiException

from ..models.k8s import K8sCluster
from ..schemas.k8s import ListLocalQueuesResponse, LocalQueueInfo
from .k8s_connection import get_custom_objects_api

logger = logging.getLogger(__name__)

KUEUE_GROUP = "kueue.x-k8s.io"
KUEUE_VERSION = "v1beta1"
LOCAL_QUEUE_PLURAL = "localqueues"

# Cache for namespace lookups: (cluster_id, queue_name) -> namespace
_namespace_cache: dict[tuple[int, str], str] = {}


def list_queues(cluster: K8sCluster) -> ListLocalQueuesResponse:
    """List all LocalQueues in the cluster.

    Args:
        cluster: Target cluster

    Returns:
        List of LocalQueue information
    """
    try:
        custom_api = get_custom_objects_api(cluster)

        response = custom_api.list_cluster_custom_object(
            group=KUEUE_GROUP,
            version=KUEUE_VERSION,
            plural=LOCAL_QUEUE_PLURAL,
        )

        queues = []
        for item in response.get("items", []):
            metadata = item.get("metadata", {})
            spec = item.get("spec", {})
            status = item.get("status", {})

            queues.append(
                LocalQueueInfo(
                    name=metadata.get("name", ""),
                    namespace=metadata.get("namespace", ""),
                    cluster_queue=spec.get("clusterQueue"),
                    pending_workloads=status.get("pendingWorkloads", 0),
                    admitted_workloads=status.get("admittedWorkloads", 0),
                )
            )

        return ListLocalQueuesResponse(queues=queues)

    except ApiException as e:
        logger.error("Failed to list LocalQueues: %s", e)
        raise


def get_namespace_for_queue(
    queue_name: str,
    cluster: K8sCluster,
) -> str:
    """Get the namespace for a given LocalQueue.

    Args:
        queue_name: Name of the LocalQueue
        cluster: Target cluster

    Returns:
        Namespace name

    Raises:
        RuntimeError: If the queue is not found
    """
    # Check cache first
    cache_key = (cluster.id, queue_name)
    if cache_key in _namespace_cache:
        return _namespace_cache[cache_key]

    try:
        custom_api = get_custom_objects_api(cluster)

        response = custom_api.list_cluster_custom_object(
            group=KUEUE_GROUP,
            version=KUEUE_VERSION,
            plural=LOCAL_QUEUE_PLURAL,
        )

        for item in response.get("items", []):
            metadata = item.get("metadata", {})
            if metadata.get("name") == queue_name:
                namespace = metadata.get("namespace", "")
                _namespace_cache[cache_key] = namespace
                return namespace

        raise RuntimeError(f"LocalQueue '{queue_name}' not found in any namespace")

    except ApiException as e:
        logger.error("Failed to get namespace for queue %s: %s", queue_name, e)
        raise


def get_queue_info(
    queue_name: str,
    cluster: K8sCluster,
) -> LocalQueueInfo:
    """Get information about a specific LocalQueue.

    Args:
        queue_name: Name of the LocalQueue
        cluster: Target cluster

    Returns:
        LocalQueue information

    Raises:
        RuntimeError: If the queue is not found
    """
    try:
        custom_api = get_custom_objects_api(cluster)

        response = custom_api.list_cluster_custom_object(
            group=KUEUE_GROUP,
            version=KUEUE_VERSION,
            plural=LOCAL_QUEUE_PLURAL,
        )

        for item in response.get("items", []):
            metadata = item.get("metadata", {})
            if metadata.get("name") == queue_name:
                spec = item.get("spec", {})
                status = item.get("status", {})

                return LocalQueueInfo(
                    name=metadata.get("name", ""),
                    namespace=metadata.get("namespace", ""),
                    cluster_queue=spec.get("clusterQueue"),
                    pending_workloads=status.get("pendingWorkloads", 0),
                    admitted_workloads=status.get("admittedWorkloads", 0),
                )

        raise RuntimeError(f"LocalQueue '{queue_name}' not found")

    except ApiException as e:
        logger.error("Failed to get queue info for %s: %s", queue_name, e)
        raise


def clear_namespace_cache() -> None:
    """Clear the namespace cache."""
    _namespace_cache.clear()
