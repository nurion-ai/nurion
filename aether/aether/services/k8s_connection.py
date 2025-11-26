"""Helper module for establishing Kubernetes connections from database config."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from kubernetes import client
from kubernetes import config as k8s_config

from ..models.k8s import K8sCluster

logger = logging.getLogger(__name__)

# Cache for k8s clients by cluster ID
_k8s_client_cache: dict[int, client.ApiClient] = {}


def get_k8s_client_for_cluster(cluster: K8sCluster) -> client.ApiClient:
    """Get or create a Kubernetes API client for a cluster configuration.

    Args:
        cluster: K8sCluster model instance with kubeconfig content

    Returns:
        Configured Kubernetes API client
    """
    # Check cache
    if cluster.id in _k8s_client_cache:
        return _k8s_client_cache[cluster.id]

    if cluster.kubeconfig:
        # Write kubeconfig to a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as tmp_file:
            tmp_file.write(cluster.kubeconfig)
            tmp_path = tmp_file.name

        try:
            k8s_config.load_kube_config(config_file=tmp_path)
            api_client = client.ApiClient()
            _k8s_client_cache[cluster.id] = api_client
            logger.info("Created K8s client for cluster: %s", cluster.name)
            return api_client
        finally:
            # Clean up temp file
            Path(tmp_path).unlink(missing_ok=True)
    else:
        # Try in-cluster config or default kubeconfig
        try:
            k8s_config.load_incluster_config()
            logger.info("Using in-cluster K8s config for cluster: %s", cluster.name)
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
            logger.info("Using default kubeconfig for cluster: %s", cluster.name)

        api_client = client.ApiClient()
        _k8s_client_cache[cluster.id] = api_client
        return api_client


def get_custom_objects_api(cluster: K8sCluster) -> client.CustomObjectsApi:
    """Get CustomObjectsApi for a cluster."""
    api_client = get_k8s_client_for_cluster(cluster)
    return client.CustomObjectsApi(api_client)


def get_core_api(cluster: K8sCluster) -> client.CoreV1Api:
    """Get CoreV1Api for a cluster."""
    api_client = get_k8s_client_for_cluster(cluster)
    return client.CoreV1Api(api_client)


def get_version_api(cluster: K8sCluster) -> client.VersionApi:
    """Get VersionApi for a cluster."""
    api_client = get_k8s_client_for_cluster(cluster)
    return client.VersionApi(api_client)


def clear_client_cache(cluster_id: int | None = None) -> None:
    """Clear the client cache.

    Args:
        cluster_id: If provided, only clear cache for this cluster.
                   If None, clear all cached clients.
    """
    if cluster_id is not None:
        if cluster_id in _k8s_client_cache:
            _k8s_client_cache[cluster_id].close()
            del _k8s_client_cache[cluster_id]
    else:
        for api_client in _k8s_client_cache.values():
            api_client.close()
        _k8s_client_cache.clear()


def check_cluster_connection(
    cluster: K8sCluster,
) -> tuple[bool, str | None, str | None, int | None]:
    """Check/test connection to a Kubernetes cluster.

    Args:
        cluster: K8sCluster model instance

    Returns:
        Tuple of (connected, server_version, error, node_count)
    """
    try:
        version_api = get_version_api(cluster)
        version = version_api.get_code()
        server_version = f"{version.major}.{version.minor}"

        core_api = get_core_api(cluster)
        nodes = core_api.list_node()
        node_count = len(nodes.items)

        return True, server_version, None, node_count
    except Exception as e:
        logger.exception("Failed to connect to cluster %s", cluster.name)
        return False, None, str(e), None

