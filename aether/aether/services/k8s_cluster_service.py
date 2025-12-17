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

"""Service for managing Kubernetes cluster configurations."""

from __future__ import annotations

import logging

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.session import get_session
from ..models.k8s import K8sCluster
from ..schemas.k8s import (
    ClusterConfig,
    ClusterConfigCreate,
    ClusterConfigResponse,
    ClusterConfigUpdate,
    ClusterStatus,
    ClusterStatusResponse,
    ListClustersResponse,
)
from .k8s_connection import check_cluster_connection, clear_client_cache

logger = logging.getLogger(__name__)


async def _resolve_session(db: AsyncSession | None) -> AsyncSession:
    if db is None:
        async for session in get_session():
            return session
    return db


def _cluster_to_config(cluster: K8sCluster) -> ClusterConfig:
    """Convert K8sCluster model to ClusterConfig schema."""
    return ClusterConfig(
        name=cluster.name,
        kubeconfig=cluster.kubeconfig,
        default_queue=cluster.default_queue,
        default_ray_image=cluster.default_ray_image,
        image_pull_secret=cluster.image_pull_secret,
        is_default=cluster.is_default,
    )


async def list_clusters(db: AsyncSession | None = None) -> ListClustersResponse:
    """List all registered cluster configurations."""
    session = await _resolve_session(db)
    result = await session.execute(select(K8sCluster).order_by(K8sCluster.name))
    clusters = result.scalars().all()

    return ListClustersResponse(clusters=[_cluster_to_config(c) for c in clusters])


async def create_cluster(
    request: ClusterConfigCreate, db: AsyncSession | None = None
) -> ClusterConfigResponse:
    """Register a new cluster configuration."""
    session = await _resolve_session(db)

    # If this cluster should be default, unset other defaults first
    if request.is_default:
        await session.execute(
            update(K8sCluster).where(K8sCluster.is_default.is_(True)).values(is_default=False)
        )

    cluster = K8sCluster(
        name=request.name,
        kubeconfig=request.kubeconfig,
        default_queue=request.default_queue,
        default_ray_image=request.default_ray_image,
        image_pull_secret=request.image_pull_secret,
        is_default=request.is_default,
    )
    session.add(cluster)
    await session.commit()
    await session.refresh(cluster)

    return ClusterConfigResponse(cluster=_cluster_to_config(cluster))


async def get_cluster(name: str, db: AsyncSession | None = None) -> ClusterConfigResponse:
    """Get a cluster configuration by name."""
    session = await _resolve_session(db)
    result = await session.execute(select(K8sCluster).where(K8sCluster.name == name))
    cluster = result.scalar_one_or_none()

    if not cluster:
        raise ValueError(f"Cluster not found: {name}")

    return ClusterConfigResponse(cluster=_cluster_to_config(cluster))


async def get_cluster_by_name(name: str, db: AsyncSession | None = None) -> K8sCluster | None:
    """Get cluster model by name (internal use)."""
    session = await _resolve_session(db)
    result = await session.execute(select(K8sCluster).where(K8sCluster.name == name))
    return result.scalar_one_or_none()


async def get_default_cluster(db: AsyncSession | None = None) -> K8sCluster | None:
    """Get the default cluster."""
    session = await _resolve_session(db)
    result = await session.execute(select(K8sCluster).where(K8sCluster.is_default.is_(True)))
    return result.scalar_one_or_none()


async def get_cluster_or_default(
    cluster_name: str | None = None, db: AsyncSession | None = None
) -> K8sCluster | None:
    """Get cluster by name, or get default cluster if name is None."""
    if cluster_name:
        return await get_cluster_by_name(cluster_name, db)
    return await get_default_cluster(db)


async def update_cluster(
    name: str, request: ClusterConfigUpdate, db: AsyncSession | None = None
) -> ClusterConfigResponse:
    """Update a cluster configuration."""
    session = await _resolve_session(db)
    cluster = await get_cluster_by_name(name, session)
    if not cluster:
        raise ValueError(f"Cluster not found: {name}")

    # Clear cached client if kubeconfig changes
    if request.kubeconfig is not None:
        clear_client_cache(cluster.id)
        cluster.kubeconfig = request.kubeconfig

    if request.default_queue is not None:
        cluster.default_queue = request.default_queue
    if request.default_ray_image is not None:
        cluster.default_ray_image = request.default_ray_image
    if request.image_pull_secret is not None:
        cluster.image_pull_secret = request.image_pull_secret
    if request.is_default is not None and request.is_default:
        # Unset other defaults first
        await session.execute(
            update(K8sCluster)
            .where(K8sCluster.is_default.is_(True), K8sCluster.name != name)
            .values(is_default=False)
        )
        cluster.is_default = True

    await session.commit()
    await session.refresh(cluster)

    return ClusterConfigResponse(cluster=_cluster_to_config(cluster))


async def delete_cluster(name: str, db: AsyncSession | None = None) -> bool:
    """Delete a cluster configuration."""
    session = await _resolve_session(db)
    cluster = await get_cluster_by_name(name, session)
    if not cluster:
        raise ValueError(f"Cluster not found: {name}")

    was_default = cluster.is_default
    cluster_id = cluster.id

    # Clear cached client
    clear_client_cache(cluster_id)

    await session.delete(cluster)
    await session.commit()

    # If the deleted cluster was default, set another one as default
    if was_default:
        result = await session.execute(select(K8sCluster).limit(1))
        new_default = result.scalar_one_or_none()
        if new_default:
            new_default.is_default = True
            await session.commit()

    return True


async def set_default_cluster(name: str, db: AsyncSession | None = None) -> ClusterConfigResponse:
    """Set a cluster as the default."""
    session = await _resolve_session(db)
    cluster = await get_cluster_by_name(name, session)
    if not cluster:
        raise ValueError(f"Cluster not found: {name}")

    # Unset all other defaults
    await session.execute(
        update(K8sCluster).where(K8sCluster.name != name).values(is_default=False)
    )
    cluster.is_default = True
    await session.commit()
    await session.refresh(cluster)

    return ClusterConfigResponse(cluster=_cluster_to_config(cluster))


async def get_cluster_status(name: str, db: AsyncSession | None = None) -> ClusterStatusResponse:
    """Get the connection status of a cluster."""
    cluster = await get_cluster_by_name(name, db)
    if not cluster:
        return ClusterStatusResponse(
            status=ClusterStatus(
                name=name,
                connected=False,
                error=f"Cluster not found: {name}",
            )
        )

    connected, server_version, error, node_count = check_cluster_connection(cluster)

    return ClusterStatusResponse(
        status=ClusterStatus(
            name=name,
            connected=connected,
            server_version=server_version,
            node_count=node_count,
            error=error,
        )
    )


async def test_connection(name: str, db: AsyncSession | None = None) -> ClusterStatusResponse:
    """Test connection to a cluster."""
    return await get_cluster_status(name, db)
