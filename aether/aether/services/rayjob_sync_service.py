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

"""Background service for syncing RayJob states from Kubernetes to database."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from kubernetes.client.rest import ApiException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.session import async_session_factory
from ..models.k8s import K8sCluster, RayJob
from .k8s_connection import get_custom_objects_api

logger = logging.getLogger(__name__)


class RayJobSyncService:
    """Service that periodically syncs RayJob states from Kubernetes to database."""

    RAY_GROUP = "ray.io"
    RAY_VERSION = "v1"
    RAY_JOBS_PLURAL = "rayjobs"

    def __init__(self, sync_interval_seconds: int = 30):
        self._sync_interval = sync_interval_seconds
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the background sync task."""
        if self._running:
            logger.warning("Sync service is already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._sync_loop())
        logger.info("RayJob sync service started with interval %ds", self._sync_interval)

    async def stop(self) -> None:
        """Stop the background sync task."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("RayJob sync service stopped")

    async def _sync_loop(self) -> None:
        """Main sync loop."""
        while self._running:
            try:
                await self.sync_all_clusters()
            except Exception as e:
                logger.exception("Error in sync loop: %s", e)

            await asyncio.sleep(self._sync_interval)

    async def sync_all_clusters(self) -> None:
        """Sync RayJobs from all configured clusters."""
        async with async_session_factory() as db:
            result = await db.execute(select(K8sCluster))
            clusters = result.scalars().all()

            for cluster in clusters:
                try:
                    await self._sync_cluster(cluster, db)
                except Exception as e:
                    logger.error("Failed to sync cluster %s: %s", cluster.name, e)

    async def _sync_cluster(self, cluster: K8sCluster, db: AsyncSession) -> None:
        """Sync RayJobs from a single cluster."""
        try:
            custom_api = get_custom_objects_api(cluster)

            # List all RayJobs in the cluster
            response = custom_api.list_cluster_custom_object(
                group=self.RAY_GROUP,
                version=self.RAY_VERSION,
                plural=self.RAY_JOBS_PLURAL,
            )

            k8s_jobs = {item["metadata"]["name"]: item for item in response.get("items", [])}

            # Get existing jobs in database for this cluster
            result = await db.execute(select(RayJob).where(RayJob.cluster_id == cluster.id))
            db_jobs = {job.job_name: job for job in result.scalars().all()}

            # Update existing jobs and create new ones
            for job_name, k8s_job in k8s_jobs.items():
                metadata = k8s_job.get("metadata", {})
                status_obj = k8s_job.get("status", {})
                spec = k8s_job.get("spec", {})
                labels = metadata.get("labels", {})
                annotations = metadata.get("annotations", {})

                job_status = status_obj.get("jobStatus", "UNKNOWN")
                namespace = metadata.get("namespace", "")
                queue_name = labels.get("kueue.x-k8s.io/queue-name", "") or annotations.get(
                    "kueue.x-k8s.io/queue-name", ""
                )

                # Parse timestamps
                start_time = None
                if start_str := status_obj.get("startTime"):
                    try:
                        start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    except ValueError:
                        pass

                end_time = None
                if end_str := status_obj.get("endTime"):
                    try:
                        end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    except ValueError:
                        pass

                # Get dashboard URL
                ray_cluster_status = status_obj.get("rayClusterStatus", {})
                dashboard_url = None
                head_info = ray_cluster_status.get("head", {})
                if head_info and isinstance(head_info, dict) and (pod_ip := head_info.get("podIP")):
                    dashboard_url = f"http://{pod_ip}:8265"

                if job_name in db_jobs:
                    # Update existing job
                    job = db_jobs[job_name]
                    job.status = job_status
                    job.namespace = namespace
                    job.queue_name = queue_name or job.queue_name
                    job.message = status_obj.get("message")
                    job.dashboard_url = dashboard_url
                    if start_time:
                        job.started_at = start_time
                    if end_time:
                        job.finished_at = end_time
                else:
                    # Create new job record (discovered from K8s)
                    new_job = RayJob(
                        job_name=job_name,
                        namespace=namespace,
                        queue_name=queue_name or "unknown",
                        entrypoint=spec.get("entrypoint", ""),
                        status=job_status,
                        message=status_obj.get("message"),
                        dashboard_url=dashboard_url,
                        user=labels.get("user"),
                        cluster_id=cluster.id,
                        started_at=start_time,
                        finished_at=end_time,
                    )
                    db.add(new_job)

            # Mark jobs that no longer exist in K8s (completed/deleted)
            for job_name, job in db_jobs.items():
                if job_name not in k8s_jobs and job.status not in [
                    "SUCCEEDED",
                    "FAILED",
                    "STOPPED",
                    "DELETED",
                ]:
                    job.status = "DELETED"
                    job.finished_at = datetime.now(UTC)

            await db.commit()
            logger.debug("Synced %d jobs from cluster %s", len(k8s_jobs), cluster.name)

        except ApiException as e:
            logger.error("K8s API error syncing cluster %s: %s", cluster.name, e)
            raise


# Global sync service instance
_sync_service: RayJobSyncService | None = None


def get_sync_service() -> RayJobSyncService:
    """Get the global sync service instance."""
    global _sync_service
    if _sync_service is None:
        _sync_service = RayJobSyncService()
    return _sync_service


async def start_sync_service() -> None:
    """Start the global sync service."""
    service = get_sync_service()
    await service.start()


async def stop_sync_service() -> None:
    """Stop the global sync service."""
    global _sync_service
    if _sync_service:
        await _sync_service.stop()
        _sync_service = None
