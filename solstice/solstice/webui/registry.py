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

"""Global job registry for tracking running Solstice jobs."""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import ray

from solstice.utils.logging import create_ray_logger


@dataclass
class StageInfo:
    """Stage information for display."""

    stage_id: str
    worker_count: int
    output_queue_size: int
    is_running: bool
    is_finished: bool
    failed: bool = False
    input_count: int = 0
    output_count: int = 0


@dataclass
class JobRegistration:
    """Registration information for a running job.

    This minimal metadata allows the Portal to list running jobs
    and route requests to the appropriate job WebUI instance.

    Note: This dataclass must be serializable for Ray Actor calls.
    Do NOT add non-serializable fields.
    """

    job_id: str
    job_name: str
    start_time: float
    status: str  # INITIALIZING, RUNNING, COMPLETED, FAILED

    # Job structure
    stage_count: int
    worker_count: int

    # For accessing job data (string, serializable)
    runner_actor_name: str  # RayJobRunner actor name for data access

    # Attempt tracking (internal, not exposed to user)
    # Distinguishes multiple runs of the same job_id
    attempt_id: str = ""

    # Stages info (updated periodically)
    stages: list = field(default_factory=list)  # List of StageInfo dicts

    # Last update timestamp
    last_update: float = field(default_factory=time.time)

    @property
    def duration(self) -> float:
        """Duration since start in seconds."""
        return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "start_time": self.start_time,
            "status": self.status,
            "stage_count": self.stage_count,
            "worker_count": self.worker_count,
            "runner_actor_name": self.runner_actor_name,
            "attempt_id": self.attempt_id,
            "stages": self.stages,
            "last_update": self.last_update,
            "duration": self.duration,
        }


@ray.remote(num_cpus=0)
class JobRegistry:
    """Global singleton registry for running Solstice jobs.

    All RayJobRunner instances register themselves here when starting
    and unregister when completing. The WebUI Portal queries this
    registry to list running jobs and route requests.

    This is a Ray Actor with 'detached' lifetime, meaning it persists
    across job lifecycles and can be shared by multiple jobs in the
    same Ray cluster.

    Usage:
        # Get or create singleton
        registry = get_or_create_registry()

        # Register a job
        ray.get(registry.register.remote(job_id, JobRegistration(...)))

        # List all jobs
        jobs = ray.get(registry.list_jobs.remote())

        # Unregister when done
        ray.get(registry.unregister.remote(job_id))
    """

    def __init__(self):
        """Initialize the registry."""
        self._jobs: Dict[str, JobRegistration] = {}
        self.logger = create_ray_logger("JobRegistry")
        self.logger.info("JobRegistry initialized")

    def register(self, job_id: str, info: JobRegistration) -> None:
        """Register a running job.

        Args:
            job_id: Job identifier
            info: Job registration information
        """
        self._jobs[job_id] = info
        self.logger.info(
            f"Registered job {job_id} ({info.job_name}) with {info.stage_count} stages"
        )

    def unregister(self, job_id: str) -> bool:
        """Unregister a job (called when job completes or fails).

        Args:
            job_id: Job identifier

        Returns:
            True if job was found and removed, False otherwise
        """
        if job_id in self._jobs:
            self._jobs.pop(job_id)
            self.logger.info(f"Unregistered job {job_id}")
            return True
        return False

    def update(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """Update job registration information.

        Args:
            job_id: Job identifier
            updates: Fields to update (status, worker_count, etc.)

        Returns:
            True if updated, False if job not found
        """
        if job_id not in self._jobs:
            return False

        job = self._jobs[job_id]

        # Update allowed fields
        if "status" in updates:
            job.status = updates["status"]
        if "worker_count" in updates:
            job.worker_count = updates["worker_count"]
        if "stages" in updates:
            job.stages = updates["stages"]
        if "last_update" in updates:
            job.last_update = updates["last_update"]
        else:
            job.last_update = time.time()

        self.logger.debug(f"Updated job {job_id}: {updates}")
        return True

    def list_jobs(self) -> Dict[str, JobRegistration]:
        """List all registered running jobs.

        Returns:
            Dictionary mapping job_id to JobRegistration
        """
        return dict(self._jobs)

    def get_job(self, job_id: str) -> Optional[JobRegistration]:
        """Get registration info for a specific job.

        Args:
            job_id: Job identifier

        Returns:
            JobRegistration or None if not found
        """
        return self._jobs.get(job_id)

    def get_job_count(self) -> int:
        """Get count of registered jobs.

        Returns:
            Number of registered jobs
        """
        return len(self._jobs)

    def get_status_summary(self) -> Dict[str, int]:
        """Get count of jobs by status.

        Returns:
            Dictionary mapping status to count
        """
        summary = {}
        for job in self._jobs.values():
            summary[job.status] = summary.get(job.status, 0) + 1
        return summary


# Singleton accessor

_REGISTRY_NAME = "solstice_job_registry"


def get_or_create_registry() -> ray.actor.ActorHandle:
    """Get or create the global JobRegistry singleton.

    This function ensures only one JobRegistry exists in the Ray cluster.

    Returns:
        Ray actor handle to the JobRegistry

    Raises:
        RuntimeError: If Ray is not initialized
    """
    if not ray.is_initialized():
        raise RuntimeError("Ray must be initialized before accessing JobRegistry")

    try:
        # Try to get existing registry
        return ray.get_actor(_REGISTRY_NAME)
    except ValueError:
        # Registry doesn't exist, create it
        logger = create_ray_logger("registry")
        logger.info("Creating new JobRegistry actor")

        return JobRegistry.options(
            name=_REGISTRY_NAME,
            lifetime="detached",  # Persist across jobs
            max_concurrency=100,  # Support many concurrent registrations
        ).remote()


def registry_exists() -> bool:
    """Check if JobRegistry exists without creating it.

    Returns:
        True if registry exists, False otherwise
    """
    if not ray.is_initialized():
        return False

    try:
        ray.get_actor(_REGISTRY_NAME)
        return True
    except ValueError:
        return False
