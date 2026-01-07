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

"""Storage backend protocols for WebUI data.

Two separate protocols for different use cases:
- JobStorageWriter: Per-job writing (path already contains job_id)
- JobStorageReader: Cross-job reading (needs job_id to locate data)
"""

from typing import Any, Dict, List, Optional, Protocol


class JobStorageWriter(Protocol):
    """Protocol for per-job storage writing.

    Used by JobStorage to write data for a single job.
    Since the storage path already contains job_id ({base_path}/{job_id}/{attempt_id}/),
    methods don't need job_id parameter.
    """

    def store_configuration(self, config_data: Dict[str, Any]) -> None:
        """Store job configuration (called at job start)."""
        ...

    def store_job_archive(self, archive_data: Dict[str, Any]) -> None:
        """Store archived job data."""
        ...

    def store_metrics_snapshot(
        self,
        stage_id: str,
        timestamp: float,
        metrics: Dict[str, Any],
    ) -> None:
        """Store a metrics snapshot."""
        ...

    def store_exception(
        self,
        exception_id: str,
        exception_data: Dict[str, Any],
    ) -> None:
        """Store exception data."""
        ...

    def store_split_lineage(
        self,
        split_id: str,
        lineage_data: Dict[str, Any],
    ) -> None:
        """Store split lineage data."""
        ...

    def store_worker_history(
        self,
        worker_id: str,
        worker_data: Dict[str, Any],
    ) -> None:
        """Store worker history snapshot."""
        ...

    def store_worker_event(
        self,
        worker_id: str,
        timestamp: float,
        event_data: Dict[str, Any],
    ) -> None:
        """Store worker lifecycle event."""
        ...

    def store_ray_event(
        self,
        event_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        """Store Ray event."""
        ...


class JobStorageReader(Protocol):
    """Protocol for cross-job storage reading.

    Used by PortalStorage to read data across multiple jobs.
    Methods need job_id to locate the correct job's storage.
    """

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List archived jobs."""
        ...

    def get_job_archive(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve archived job data."""
        ...

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Alias for get_job_archive."""
        ...

    def get_configuration(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve job configuration."""
        ...

    def get_metrics_history(
        self,
        job_id: str,
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Query metrics history."""
        ...

    def list_exceptions(
        self,
        job_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List exceptions for a job."""
        ...

    def get_split_lineage(
        self,
        job_id: str,
        split_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get split lineage data."""
        ...

    def get_lineage_graph(self, job_id: str) -> Dict[str, Any]:
        """Get complete lineage graph for a job."""
        ...

    def get_worker_history(
        self,
        job_id: str,
        worker_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get worker history."""
        ...

    def list_workers(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all workers for a job."""
        ...

    def list_worker_events(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events."""
        ...
