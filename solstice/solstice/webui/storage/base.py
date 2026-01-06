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

"""Storage backend protocol for WebUI data."""

from typing import Any, Dict, List, Optional, Protocol


class StorageBackend(Protocol):
    """Protocol for storage backends.

    All storage implementations must provide methods for storing
    and retrieving job metadata, metrics, events, and lineage data.
    """

    def store_job_archive(self, job_id: str, archive_data: Dict[str, Any]) -> None:
        """Store archived job data.

        Args:
            job_id: The job identifier
            archive_data: Complete job archive data
        """
        ...

    def get_job_archive(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve archived job data.

        Args:
            job_id: The job identifier

        Returns:
            Job archive data or None if not found
        """
        ...

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List archived jobs.

        Args:
            status: Filter by status (COMPLETED, FAILED, etc.)
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip

        Returns:
            List of job metadata
        """
        ...

    def store_metrics_snapshot(
        self,
        job_id: str,
        stage_id: str,
        timestamp: float,
        metrics: Dict[str, Any],
    ) -> None:
        """Store a metrics snapshot.

        Args:
            job_id: The job identifier
            stage_id: The stage identifier
            timestamp: Snapshot timestamp
            metrics: Metrics data
        """
        ...

    def get_metrics_history(
        self,
        job_id: str,
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Query metrics history.

        Args:
            job_id: The job identifier
            stage_id: The stage identifier
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            List of metrics snapshots
        """
        ...

    def store_exception(
        self,
        job_id: str,
        exception_id: str,
        exception_data: Dict[str, Any],
    ) -> None:
        """Store exception data.

        Args:
            job_id: The job identifier
            exception_id: Unique exception identifier
            exception_data: Exception details
        """
        ...

    def list_exceptions(
        self,
        job_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List exceptions for a job.

        Args:
            job_id: The job identifier
            limit: Maximum number of exceptions to return
            offset: Number of exceptions to skip

        Returns:
            List of exception data
        """
        ...

    def store_split_lineage(
        self,
        job_id: str,
        split_id: str,
        lineage_data: Dict[str, Any],
    ) -> None:
        """Store split lineage data.

        Args:
            job_id: The job identifier
            split_id: The split identifier
            lineage_data: Lineage information
        """
        ...

    def get_split_lineage(
        self,
        job_id: str,
        split_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get split lineage data.

        Args:
            job_id: The job identifier
            split_id: The split identifier

        Returns:
            Lineage data or None if not found
        """
        ...

    def store_worker_event(
        self,
        job_id: str,
        worker_id: str,
        timestamp: float,
        event_data: Dict[str, Any],
    ) -> None:
        """Store worker lifecycle event.

        Args:
            job_id: The job identifier
            worker_id: The worker identifier
            timestamp: Event timestamp
            event_data: Event details
        """
        ...

    def list_worker_events(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events.

        Args:
            job_id: The job identifier
            worker_id: Optional worker filter
            limit: Maximum number of events to return
            offset: Number of events to skip

        Returns:
            List of worker events
        """
        ...
