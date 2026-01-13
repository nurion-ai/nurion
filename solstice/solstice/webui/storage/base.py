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

    def store_split_lineage_with_children(
        self,
        split_id: str,
        lineage_data: Dict[str, Any],
    ) -> None:
        """Store split lineage data and update parent→child indexes atomically.

        This ensures consistency between lineage records and reverse indexes.
        All writes happen in a single transaction/batch.
        """
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

    # -------------------------------------------------------------------------
    # Job & Configuration
    # -------------------------------------------------------------------------

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List archived jobs."""
        ...

    def get_job_archive(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve archived job data (includes stages, dag_edges, etc)."""
        ...

    def get_configuration(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve job configuration."""
        ...

    # -------------------------------------------------------------------------
    # Metrics & Exceptions
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # Lineage (4 core methods)
    # -------------------------------------------------------------------------

    def get_split_lineage(
        self,
        job_id: str,
        split_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get single split's lineage details."""
        ...

    def list_splits_by_stage(
        self,
        job_id: str,
        stage_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List splits for a stage with pagination."""
        ...

    def get_lineage_overview(self, job_id: str) -> Dict[str, Any]:
        """Get stage-level lineage overview with aggregated statistics.

        Returns:
            Dict with:
            - 'stages': list of {stage_id, splits_count, total_rows, total_bytes}
            - 'edges': list of {from_stage, to_stage, splits_count, total_rows,
                total_bytes, min/max rows/bytes/processing_ms}
            - 'dag_edges': original DAG structure
        """
        ...

    def get_split_trace(
        self,
        job_id: str,
        split_id: str,
    ) -> Dict[str, Any]:
        """Get complete lineage trace for a split (both upstream and downstream).

        Returns:
            Dict with:
            - 'splits': list of split details ordered by stage
            - 'edges': list of {parent_id, child_id} relationships
        """
        ...

    # -------------------------------------------------------------------------
    # Workers
    # -------------------------------------------------------------------------

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
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List workers for a job."""
        ...

    def list_worker_events(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events for a job."""
        ...
