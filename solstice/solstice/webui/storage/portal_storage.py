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

"""Portal Storage - Read-only storage for scanning completed job archives.

This module provides storage access for the Portal service to discover
and read archived jobs from the storage directory.

Storage Structure:
    {base_path}/
    ├── job_a/
    │   ├── 20250101_120000_abc1/   # attempt 1 (SlateDB instance)
    │   └── 20250101_130000_def2/   # attempt 2 (SlateDB instance)
    └── job_b/
        └── 20250101_140000_ghi3/   # attempt 1

The Portal scans this directory structure to find completed jobs.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from solstice.utils.logging import create_ray_logger


def _open_slatedb_reader(path: str):
    """Open SlateDB in read-only mode using url parameter.

    Args:
        path: Full path to the SlateDB directory

    Returns:
        SlateDBReader instance for read-only access
    """
    from slatedb import SlateDBReader

    if path.startswith("s3://"):
        # S3 storage - use path as URL directly
        return SlateDBReader("db", url=path)
    else:
        # Local filesystem storage - use file:// URL
        url = f"file://{path}/"
        return SlateDBReader("db", url=url)


class PortalStorage:
    """Read-only storage for Portal to scan completed job archives.

    Unlike JobStorage which is used by individual jobs for writing,
    PortalStorage scans the base directory to discover all archived jobs.

    This avoids SlateDB single-writer conflicts by:
    1. Each job writes to its own isolated SlateDB instance
    2. Portal reads from all of them (read-only)
    """

    def __init__(self, base_path: str):
        """Initialize portal storage.

        Args:
            base_path: Base storage path containing job directories.
                       e.g., /tmp/solstice-webui/ or s3://bucket/solstice/
        """
        self.base_path = base_path.rstrip("/")
        self.logger = create_ray_logger("PortalStorage")
        self._is_s3 = base_path.startswith("s3://")

        self.logger.info(f"PortalStorage initialized at {self.base_path}")

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List archived jobs by scanning job directories.

        This scans {base_path}/*/* to find all attempt directories,
        reads the job archive from each, and returns them sorted by end_time.

        Args:
            status: Filter by status (COMPLETED, FAILED, etc.)
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip

        Returns:
            List of job archive data, sorted by end_time (newest first)
        """
        if self._is_s3:
            jobs = self._list_jobs_s3(status)
        else:
            jobs = self._list_jobs_local(status)

        # Sort by end_time (newest first)
        jobs.sort(key=lambda x: x.get("end_time", 0), reverse=True)

        # Apply offset and limit
        return jobs[offset : offset + limit]

    def _list_jobs_local(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs from local filesystem."""
        jobs = []
        base_dir = Path(self.base_path)

        if not base_dir.exists():
            return []

        # Scan job directories
        for job_dir in base_dir.iterdir():
            if not job_dir.is_dir():
                continue

            job_id = job_dir.name

            # Find the latest attempt (by directory name, which includes timestamp)
            attempts = sorted(job_dir.iterdir(), reverse=True)
            if not attempts:
                continue

            latest_attempt = attempts[0]
            if not latest_attempt.is_dir():
                continue

            # Try to read job archive from this attempt
            # Note: We catch exceptions here because scanning should be resilient -
            # one corrupted job shouldn't prevent listing others
            try:
                job_data = self._read_job_archive(str(latest_attempt), job_id)
                if job_data:
                    # Filter by status if specified
                    if status is None or job_data.get("status") == status:
                        jobs.append(job_data)
            except Exception as e:
                self.logger.debug(f"Skipping job {job_id}: {e}")

        return jobs

    def _list_jobs_s3(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs from S3 storage.

        Note: This is a placeholder - S3 scanning requires boto3 or similar.
        """
        self.logger.warning("S3 storage scanning not yet implemented")
        return []

    def _read_job_archive(self, attempt_path: str, job_id: str) -> Optional[Dict[str, Any]]:
        """Read job archive from an attempt directory using SlateDB.

        Args:
            attempt_path: Path to the attempt directory
            job_id: Expected job_id (unused, kept for API compatibility)

        Returns:
            Job archive data or None if not found
        """
        db = _open_slatedb_reader(attempt_path)
        # Key is just "job" since path already contains job_id
        key = b"job"
        data = db.get(key)
        db.close()
        if data:
            return json.loads(data.decode())
        return None

    def get_job_archive(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get archived job data by job_id.

        Scans the job's directory to find the latest attempt and read its archive.

        Args:
            job_id: The job identifier

        Returns:
            Job archive data or None if not found
        """
        if self._is_s3:
            return self._get_job_archive_s3(job_id)
        return self._get_job_archive_local(job_id)

    def _get_job_archive_local(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job archive from local filesystem."""
        job_dir = Path(self.base_path) / job_id

        if not job_dir.exists():
            return None

        # Find the latest attempt
        attempts = sorted(job_dir.iterdir(), reverse=True)
        if not attempts:
            return None

        latest_attempt = attempts[0]
        if not latest_attempt.is_dir():
            return None

        return self._read_job_archive(str(latest_attempt), job_id)

    def _get_job_archive_s3(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job archive from S3."""
        self.logger.warning("S3 job archive retrieval not yet implemented")
        return None

    def _get_latest_attempt_path(self, job_id: str) -> Optional[Path]:
        """Get the path to the latest attempt directory for a job."""
        if self._is_s3:
            return None

        job_dir = Path(self.base_path) / job_id
        if not job_dir.exists():
            return None

        attempts = sorted(job_dir.iterdir(), reverse=True)
        if not attempts:
            return None

        latest_attempt = attempts[0]
        if not latest_attempt.is_dir():
            return None

        return latest_attempt

    def list_exceptions(
        self,
        job_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List exceptions for a job by scanning its SlateDB."""
        if self._is_s3:
            return []

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return []

        db = _open_slatedb_reader(str(latest_attempt))
        # Key is "exception:" since path already contains job_id
        prefix = b"exception:"
        results = []

        for key, value in db.scan_prefix(prefix):
            results.append(json.loads(value.decode()))
            if len(results) >= offset + limit:
                break

        db.close()
        return results[offset : offset + limit]

    def list_worker_events(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events for a job."""
        if self._is_s3:
            return []

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return []

        db = _open_slatedb_reader(str(latest_attempt))

        # Keys don't contain job_id since path already has it
        if worker_id:
            prefix = f"worker_event:{worker_id}:".encode()
        else:
            prefix = b"worker_event:"

        results = []
        for key, value in db.scan_prefix(prefix):
            results.append(json.loads(value.decode()))
            if len(results) >= offset + limit:
                break

        db.close()
        return results[offset : offset + limit]

    def get_metrics_history(
        self,
        job_id: str,
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Get metrics history for a stage."""
        if self._is_s3:
            return []

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return []

        db = _open_slatedb_reader(str(latest_attempt))
        # Key is "metrics:{stage_id}:" since path already contains job_id
        prefix = f"metrics:{stage_id}:".encode()

        results = []
        for key, value in db.scan_prefix(prefix):
            # Extract timestamp from key: metrics:{stage_id}:{timestamp}
            parts = key.decode().split(":")
            if len(parts) >= 3:
                ts = float(parts[2])
                if start_time <= ts <= end_time:
                    results.append(json.loads(value.decode()))

        db.close()
        return sorted(results, key=lambda x: x.get("timestamp", 0))

    def get_lineage_graph(self, job_id: str) -> Dict[str, Any]:
        """Get complete lineage graph for a job."""
        if self._is_s3:
            return {"nodes": [], "edges": []}

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return {"nodes": [], "edges": []}

        db = _open_slatedb_reader(str(latest_attempt))
        # Key is "lineage:" since path already contains job_id
        prefix = b"lineage:"

        nodes = []
        edges = []

        for _, value in db.scan_prefix(prefix):
            lineage = json.loads(value.decode())
            split_id = lineage["split_id"]

            nodes.append(
                {
                    "id": split_id,
                    "split_id": split_id,
                    "stage_id": lineage.get("stage_id"),
                    "worker_id": lineage.get("worker_id"),
                    "timestamp": lineage.get("timestamp"),
                }
            )

            for parent_id in lineage.get("parent_ids", []):
                edges.append({"source": parent_id, "target": split_id})

        db.close()
        return {"nodes": nodes, "edges": edges}

    def get_split_lineage(self, job_id: str, split_id: str) -> Optional[Dict[str, Any]]:
        """Get lineage for a specific split."""
        if self._is_s3:
            return None

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return None

        db = _open_slatedb_reader(str(latest_attempt))
        # Key is "lineage:{split_id}" since path already contains job_id
        key = f"lineage:{split_id}".encode()
        data = db.get(key)
        db.close()
        if data:
            return json.loads(data.decode())
        return None

    def list_workers(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all workers for a job."""
        if self._is_s3:
            return []

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return []

        db = _open_slatedb_reader(str(latest_attempt))
        # Key is "worker:" since path already contains job_id
        prefix = b"worker:"

        workers = []
        for _, value in db.scan_prefix(prefix):
            worker = json.loads(value.decode())

            # Filter by stage_id if specified
            if stage_id and worker.get("stage_id") != stage_id:
                continue

            # Filter by status if specified
            if status and worker.get("status") != status:
                continue

            workers.append(worker)

        db.close()
        sorted_workers = sorted(
            workers, key=lambda x: x.get("start_time", 0), reverse=True
        )
        return sorted_workers[offset : offset + limit]

    def get_worker_history(
        self,
        job_id: str,
        worker_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get worker history."""
        if self._is_s3:
            return None

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return None

        db = _open_slatedb_reader(str(latest_attempt))
        # Key is "worker:{worker_id}" since path already contains job_id
        key = f"worker:{worker_id}".encode()
        data = db.get(key)
        db.close()
        if data:
            return json.loads(data.decode())
        return None
