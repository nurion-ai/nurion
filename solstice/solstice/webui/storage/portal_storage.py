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
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from solstice.utils.logging import create_ray_logger


@contextmanager
def _open_slatedb(path: str) -> Generator:
    """Open SlateDB reader as context manager.

    Args:
        path: Full path to the SlateDB directory

    Yields:
        SlateDBReader instance for read-only access
    """
    from slatedb import SlateDBReader

    if path.startswith("s3://"):
        db = SlateDBReader("db", url=path)
    else:
        db = SlateDBReader("db", url=f"file://{path}/")
    try:
        yield db
    finally:
        db.close()


class PortalStorage:
    """Read-only storage for Portal to scan completed job archives.

    Unlike JobStorage which is used by individual jobs for writing,
    PortalStorage scans the base directory to discover all archived jobs.

    This avoids SlateDB single-writer conflicts by:
    1. Each job writes to its own isolated SlateDB instance
    2. Portal reads from all of them (read-only)

    Note: SlateDBReader is a read-only snapshot that doesn't see updates.
    Each method opens a fresh connection to ensure reading latest data.
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

        # Sort by end_time (newest first), handle None values
        jobs.sort(key=lambda x: x.get("end_time") or 0, reverse=True)

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
        with _open_slatedb(attempt_path) as db:
            data = db.get(b"job")
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

    def get_configuration(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job configuration from storage.

        Tries to read from dedicated 'config' key first (written by JobWebUI),
        then falls back to extracting from 'job' archive data.

        Args:
            job_id: The job identifier

        Returns:
            Configuration data with job_config, stage_configs, environment
        """
        if self._is_s3:
            return None

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return None

        with _open_slatedb(str(latest_attempt)) as db:
            # Try dedicated config key first
            config_data = db.get(b"config")
            if config_data:
                return json.loads(config_data.decode())

            # Fallback: extract from job archive
            job_data = db.get(b"job")
            if not job_data:
                return None

            job_archive = json.loads(job_data.decode())
            return self._extract_config_from_archive(job_archive)

    def _extract_config_from_archive(self, job_archive: Dict[str, Any]) -> Dict[str, Any]:
        """Extract configuration from job archive data.

        Args:
            job_archive: Job archive dictionary from SlateDB

        Returns:
            Configuration in standard format
        """
        result: Dict[str, Any] = {
            "job_config": job_archive.get("config", {}),
            "stage_configs": {},
            "environment": {},
        }

        # Build stage configs from archived stages
        for stage in job_archive.get("stages", []):
            stage_id = stage.get("stage_id", "")
            if stage_id:
                result["stage_configs"][stage_id] = {
                    "operator_type": stage.get("operator_type", "N/A"),
                    "min_parallelism": stage.get("min_parallelism", 1),
                    "max_parallelism": stage.get("max_parallelism", 1),
                    "num_cpus": stage.get("num_cpus", 0),
                    "num_gpus": stage.get("num_gpus", 0),
                    "memory_mb": stage.get("memory_mb", 0),
                }

        return result

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

        with _open_slatedb(str(latest_attempt)) as db:
            results = []
            for _, value in db.scan_prefix(b"exception:"):
                results.append(json.loads(value.decode()))
                if len(results) >= offset + limit:
                    break
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

        with _open_slatedb(str(latest_attempt)) as db:
            prefix = f"metrics:{stage_id}:".encode()
            results = []
            for key, value in db.scan_prefix(prefix):
                parts = key.decode().split(":")
                if len(parts) >= 3:
                    key_ts = float(parts[2])
                    if start_time <= key_ts <= end_time:
                        data = json.loads(value.decode())
                        if data.get("timestamp") is None:
                            data["timestamp"] = key_ts
                        results.append(data)
            return sorted(results, key=lambda x: x.get("timestamp") or 0)

    # -------------------------------------------------------------------------
    # Lineage (4 core methods)
    # -------------------------------------------------------------------------

    def get_split_lineage(self, job_id: str, split_id: str) -> Optional[Dict[str, Any]]:
        """Get lineage for a specific split."""
        if self._is_s3:
            return None

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return None

        with _open_slatedb(str(latest_attempt)) as db:
            data = db.get(f"lineage:{split_id}".encode())
            if data:
                return json.loads(data.decode())
            return None

    def list_splits_by_stage(
        self,
        job_id: str,
        stage_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List splits for a stage."""
        if self._is_s3:
            return []

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return []

        with _open_slatedb(str(latest_attempt)) as db:
            prefix = f"lineage_by_stage:{stage_id}:".encode()
            splits = []
            for _, split_id_bytes in db.scan_prefix(prefix):
                split_id = split_id_bytes.decode()
                lineage_data = db.get(f"lineage:{split_id}".encode())
                if lineage_data:
                    splits.append(json.loads(lineage_data.decode()))

            splits = sorted(splits, key=lambda x: x.get("timestamp") or 0, reverse=True)
            return splits[offset : offset + limit]

    def list_workers(
        self,
        job_id: str,
        stage_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all workers for a job."""
        if self._is_s3:
            return []

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return []

        with _open_slatedb(str(latest_attempt)) as db:
            workers = []
            for _, value in db.scan_prefix(b"worker:"):
                worker = json.loads(value.decode())
                if stage_id and worker.get("stage_id") != stage_id:
                    continue
                workers.append(worker)

            sorted_workers = sorted(workers, key=lambda x: x.get("start_time") or 0, reverse=True)
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

        with _open_slatedb(str(latest_attempt)) as db:
            data = db.get(f"worker:{worker_id}".encode())
            if data:
                return json.loads(data.decode())
            return None

    # -------------------------------------------------------------------------
    # Lineage (continued - overview and trace)
    # -------------------------------------------------------------------------

    def get_lineage_overview(self, job_id: str) -> Dict[str, Any]:
        """Get stage-level lineage overview with aggregated statistics.

        Returns:
            Dict with 'stages', 'edges', and 'dag_edges'
        """
        if self._is_s3:
            return {"stages": [], "edges": [], "dag_edges": {}}

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return {"stages": [], "edges": [], "dag_edges": {}}

        with _open_slatedb(str(latest_attempt)) as db:
            job_data = db.get(b"job")
            if not job_data:
                return {"stages": [], "edges": [], "dag_edges": {}}

            job_info = json.loads(job_data.decode())
            dag_edges = job_info.get("dag_edges", {})
            stages_list = job_info.get("stages", [])
            stage_order = [s.get("stage_id") for s in stages_list]

            # Collect all lineage records grouped by stage
            stage_splits: Dict[str, List[Dict]] = {}
            for _, value in db.scan_prefix(b"lineage:"):
                lineage = json.loads(value.decode())
                stage_id = lineage.get("stage_id", "")
                if stage_id not in stage_splits:
                    stage_splits[stage_id] = []
                stage_splits[stage_id].append(lineage)

            # Calculate edge statistics
            edges = []
            for from_stage, to_stages in dag_edges.items():
                for to_stage in to_stages:
                    to_splits = stage_splits.get(to_stage, [])
                    if not to_splits:
                        edges.append(
                            {
                                "from_stage": from_stage,
                                "to_stage": to_stage,
                                "splits_count": 0,
                                "total_rows": 0,
                                "total_bytes": 0,
                            }
                        )
                        continue

                    total_rows = sum(s.get("output_records", 0) for s in to_splits)
                    total_bytes = sum(s.get("output_bytes", 0) for s in to_splits)
                    rows_list = [s.get("output_records", 0) for s in to_splits]
                    bytes_list = [s.get("output_bytes", 0) for s in to_splits]
                    proc_times = [s.get("processing_time_ms", 0) for s in to_splits]

                    edges.append(
                        {
                            "from_stage": from_stage,
                            "to_stage": to_stage,
                            "splits_count": len(to_splits),
                            "total_rows": total_rows,
                            "total_bytes": total_bytes,
                            "min_rows": min(rows_list) if rows_list else 0,
                            "max_rows": max(rows_list) if rows_list else 0,
                            "min_bytes": min(bytes_list) if bytes_list else 0,
                            "max_bytes": max(bytes_list) if bytes_list else 0,
                            "min_processing_ms": min(proc_times) if proc_times else 0,
                            "max_processing_ms": max(proc_times) if proc_times else 0,
                            "avg_processing_ms": sum(proc_times) / len(proc_times)
                            if proc_times
                            else 0,
                        }
                    )

            # Stage stats
            stage_stats = []
            for stage_id in stage_order:
                splits = stage_splits.get(stage_id, [])
                if not splits:
                    stage_stats.append(
                        {
                            "stage_id": stage_id,
                            "splits_count": 0,
                            "total_output_rows": 0,
                            "total_output_bytes": 0,
                        }
                    )
                    continue

                total_rows = sum(s.get("output_records", 0) for s in splits)
                total_bytes = sum(s.get("output_bytes", 0) for s in splits)

                stage_stats.append(
                    {
                        "stage_id": stage_id,
                        "splits_count": len(splits),
                        "total_output_rows": total_rows,
                        "total_output_bytes": total_bytes,
                    }
                )

            return {"stages": stage_stats, "edges": edges, "dag_edges": dag_edges}

    def get_split_trace(self, job_id: str, split_id: str) -> Dict[str, Any]:
        """Get complete lineage trace for a split (both upstream and downstream).

        Returns:
            Dict with 'splits' (ordered by stage), 'edges', and 'root_split_id'
        """
        if self._is_s3:
            return {"splits": [], "edges": [], "root_split_id": split_id}

        latest_attempt = self._get_latest_attempt_path(job_id)
        if not latest_attempt:
            return {"splits": [], "edges": [], "root_split_id": split_id}

        with _open_slatedb(str(latest_attempt)) as db:
            visited: set = set()
            splits: list = []
            edges: list = []

            def collect_upstream(current_id: str):
                if current_id in visited:
                    return
                visited.add(current_id)

                lineage_data = db.get(f"lineage:{current_id}".encode())
                if not lineage_data:
                    return

                lineage = json.loads(lineage_data.decode())
                splits.append(lineage)

                for parent_id in lineage.get("parent_split_ids", []):
                    edges.append({"source": parent_id, "target": current_id})
                    collect_upstream(parent_id)

            def collect_downstream(current_id: str):
                if current_id in visited:
                    return
                visited.add(current_id)

                lineage_data = db.get(f"lineage:{current_id}".encode())
                if not lineage_data:
                    return

                lineage = json.loads(lineage_data.decode())
                if current_id not in [s.get("split_id") for s in splits]:
                    splits.append(lineage)

                for _, child_id_bytes in db.scan_prefix(
                    f"lineage_by_parent:{current_id}:".encode()
                ):
                    child_id = child_id_bytes.decode()
                    edges.append({"source": current_id, "target": child_id})
                    visited.discard(current_id)
                    collect_downstream(child_id)

            collect_upstream(split_id)
            visited.clear()
            collect_downstream(split_id)

            # Sort splits by stage order
            job_data = db.get(b"job")
            stage_order = {}
            if job_data:
                job_info = json.loads(job_data.decode())
                for i, s in enumerate(job_info.get("stages", [])):
                    stage_order[s.get("stage_id")] = i

            splits.sort(key=lambda x: stage_order.get(x.get("stage_id"), 999))

            return {"splits": splits, "edges": edges, "root_split_id": split_id}
