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

"""SlateDB storage backend for WebUI data persistence."""

import json
import time
from typing import Any, Dict, List, Optional

from solstice.webui.storage.base import StorageBackend
from solstice.utils.logging import create_ray_logger


class SlateDBStorage(StorageBackend):
    """SlateDB-backed storage for WebUI historical data.

    Supports S3-backed storage for History Server scenarios.
    Storage path can be:
    - Local: /tmp/solstice-webui/
    - S3: s3://bucket/solstice-history/

    Key schema:
    - job:{job_id} -> JobArchive JSON
    - jobs_by_time:{timestamp}:{job_id} -> job_id (index)
    - jobs_by_status:{status}:{job_id} -> job_id (index)
    - metrics:{job_id}:{stage_id}:{timestamp} -> Metrics JSON
    - exception:{job_id}:{exception_id} -> Exception JSON
    - lineage:{job_id}:{split_id} -> Lineage JSON
    - worker_event:{job_id}:{worker_id}:{timestamp} -> Event JSON
    """

    def __init__(self, path: str = "/tmp/solstice-webui/"):
        """Initialize SlateDB storage.

        Args:
            path: Storage path (local or S3)
                - Local: /tmp/solstice-webui/
                - S3: s3://bucket/path/
        """
        import os
        from pathlib import Path

        self.path = path
        self.logger = create_ray_logger("SlateDBStorage")

        from slatedb import SlateDB

        # Configure SlateDB based on path
        if path.startswith("s3://"):
            # S3 storage
            os.environ.setdefault("CLOUD_PROVIDER", "aws")
            # S3 credentials should be in environment (AWS_ACCESS_KEY_ID, etc.)
        else:
            # Local filesystem storage - ensure directory exists
            Path(path).mkdir(parents=True, exist_ok=True)
            os.environ.setdefault("CLOUD_PROVIDER", "local")
            os.environ.setdefault("LOCAL_PATH", path)

        self.db = SlateDB(path)
        self.logger.info(f"Initialized SlateDB storage at {path}")

    # === Job Archive ===

    def store_job_archive(self, job_id: str, archive_data: Dict[str, Any]) -> None:
        """Store archived job data with indexing."""
        # Main data
        key = f"job:{job_id}"
        self.db.put(key.encode(), json.dumps(archive_data).encode())

        # Time-based index (for sorting)
        end_time = archive_data.get("end_time", time.time())
        time_key = f"jobs_by_time:{int(end_time)}:{job_id}"
        self.db.put(time_key.encode(), job_id.encode())

        # Status-based index (for filtering)
        status = archive_data.get("status", "UNKNOWN")
        status_key = f"jobs_by_status:{status}:{job_id}"
        self.db.put(status_key.encode(), job_id.encode())

        self.logger.info(f"Archived job {job_id} with status {status}")

    def get_job_archive(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve archived job data."""
        key = f"job:{job_id}"
        try:
            data = self.db.get(key.encode())
            if data:
                return json.loads(data.decode())
        except Exception as e:
            self.logger.warning(f"Failed to get job archive {job_id}: {e}")
        return None

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List archived jobs."""
        jobs = []

        try:
            if status:
                prefix = f"jobs_by_status:{status}:"
            else:
                prefix = "job:"

            # Scan with prefix
            # Note: SlateDB scan API may vary, this is a placeholder
            # We'll need to check the actual API when slatedb is available
            results = self._scan_prefix(prefix.encode(), limit=limit + offset)

            # Skip offset and take limit
            for key, value in results[offset : offset + limit]:
                if status:
                    # Index key, need to fetch actual job
                    job_id = value.decode()
                    job_data = self.get_job_archive(job_id)
                    if job_data:
                        jobs.append(job_data)
                else:
                    # Direct job data
                    jobs.append(json.loads(value.decode()))

        except Exception as e:
            self.logger.warning(f"Failed to list jobs: {e}")

        return jobs

    def _scan_prefix(self, prefix: bytes, limit: int = 1000) -> List[tuple]:
        """Scan keys with prefix using SlateDB scan API.

        Args:
            prefix: Key prefix to scan
            limit: Maximum number of results

        Returns:
            List of (key, value) tuples
        """
        results = []
        try:
            # SlateDB.scan returns iterator of (key, value) tuples
            for key, value in self.db.scan(prefix):
                results.append((key, value))
                if len(results) >= limit:
                    break
            return results
        except Exception as e:
            self.logger.warning(f"Failed to scan prefix {prefix}: {e}")
            return []

    # === Metrics Snapshots ===

    def store_metrics_snapshot(
        self,
        job_id: str,
        stage_id: str,
        timestamp: float,
        metrics: Dict[str, Any],
    ) -> None:
        """Store a metrics snapshot."""
        key = f"metrics:{job_id}:{stage_id}:{int(timestamp)}"
        self.db.put(key.encode(), json.dumps(metrics).encode())
        self.logger.debug(f"Stored metrics snapshot for {job_id}/{stage_id}")

    def get_metrics_history(
        self,
        job_id: str,
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Query metrics history."""
        prefix = f"metrics:{job_id}:{stage_id}:"

        try:
            results = self._scan_prefix(prefix.encode())

            # Filter by time range and parse
            metrics_list = []
            for key, value in results:
                # Extract timestamp from key
                parts = key.decode().split(":")
                if len(parts) >= 4:
                    ts = float(parts[3])
                    if start_time <= ts <= end_time:
                        metrics_list.append(json.loads(value.decode()))

            return sorted(metrics_list, key=lambda x: x.get("timestamp", 0))
        except Exception as e:
            self.logger.warning(f"Failed to get metrics history: {e}")
            return []

    # === Exceptions ===

    def store_exception(
        self,
        job_id: str,
        exception_id: str,
        exception_data: Dict[str, Any],
    ) -> None:
        """Store exception data."""
        key = f"exception:{job_id}:{exception_id}"
        self.db.put(key.encode(), json.dumps(exception_data).encode())
        self.logger.debug(f"Stored exception {exception_id} for job {job_id}")

    def list_exceptions(
        self,
        job_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List exceptions for a job."""
        prefix = f"exception:{job_id}:"

        try:
            results = self._scan_prefix(prefix.encode(), limit=limit + offset)
            # Apply offset
            results = results[offset : offset + limit]
            return [json.loads(value.decode()) for _, value in results]
        except Exception as e:
            self.logger.warning(f"Failed to list exceptions: {e}")
            return []

    # === Split Lineage ===

    def store_split_lineage(
        self,
        job_id: str,
        split_id: str,
        lineage_data: Dict[str, Any],
    ) -> None:
        """Store split lineage data."""
        key = f"lineage:{job_id}:{split_id}"
        self.db.put(key.encode(), json.dumps(lineage_data).encode())
        self.logger.debug(f"Stored lineage for split {split_id}")

    def get_split_lineage(
        self,
        job_id: str,
        split_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get split lineage data."""
        key = f"lineage:{job_id}:{split_id}"

        try:
            data = self.db.get(key.encode())
            if data:
                return json.loads(data.decode())
        except Exception as e:
            self.logger.warning(f"Failed to get split lineage: {e}")
        return None

    def get_lineage_graph(self, job_id: str) -> Dict[str, Any]:
        """Get complete lineage graph for a job.

        Returns:
            Graph data with nodes and edges
        """
        prefix = f"lineage:{job_id}:"

        try:
            results = self._scan_prefix(prefix.encode())

            nodes = []
            edges = []

            for _, value in results:
                lineage = json.loads(value.decode())
                split_id = lineage["split_id"]

                # Add node
                nodes.append(
                    {
                        "id": split_id,
                        "split_id": split_id,
                        "worker_id": lineage.get("worker_id"),
                        "timestamp": lineage.get("timestamp"),
                    }
                )

                # Add edges from parents
                for parent_id in lineage.get("parent_ids", []):
                    edges.append(
                        {
                            "source": parent_id,
                            "target": split_id,
                        }
                    )

            return {
                "nodes": nodes,
                "edges": edges,
            }
        except Exception as e:
            self.logger.warning(f"Failed to get lineage graph: {e}")
            return {"nodes": [], "edges": []}

    # === Worker Events ===

    def store_worker_event(
        self,
        job_id: str,
        worker_id: str,
        timestamp: float,
        event_data: Dict[str, Any],
    ) -> None:
        """Store worker lifecycle event."""
        key = f"worker_event:{job_id}:{worker_id}:{int(timestamp * 1000)}"
        self.db.put(key.encode(), json.dumps(event_data).encode())
        self.logger.debug(f"Stored worker event for {worker_id}")

    def list_worker_events(
        self,
        job_id: str,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events."""
        if worker_id:
            prefix = f"worker_event:{job_id}:{worker_id}:"
        else:
            prefix = f"worker_event:{job_id}:"

        try:
            results = self._scan_prefix(prefix.encode(), limit=limit + offset)
            events = [json.loads(value.decode()) for _, value in results]
            # Sort by timestamp descending (newest first)
            sorted_events = sorted(events, key=lambda x: x.get("timestamp", 0), reverse=True)
            # Apply offset and limit
            return sorted_events[offset : offset + limit]
        except Exception as e:
            self.logger.warning(f"Failed to list worker events: {e}")
            return []

    # === Ray Events ===

    def store_ray_event(
        self,
        job_id: str,
        event_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        """Store Ray event."""
        key = f"ray_event:{job_id}:{event_id}"
        self.db.put(key.encode(), json.dumps(event_data).encode())

    def list_ray_events(
        self,
        job_id: str,
        event_types: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List Ray events."""
        prefix = f"ray_event:{job_id}:"

        try:
            # Fetch more to account for filtering and offset
            fetch_limit = (limit + offset) * 2 if event_types else (limit + offset)
            results = self._scan_prefix(prefix.encode(), limit=fetch_limit)
            events = [json.loads(value.decode()) for _, value in results]

            # Filter by event types if specified
            if event_types:
                events = [e for e in events if e.get("event_type") in event_types]

            # Sort by timestamp descending (newest first)
            sorted_events = sorted(events, key=lambda x: x.get("timestamp", 0), reverse=True)

            # Apply offset and limit
            return sorted_events[offset : offset + limit]
        except Exception as e:
            self.logger.warning(f"Failed to list ray events: {e}")
            return []
