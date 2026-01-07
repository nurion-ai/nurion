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
from typing import Any, Dict, List, Optional

from solstice.utils.logging import create_ray_logger


class JobStorage:
    """Per-job SlateDB storage for writing WebUI data.

    Each running job creates its own JobStorage instance to write metrics,
    events, and archives. This ensures SlateDB's single-writer constraint
    is satisfied.

    Storage path format: {base_path}/{job_id}/{attempt_id}/
    - Local: /tmp/solstice-webui/my_job/20250101_120000_abc1/
    - S3: s3://bucket/solstice/my_job/20250101_120000_abc1/

    Key schema (no job_id in keys since path already contains job_id):
    - job -> JobArchive JSON (single entry per SlateDB instance)
    - metrics:{stage_id}:{timestamp} -> Metrics JSON
    - exception:{exception_id} -> Exception JSON
    - lineage:{split_id} -> Lineage JSON
    - worker:{worker_id} -> Worker history JSON
    - worker_event:{worker_id}:{timestamp} -> Event JSON
    - ray_event:{event_id} -> Ray event JSON

    See also: PortalStorage for read-only access across all jobs.
    """

    def __init__(self, path: str = "/tmp/solstice-webui/"):
        """Initialize SlateDB storage.

        Args:
            path: Storage path (local or S3)
                - Local: /tmp/solstice-webui/
                - S3: s3://bucket/path/
        """
        from pathlib import Path

        self.path = path
        self.logger = create_ray_logger("JobStorage")

        from slatedb import SlateDB

        # Configure SlateDB based on path
        if path.startswith("s3://"):
            # S3 storage - use the path directly as URL
            self.db = SlateDB("db", url=path)
        else:
            # Local filesystem storage
            # Ensure directory exists
            Path(path).mkdir(parents=True, exist_ok=True)
            # Use file:// URL for local storage
            url = f"file://{path}/"
            self.db = SlateDB("db", url=url)
        self.logger.info(f"Initialized SlateDB storage at {path}")

    # === Job Configuration ===

    def store_configuration(self, config_data: Dict[str, Any]) -> None:
        """Store job configuration.

        Should be called at job start with complete configuration.

        Args:
            config_data: Configuration dictionary with:
                - job_config: Job-level settings (job_id, queue_type, etc.)
                - stage_configs: Per-stage settings (operator_type, parallelism, etc.)
                - environment: Environment variables
        """
        key = "config"
        self.db.put(key.encode(), json.dumps(config_data).encode())
        self.db.flush()
        self.logger.debug("Stored job configuration")

    def get_configuration(self) -> Optional[Dict[str, Any]]:
        """Retrieve job configuration from this storage."""
        key = "config"
        data = self.db.get(key.encode())
        if data:
            return json.loads(data.decode())
        return None

    # === Job Archive ===

    def store_job_archive(self, archive_data: Dict[str, Any]) -> None:
        """Store archived job data."""
        key = "job"
        self.db.put(key.encode(), json.dumps(archive_data).encode())

        # Flush to ensure data is persisted to disk
        self.db.flush()

        status = archive_data.get("status", "UNKNOWN")
        job_id = archive_data.get("job_id", "unknown")
        self.logger.info(f"Archived job {job_id} with status {status}")

    def get_job_archive(self) -> Optional[Dict[str, Any]]:
        """Retrieve archived job data from this storage."""
        key = "job"
        data = self.db.get(key.encode())
        if data:
            return json.loads(data.decode())
        return None

    def _scan_prefix(self, prefix: bytes, limit: int = 1000) -> List[tuple]:
        """Scan keys with prefix using SlateDB scan API.

        Args:
            prefix: Key prefix to scan
            limit: Maximum number of results

        Returns:
            List of (key, value) tuples
        """
        results = []
        for key, value in self.db.scan(prefix):
            results.append((key, value))
            if len(results) >= limit:
                break
        return results

    # === Metrics Snapshots ===

    def store_metrics_snapshot(
        self,
        stage_id: str,
        timestamp: float,
        metrics: Dict[str, Any],
    ) -> None:
        """Store a metrics snapshot."""
        key = f"metrics:{stage_id}:{int(timestamp)}"
        # Ensure timestamp is included in the data
        data = {**metrics, "timestamp": timestamp}
        self.db.put(key.encode(), json.dumps(data).encode())
        self.logger.debug(f"Stored metrics snapshot for {stage_id}")

    def get_metrics_history(
        self,
        stage_id: str,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Query metrics history for a stage."""
        prefix = f"metrics:{stage_id}:"
        results = self._scan_prefix(prefix.encode())

        # Filter by time range and parse
        metrics_list = []
        for key, value in results:
            # Extract timestamp from key: metrics:{stage_id}:{timestamp}
            parts = key.decode().split(":")
            if len(parts) >= 3:
                ts = float(parts[2])
                if start_time <= ts <= end_time:
                    metrics_list.append(json.loads(value.decode()))

        return sorted(metrics_list, key=lambda x: x.get("timestamp", 0))

    def get_latest_stage_metrics(self, stage_id: str) -> Optional[Dict[str, Any]]:
        """Get the best metrics snapshot for a stage.

        This returns the snapshot with the highest input_records + output_records,
        since later snapshots may show 0 after workers stop.

        Returns:
            Best metrics dict or None if no metrics found
        """
        prefix = f"metrics:{stage_id}:"
        results = self._scan_prefix(prefix.encode())

        if not results:
            return None

        # Find the snapshot with highest input + output records
        # (later snapshots may be 0 after workers stop)
        best = None
        best_total = -1
        for key, value in results:
            metrics = json.loads(value.decode())
            total = metrics.get("input_records", 0) + metrics.get("output_records", 0)
            if total > best_total:
                best_total = total
                best = metrics

        return best

    # === Exceptions ===

    def store_exception(
        self,
        exception_id: str,
        exception_data: Dict[str, Any],
    ) -> None:
        """Store exception data."""
        key = f"exception:{exception_id}"
        self.db.put(key.encode(), json.dumps(exception_data).encode())
        self.logger.debug(f"Stored exception {exception_id}")

    def list_exceptions(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List exceptions in this storage."""
        prefix = b"exception:"
        results = self._scan_prefix(prefix, limit=limit + offset)
        # Apply offset
        results = results[offset : offset + limit]
        return [json.loads(value.decode()) for _, value in results]

    # === Split Lineage ===

    def store_split_lineage(
        self,
        split_id: str,
        lineage_data: Dict[str, Any],
    ) -> None:
        """Store split lineage data."""
        key = f"lineage:{split_id}"
        self.db.put(key.encode(), json.dumps(lineage_data).encode())
        self.logger.debug(f"Stored lineage for split {split_id}")

    def get_split_lineage(self, split_id: str) -> Optional[Dict[str, Any]]:
        """Get split lineage data."""
        key = f"lineage:{split_id}"
        data = self.db.get(key.encode())
        if data:
            return json.loads(data.decode())
        return None

    def get_lineage_graph(self) -> Dict[str, Any]:
        """Get complete lineage graph.

        Returns:
            Graph data with nodes and edges
        """
        prefix = b"lineage:"
        results = self._scan_prefix(prefix)

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

    # === Worker History ===

    def store_worker_history(
        self,
        worker_id: str,
        worker_data: Dict[str, Any],
    ) -> None:
        """Store worker history snapshot.

        Args:
            worker_id: Worker identifier
            worker_data: Worker data including:
                - stage_id: Stage the worker belongs to
                - status: RUNNING, COMPLETED, FAILED
                - start_time: When worker started
                - end_time: When worker finished (if completed)
                - input_records: Total input records processed
                - output_records: Total output records produced
                - processed_splits: List of split IDs processed
                - actor_id, node_id, pid: Ray actor info
        """
        key = f"worker:{worker_id}"
        self.db.put(key.encode(), json.dumps(worker_data).encode())
        self.logger.debug(f"Stored worker history for {worker_id}")

    def get_worker_history(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get worker history."""
        key = f"worker:{worker_id}"
        data = self.db.get(key.encode())
        if data:
            return json.loads(data.decode())
        return None

    def list_workers(
        self,
        stage_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all workers with optional filtering."""
        prefix = b"worker:"
        results = self._scan_prefix(prefix, limit=1000)  # Get all workers
        workers = [json.loads(value.decode()) for _, value in results]

        # Filter by stage_id if specified
        if stage_id:
            workers = [w for w in workers if w.get("stage_id") == stage_id]

        # Filter by status if specified
        if status:
            workers = [w for w in workers if w.get("status") == status]

        # Sort by start_time descending (newest first)
        sorted_workers = sorted(workers, key=lambda x: x.get("start_time", 0), reverse=True)

        return sorted_workers[offset : offset + limit]

    # === Worker Events ===

    def store_worker_event(
        self,
        worker_id: str,
        timestamp: float,
        event_data: Dict[str, Any],
    ) -> None:
        """Store worker lifecycle event."""
        key = f"worker_event:{worker_id}:{int(timestamp * 1000)}"
        self.db.put(key.encode(), json.dumps(event_data).encode())
        self.logger.debug(f"Stored worker event for {worker_id}")

    def list_worker_events(
        self,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List worker events."""
        if worker_id:
            prefix = f"worker_event:{worker_id}:"
        else:
            prefix = "worker_event:"

        results = self._scan_prefix(prefix.encode(), limit=limit + offset)
        events = [json.loads(value.decode()) for _, value in results]
        # Sort by timestamp descending (newest first)
        sorted_events = sorted(events, key=lambda x: x.get("timestamp", 0), reverse=True)
        # Apply offset and limit
        return sorted_events[offset : offset + limit]

    # === Ray Events ===

    def store_ray_event(
        self,
        event_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        """Store Ray event."""
        key = f"ray_event:{event_id}"
        self.db.put(key.encode(), json.dumps(event_data).encode())

    def list_ray_events(
        self,
        event_types: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List Ray events."""
        prefix = b"ray_event:"

        # Fetch more to account for filtering and offset
        fetch_limit = (limit + offset) * 2 if event_types else (limit + offset)
        results = self._scan_prefix(prefix, limit=fetch_limit)
        events = [json.loads(value.decode()) for _, value in results]

        # Filter by event types if specified
        if event_types:
            events = [e for e in events if e.get("event_type") in event_types]

        # Sort by timestamp descending (newest first)
        sorted_events = sorted(events, key=lambda x: x.get("timestamp", 0), reverse=True)

        # Apply offset and limit
        return sorted_events[offset : offset + limit]
