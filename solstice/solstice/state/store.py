"""Checkpoint storage abstraction layer.

Provides a simple key-value interface for checkpoint persistence.
The default implementation uses SlateDB, but can be swapped for other backends.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import json
import logging
import time


@dataclass
class SplitCheckpointData:
    """Checkpoint data for a single split.

    This is the core unit of checkpoint - represents the state needed
    to resume processing from a specific split.
    """

    split_id: str
    stage_id: str
    parent_split_ids: List[str] = field(default_factory=list)

    # Processing state
    status: str = "pending"  # pending, processing, completed
    attempt: int = 0

    # For source splits: reading offset
    source_offset: Dict[str, Any] = field(default_factory=dict)

    # For sink splits: commit info
    commit_offset: Dict[str, Any] = field(default_factory=dict)

    # Operator state (for stateful operators)
    operator_state: Dict[str, Any] = field(default_factory=dict)

    # Timestamps
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SplitCheckpointData":
        return cls(**data)


@dataclass
class StageCheckpointData:
    """Checkpoint data for a stage.

    Tracks which splits have been completed and which are in-flight.
    """

    stage_id: str

    # Completed splits (successfully processed)
    completed_splits: Set[str] = field(default_factory=set)

    # In-flight splits (being processed when checkpoint triggered)
    inflight_splits: Set[str] = field(default_factory=set)

    # Stage-level offset (e.g., for source stages)
    offset: Dict[str, Any] = field(default_factory=dict)

    # Last checkpoint ID this stage was part of
    last_checkpoint_id: Optional[str] = None

    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "completed_splits": list(self.completed_splits),
            "inflight_splits": list(self.inflight_splits),
            "offset": self.offset,
            "last_checkpoint_id": self.last_checkpoint_id,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StageCheckpointData":
        return cls(
            stage_id=data["stage_id"],
            completed_splits=set(data.get("completed_splits", [])),
            inflight_splits=set(data.get("inflight_splits", [])),
            offset=data.get("offset", {}),
            last_checkpoint_id=data.get("last_checkpoint_id"),
            timestamp=data.get("timestamp", time.time()),
        )


@dataclass
class CheckpointManifest:
    """Complete checkpoint manifest for a job.

    Contains all information needed to restore job state.
    """

    checkpoint_id: str
    job_id: str
    timestamp: float = field(default_factory=time.time)

    # Stage-level checkpoint data
    stages: Dict[str, StageCheckpointData] = field(default_factory=dict)

    # Global metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "job_id": self.job_id,
            "timestamp": self.timestamp,
            "stages": {k: v.to_dict() for k, v in self.stages.items()},
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointManifest":
        stages = {k: StageCheckpointData.from_dict(v) for k, v in data.get("stages", {}).items()}
        return cls(
            checkpoint_id=data["checkpoint_id"],
            job_id=data["job_id"],
            timestamp=data.get("timestamp", time.time()),
            stages=stages,
            metadata=data.get("metadata", {}),
        )


class CheckpointStore(ABC):
    """Abstract interface for checkpoint storage.

    Provides simple key-value operations for checkpoint data.
    Implementations can use local storage, S3, SlateDB, etc.
    """

    @abstractmethod
    def put(self, key: str, value: bytes) -> None:
        """Store a value by key."""
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[bytes]:
        """Retrieve a value by key. Returns None if not found."""
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a key."""
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        pass

    @abstractmethod
    def list_keys(self, prefix: str) -> List[str]:
        """List all keys with given prefix."""
        pass

    def close(self) -> None:
        """Close the store and release resources."""
        pass

    # Convenience methods for JSON serialization
    def put_json(self, key: str, value: Dict[str, Any]) -> None:
        """Store a JSON-serializable value."""
        self.put(key, json.dumps(value).encode("utf-8"))

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve a JSON value."""
        data = self.get(key)
        if data is None:
            return None
        return json.loads(data.decode("utf-8"))


class LocalCheckpointStore(CheckpointStore):
    """Local filesystem checkpoint store.

    Simple implementation for development and testing.
    """

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _resolve_path(self, key: str) -> Path:
        # Preserve directory structure, only sanitize colons
        safe_key = key.replace(":", "_")
        return self.base_path / safe_key

    def put(self, key: str, value: bytes) -> None:
        path = self._resolve_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
        self.logger.debug(f"Put {key} ({len(value)} bytes)")

    def get(self, key: str) -> Optional[bytes]:
        path = self._resolve_path(key)
        if not path.exists():
            return None
        return path.read_bytes()

    def delete(self, key: str) -> None:
        path = self._resolve_path(key)
        if path.exists():
            path.unlink()
            self.logger.debug(f"Deleted {key}")

    def exists(self, key: str) -> bool:
        return self._resolve_path(key).exists()

    def list_keys(self, prefix: str) -> List[str]:
        safe_prefix = prefix.replace(":", "_")
        prefix_path = self.base_path / safe_prefix
        keys = []

        # If prefix is a directory, recursively find all files
        if prefix_path.exists() and prefix_path.is_dir():
            for path in prefix_path.rglob("*"):
                if path.is_file():
                    rel = path.relative_to(self.base_path)
                    # Convert path back to key format (replace first _ with :)
                    key = str(rel)
                    keys.append(key)
        else:
            # Glob with prefix pattern in parent directory
            parent = prefix_path.parent
            if parent.exists():
                pattern = prefix_path.name + "*"
                for path in parent.rglob(pattern):
                    if path.is_file():
                        rel = path.relative_to(self.base_path)
                        key = str(rel)
                        keys.append(key)

        return sorted(keys)


class S3CheckpointStore(CheckpointStore):
    """S3-backed checkpoint store.

    Uses fsspec for S3 access.
    """

    def __init__(self, bucket: str, prefix: str = "", **storage_options):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.logger = logging.getLogger(self.__class__.__name__)

        import fsspec

        self.fs = fsspec.filesystem("s3", **storage_options)

    def _s3_path(self, key: str) -> str:
        if self.prefix:
            return f"{self.bucket}/{self.prefix}/{key}"
        return f"{self.bucket}/{key}"

    def put(self, key: str, value: bytes) -> None:
        path = self._s3_path(key)
        with self.fs.open(path, "wb") as f:
            f.write(value)
        self.logger.debug(f"Put s3://{path} ({len(value)} bytes)")

    def get(self, key: str) -> Optional[bytes]:
        path = self._s3_path(key)
        try:
            with self.fs.open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def delete(self, key: str) -> None:
        path = self._s3_path(key)
        try:
            self.fs.delete(path)
            self.logger.debug(f"Deleted s3://{path}")
        except FileNotFoundError:
            pass

    def exists(self, key: str) -> bool:
        return self.fs.exists(self._s3_path(key))

    def list_keys(self, prefix: str) -> List[str]:
        full_prefix = self._s3_path(prefix)
        try:
            paths = self.fs.glob(f"{full_prefix}*")
            # Extract key part from full path
            base_len = len(self._s3_path(""))
            return sorted([p[base_len:] for p in paths])
        except Exception:
            return []


class SlateDBCheckpointStore(CheckpointStore):
    """SlateDB-backed checkpoint store.

    SlateDB is an embedded LSM storage engine built on object storage,
    providing the benefits of RocksDB with cloud-native storage separation.

    Supports multiple backends:
    - memory:/// - In-memory (for testing)
    - file:///path/to/dir - Local filesystem
    - s3://bucket/prefix - AWS S3
    - gs://bucket/prefix - Google Cloud Storage
    - az://container/prefix - Azure Blob Storage

    Install: pip install slatedb
    """

    def __init__(
        self,
        path: str,
        url: Optional[str] = None,
        **options,
    ):
        """Initialize SlateDB store.

        Args:
            path: Database path (used by SlateDB internally)
            url: Object store URL. Examples:
                - "memory:///" - In-memory store (for testing)
                - "file:///tmp/slatedb" - Local filesystem
                - "s3://bucket/prefix" - AWS S3
            **options: Additional SlateDB options
        """
        self.path = path
        self.url = url
        self.options = options
        self.logger = logging.getLogger(self.__class__.__name__)
        self._db = None
        self._native = False
        self._fallback: Optional[CheckpointStore] = None

        # Try to import slatedb
        try:
            from slatedb import SlateDB

            # Determine URL based on path if not provided
            if url is None:
                if path.startswith("s3://"):
                    url = path
                    path = "/tmp/slatedb-checkpoint"
                elif path.startswith("gs://") or path.startswith("az://"):
                    url = path
                    path = "/tmp/slatedb-checkpoint"
                else:
                    # Local filesystem
                    url = f"file://{path}"

            self._db = SlateDB(path, url=url, **options)
            self._native = True
            self.logger.info(f"Using SlateDB at {path} with {url}")

        except ImportError:
            self.logger.warning(
                "SlateDB Python bindings not available. Install with: pip install slatedb"
            )
            self._native = False
            # Use fallback
            if url and url.startswith("s3://"):
                parts = url[5:].split("/", 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ""
                self._fallback = S3CheckpointStore(bucket, prefix)
            else:
                self._fallback = LocalCheckpointStore(path)

    def put(self, key: str, value: bytes) -> None:
        if self._native and self._db is not None:
            self._db.put(key.encode(), value)
        elif self._fallback:
            self._fallback.put(key, value)

    def get(self, key: str) -> Optional[bytes]:
        if self._native and self._db is not None:
            result = self._db.get(key.encode())
            return result if result else None
        elif self._fallback:
            return self._fallback.get(key)
        return None

    def delete(self, key: str) -> None:
        if self._native and self._db is not None:
            # SlateDB uses WriteBatch for deletes
            from slatedb import WriteBatch

            wb = WriteBatch()
            wb.delete(key.encode())
            self._db.write(wb)
        elif self._fallback:
            self._fallback.delete(key)

    def exists(self, key: str) -> bool:
        if self._native and self._db is not None:
            return self._db.get(key.encode()) is not None
        elif self._fallback:
            return self._fallback.exists(key)
        return False

    def list_keys(self, prefix: str) -> List[str]:
        if self._native and self._db is not None:
            # Use SlateDB's scan for prefix queries
            keys = []
            prefix_bytes = prefix.encode()
            for kv in self._db.scan(prefix_bytes):
                key = kv[0] if isinstance(kv, tuple) else kv.key
                if isinstance(key, bytes):
                    key_str = key.decode()
                else:
                    key_str = str(key)
                if key_str.startswith(prefix):
                    keys.append(key_str)
                else:
                    break  # Past prefix range
            return sorted(keys)
        elif self._fallback:
            return self._fallback.list_keys(prefix)
        return []

    def flush(self) -> None:
        """Flush pending writes to storage."""
        if self._native and self._db is not None:
            self._db.flush_with_options("wal")

    def create_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Create a durable checkpoint in SlateDB.

        Returns checkpoint info dict or None if not using native SlateDB.
        """
        if self._native and self._db is not None:
            return self._db.create_checkpoint(scope="durable")
        return None

    def close(self) -> None:
        if self._native and self._db is not None:
            self._db.close()
            self._db = None
        elif self._fallback and hasattr(self._fallback, "close"):
            self._fallback.close()


# Factory function
def create_checkpoint_store(uri: str, **options) -> CheckpointStore:
    """Create a checkpoint store from URI.

    Args:
        uri: Storage URI. Formats:
            - "local:/path/to/dir" or "/path/to/dir" - Local filesystem
            - "s3://bucket/prefix" - S3 storage (uses fsspec)
            - "slatedb://memory:///" - SlateDB with in-memory store
            - "slatedb://file:///path/to/dir" - SlateDB with local storage
            - "slatedb://s3://bucket/prefix" - SlateDB with S3 storage
        **options: Additional storage options

    Returns:
        CheckpointStore instance

    Examples:
        >>> # Simple local storage
        >>> store = create_checkpoint_store("/tmp/checkpoints")

        >>> # SlateDB with in-memory (for testing)
        >>> store = create_checkpoint_store("slatedb://memory:///")

        >>> # SlateDB with S3 (recommended for production)
        >>> store = create_checkpoint_store("slatedb://s3://my-bucket/checkpoints")
    """
    # SlateDB URIs - recommended for production
    if uri.startswith("slatedb://"):
        inner_uri = uri[10:]  # Remove "slatedb://"
        # Pass the object store URL directly to SlateDB
        return SlateDBCheckpointStore(path="/tmp/slatedb-checkpoint", url=inner_uri, **options)

    # S3 URIs (direct, without SlateDB)
    if uri.startswith("s3://"):
        parts = uri[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return S3CheckpointStore(bucket, prefix, **options)

    # Default to local
    path = uri.replace("local:", "")
    return LocalCheckpointStore(path)
