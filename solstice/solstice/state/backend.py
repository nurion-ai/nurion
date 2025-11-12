"""State backend implementations for remote storage"""

import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict
import logging
import fsspec


class StateBackend(ABC):
    """Abstract interface for state storage backend"""

    @abstractmethod
    def save_state(self, path: str, state: Dict[str, Any]) -> None:
        """Save state to remote storage"""
        pass

    @abstractmethod
    def load_state(self, path: str) -> Dict[str, Any]:
        """Load state from remote storage"""
        pass

    @abstractmethod
    def delete_state(self, path: str) -> None:
        """Delete state from remote storage"""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if state exists"""
        pass

    @abstractmethod
    def list_checkpoints(self, prefix: str) -> list:
        """List all checkpoints under a prefix"""
        pass


class LocalStateBackend(StateBackend):
    """Local filesystem state backend (for testing)"""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)

    def save_state(self, path: str, state: Dict[str, Any]) -> None:
        """Save state to local file"""
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_path, "wb") as f:
            pickle.dump(state, f)

        self.logger.info(f"Saved state to {full_path}")

    def load_state(self, path: str) -> Dict[str, Any]:
        """Load state from local file"""
        full_path = self.base_path / path

        with open(full_path, "rb") as f:
            state = pickle.load(f)

        self.logger.info(f"Loaded state from {full_path}")
        return state

    def delete_state(self, path: str) -> None:
        """Delete state file"""
        full_path = self.base_path / path
        if full_path.exists():
            full_path.unlink()
            self.logger.info(f"Deleted state at {full_path}")

    def exists(self, path: str) -> bool:
        """Check if state file exists"""
        full_path = self.base_path / path
        return full_path.exists()

    def list_checkpoints(self, prefix: str) -> list:
        """List all checkpoint files under prefix"""
        full_prefix = self.base_path / prefix
        if not full_prefix.exists():
            return []

        checkpoints = []
        for path in full_prefix.rglob("*"):
            if path.is_file():
                checkpoints.append(str(path.relative_to(self.base_path)))

        return sorted(checkpoints)


class S3StateBackend(StateBackend):
    """S3-based state backend"""

    def __init__(self, s3_path: str, **storage_options):
        """
        Args:
            s3_path: Base S3 URI (e.g., s3://bucket/prefix or bucket/prefix).
            **storage_options: Additional options passed to fsspec.filesystem,
                such as credentials or configuration values.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        self.fs = fsspec.filesystem("s3", **storage_options)

        # Normalize the base path and extract bucket/prefix information.
        normalized = s3_path.strip()
        if normalized.startswith("s3://"):
            normalized = normalized[5:]

        normalized = normalized.strip("/")
        if not normalized:
            raise ValueError("s3_path must include an S3 bucket")

        parts = normalized.split("/", 1)
        self.bucket = parts[0]
        self.prefix = parts[1].strip("/") if len(parts) > 1 else ""

        if not self.bucket:
            raise ValueError("s3_path must include a valid S3 bucket name")

        if self.prefix:
            self.base_display_path = f"s3://{self.bucket}/{self.prefix}"
        else:
            self.base_display_path = f"s3://{self.bucket}"

    def _fs_path(self, path: str) -> str:
        """Return the filesystem path understood by fsspec (bucket/prefix/path)."""
        relative_path = path.lstrip("/")
        components = [self.bucket]
        if self.prefix:
            components.append(self.prefix)
        if relative_path:
            components.append(relative_path)
        return "/".join(components)

    def _display_path(self, path: str) -> str:
        """Return a human-readable S3 URI for logging."""
        relative_path = path.lstrip("/")
        if relative_path:
            return f"{self.base_display_path}/{relative_path}"
        return self.base_display_path

    def _relative_from_fs_path(self, fs_path: str) -> str:
        """Convert a filesystem path (bucket/...) to a backend-relative path."""
        if not fs_path.startswith(f"{self.bucket}"):
            return fs_path

        without_bucket = fs_path[len(self.bucket) :].lstrip("/")
        if self.prefix:
            prefix_with_sep = f"{self.prefix}/"
            if without_bucket.startswith(prefix_with_sep):
                return without_bucket[len(prefix_with_sep) :]
            if without_bucket == self.prefix:
                return ""
        return without_bucket

    def save_state(self, path: str, state: Dict[str, Any]) -> None:
        """Save state to S3."""
        fs_path = self._fs_path(path)
        serialized = pickle.dumps(state)

        with self.fs.open(fs_path, "wb") as f:
            f.write(serialized)

        self.logger.info(f"Saved state to {self._display_path(path)}")

    def load_state(self, path: str) -> Dict[str, Any]:
        """Load state from S3."""
        fs_path = self._fs_path(path)

        with self.fs.open(fs_path, "rb") as f:
            state = pickle.load(f)

        self.logger.info(f"Loaded state from {self._display_path(path)}")
        return state

    def delete_state(self, path: str) -> None:
        """Delete state from S3."""
        fs_path = self._fs_path(path)

        try:
            self.fs.delete(fs_path, recursive=False)
            self.logger.info(f"Deleted state from {self._display_path(path)}")
        except FileNotFoundError:
            self.logger.debug(f"State not found at {self._display_path(path)}; nothing to delete.")

    def exists(self, path: str) -> bool:
        """Check if state exists in S3."""
        fs_path = self._fs_path(path)
        return self.fs.exists(fs_path)

    def list_checkpoints(self, prefix: str) -> list:
        """List all checkpoints in S3 under prefix."""
        fs_prefix = self._fs_path(prefix)

        try:
            objects = self.fs.find(fs_prefix)
        except (FileNotFoundError, OSError):
            return []

        checkpoints = []
        for obj_path in objects:
            relative_path = self._relative_from_fs_path(obj_path)
            if relative_path:
                checkpoints.append(relative_path)

        return sorted(checkpoints)
