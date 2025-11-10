"""State backend implementations for remote storage"""

import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict
import logging


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

    def __init__(self, bucket: str, prefix: str = "checkpoints"):
        self.bucket = bucket
        self.prefix = prefix
        self.logger = logging.getLogger(self.__class__.__name__)

        try:
            import boto3

            self.s3_client = boto3.client("s3")
        except ImportError:
            raise ImportError("boto3 is required for S3StateBackend")

    def _get_key(self, path: str) -> str:
        """Get full S3 key from path"""
        return f"{self.prefix}/{path}"

    def save_state(self, path: str, state: Dict[str, Any]) -> None:
        """Save state to S3"""
        key = self._get_key(path)
        serialized = pickle.dumps(state)

        self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=serialized)

        self.logger.info(f"Saved state to s3://{self.bucket}/{key}")

    def load_state(self, path: str) -> Dict[str, Any]:
        """Load state from S3"""
        key = self._get_key(path)

        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)

        state = pickle.loads(response["Body"].read())
        self.logger.info(f"Loaded state from s3://{self.bucket}/{key}")
        return state

    def delete_state(self, path: str) -> None:
        """Delete state from S3"""
        key = self._get_key(path)

        self.s3_client.delete_object(Bucket=self.bucket, Key=key)

        self.logger.info(f"Deleted state from s3://{self.bucket}/{key}")

    def exists(self, path: str) -> bool:
        """Check if state exists in S3"""
        key = self._get_key(path)

        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def list_checkpoints(self, prefix: str) -> list:
        """List all checkpoints in S3 under prefix"""
        full_prefix = self._get_key(prefix)

        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=full_prefix)

        checkpoints = []
        if "Contents" in response:
            for obj in response["Contents"]:
                # Remove the full prefix to get relative path
                relative_path = obj["Key"][len(self.prefix) + 1 :]
                checkpoints.append(relative_path)

        return sorted(checkpoints)
