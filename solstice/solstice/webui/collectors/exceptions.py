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

"""Exception aggregator for tracking and analyzing errors."""

import hashlib
import time
import traceback
from typing import Any, Dict, List, Optional

from solstice.webui.storage import SlateDBStorage
from solstice.webui.models import ExceptionInfo
from solstice.utils.logging import create_ray_logger


class ExceptionAggregator:
    """Aggregate and analyze exceptions for debugging.

    Features:
    - Groups similar exceptions by type and message
    - Tracks occurrence count and timestamps
    - Provides root cause analysis hints

    Usage:
        aggregator = ExceptionAggregator(job_id, storage)
        aggregator.record_exception(exc, stage_id, worker_id, split_id)
    """

    def __init__(self, job_id: str, storage: SlateDBStorage):
        """Initialize exception aggregator.

        Args:
            job_id: Job identifier
            storage: SlateDB storage instance
        """
        self.job_id = job_id
        self.storage = storage
        self.logger = create_ray_logger(f"ExceptionAggregator-{job_id}")

        # In-memory cache for deduplication
        self._exception_cache: Dict[str, ExceptionInfo] = {}

    def record_exception(
        self,
        exception: Exception,
        stage_id: str,
        worker_id: Optional[str] = None,
        split_id: Optional[str] = None,
    ) -> str:
        """Record an exception.

        Args:
            exception: The exception that occurred
            stage_id: Stage where exception occurred
            worker_id: Optional worker identifier
            split_id: Optional split identifier

        Returns:
            Exception ID
        """
        # Generate exception ID based on type and message
        exc_type = type(exception).__name__
        exc_message = str(exception)
        exc_signature = f"{exc_type}:{exc_message[:100]}"
        exception_id = hashlib.md5(exc_signature.encode()).hexdigest()[:16]

        current_time = time.time()

        # Check if we've seen this exception before
        if exception_id in self._exception_cache:
            # Update existing
            cached = self._exception_cache[exception_id]
            cached.occurrence_count += 1
            cached.last_seen = current_time

            # Update in storage
            self.storage.store_exception(
                self.job_id,
                exception_id,
                self._exception_to_dict(cached),
            )

            self.logger.debug(
                f"Updated exception {exception_id} (count: {cached.occurrence_count})"
            )
        else:
            # New exception
            exc_info = ExceptionInfo(
                exception_id=exception_id,
                timestamp=current_time,
                exception_type=exc_type,
                message=exc_message,
                stacktrace=traceback.format_exc(),
                job_id=self.job_id,
                stage_id=stage_id,
                worker_id=worker_id,
                split_id=split_id,
                occurrence_count=1,
                first_seen=current_time,
                last_seen=current_time,
            )

            self._exception_cache[exception_id] = exc_info

            # Store in SlateDB
            self.storage.store_exception(
                self.job_id,
                exception_id,
                self._exception_to_dict(exc_info),
            )

            self.logger.info(f"Recorded new exception {exception_id}: {exc_type}")

        return exception_id

    def _exception_to_dict(self, exc_info: ExceptionInfo) -> Dict[str, Any]:
        """Convert ExceptionInfo to dict."""
        return {
            "exception_id": exc_info.exception_id,
            "timestamp": exc_info.timestamp,
            "exception_type": exc_info.exception_type,
            "message": exc_info.message,
            "stacktrace": exc_info.stacktrace,
            "job_id": exc_info.job_id,
            "stage_id": exc_info.stage_id,
            "worker_id": exc_info.worker_id,
            "split_id": exc_info.split_id,
            "occurrence_count": exc_info.occurrence_count,
            "first_seen": exc_info.first_seen,
            "last_seen": exc_info.last_seen,
        }

    def aggregate_by_type(self) -> Dict[str, List[ExceptionInfo]]:
        """Group exceptions by type.

        Returns:
            Dictionary mapping exception type to list of exceptions
        """
        groups: Dict[str, List[ExceptionInfo]] = {}

        for exc_info in self._exception_cache.values():
            exc_type = exc_info.exception_type
            if exc_type not in groups:
                groups[exc_type] = []
            groups[exc_type].append(exc_info)

        return groups

    def get_root_cause_analysis(self, exception_id: str) -> Optional[str]:
        """Analyze exception and suggest root cause.

        Args:
            exception_id: Exception identifier

        Returns:
            Suggested root cause analysis or None
        """
        exc_info = self._exception_cache.get(exception_id)
        if not exc_info:
            return None

        # Simple heuristics for common issues
        exc_type = exc_info.exception_type
        message = exc_info.message.lower()

        if exc_type == "OutOfMemoryError" or "memory" in message:
            return "Memory exhaustion. Consider increasing worker memory or reducing batch size."

        if exc_type == "TimeoutError" or "timeout" in message:
            return "Operation timeout. Check for slow data sources or network issues."

        if "connection" in message or "network" in message:
            return "Network connectivity issue. Check queue backend and network configuration."

        if "permission" in message or "access denied" in message:
            return "Permission issue. Check file system or S3 bucket permissions."

        return "No specific root cause identified. Check full stacktrace for details."
