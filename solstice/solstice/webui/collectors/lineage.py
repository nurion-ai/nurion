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

"""Lineage tracker for recording split processing history."""

import time
from typing import TYPE_CHECKING

from solstice.webui.storage import JobStorage
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.core.models import Split


class LineageTracker:
    """Track split lineage and processing history.

    Records which worker processed each split and maintains
    parent-child relationships for lineage visualization.

    Usage:
        tracker = LineageTracker(job_id, storage)
        tracker.record_split_processed(split, worker_id, processing_time)
    """

    def __init__(self, job_id: str, storage: JobStorage):
        """Initialize lineage tracker.

        Args:
            job_id: Job identifier
            storage: SlateDB storage instance
        """
        self.job_id = job_id
        self.storage = storage
        self.logger = create_ray_logger(f"LineageTracker-{job_id}")

    def record_split_processed(
        self,
        split: "Split",
        worker_id: str,
        processing_time: float,
        input_records: int = 0,
        output_records: int = 0,
    ) -> None:
        """Record that a split was processed.

        Args:
            split: Split that was processed
            worker_id: Worker that processed it
            processing_time: Processing duration in seconds
            input_records: Number of input records
            output_records: Number of output records
        """
        try:
            lineage_data = {
                "split_id": split.split_id,
                "stage_id": split.stage_id,
                "parent_ids": split.parent_split_ids,
                "worker_id": worker_id,
                "processing_time": processing_time,
                "input_records": input_records,
                "output_records": output_records,
                "timestamp": time.time(),
                "data_range": split.data_range,
            }

            self.storage.store_split_lineage(
                split.split_id,
                lineage_data,
            )

            self.logger.debug(f"Recorded lineage for split {split.split_id}")

        except Exception as e:
            self.logger.warning(f"Failed to record split lineage: {e}")
