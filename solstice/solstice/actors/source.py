"""Operator Master interface for operator-specific control logic."""

import time

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List

from solstice.core.stage import Stage
from solstice.core.models import Split
from solstice.state.backend import StateBackend
from solstice.solstice.actors.stage_master import StageMasterActor
from solstice.utils.logging import create_ray_logger


class SourceStageMaster(StageMasterActor):
    """Master for source operators that handles split planning and generation."""

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        stage: Stage,
    ):
        super().__init__(job_id, state_backend, stage, None)
        self._planned_splits = self.plan_splits()

        self.logger.info(
            "Source operator master planned %d splits for stage %s",
            len(self._planned_splits),
            self.stage_id,
        )

    @abstractmethod
    def plan_splits(self) -> List[Split]:
        pass

    def run(self, poll_interval: float = 0.05) -> None:
        if self._running:
            return
        self._running = True
        self.logger.info("Stage %s run loop started", self.stage_id)
        try:
            while self._running:
                self.logger.debug(f"This is a source stage, requesting splits from source")
                self._request_splits_from_source()

                self._schedule_pending_splits()
                self._drain_completed_results(timeout=0.0)
                time.sleep(poll_interval)
        finally:
            self.logger.info("Stage %s run loop stopped", self.stage_id)

    def _request_splits_from_source(self) -> None:
        available_capacity = self.max_queue_size - len(self.pending_splits)
        if available_capacity <= 0:
            return

        splits = self.fetch_splits(max_count=available_capacity)
        for split in splits:
            self.enqueue_split(split, payload_ref=None)


    def fetch_splits(self, max_count: int = 1) -> Iterator[Split]:
        """Event handler: Generate splits when requested by StageMaster."""
        if self._source_finished:
            return

        remaining = min(max_count, len(self._planned_splits) - self._source_split_counter)

        for _ in range(remaining):
            split = self._planned_splits[self._source_split_counter]
            self._source_split_counter += 1

            # Ensure split has correct stage_id
            if split.stage_id != self.stage_id:
                split = Split(
                    split_id=split.split_id,
                    stage_id=self.stage_id,
                    data_range=split.data_range,
                    parent_split_ids=split.parent_split_ids,
                    attempt=split.attempt,
                    status=split.status,
                    assigned_worker=split.assigned_worker,
                    retry_count=split.retry_count,
                    created_at=split.created_at,
                    updated_at=split.updated_at,
                    metadata=split.metadata,
                    record_count=split.record_count,
                    is_terminal=split.is_terminal,
                )

            yield split

        if self._source_split_counter >= len(self._planned_splits):
            self._source_finished = True


