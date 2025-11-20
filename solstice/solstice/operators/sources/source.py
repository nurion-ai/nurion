"""Operator Master interface for operator-specific control logic."""

import time

from abc import abstractmethod
from typing import Iterator, List, Any

from solstice.core.stage import Stage
from solstice.core.models import Split
from solstice.core.stage_master import StageMasterActor
from solstice.state.backend import StateBackend


class SourceStageMaster(StageMasterActor):
    """Master for source operators that handles split planning and generation."""

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        stage: Stage,
        upstream_stages: List[str] | None = None,
    ):
        super().__init__(job_id, state_backend, stage, upstream_stages)

        self.logger.info( f"Source operator master for stage {self.stage_id}")


    def run(self, poll_interval: float = 0.05) -> None:
        if self._running:
            return
        self._running = True
        self.logger.info(f"Stage {self.stage_id} run loop started")
        try:
            while self._running:
                self.logger.debug(f"This is a source stage, requesting splits from source")
                self._request_splits_from_source()

                self._schedule_pending_splits()
                self._drain_completed_results(timeout=0.0)
                time.sleep(poll_interval)
        finally:
            self.logger.info(f"Stage {self.stage_id} run loop stopped")

    def _request_splits_from_source(self) -> None:
        available_capacity = self.max_queue_size - len(self.pending_splits)
        if available_capacity <= 0:
            return

        for split in self.fetch_splits():
            self.enqueue_split(split, payload_ref=None)

    @abstractmethod
    def fetch_splits(self) -> Iterator[Split]:
        raise NotImplementedError("fetch_splits must be implemented by subclasses")

