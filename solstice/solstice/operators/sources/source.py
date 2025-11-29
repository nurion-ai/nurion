"""Operator Master interface for operator-specific control logic."""

import time

from abc import abstractmethod
from typing import TYPE_CHECKING, Iterator, List

from solstice.core.models import Split
from solstice.core.stage_master import StageMasterActor
from solstice.state.backend import StateBackend

if TYPE_CHECKING:
    from solstice.core.stage import Stage


class SourceStageMaster(StageMasterActor):
    """Master for source operators that handles split planning and generation."""

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        stage: "Stage",
        upstream_stages: List[str] | None = None,
    ):
        super().__init__(job_id, state_backend, stage, upstream_stages)

        self.logger.info(f"Source operator master for stage {self.stage_id}")

    def run(self, poll_interval: float = 0.05) -> bool:
        if self._running:
            return False
        self._running = True
        self.logger.info(f"Stage {self.stage_id} run loop started")
        try:
            split_iterator = self.fetch_splits()
            has_more_splits = True

            def need_running() -> bool:
                return self._running and (
                    has_more_splits
                    or len(self._pending_splits) > 0
                    or len(self._inflight_results) > 0
                )

            while need_running():
                self.logger.debug("This is a source stage, requesting splits from source")
                has_more_splits = self._request_splits_from_source(split_iterator)
                self._schedule_pending_splits()
                self._drain_completed_results(timeout=poll_interval * 2)
                time.sleep(poll_interval)
            for actor_ref in self.downstream_stage_refs.values():
                actor_ref.set_upstream_finished.remote(self.stage_id)
            self._running = False
            return True
        finally:
            self.logger.info(f"Stage {self.stage_id} run loop stopped")
            self._running = False
            return False

    def _request_splits_from_source(self, split_iterator: Iterator[Split]) -> bool:
        available_capacity = self.max_queue_size - len(self._pending_splits)
        if available_capacity <= 0:
            return True  # Still has capacity, iterator might have more splits

        try:
            for split in split_iterator:
                self.enqueue_split(split)
                if self.backpressure_active:
                    self.logger.warning(
                        f"Backpressure active for stage {self.stage_id}, stop enqueuing splits"
                    )
                    return True  # Iterator might still have more splits
            # Iterator exhausted
            return False
        except StopIteration:
            # Iterator exhausted
            return False

    @abstractmethod
    def fetch_splits(self) -> Iterator[Split]:
        raise NotImplementedError("fetch_splits must be implemented by subclasses")
