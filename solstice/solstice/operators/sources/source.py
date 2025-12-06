"""Source stage master for operator-specific control logic."""

import time

from abc import abstractmethod
from typing import TYPE_CHECKING, Iterator

from solstice.core.models import Split
from solstice.core.stage_master import StageMasterActor

if TYPE_CHECKING:
    pass


class SourceStageMaster(StageMasterActor):
    """Master for source stages that generates splits internally.

    Unlike regular stages that pull from upstream, source stages generate
    their own splits via the abstract `plan_splits()` method.
    """

    def run(self, poll_interval: float = 0.05) -> bool:
        """Run loop for source stages - generates splits instead of pulling."""
        if self._running:
            return False
        self._running = True

        try:
            split_iterator = self.plan_splits()
            has_more = True

            while (
                not self._failed
                and self._running
                and (has_more or self._pending_splits or self._inflight_results)
            ):
                has_more = self._generate_splits(split_iterator)
                self._schedule_pending_splits()
                self._drain_completed_results(timeout=poll_interval * 2)
                self.output_buffer.gc()
                time.sleep(poll_interval)

            if self._failed and self._failure_exception:
                raise self._failure_exception

            self.output_buffer.mark_upstream_finished()
            return True
        finally:
            self._running = False
            self._cleanup_workers()

    def _generate_splits(self, split_iterator: Iterator[Split]) -> bool:
        """Generate splits from the source iterator. Returns True if more splits may exist."""
        if self._backpressure or len(self._pending_splits) >= self._max_queue_size:
            return True

        try:
            for split in split_iterator:
                if self.checkpoint_tracker.is_split_completed(split.split_id):
                    continue
                self._pending_splits.append(split)
                if len(self._pending_splits) >= self._max_queue_size:
                    self._backpressure = True
                    return True
            return False
        except StopIteration:
            return False

    @abstractmethod
    def plan_splits(self) -> Iterator[Split]:
        """Plan and generate splits for this source stage."""
        raise NotImplementedError("plan_splits must be implemented by subclasses")
