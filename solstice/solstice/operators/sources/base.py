"""Shared Arrow-based source operator utilities."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, Optional, Union, List

import pyarrow as pa

from solstice.core.models import Split, SplitStatus
from solstice.core.operator import SourceOperator


class ArrowStreamingSource(SourceOperator):
    """Base class for sources that materialize Arrow batches."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.batch_size: Optional[int] = cfg.get("batch_size")
        self._resume_offset: int = 0
        self._emitted_offset: int = 0
        self._batch_counter: int = 0
        self._split_counter: int = 0

    def plan_splits(self) -> List[Split]:
        """Default split planning for Arrow-based sources.

        By default we create a single split that captures the operator configuration.
        Subclasses can override this to produce more fine-grained work units.
        """
        config_copy = dict(self.config) if isinstance(self.config, dict) else {}
        stage_id = config_copy.get("stage_id", "source")
        split_id = f"{self.__class__.__name__.lower()}_planned_split_0"

        return [
            Split(
                split_id=split_id,
                stage_id=stage_id,
                data_range={"config": config_copy},
                metadata={"source": self.__class__.__name__},
                status=SplitStatus.PENDING,
            )
        ]