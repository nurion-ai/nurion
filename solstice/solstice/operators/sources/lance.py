"""Lance table source operator."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional

import lance

from solstice.core.models import Split, SplitPayload
from solstice.operators.sources.source import SourceStageMaster
from solstice.state.backend import StateBackend
from solstice.core.stage import Stage
from solstice.core.operator import SourceOperator


class LanceTableSource(SourceOperator):
    """Source operator for reading from Lance tables."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.dataset_uri: Optional[str] = cfg.get("dataset_uri")
        if not self.dataset_uri:
            raise ValueError("dataset_uri is required for LanceTableSource")

    def read(self, split: Split) -> Optional[SplitPayload]:
        from lance.dataset import ColumnOrdering
        dataset = lance.dataset(self.dataset_uri)
        fragment = dataset.get_fragment(split.data_range.pop("fragment_id"))
        fragment_scanner = fragment.scanner(
            **split.data_range,
            with_row_id=True,
            # order_by=[ColumnOrdering(column_name="_row_id")],
        )
        table = fragment_scanner.to_table()
        if table.num_rows == 0:
            return SplitPayload.empty(split_id=f"{split.split_id}:read_{self.worker_id}")

        return SplitPayload.from_arrow(
            table,
            split_id=f"{split.split_id}:read_{self.worker_id}",
        )

    def close(self) -> None:
        self.dataset_uri = None

class LanceSourceStageMaster(SourceStageMaster):
    """Planner for Lance tables."""

    def __init__(self, job_id: str, state_backend: StateBackend, stage: Stage, upstream_stages: List[str] | None = None):
        super().__init__(job_id, state_backend, stage, upstream_stages)
        self.config = stage.operator_config or {}
        self.dataset_uri: str = self.config.get("dataset_uri")
        if not self.dataset_uri:
            raise ValueError("dataset_uri is required for LancePlanner")
        self.filter: Optional[str] = self.config.get("filter")
        self.columns: Optional[Iterable[str]] = self.config.get("columns")
        # self.namespace: str = config.get("namespace")
        # self.table_name: str = config.get("table_name")
        # if not self.dataset_uri and (not self.namespace or not self.table_name):
        #     raise ValueError("dataset_uri or (namespace and table_name) is required for LancePlanner")
        
        self.dataset = lance.dataset(self.dataset_uri)
        self.split_size = self.config.get("split_size", 1024)

    def fetch_splits(self) -> Iterator[Split]:
        sorted_fragments = sorted(self.dataset.get_fragments(), key=lambda x: x.fragment_id)
        for frag in sorted_fragments:
            row_count = frag.count_rows()
            for i in range(0, row_count, self.split_size):
                yield Split(
                    split_id=f"{self.stage.stage_id}_{i}",
                    stage_id=self.stage.stage_id,
                    data_range={
                        "filter": self.filter,
                        "columns": self.columns,
                        "fragment_id": frag.fragment_id,
                        "offset": i,
                        "limit": self.split_size,
                    },
                )