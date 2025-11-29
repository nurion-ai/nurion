"""Lance table source operator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Iterator, List, Optional

import lance

from solstice.core.models import Split, SplitPayload
from solstice.operators.sources.source import SourceStageMaster
from solstice.state.backend import StateBackend
from solstice.core.operator import SourceOperator, OperatorConfig
from solstice.core.stage_master import StageMasterConfig

if TYPE_CHECKING:
    from solstice.core.stage import Stage


@dataclass
class LanceTableSourceConfig(OperatorConfig):
    """Configuration for LanceTableSource operator."""
    
    dataset_uri: str
    """URI of the Lance dataset."""
    
    filter: Optional[str] = None
    """Filter expression to apply when reading."""
    
    columns: Optional[Iterable[str]] = None
    """Columns to read from the dataset."""
    
    split_size: int = 1024
    """Number of rows per split."""


class LanceTableSource(SourceOperator):
    """Source operator for reading from Lance tables."""

    def __init__(self, config: LanceTableSourceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        if not config.dataset_uri:
            raise ValueError("dataset_uri is required for LanceTableSource")
        self.dataset_uri: str = config.dataset_uri

    def read(self, split: Split) -> Optional[SplitPayload]:
        dataset = lance.dataset(self.dataset_uri)
        fragment = dataset.get_fragment(split.data_range.pop("fragment_id"))
        fragment_scanner = fragment.scanner(
            **split.data_range,
            with_row_id=True,
            # order_by=[ColumnOrdering(column_name="_row_id")],
        )
        table = fragment_scanner.to_table()
        if table.num_rows == 0:
            return SplitPayload.empty(split_id=f"{split.split_id}:{self.worker_id}")

        return SplitPayload.from_arrow(
            table,
            split_id=f"{split.split_id}:{self.worker_id}",
        )

    def close(self) -> None:
        self.dataset_uri = None


# Set operator_class after class definition
LanceTableSourceConfig.operator_class = LanceTableSource


@dataclass
class LanceSourceStageMasterConfig(StageMasterConfig):
    """Configuration for LanceSourceStageMaster."""
    
    dataset_uri: Optional[str] = None
    """URI of the Lance dataset (required)."""
    
    filter: Optional[str] = None
    """Filter expression to apply when reading."""
    
    columns: Optional[Iterable[str]] = None
    """Columns to read from the dataset."""
    
    split_size: int = 1024
    """Number of rows per split."""
    
    def __post_init__(self):
        if not self.dataset_uri:
            raise ValueError("dataset_uri is required for LanceSourceStageMasterConfig")


class LanceSourceStageMaster(SourceStageMaster):
    """Planner for Lance tables."""

    def __init__(
        self,
        job_id: str,
        state_backend: StateBackend,
        stage: "Stage",
        upstream_stages: List[str] | None = None,
    ):
        super().__init__(job_id, state_backend, stage, upstream_stages)
        
        # Get the operator config which contains Lance-specific settings
        operator_cfg = stage.operator_config
        if not isinstance(operator_cfg, LanceTableSourceConfig):
            raise TypeError(
                f"LanceSourceStageMaster requires LanceTableSourceConfig, got {type(operator_cfg)}"
            )
        
        self.dataset_uri: str = operator_cfg.dataset_uri
        if not self.dataset_uri:
            raise ValueError("dataset_uri is required for LanceSourceStageMaster")
        self.filter: Optional[str] = operator_cfg.filter
        self.columns: Optional[Iterable[str]] = operator_cfg.columns
        self.split_size: int = operator_cfg.split_size

        self.dataset = lance.dataset(self.dataset_uri)

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


# Set master_class after class definition
LanceSourceStageMasterConfig.master_class = LanceSourceStageMaster
