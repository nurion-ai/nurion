"""Lance table source operator."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from lance.dataset import LanceDataset

from solstice.core.models import Batch, Split, SplitStatus
from solstice.operators.sources.base import ArrowStreamingSource


class LanceTableSource(ArrowStreamingSource):
    """Source operator for reading from Lance tables."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.table_path: Optional[str] = cfg.get("table_path")
        self.columns: Optional[Iterable[str]] = cfg.get("columns")
        self.filter_expr: Optional[str] = cfg.get("filter")

        self.table: Optional[LanceDataset] = None
        self.scanner = None

    def open(self, context) -> None:
        super().open(context)
        if not self.table_path:
            raise ValueError("table_path is required for LanceTableSource")

        if not Path(self.table_path).exists():
            raise FileNotFoundError(f"Lance table not found: {self.table_path}")

        self.table = LanceDataset(self.table_path)

        scanner_kwargs: Dict[str, Any] = {}
        if self.columns:
            scanner_kwargs["columns"] = list(self.columns)
        if self.filter_expr:
            scanner_kwargs["filter"] = self.filter_expr

        self.scanner = self.table.scanner(**scanner_kwargs)

    def plan_splits(self) -> List[Split]:
        if not self.table_path:
            raise ValueError("table_path is required for LanceTableSource")

        stage_id = (self.config or {}).get("stage_id", "lance_source")
        data_range = {
            "table_path": self.table_path,
            "columns": list(self.columns) if self.columns else None,
            "filter": self.filter_expr,
        }
        return [
            Split(
                split_id=f"{stage_id}_split_0",
                stage_id=stage_id,
                data_range=data_range,
                metadata={"table_path": self.table_path},
                status=SplitStatus.PENDING,
            )
        ]

    def read(self, split: Split) -> Optional[Batch]:
        table_path = split.data_range.get("table_path") or self.table_path
        if not table_path:
            raise ValueError("Split missing table_path for LanceTableSource")

        dataset = LanceDataset(table_path)
        scanner_kwargs: Dict[str, Any] = {}

        columns = split.data_range.get("columns") or self.columns
        if columns:
            scanner_kwargs["columns"] = list(columns)

        filter_expr = split.data_range.get("filter") or self.filter_expr
        if filter_expr:
            scanner_kwargs["filter"] = filter_expr

        table = dataset.scanner(**scanner_kwargs).to_table()
        if table.num_rows == 0:
            return None

        metadata = dict(split.metadata)
        metadata.update({"table_path": table_path, "source": "LanceTableSource"})

        return Batch.from_arrow(
            table,
            batch_id=f"{split.stage_id}_batch_{split.split_id}",
            source_split=split.split_id,
            metadata=metadata,
        )

    def close(self) -> None:
        self.scanner = None
        self.table = None
