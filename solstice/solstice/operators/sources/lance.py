"""Lance table source operator."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pyarrow as pa
from lance.dataset import LanceDataset

from solstice.core.models import Batch
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

    def read(self) -> Iterable[Batch]:
        if not self.scanner:
            raise RuntimeError("Source not opened. Call open() first.")

        metadata = {"table": self.table_path}
        for record_batch in self.scanner.to_batches():
            table = pa.Table.from_batches([record_batch])
            yield from self._emit_table(table, metadata=metadata)

    def close(self) -> None:
        self.scanner = None
        self.table = None

