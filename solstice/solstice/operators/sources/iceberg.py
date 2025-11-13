"""Iceberg source operator built on top of Arrow batching base."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional
from pyiceberg.catalog import load_catalog

from solstice.core.models import Batch
from solstice.operators.sources.base import ArrowStreamingSource


class IcebergSource(ArrowStreamingSource):
    """Source operator for reading from Iceberg tables."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        cfg = config or {}
        self.catalog_uri: Optional[str] = cfg.get("catalog_uri")
        self.table_name: Optional[str] = cfg.get("table_name")
        self.filter_expr: Optional[str] = cfg.get("filter")
        self.snapshot_id: Optional[int] = cfg.get("snapshot_id")

        self.catalog = None
        self.table = None
        self.scan = None

    def open(self, context) -> None:
        super().open(context)

        if not self.catalog_uri:
            raise ValueError("catalog_uri is required for IcebergSource")
        if not self.table_name:
            raise ValueError("table_name is required for IcebergSource")

        self.catalog = load_catalog(name="default", **{"uri": self.catalog_uri})
        self.table = self.catalog.load_table(self.table_name)

        scan = self.table.scan()
        if self.filter_expr:
            scan = scan.filter(self.filter_expr)
        if self.snapshot_id:
            scan = scan.use_snapshot(self.snapshot_id)
        self.scan = scan

    def read(self) -> Iterable[Batch]:
        if not self.scan:
            raise RuntimeError("Source not opened. Call open() first.")

        arrow_table = self.scan.to_arrow()
        metadata = {
            "table": self.table_name,
            "catalog_uri": self.catalog_uri,
        }
        yield from self._emit_table(arrow_table, metadata=metadata)

    def close(self) -> None:
        self.scan = None
        self.table = None
        self.catalog = None

