"""Iceberg source operator built on top of Arrow batching base."""

from __future__ import annotations

from typing import Any, Dict, Optional
from pyiceberg.catalog import load_catalog

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SourceOperator


class IcebergSource(SourceOperator):
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

    def read(self, split: Split) -> Optional[SplitPayload]:
        catalog_uri = split.data_range.get("catalog_uri") or self.catalog_uri
        table_name = split.data_range.get("table_name") or self.table_name

        if not catalog_uri or not table_name:
            raise ValueError("Split missing catalog_uri or table_name for IcebergSource")

        catalog = load_catalog(name="default", **{"uri": catalog_uri})
        table = catalog.load_table(table_name)

        scan = table.scan()
        filter_expr = split.data_range.get("filter") or self.filter_expr
        if filter_expr:
            scan = scan.filter(filter_expr)

        snapshot_id = split.data_range.get("snapshot_id") or self.snapshot_id
        if snapshot_id:
            scan = scan.use_snapshot(snapshot_id)

        arrow_table = scan.to_arrow()
        if arrow_table.num_rows == 0:
            return None

        metadata = dict(split.metadata)
        metadata.update(
            {
                "table": table_name,
                "catalog_uri": catalog_uri,
            }
        )

        return SplitPayload.from_arrow(
            arrow_table,
            split_id=split.split_id,
            metadata=metadata,
        )

    def close(self) -> None:
        self.scan = None
        self.table = None
        self.catalog = None
