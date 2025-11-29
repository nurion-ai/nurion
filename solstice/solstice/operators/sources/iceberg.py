"""Iceberg source operator built on top of Arrow batching base."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from pyiceberg.catalog import load_catalog

from solstice.core.models import Split, SplitPayload
from solstice.core.operator import SourceOperator, OperatorConfig


@dataclass
class IcebergSourceConfig(OperatorConfig):
    """Configuration for IcebergSource operator."""
    
    catalog_uri: Optional[str] = None
    """URI of the Iceberg catalog."""
    
    table_name: Optional[str] = None
    """Full name of the Iceberg table (namespace.table)."""
    
    filter: Optional[str] = None
    """Filter expression to apply when reading."""
    
    snapshot_id: Optional[int] = None
    """Specific snapshot ID to read from."""


class IcebergSource(SourceOperator):
    """Source operator for reading from Iceberg tables."""

    def __init__(self, config: IcebergSourceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        self.catalog_uri: Optional[str] = config.catalog_uri
        self.table_name: Optional[str] = config.table_name
        self.filter_expr: Optional[str] = config.filter
        self.snapshot_id: Optional[int] = config.snapshot_id

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

        arrow_table = scan.to_table()
        if arrow_table.num_rows == 0:
            return None

        return SplitPayload.from_arrow(
            arrow_table,
            split_id=split.split_id,
        )

    def close(self) -> None:
        self.scan = None
        self.table = None
        self.catalog = None


# Set operator_class after class definition
IcebergSourceConfig.operator_class = IcebergSource
