"""Unit-style integration tests for IcebergSource (mocked catalog)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from solstice.core.models import Split
from solstice.operators.sources import IcebergSource, IcebergSourceConfig


def _mock_catalog(table_rows: list[dict]):
    fake_scan = MagicMock()
    fake_scan.filter.return_value = fake_scan
    fake_scan.use_snapshot.return_value = fake_scan
    fake_scan.to_table.return_value = pa.Table.from_pylist(table_rows)

    fake_table = MagicMock()
    fake_table.scan.return_value = fake_scan

    catalog = MagicMock()
    catalog.load_table.return_value = fake_table
    return catalog, fake_scan, fake_table


@pytest.mark.integration
@patch("solstice.operators.sources.iceberg.load_catalog")
def test_iceberg_source_reads_rows(mock_load_catalog):
    catalog, fake_scan, fake_table = _mock_catalog([{"id": 1, "value": 10}, {"id": 2, "value": 20}])
    mock_load_catalog.return_value = catalog

    config = IcebergSourceConfig(catalog_uri="http://localhost/catalog", table_name="db.tbl")
    source = config.setup()
    split = Split(
        split_id="split-0",
        stage_id="source",
        data_range={
            "catalog_uri": "http://localhost/catalog",
            "table_name": "db.tbl",
            "filter": "value > 5",
            "snapshot_id": 42,
        },
    )

    batch = source.process_split(split)

    assert len(batch) == 2
    records = batch.to_records()
    assert [row.value["value"] for row in records] == [10, 20]
    fake_table.scan.assert_called_once()
    fake_scan.filter.assert_called_once_with("value > 5")
    fake_scan.use_snapshot.assert_called_once_with(42)
