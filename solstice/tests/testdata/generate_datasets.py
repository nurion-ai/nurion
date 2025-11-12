"""
Utility script to materialize Lance and Iceberg datasets for integration tests.

This script is intended to be executed manually (or via CI) to refresh the on-disk
test resources under `solstice/tests/testdata/resources/`.

Example:
    uv run python solstice/tests/testdata/generate_datasets.py
"""

from __future__ import annotations

import logging
from typing import Dict

from . import data_root, ensure_iceberg_catalog, ensure_lance_dataset

LOGGER = logging.getLogger("generate_datasets")


def build_lance_dataset() -> None:
    """Create or refresh the Lance dataset used by tests."""
    dataset_path = ensure_lance_dataset(refresh=True)
    LOGGER.info("Lance dataset is ready at %s", dataset_path)


def _refresh_iceberg_table() -> Dict[str, str]:
    """Recreate the Iceberg table with fresh deterministic data."""
    metadata = ensure_iceberg_catalog(refresh=True)

    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        metadata["catalog_name"],
        uri=metadata["catalog_uri"],
        warehouse=metadata["warehouse_uri"],
    )
    identifier = tuple(metadata["table_identifier"].split("."))
    table = catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    row_count = snapshot.summary.get("total-records", "0") if snapshot else "0"
    LOGGER.info(
        "Iceberg table %s refreshed with %s rows",
        metadata["table_identifier"],
        row_count,
    )
    return metadata


def build_iceberg_dataset() -> None:
    """Create or refresh the Iceberg dataset used by tests."""
    metadata = _refresh_iceberg_table()
    LOGGER.info(
        "Iceberg catalog is ready at %s with table %s",
        metadata["catalog_uri"],
        metadata["table_identifier"],
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    data_root().mkdir(parents=True, exist_ok=True)

    build_lance_dataset()
    build_iceberg_dataset()


if __name__ == "__main__":
    main()
