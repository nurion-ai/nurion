"""Utilities for creating reusable test datasets."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Dict

import pyarrow as pa
from lance.dataset import write_dataset
from pyiceberg.catalog import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import NestedField, Schema
from pyiceberg.table import PartitionSpec
from pyiceberg.types import DoubleType, IntegerType, StringType

LOGGER = logging.getLogger(__name__)

_DATA_ROOT = Path(__file__).resolve().parent / "resources"
_LANCE_CACHE: Dict[str, Path] = {}
_ICEBERG_CACHE: Dict[str, Dict[str, str]] = {}

LANCE_NUM_ROWS = 1000
ICEBERG_NUM_ROWS = 1000


def data_root() -> Path:
    """Return the root directory for test data resources."""
    _DATA_ROOT.mkdir(parents=True, exist_ok=True)
    return _DATA_ROOT


def ensure_lance_dataset(name: str = "sample_lance", refresh: bool = False) -> Path:
    """Create (if needed) and return a Lance dataset for testing.

    The dataset contains deterministic values so tests can make strong assertions.
    Set ``refresh=True`` to rebuild the dataset even if it already exists.
    """
    if refresh:
        _LANCE_CACHE.pop(name, None)
    elif name in _LANCE_CACHE:
        return _LANCE_CACHE[name]

    dataset_dir = data_root() / "lance" / name
    if refresh and dataset_dir.exists():
        shutil.rmtree(dataset_dir, ignore_errors=True)
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # If the dataset already contains data, reuse it.
    if not refresh and any(dataset_dir.iterdir()):
        _LANCE_CACHE[name] = dataset_dir
        return dataset_dir

    table = pa.table(
        {
            "id": list(range(1, LANCE_NUM_ROWS + 1)),
            "value": [i * 5 for i in range(1, LANCE_NUM_ROWS + 1)],
            "name": [f"name_{i}" for i in range(1, LANCE_NUM_ROWS + 1)],
        }
    )

    write_dataset(table, str(dataset_dir))
    LOGGER.info("Created Lance test dataset at %s", dataset_dir)

    _LANCE_CACHE[name] = dataset_dir
    return dataset_dir


def ensure_iceberg_catalog(name: str = "sample_iceberg", refresh: bool = False) -> Dict[str, str]:
    """Create (if needed) a self-contained Iceberg catalog with sample data.

    Returns a dictionary containing:
        catalog_uri: URI for loading the catalog via pyiceberg.
        table_identifier: Dot-delimited table identifier within the catalog.
    """
    if refresh:
        _ICEBERG_CACHE.pop(name, None)
    elif name in _ICEBERG_CACHE:
        return _ICEBERG_CACHE[name]

    warehouse_path = data_root() / "iceberg" / name / "warehouse"
    catalog_db_path = data_root() / "iceberg" / name / "catalog.db"

    if refresh and warehouse_path.exists():
        shutil.rmtree(warehouse_path, ignore_errors=True)
    if refresh and catalog_db_path.exists():
        catalog_db_path.unlink()

    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog_db_path.parent.mkdir(parents=True, exist_ok=True)

    catalog_uri = f"sqlite:///{catalog_db_path}"
    warehouse_uri = warehouse_path.resolve().as_uri()

    catalog = SqlCatalog(
        name,
        uri=catalog_uri,
        warehouse=warehouse_uri,
    )

    namespace = ("default", "analytics")
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass
    except NoSuchNamespaceError:
        pass

    table_identifier = namespace + ("events",)

    schema = Schema(
        NestedField(1, "event_id", IntegerType(), required=True),
        NestedField(2, "event_type", StringType(), required=True),
        NestedField(3, "amount", DoubleType(), required=False),
        NestedField(4, "region", StringType(), required=False),
    )

    arrow_schema = pa.schema(
        [
            pa.field("event_id", pa.int32(), nullable=False),
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
            pa.field("region", pa.string(), nullable=True),
        ]
    )

    table = None
    if not refresh:
        try:
            table = catalog.load_table(table_identifier)
        except NoSuchTableError:
            table = None
        else:
            current_snapshot = table.current_snapshot()
            current_records = (
                int(current_snapshot.summary.get("total-records", 0))
                if current_snapshot is not None
                else 0
            )
            if current_records != ICEBERG_NUM_ROWS:
                catalog.drop_table(table_identifier)
                table = None

    if table is None:
        table = catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=PartitionSpec(schema=schema),
            properties={"format-version": "2"},
        )

    if table.current_snapshot() is None or refresh:
        event_types = ["signup", "purchase", "refund", "support", "churn"]
        amount_pattern = [0.0, 19.99, -19.99, 0.0, 0.0]
        region_pattern = ["us-east", "eu-west", "ap-southeast", "us-west", "latam-south"]
        batch = pa.table(
            {
                "event_id": list(range(1000, 1000 + ICEBERG_NUM_ROWS)),
                "event_type": [event_types[i % len(event_types)] for i in range(ICEBERG_NUM_ROWS)],
                "amount": [
                    amount_pattern[i % len(amount_pattern)] for i in range(ICEBERG_NUM_ROWS)
                ],
                "region": [
                    region_pattern[i % len(region_pattern)] for i in range(ICEBERG_NUM_ROWS)
                ],
            },
            schema=arrow_schema,
        )
        table.append(batch)
        LOGGER.info(
            "Populated Iceberg test table %s with %d rows",
            ".".join(table_identifier),
            ICEBERG_NUM_ROWS,
        )

    result = {
        "catalog_name": name,
        "catalog_uri": catalog_uri,
        "warehouse_uri": warehouse_uri,
        "table_identifier": ".".join(table_identifier),
    }
    _ICEBERG_CACHE[name] = result
    return result
