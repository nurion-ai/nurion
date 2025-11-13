"""Integration tests for Iceberg with real REST catalog"""

import pytest

from solstice.core.operator import OperatorContext
from solstice.operators.sources import IcebergSource


@pytest.fixture(scope="module")
def iceberg_catalog():
    """Get Iceberg REST catalog connection (requires aether service running)"""
    from pyiceberg.catalog import load_catalog

    # Connect to aether REST catalog
    catalog = load_catalog(
        "aether",
        **{
            "uri": "http://localhost:8000/api/iceberg-catalog",
            "type": "rest",
        },
    )
    return catalog


@pytest.mark.integration
class TestIcebergCatalogConnection:
    """Integration tests for Iceberg catalog connection"""

    def test_catalog_connection(self, iceberg_catalog):
        """Test that we can connect to aether Iceberg catalog"""
        assert iceberg_catalog is not None

        # List namespaces
        namespaces = list(iceberg_catalog.list_namespaces())
        assert isinstance(namespaces, list)

    def test_iceberg_source_initialization(self):
        """Test IcebergSource can be initialized"""
        config = {
            "catalog_uri": "http://localhost:8000/api/iceberg-catalog",
            "table_name": "test.table",
            "batch_size": 100,
        }

        source = IcebergSource(config)

        assert source.catalog_uri == "http://localhost:8000/api/iceberg-catalog"
        assert source.table_name == "test.table"
        assert source.batch_size == 100

    def test_iceberg_source_checkpoint(self):
        """Test IcebergSource checkpoint mechanism"""
        config = {
            "catalog_uri": "http://localhost:8000/api/iceberg-catalog",
            "table_name": "test.table",
        }

        source = IcebergSource(config)
        context = OperatorContext("task1", "stage1", "worker1")

        # Set some offset
        context.set_state("offset", 42)
        source._context = context

        # Checkpoint
        checkpoint = source.checkpoint()

        assert checkpoint["offset"] == 42

        # Restore
        context2 = OperatorContext("task1", "stage1", "worker1")
        source._context = context2
        source.restore(checkpoint)

        assert context2.get_state("offset") == 42
