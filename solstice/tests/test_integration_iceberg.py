"""Integration tests for Iceberg with real REST catalog"""

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from pathlib import Path

from solstice.core.operator import OperatorContext
from solstice.core.models import Record
from solstice.operators.source import IcebergSource


@pytest.fixture(scope="module")
def iceberg_catalog():
    """Get Iceberg REST catalog connection (requires aether service running)"""
    try:
        from pyiceberg.catalog import load_catalog
        
        # Connect to aether REST catalog
        catalog = load_catalog(
            "aether",
            **{
                "uri": "http://localhost:8181",
                "type": "rest",
            }
        )
        return catalog
    except Exception as e:
        pytest.skip(f"Iceberg catalog not available: {e}")


@pytest.fixture
def test_iceberg_table(iceberg_catalog):
    """Create a test Iceberg table"""
    try:
        # Create namespace
        try:
            iceberg_catalog.create_namespace("test")
        except:
            pass  # Namespace might already exist
        
        # Create table
        schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.int64()),
            ('name', pa.string()),
        ])
        
        table_name = "test.solstice_test_table"
        
        # Drop if exists
        try:
            iceberg_catalog.drop_table(table_name)
        except:
            pass
        
        # Create table
        table = iceberg_catalog.create_table(
            table_name,
            schema=schema
        )
        
        # Write test data
        data = pa.table({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
            'name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve']
        })
        
        table.append(data)
        
        yield table_name
        
        # Cleanup
        try:
            iceberg_catalog.drop_table(table_name)
        except:
            pass
        
    except Exception as e:
        pytest.skip(f"Could not create test table: {e}")


class TestIcebergSourceIntegration:
    """Integration tests for IcebergSource with real catalog"""
    
    def test_iceberg_source_read_real(self, test_iceberg_table):
        """Test reading from real Iceberg table"""
        config = {
            'catalog_uri': 'http://localhost:8181',
            'table_name': test_iceberg_table,
            'batch_size': 10,
        }
        
        source = IcebergSource(config)
        context = OperatorContext('task1', 'stage1', 'worker1')
        
        # Open source
        source.open(context)
        
        # Read records
        records = list(source.read())
        
        # Verify
        assert len(records) == 5
        assert records[0].value['id'] == 1
        assert records[0].value['name'] == 'Alice'
        assert records[4].value['id'] == 5
        assert records[4].value['name'] == 'Eve'
        
        # Cleanup
        source.close()
    
    def test_iceberg_source_checkpoint_restore(self, test_iceberg_table):
        """Test checkpoint and restore with real table"""
        config = {
            'catalog_uri': 'http://localhost:8181',
            'table_name': test_iceberg_table,
            'batch_size': 2,
        }
        
        # Read first 2 records
        source1 = IcebergSource(config)
        context1 = OperatorContext('task1', 'stage1', 'worker1')
        source1.open(context1)
        
        records = []
        for i, record in enumerate(source1.read()):
            records.append(record)
            if i >= 1:  # Read 2 records
                break
        
        # Checkpoint
        checkpoint_state = source1.checkpoint()
        assert checkpoint_state['offset'] == 2
        
        source1.close()
        
        # Restore and continue reading
        source2 = IcebergSource(config)
        context2 = OperatorContext('task1', 'stage1', 'worker1')
        source2.open(context2)
        source2.restore(checkpoint_state)
        
        # Should start from offset 2 (3rd record)
        remaining_records = list(source2.read())
        
        # Should get records 3, 4, 5
        assert len(remaining_records) == 3
        assert remaining_records[0].value['id'] == 3
        
        source2.close()


# Mark all tests to require the service
pytestmark = pytest.mark.integration

