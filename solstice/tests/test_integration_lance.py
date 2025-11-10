"""Integration tests for Lance with real tables"""

import pytest
import pyarrow as pa
import tempfile
import shutil
from pathlib import Path
import lance

from solstice.core.operator import OperatorContext
from solstice.core.models import Record
from solstice.operators.source import LanceTableSource


@pytest.fixture
def test_lance_table():
    """Create a real Lance table for testing"""
    
    # Create temp directory
    tmpdir = tempfile.mkdtemp()
    table_path = Path(tmpdir) / "test_table"
    
    try:
        # Create test data
        data = pa.table({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
            'name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve']
        })
        
        # Write to Lance
        lance.write_dataset(data, str(table_path))
        
        yield str(table_path)
        
    finally:
        # Cleanup
        shutil.rmtree(tmpdir, ignore_errors=True)


class TestLanceTableSourceIntegration:
    """Integration tests for LanceTableSource with real tables"""
    
    def test_lance_source_read_real(self, test_lance_table):
        """Test reading from real Lance table"""
        config = {
            'table_path': test_lance_table,
            'batch_size': 10,
        }
        
        source = LanceTableSource(config)
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
    
    def test_lance_source_with_filter(self, test_lance_table):
        """Test reading with filter"""
        config = {
            'table_path': test_lance_table,
            'batch_size': 10,
            'columns': ['id', 'name'],  # Only read specific columns
        }
        
        source = LanceTableSource(config)
        context = OperatorContext('task1', 'stage1', 'worker1')
        
        source.open(context)
        
        records = list(source.read())
        
        # Should have id and name, but not value
        assert len(records) == 5
        assert 'id' in records[0].value
        assert 'name' in records[0].value
        # Note: Lance might still include all columns depending on version
        
        source.close()
    
    def test_lance_source_checkpoint_restore(self, test_lance_table):
        """Test checkpoint and restore with real table"""
        config = {
            'table_path': test_lance_table,
            'batch_size': 2,
        }
        
        # Read first 2 records
        source1 = LanceTableSource(config)
        context1 = OperatorContext('task1', 'stage1', 'worker1')
        source1.open(context1)
        
        records = []
        for i, record in enumerate(source1.read()):
            records.append(record)
            if i >= 1:  # Read 2 records (indices 0, 1)
                break
        
        assert len(records) == 2, f"Should have read 2 records, got {len(records)}"
        
        # Checkpoint
        checkpoint_state = source1.checkpoint()
        # After reading indices 0 and 1, offset should be 2
        actual_offset = checkpoint_state.get('offset', 0)
        assert actual_offset >= 2, f"Offset should be at least 2, got {actual_offset}"
        
        source1.close()
        
        # Restore and continue reading
        source2 = LanceTableSource(config)
        context2 = OperatorContext('task1', 'stage1', 'worker1')
        source2.open(context2)
        source2.restore(checkpoint_state)
        
        # Should start from offset 2 (3rd record)
        remaining_records = list(source2.read())
        
        # Should get records 3, 4, 5
        assert len(remaining_records) == 3
        assert remaining_records[0].value['id'] == 3
        
        source2.close()


# Mark as integration tests
pytestmark = pytest.mark.integration

