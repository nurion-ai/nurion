"""Integration tests for Lance with real tables"""

import pytest
import pyarrow as pa
import tempfile
import shutil
from pathlib import Path
from lance.dataset import write_dataset

from solstice.operators.sources import LanceTableSource


@pytest.fixture
def test_lance_table():
    """Create a real Lance table for testing"""

    # Create temp directory
    tmpdir = tempfile.mkdtemp()
    table_path = Path(tmpdir) / "test_table"

    try:
        # Create test data
        data = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
                "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            }
        )

        # Write to Lance
        write_dataset(data, str(table_path))

        yield str(table_path)

    finally:
        # Cleanup
        shutil.rmtree(tmpdir, ignore_errors=True)


class TestLanceTableSourceIntegration:
    """Integration tests for LanceTableSource with real tables"""

    def test_lance_source_read_real(self, test_lance_table):
        """Test reading from real Lance table"""
        config = {
            "table_path": test_lance_table,
            "batch_size": 10,
        }

        source = LanceTableSource(config)

        batches = []
        for split in source.plan_splits():
            batch = source.read(split)
            if batch is not None:
                batches.append(batch)

        total_rows = sum(len(batch) for batch in batches)
        assert total_rows == 5

        table = batches[0].to_table()
        assert table.column("id").to_pylist() == [1, 2, 3, 4, 5]
        assert table.column("name").to_pylist() == ["Alice", "Bob", "Charlie", "Dave", "Eve"]

        # Cleanup
        source.close()

    def test_lance_source_with_filter(self, test_lance_table):
        """Test reading with filter"""
        config = {
            "table_path": test_lance_table,
            "batch_size": 10,
            "columns": ["id", "name"],  # Only read specific columns
        }

        source = LanceTableSource(config)
        batches = []
        for split in source.plan_splits():
            batch = source.read(split)
            if batch is not None:
                batches.append(batch)

        total_rows = sum(len(batch) for batch in batches)
        assert total_rows == 5

        table = batches[0].to_table()
        assert table.schema.names == ["id", "name"]

        source.close()



# Mark as integration tests
pytestmark = pytest.mark.integration
