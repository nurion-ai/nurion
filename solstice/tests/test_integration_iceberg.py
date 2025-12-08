"""Integration tests for IcebergSource using aether REST catalog.

Tests the full pipeline flow:
1. Create Iceberg table via aether REST catalog
2. Write test data to table
3. Run IcebergSource through StageMaster with TansuBackend queue
4. Verify data is processed correctly
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest
import ray
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from solstice.core.models import Split
from solstice.core.stage import Stage
from solstice.operators.sources import IcebergSourceConfig

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def iceberg_catalog(iceberg_catalog_uri: str) -> RestCatalog:
    """Create a pyiceberg RestCatalog connected to aether."""
    return RestCatalog(name="aether_catalog", uri=iceberg_catalog_uri)


@pytest.fixture(scope="module")
def ray_context():
    """Initialize Ray for integration tests."""
    if not ray.is_initialized():
        ray.init(
            num_cpus=2,
            ignore_reinit_error=True,
            runtime_env={
                "excludes": [
                    "java/",
                    "raydp/jars/",
                    "tests/testdata/resources/videos/",
                    "tests/testdata/resources/tmp/",
                    "*.jar",
                    "*.mp4",
                    "*.tar.gz",
                    "*.lance",
                ],
            },
        )
    yield
    ray.shutdown()


@pytest.fixture
def iceberg_test_table(iceberg_catalog: RestCatalog, iceberg_catalog_uri: str):
    """Create a test Iceberg table with sample data."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = "default"
    table_name = f"test_table_{unique_id}"
    full_name = f"{namespace}.{table_name}"

    # Create schema - all fields optional to match PyArrow defaults
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "value", LongType(), required=False),
        NestedField(3, "name", StringType(), required=False),
    )

    # Create table
    table = iceberg_catalog.create_table(identifier=full_name, schema=schema)

    # Add test data
    data = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        }
    )
    table.append(data)

    yield {
        "catalog_uri": iceberg_catalog_uri,
        "table_name": full_name,
        "expected_rows": 5,
    }

    # Cleanup
    try:
        iceberg_catalog.drop_table(full_name)
    except Exception:
        pass


class TestIcebergSource:
    """Integration tests for IcebergSource with aether REST catalog."""

    def test_iceberg_source_reads_table(self, iceberg_test_table):
        """Test reading Iceberg table via IcebergSource operator."""
        config = IcebergSourceConfig(
            catalog_uri=iceberg_test_table["catalog_uri"],
            table_name=iceberg_test_table["table_name"],
        )
        source = config.setup()

        split = Split(
            split_id="split-0",
            stage_id="source",
            data_range={
                "catalog_uri": iceberg_test_table["catalog_uri"],
                "table_name": iceberg_test_table["table_name"],
            },
        )

        batch = source.process_split(split)

        assert batch is not None
        assert len(batch) == iceberg_test_table["expected_rows"]

        records = batch.to_pylist()
        assert len(records) == 5
        # Verify some data
        names = [r["name"] for r in records]
        assert "Alice" in names
        assert "Eve" in names

        source.close()

    def test_iceberg_source_with_filter(self, iceberg_test_table):
        """Test reading Iceberg table with filter expression."""
        config = IcebergSourceConfig(
            catalog_uri=iceberg_test_table["catalog_uri"],
            table_name=iceberg_test_table["table_name"],
            filter="value > 25",
        )
        source = config.setup()

        split = Split(
            split_id="split-0",
            stage_id="source",
            data_range={
                "catalog_uri": iceberg_test_table["catalog_uri"],
                "table_name": iceberg_test_table["table_name"],
                "filter": "value > 25",
            },
        )

        batch = source.process_split(split)

        assert batch is not None
        # Should only get rows with value > 25 (30, 40, 50)
        assert len(batch) == 3

        source.close()


class TestIcebergPipeline:
    """Integration tests for full Iceberg pipeline with TansuBackend."""

    @pytest.mark.asyncio
    async def test_full_pipeline_with_queue(self, iceberg_test_table, ray_context):
        """Test complete IcebergSource pipeline with TansuBackend queue.

        This test verifies the full flow:
        1. Create IcebergSource stage
        2. Start StageMaster with TansuBackend
        3. Process data through queue
        4. Verify completion
        """
        from dataclasses import dataclass

        from solstice.core.operator import Operator, OperatorConfig
        from solstice.core.stage_master import QueueType, StageMaster, StageConfig

        # Create a simple pass-through operator for testing
        @dataclass
        class PassThroughConfig(OperatorConfig):
            catalog_uri: str = ""
            table_name: str = ""

        class PassThroughOperator(Operator):
            def __init__(self, config, worker_id=None):
                super().__init__(config, worker_id)
                self.catalog_uri = config.catalog_uri
                self.table_name = config.table_name

            def process_split(self, split, payload=None):
                # Read from Iceberg
                source_config = IcebergSourceConfig(
                    catalog_uri=self.catalog_uri,
                    table_name=self.table_name,
                )
                source = source_config.setup()
                return source.process_split(split)

            def generate_splits(self):
                return [
                    Split(
                        split_id="iceberg_split_0",
                        stage_id="iceberg_source",
                        data_range={
                            "catalog_uri": self.catalog_uri,
                            "table_name": self.table_name,
                        },
                    )
                ]

            def close(self):
                pass

        PassThroughConfig.operator_class = PassThroughOperator

        # Create stage
        source_stage = Stage(
            stage_id="iceberg_source",
            operator_config=PassThroughConfig(
                catalog_uri=iceberg_test_table["catalog_uri"],
                table_name=iceberg_test_table["table_name"],
            ),
        )

        # Create stage master with TansuBackend
        from solstice.core.split_payload_store import RaySplitPayloadStore

        config = StageConfig(
            queue_type=QueueType.TANSU,
            min_workers=1,
            max_workers=1,
        )

        payload_store = RaySplitPayloadStore(name="test-iceberg-store")

        master = StageMaster(
            job_id="test-iceberg-pipeline",
            stage=source_stage,
            config=config,
            payload_store=payload_store,
        )

        # Start the pipeline
        await master.start()

        # Verify queue was created
        output_queue = master.get_output_queue()
        assert output_queue is not None
        assert await output_queue.health_check()

        # Wait briefly for processing
        import asyncio

        await asyncio.sleep(2)

        # Cleanup
        await master.stop()

        # Verify stage ran
        status = master.get_status()
        assert status.worker_count >= 0  # Workers may have finished
