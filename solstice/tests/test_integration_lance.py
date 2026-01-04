# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Integration tests for LanceTableSource using testcontainers.

Tests the full pipeline flow:
1. Create Lance dataset (local or S3)
2. Run LanceSourceMaster through full pipeline with TansuBackend queue
3. Verify data is processed correctly
"""

from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from lance.dataset import write_dataset

from solstice.core.models import Split
from solstice.core.stage import Stage
from solstice.operators.sources import LanceTableSourceConfig
from solstice.operators.sources.lance import LanceSourceMaster

pytestmark = pytest.mark.integration


def build_lance_splits(
    dataset_uri: str, *, split_size: int, storage_options: dict = None
) -> list[Split]:
    """Build splits from a Lance dataset."""
    dataset = lance.dataset(dataset_uri, storage_options=storage_options)
    splits: list[Split] = []
    for fragment in sorted(dataset.get_fragments(), key=lambda frag: frag.fragment_id):
        row_count = fragment.count_rows()
        for offset in range(0, row_count, split_size):
            splits.append(
                Split(
                    split_id=f"fragment_{fragment.fragment_id}_{offset}",
                    stage_id="source",
                    data_range={
                        "fragment_id": fragment.fragment_id,
                        "offset": offset,
                        "limit": split_size,
                    },
                )
            )
    return splits


@pytest.fixture
def lance_dataset_local():
    """Create a local Lance dataset for basic tests."""
    tmpdir = tempfile.mkdtemp()
    table_path = Path(tmpdir) / "table.lance"

    data = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        }
    )
    write_dataset(data, str(table_path))

    try:
        yield str(table_path)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ============================================================================
# Basic Lance Source Tests (local filesystem)
# ============================================================================


class TestLanceSourceLocal:
    """Basic tests for LanceTableSource using local filesystem."""

    def test_lance_source_reads_fragments(self, lance_dataset_local):
        """Test reading Lance dataset fragments."""
        config = LanceTableSourceConfig(dataset_uri=lance_dataset_local, split_size=2)
        source = config.setup()
        splits = build_lance_splits(lance_dataset_local, split_size=2)

        batches = []
        for split in splits:
            batch = source.process_split(split)
            if batch:
                batches.append(batch)

        assert sum(len(batch) for batch in batches) == 5
        column_names = set(batches[0].column_names)
        assert {"id", "value", "name"}.issubset(column_names)

        source.close()

    def test_lance_source_respects_column_selection(self, lance_dataset_local):
        """Test reading specific columns from Lance dataset."""
        config = LanceTableSourceConfig(
            dataset_uri=lance_dataset_local, split_size=10, columns=["id", "name"]
        )
        source = config.setup()
        splits = build_lance_splits(lance_dataset_local, split_size=10)
        for split in splits:
            split.data_range["columns"] = ["id", "name"]

        batches = [source.process_split(split) for split in splits]
        batches = [batch for batch in batches if batch]
        assert len(batches) == 1
        column_names = set(batches[0].column_names)
        assert {"id", "name"}.issubset(column_names)

        source.close()


# ============================================================================
# S3 Integration Tests (requires testcontainers MinIO)
# ============================================================================


class TestLanceSourceS3:
    """Integration tests for LanceTableSource with S3 storage via testcontainers."""

    def test_lance_source_reads_s3(self, minio_endpoint, minio_credentials, s3_storage_options):
        """Test reading Lance dataset from S3."""
        unique_id = str(uuid.uuid4())[:8]
        s3_path = f"s3://warehouse/lance_read_test_{unique_id}"

        # Create test data in S3
        data = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
                "name": ["A", "B", "C", "D", "E"],
            }
        )
        write_dataset(data, s3_path, storage_options=s3_storage_options)

        # Set environment for S3 access
        os.environ["AWS_ACCESS_KEY_ID"] = minio_credentials["access_key"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = minio_credentials["secret_key"]
        os.environ["AWS_ENDPOINT"] = minio_endpoint
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ALLOW_HTTP"] = "true"

        config = LanceTableSourceConfig(dataset_uri=s3_path, split_size=5)
        source = config.setup()

        splits = build_lance_splits(s3_path, split_size=5, storage_options=s3_storage_options)

        total_rows = 0
        for split in splits:
            batch = source.process_split(split)
            if batch:
                total_rows += len(batch)

        assert total_rows == 5
        source.close()


# ============================================================================
# Full Pipeline Tests (requires tansu)
# ============================================================================


class TestLancePipeline:
    """Integration tests for full Lance pipeline with TansuBackend."""

    @pytest.mark.asyncio
    async def test_full_pipeline_with_queue(self, lance_dataset_local, ray_cluster):
        """Test complete LanceSource pipeline with TansuBackend queue.

        This test verifies the full flow:
        1. LanceSourceMaster starts and creates source queue
        2. Splits are written to source queue
        3. Workers consume splits and produce to output queue
        4. All data is processed through the pipeline
        """
        source_stage = Stage(
            stage_id="lance_source",
            operator_config=LanceTableSourceConfig(
                dataset_uri=lance_dataset_local,
                split_size=2,
            ),
        )

        from solstice.core.split_payload_store import RaySplitPayloadStore

        payload_store = RaySplitPayloadStore(name="test-lance-pipeline_store")
        master = LanceSourceMaster(
            job_id="test-lance-pipeline",
            stage=source_stage,
            payload_store=payload_store,
        )

        # Start the full pipeline (creates queues, spawns workers)
        await master.start()

        # Verify source queue was created and splits were produced
        source_queue = master.get_source_client()
        assert source_queue is not None
        assert await source_queue.health_check()

        # Check splits were produced to source queue
        status = await master.get_status_async()
        splits_produced = status.metrics.get("splits_produced", 0)
        assert splits_produced > 0
        print(f"Produced {splits_produced} splits to source queue")

        # Verify output queue was created
        output_queue = master.get_output_queue()
        assert output_queue is not None

        # Wait for workers to process (with timeout)
        import asyncio

        max_wait = 30  # seconds
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < max_wait:
            status = await master.get_status_async()
            if status.is_finished:
                break
            await asyncio.sleep(0.5)

        # Cleanup
        await master.stop()

        # Verify processing completed
        assert splits_produced > 0
        print(f"Pipeline completed: {splits_produced} splits processed")

    @pytest.mark.asyncio
    async def test_pipeline_with_s3_dataset(
        self, minio_endpoint, minio_credentials, s3_storage_options, ray_cluster
    ):
        """Test Lance pipeline with S3 dataset using testcontainers MinIO."""
        unique_id = str(uuid.uuid4())[:8]
        s3_path = f"s3://warehouse/lance_pipeline_test_{unique_id}"

        # Create test data in S3
        data = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
                "name": ["A", "B", "C", "D", "E"],
            }
        )
        write_dataset(data, s3_path, storage_options=s3_storage_options)

        # Set environment for S3 access
        os.environ["AWS_ACCESS_KEY_ID"] = minio_credentials["access_key"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = minio_credentials["secret_key"]
        os.environ["AWS_ENDPOINT"] = minio_endpoint
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ALLOW_HTTP"] = "true"

        source_stage = Stage(
            stage_id="lance_source",
            operator_config=LanceTableSourceConfig(
                dataset_uri=s3_path,
                split_size=5,
            ),
        )

        from solstice.core.split_payload_store import RaySplitPayloadStore

        payload_store = RaySplitPayloadStore(name="test-lance-s3-pipeline_store")
        master = LanceSourceMaster(
            job_id="test-lance-s3-pipeline",
            stage=source_stage,
            payload_store=payload_store,
        )

        await master.start()

        # Verify splits were produced
        status = await master.get_status_async()
        splits_produced = status.metrics.get("splits_produced", 0)
        assert splits_produced > 0

        # Cleanup
        await master.stop()

        print(f"S3 Pipeline completed: {splits_produced} splits")
