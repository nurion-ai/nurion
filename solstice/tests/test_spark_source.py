"""Tests for SparkSource operator using raydp."""

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest
import pyarrow as pa
import ray
from ray.job_config import JobConfig

from solstice.core.job import Job
from solstice.core.models import Split
from solstice.core.stage import Stage
from solstice.operators.filter import FilterOperatorConfig
from solstice.operators.map import MapOperatorConfig
from solstice.operators.sources.spark import (
    SparkSourceConfig,
    SparkSourceStageMaster,
    SparkSourceStageMasterConfig,
)
from solstice.runtime.local_runner import LocalJobRunner
from solstice.state.store import LocalCheckpointStore


# Test data path
TESTDATA_DIR = Path(__file__).parent / "testdata" / "resources" / "spark"
TEST_DATA_1000 = TESTDATA_DIR / "test_data_1000.parquet"
TEST_DATA_100 = TESTDATA_DIR / "test_data_100.parquet"


@pytest.fixture
def local_checkpoint_store(tmp_path):
    """Create a local checkpoint store for testing."""
    return LocalCheckpointStore(str(tmp_path / "checkpoints"))


@pytest.fixture
def ray_local():
    """Initialize Ray for simple tests with minimal configuration."""
    if ray.is_initialized():
        ray.shutdown()
    # Initialize Ray with minimal config to avoid CI issues
    # Setting runtime_env with working_dir=None prevents automatic code upload
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        ignore_reinit_error=True,
    )
    yield
    ray.shutdown()


def make_job(checkpoint_store, stages: List[Stage]) -> Job:
    """Create a job with the given stages."""
    job = Job(job_id="spark-source-test", checkpoint_store=checkpoint_store)
    for i, stage in enumerate(stages):
        upstream = [stages[i - 1].stage_id] if i > 0 else None
        job.add_stage(stage, upstream_stages=upstream)
    return job


class TestSparkSourceOperator:
    """Tests for SparkSource operator reading from ObjectRefs."""

    def test_spark_source_read_arrow_table(self, ray_local):
        """Test reading Arrow table from object store."""
        # Create test data and put in object store
        test_data = pa.Table.from_pylist(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
                {"id": 3, "name": "Charlie", "age": 35},
            ]
        )

        object_ref = ray.put(test_data)

        # Create source and read
        config = SparkSourceConfig()
        source = config.setup()

        split = Split(
            split_id="test_split_0",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 3,
            },
        )

        payload = source.read(split)

        assert payload is not None
        assert len(payload) == 3
        assert "id" in payload.column_names
        assert "name" in payload.column_names
        assert "age" in payload.column_names

        records = payload.to_pylist()
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
        assert records[2]["name"] == "Charlie"

    def test_spark_source_read_record_batch(self, ray_local):
        """Test reading Arrow RecordBatch from object store."""
        # Create test data as RecordBatch
        test_batch = pa.RecordBatch.from_pydict(
            {
                "value": [1, 2, 3, 4, 5],
                "label": ["a", "b", "c", "d", "e"],
            }
        )

        object_ref = ray.put(test_batch)

        config = SparkSourceConfig()
        source = config.setup()

        split = Split(
            split_id="test_split_batch",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 5,
            },
        )

        payload = source.read(split)

        assert payload is not None
        assert len(payload) == 5
        assert "value" in payload.column_names
        assert "label" in payload.column_names

    def test_spark_source_empty_table(self, ray_local):
        """Test reading empty Arrow table returns None."""
        empty_table = pa.Table.from_pylist([])

        object_ref = ray.put(empty_table)

        config = SparkSourceConfig()
        source = config.setup()

        split = Split(
            split_id="test_split_empty",
            stage_id="spark_source",
            data_range={
                "object_ref": object_ref,
                "block_size": 0,
            },
        )

        payload = source.read(split)
        assert payload is None

    def test_spark_source_missing_object_ref(self):
        """Test error when object_ref is missing."""
        config = SparkSourceConfig()
        source = config.setup()

        split = Split(
            split_id="test_split_no_ref",
            stage_id="spark_source",
            data_range={},  # Missing object_ref
        )

        with pytest.raises(ValueError, match="missing 'object_ref'"):
            source.read(split)


class TestSparkSourcePipeline:
    """Test SparkSource in a pipeline with pre-created ObjectRefs."""

    def test_spark_source_to_filter_pipeline(self, ray_local, local_checkpoint_store):
        """Test reading from ObjectRefs and filtering through pipeline."""
        # Create test data
        test_data = pa.Table.from_pylist(
            [
                {"id": i, "department": "engineering" if i % 3 == 0 else "sales", "score": i * 10}
                for i in range(100)
            ]
        )
        object_ref = ray.put(test_data)

        # Define stages
        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
        )

        filter_stage = Stage(
            stage_id="filter_engineering",
            operator_config=FilterOperatorConfig(
                filter_fn=lambda row: row.get("department") == "engineering",
            ),
        )

        job = make_job(local_checkpoint_store, [source_stage, filter_stage])

        splits = [
            Split(
                split_id="spark_split_0",
                stage_id="spark_source",
                data_range={
                    "object_ref": object_ref,
                    "block_size": 100,
                },
            )
        ]

        runner = LocalJobRunner(job)
        results = runner.run(source_splits={"spark_source": splits})

        assert "filter_engineering" in results
        filtered_batches = results["filter_engineering"]

        total_engineering = 0
        for batch in filtered_batches:
            for record in batch.to_pylist():
                assert record["department"] == "engineering"
                total_engineering += 1

        # Every 3rd record (id % 3 == 0) should be engineering
        expected_count = len([i for i in range(100) if i % 3 == 0])
        assert total_engineering == expected_count

    def test_spark_source_to_map_pipeline(self, ray_local, local_checkpoint_store):
        """Test reading from ObjectRefs and transforming."""
        test_data = pa.Table.from_pylist([{"id": i, "value": i * 2} for i in range(50)])
        object_ref = ray.put(test_data)

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
        )

        map_stage = Stage(
            stage_id="double_value",
            operator_config=MapOperatorConfig(
                map_fn=lambda row: {
                    **row,
                    "doubled": row["value"] * 2,
                },
            ),
        )

        job = make_job(local_checkpoint_store, [source_stage, map_stage])

        splits = [
            Split(
                split_id="spark_split_0",
                stage_id="spark_source",
                data_range={
                    "object_ref": object_ref,
                    "block_size": 50,
                },
            )
        ]

        runner = LocalJobRunner(job)
        results = runner.run(source_splits={"spark_source": splits})

        assert "double_value" in results
        mapped_batches = results["double_value"]

        total_records = 0
        for batch in mapped_batches:
            for record in batch.to_pylist():
                assert "doubled" in record
                assert record["doubled"] == record["value"] * 2
                total_records += 1

        assert total_records == 50

    def test_multiple_blocks_pipeline(self, ray_local, local_checkpoint_store):
        """Test processing multiple blocks through pipeline."""
        # Create multiple blocks
        blocks = []
        for block_idx in range(5):
            block_data = pa.Table.from_pylist(
                [{"block": block_idx, "id": i, "value": block_idx * 100 + i} for i in range(20)]
            )
            blocks.append(ray.put(block_data))

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
        )

        map_stage = Stage(
            stage_id="add_processed",
            operator_config=MapOperatorConfig(
                map_fn=lambda row: {**row, "processed": True},
            ),
        )

        job = make_job(local_checkpoint_store, [source_stage, map_stage])

        splits = [
            Split(
                split_id=f"spark_split_{idx}",
                stage_id="spark_source",
                data_range={
                    "object_ref": block_ref,
                    "block_size": 20,
                },
            )
            for idx, block_ref in enumerate(blocks)
        ]

        runner = LocalJobRunner(job)
        results = runner.run(source_splits={"spark_source": splits})

        assert "add_processed" in results

        total_records = 0
        for batch in results["add_processed"]:
            for record in batch.to_pylist():
                assert record["processed"] is True
                total_records += 1

        assert total_records == 100  # 5 blocks * 20 records


@pytest.mark.integration
class TestSparkSourceStageMaster:
    """Integration tests for SparkSourceStageMaster using raydp.

    These tests verify that SparkSourceStageMaster correctly:
    1. Initializes Spark via raydp.init_spark() using config parameters
    2. Calls dataframe_fn to load data
    3. Persists data to Ray object store using raydp
    4. Returns splits with ObjectRefs

    Note: These tests require Java 11+ runtime for Spark.
    """

    @pytest.fixture(scope="function")
    def ray_context(self):
        """Initialize Ray with raydp jars for each test.

        Uses function scope to ensure clean state between tests.
        """
        import raydp
        from raydp.utils import code_search_path

        # Make sure Ray is not running
        if ray.is_initialized():
            ray.shutdown()

        # Get raydp jars path
        jars_paths = code_search_path()
        print(f"[DEBUG] raydp JAR paths: {jars_paths}")

        # Initialize Ray with job config for cross-language support
        # Exclude large files and build artifacts from being uploaded
        ray.init(
            job_config=JobConfig(
                code_search_path=jars_paths,
                runtime_env={
                    "excludes": [
                        "java/raydp-main/target/",
                        "java/shims/*/target/",
                        "*.jar",
                        "__pycache__/",
                        ".git/",
                    ],
                },
            ),
            log_to_driver=True,
            logging_level="info",
        )

        yield

        # Cleanup: stop Spark first, then Ray
        try:
            raydp.stop_spark()
        except Exception:
            pass  # Ignore errors if Spark not initialized
        ray.shutdown()

    def test_stage_master_fetch_splits_with_parquet(self, ray_context, local_checkpoint_store):
        """Test SparkSourceStageMaster.fetch_splits() with parquet file.

        Verifies the full StageMaster flow:
        - StageMaster initializes Spark via raydp.init_spark() using config
        - dataframe_fn is called to read parquet
        - Data is persisted to object store via raydp
        - Splits contain valid ObjectRefs
        """
        test_path = str(TEST_DATA_100)

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
            master_config=SparkSourceStageMasterConfig(
                app_name="test-fetch-splits",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
            ),
        )

        # Create StageMaster directly
        master = SparkSourceStageMaster(
            job_id="test-job",
            checkpoint_store=local_checkpoint_store,
            stage=source_stage,
            upstream_stages=[],
        )

        # Fetch splits using the master
        splits = list(master.fetch_splits())

        assert len(splits) > 0
        total_records = sum(s.data_range["block_size"] for s in splits)
        assert total_records == 100

        # Verify splits have correct structure
        for split in splits:
            assert "object_ref" in split.data_range
            assert "block_size" in split.data_range
            assert split.stage_id == "spark_source"

        # Use SparkSource operator to read the splits
        source = SparkSourceConfig().setup()
        all_records = []
        for split in splits:
            payload = source.read(split)
            if payload:
                all_records.extend(payload.to_pylist())

        assert len(all_records) == 100

        # Cleanup
        master.stop()

    def test_stage_master_with_sql_query(self, ray_context, local_checkpoint_store):
        """Test SparkSourceStageMaster with SQL query in dataframe_fn.

        The dataframe_fn can use any Spark operations including SQL.
        This test creates a temp view and queries it within the dataframe_fn.
        """
        test_path = str(TEST_DATA_100)

        def sql_dataframe_fn(spark):
            """Load data, create temp view, then query with SQL."""
            df = spark.read.parquet(test_path)
            df.createOrReplaceTempView("employees")
            return spark.sql("SELECT id, name, department, salary FROM employees WHERE age > 40")

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
            master_config=SparkSourceStageMasterConfig(
                app_name="test-sql-query",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=sql_dataframe_fn,
            ),
        )

        # Create StageMaster - it will initialize Spark internally
        master = SparkSourceStageMaster(
            job_id="test-job",
            checkpoint_store=local_checkpoint_store,
            stage=source_stage,
            upstream_stages=[],
        )

        # Fetch splits - this triggers Spark init via raydp.init_spark()
        splits = list(master.fetch_splits())

        assert len(splits) > 0
        total_records = sum(s.data_range["block_size"] for s in splits)
        print(f"SQL query returned {total_records} records")

        # Read and verify
        source = SparkSourceConfig().setup()
        all_records = []
        for split in splits:
            payload = source.read(split)
            if payload:
                all_records.extend(payload.to_pylist())

        assert len(all_records) == total_records
        assert len(all_records) > 0

        # Cleanup Spark via master.stop() which calls raydp.stop_spark()
        master.stop()

    def test_stage_master_1000_records_full_pipeline(self, ray_context, local_checkpoint_store):
        """Test SparkSourceStageMaster with 1000 records through full pipeline.

        End-to-end test:
        1. StageMaster initializes Spark via config and fetches splits
        2. Pipeline processes splits through filter and map stages
        """
        test_path = str(TEST_DATA_1000)

        # Create stage with config
        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
            master_config=SparkSourceStageMasterConfig(
                app_name="test-1000-records",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
            ),
        )

        # Create StageMaster and fetch splits
        master = SparkSourceStageMaster(
            job_id="test-job",
            checkpoint_store=local_checkpoint_store,
            stage=source_stage,
            upstream_stages=[],
        )

        splits = list(master.fetch_splits())
        total_records = sum(s.data_range["block_size"] for s in splits)
        assert total_records == 1000
        print(f"Fetched {len(splits)} splits with {total_records} total records")

        # Create pipeline stages
        filter_stage = Stage(
            stage_id="filter_high_performers",
            operator_config=FilterOperatorConfig(
                filter_fn=lambda row: (
                    row.get("status") == "active" and row.get("performance_score", 0) >= 4.0
                ),
            ),
        )

        map_stage = Stage(
            stage_id="create_summary",
            operator_config=MapOperatorConfig(
                map_fn=lambda row: {
                    "id": row["id"],
                    "name": row["name"],
                    "department": row["department"],
                    "performance_score": row["performance_score"],
                    "high_performer": True,
                },
            ),
        )

        job = make_job(local_checkpoint_store, [source_stage, filter_stage, map_stage])

        runner = LocalJobRunner(job)
        results = runner.run(source_splits={"spark_source": splits})

        assert "create_summary" in results
        summary_batches = results["create_summary"]

        total_high_performers = 0
        for batch in summary_batches:
            for record in batch.to_pylist():
                assert record["high_performer"] is True
                assert record["performance_score"] >= 4.0
                total_high_performers += 1

        assert total_high_performers > 0
        print(f"Found {total_high_performers} high performers out of 1000 records")

        master.stop()

    def test_stage_master_with_parallelism(self, ray_context, local_checkpoint_store):
        """Test SparkSourceStageMaster with custom parallelism setting.

        The parallelism config controls how many partitions/splits are created.
        """
        test_path = str(TEST_DATA_100)

        # Create stage with parallelism=4
        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
            master_config=SparkSourceStageMasterConfig(
                app_name="test-parallelism",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=lambda spark: spark.read.parquet(test_path),
                parallelism=4,  # Force 4 partitions
            ),
        )

        master = SparkSourceStageMaster(
            job_id="test-job",
            checkpoint_store=local_checkpoint_store,
            stage=source_stage,
            upstream_stages=[],
        )

        splits = list(master.fetch_splits())

        # Should have 4 splits due to parallelism setting
        assert len(splits) == 4

        total_records = sum(s.data_range["block_size"] for s in splits)
        assert total_records == 100

        master.stop()

    def test_stage_master_complex_dataframe_fn(self, ray_context, local_checkpoint_store):
        """Test SparkSourceStageMaster with complex dataframe_fn logic.

        The dataframe_fn can contain arbitrary Spark transformations.
        """
        test_path = str(TEST_DATA_100)

        def complex_load(spark):
            """Complex data loading with transformations."""
            df = spark.read.parquet(test_path)
            # Apply some Spark transformations
            return (
                df.filter(df.age > 30)
                .select("id", "name", "department", "salary", "age")
                .orderBy("salary", ascending=False)
                .limit(50)
            )

        source_stage = Stage(
            stage_id="spark_source",
            operator_config=SparkSourceConfig(),
            master_config=SparkSourceStageMasterConfig(
                app_name="test-complex",
                num_executors=1,
                executor_cores=1,
                executor_memory="512m",
                dataframe_fn=complex_load,
            ),
        )

        master = SparkSourceStageMaster(
            job_id="test-job",
            checkpoint_store=local_checkpoint_store,
            stage=source_stage,
            upstream_stages=[],
        )

        splits = list(master.fetch_splits())
        total_records = sum(s.data_range["block_size"] for s in splits)

        # Should have at most 50 records (limit in dataframe_fn)
        assert total_records <= 50

        # Verify all records have age > 30
        source = SparkSourceConfig().setup()
        for split in splits:
            payload = source.read(split)
            if payload:
                for record in payload.to_pylist():
                    assert record["age"] > 30

        master.stop()
