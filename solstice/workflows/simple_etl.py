"""
Simple ETL workflow example

This demonstrates a basic ETL pipeline with:
1. Source: Read from Lance table
2. Transform: Map and filter operations
3. Sink: Write to output file

All configuration is defined here. CLI parameters can override defaults.
"""

import logging
from typing import Any, Dict

from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators.source import LanceTableSource
from solstice.operators.map import MapOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sink import FileSink, PrintSink
from solstice.state.backend import StateBackend


def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Example transformation function"""
    # Add a processed flag
    record["processed"] = True

    # Example: convert some fields
    if "value" in record:
        record["value_doubled"] = record["value"] * 2

    return record


def filter_predicate(record: Dict[str, Any]) -> bool:
    """Example filter predicate"""
    # Only keep records where value > 10
    return record.get("value", 0) > 10


def create_job(
    job_id: str,
    config: Dict[str, Any],
    state_backend: StateBackend,
) -> Job:
    """
    Create a simple ETL job.

    DAG structure:
        Source -> Map -> Filter -> Sink

    Config parameters:
        - input: Input Lance table path (required)
        - output: Output file path (optional, prints if not provided)
        - source_batch_size: Batch size for source (default: 1000)
        - transform_parallelism: Transform workers, int or (min, max) (default: (2, 8))
        - filter_parallelism: Filter workers (default: 2)
        - output_format: Output format - json/parquet/csv (default: json)
    """
    logger = logging.getLogger(__name__)
    logger.info("Creating Simple ETL job")

    # Extract configuration with defaults
    input_path = config.get("input")
    output_path = config.get("output")

    if not input_path:
        raise ValueError("'input' parameter is required (Lance table path)")

    # Create job
    job = Job(
        job_id=job_id,
        state_backend=state_backend,
        checkpoint_interval_secs=config.get("checkpoint_interval_secs", 300),
        checkpoint_interval_records=config.get("checkpoint_interval_records"),
        config=config,
    )

    # Stage 1: Source - Read from Lance table (fixed 1 worker)
    source_stage = Stage(
        stage_id="source",
        operator_class=LanceTableSource,
        operator_config={
            "table_path": input_path,
            "batch_size": config.get("source_batch_size", 1000),
            "columns": config.get("source_columns"),
        },
        parallelism=1,  # Fixed 1 worker for source
        worker_resources={
            "num_cpus": 1,
            "memory": 2 * 1024**3,
        },
    )

    # Stage 2: Map - Transform records (auto-scale by default)
    transform_parallelism = config.get("transform_parallelism", (2, 8))
    map_stage = Stage(
        stage_id="transform",
        operator_class=MapOperator,
        operator_config={
            "map_fn": transform_record,
            "skip_on_error": True,
        },
        parallelism=transform_parallelism,
        worker_resources={
            "num_cpus": 1,
            "memory": 2 * 1024**3,
        },
    )

    # Stage 3: Filter - Filter records
    filter_parallelism = config.get("filter_parallelism", 2)
    filter_stage = Stage(
        stage_id="filter",
        operator_class=FilterOperator,
        operator_config={
            "filter_fn": filter_predicate,
            "skip_on_error": True,
        },
        parallelism=filter_parallelism,
        worker_resources={
            "num_cpus": 1,
            "memory": 1 * 1024**3,
        },
    )

    # Stage 4: Sink - Write to file or print
    output_format = config.get("output_format", "json")
    if output_path:
        sink_stage = Stage(
            stage_id="sink",
            operator_class=FileSink,
            operator_config={
                "output_path": output_path,
                "format": output_format,
                "buffer_size": config.get("sink_buffer_size", 1000),
            },
            parallelism=1,
            worker_resources={
                "num_cpus": 1,
                "memory": 2 * 1024**3,
            },
        )
    else:
        # Print to stdout if no output path
        sink_stage = Stage(
            stage_id="sink",
            operator_class=PrintSink,
            operator_config={},
            parallelism=1,
            worker_resources={
                "num_cpus": 1,
                "memory": 1 * 1024**3,
            },
        )

    # Build DAG
    job.add_stage(source_stage)
    job.add_stage(map_stage, upstream_stages=["source"])
    job.add_stage(filter_stage, upstream_stages=["transform"])
    job.add_stage(sink_stage, upstream_stages=["filter"])

    logger.info(f"Created ETL job with {len(job.stages)} stages")
    logger.info(f"Configuration: {config}")

    return job


# CLI usage example:
# python -m solstice.main \
#   --workflow workflows.simple_etl \
#   --job-id etl_001 \
#   --input /data/lance_table \
#   --output /data/output.json \
#   --transform-parallelism 4 \
#   --filter-parallelism 2
