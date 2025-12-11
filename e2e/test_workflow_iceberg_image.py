"""E2E tests for Workflow 2: Iceberg Image Processing.

Pipeline:
    Iceberg Table (image data)
    → SparkV2Source (read via Spark)
    → ImageLoadOperator (load images from binary)
    → ImageResizeOperator (resize)
    → ImageFilterOperator (quality filter)
    → JsonFileSink (write metadata)
"""

from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from e2e.utils.aether_client import AetherClient
from e2e.utils.test_data import TestDataManager

pytestmark = [pytest.mark.e2e, pytest.mark.nightly, pytest.mark.slow]


def create_image_workflow_entrypoint(output_location: str, limit: int = 100) -> str:
    """Create the Python entrypoint for image workflow.
    
    Args:
        output_location: S3 location for output
        limit: Maximum number of images to process
        
    Returns:
        Python script as string
    """
    return f'''
import os
import json
import ray

# Initialize Ray
ray.init()

# Import solstice components
from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators import (
    ImageResizeOperatorConfig,
    ImageFilterOperatorConfig,
    ImageMetadataOperatorConfig,
    FileSinkConfig,
)
from solstice.operators.sources.sparkv2 import SparkV2SourceConfig

# Storage options for S3
storage_options = {{
    "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    "aws_endpoint": os.environ.get("AWS_ENDPOINT_URL"),
    "aws_region": os.environ.get("AWS_DEFAULT_REGION", ""),
}}

# Create workflow
job = Job(name="image-process-e2e-test")

# Source stage - read from Iceberg via Spark
source_stage = Stage(
    name="source",
    config=SparkV2SourceConfig(
        catalog_name="nurion_catalog",
        namespace="nurion_test",
        table_name="images_iceberg",
        batch_size=100,
        spark_config={{
            "spark.sql.catalog.nurion_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nurion_catalog.type": "rest",
            "spark.sql.catalog.nurion_catalog.uri": "http://iceberg-rest:8181",
        }},
    ),
)

# Resize stage - resize images to 512x512
resize_stage = Stage(
    name="resize",
    config=ImageResizeOperatorConfig(
        max_dimension=512,
        output_format="JPEG",
        quality=85,
    ),
)

# Filter stage - remove blurry/dark images
filter_stage = Stage(
    name="filter",
    config=ImageFilterOperatorConfig(
        min_blur_score=50.0,
        min_brightness=30.0,
        max_brightness=220.0,
        min_width=100,
        min_height=100,
        add_quality_metrics=True,
    ),
)

# Metadata stage - extract and compute metadata
metadata_stage = Stage(
    name="metadata",
    config=ImageMetadataOperatorConfig(
        extract_exif=True,
        compute_hash=True,
        compute_quality_metrics=True,
    ),
)

# Sink stage - write metadata to JSON
sink_stage = Stage(
    name="sink",
    config=FileSinkConfig(
        output_path="{output_location}",
        format="json",
        partition_by=["format"],
    ),
)

# Build pipeline
job.add_stage(source_stage)
job.add_stage(resize_stage, depends_on=["source"])
job.add_stage(filter_stage, depends_on=["resize"])
job.add_stage(metadata_stage, depends_on=["filter"])
job.add_stage(sink_stage, depends_on=["metadata"])

# Run with limit for testing
result = job.run(max_records={limit})
print(f"Job completed: {{result}}")
'''


def create_simple_image_workflow_entrypoint(output_location: str, limit: int = 100) -> str:
    """Create a simpler image workflow for testing (without Spark).
    
    Args:
        output_location: S3 location for output
        limit: Maximum number of images to process
        
    Returns:
        Python script as string
    """
    return f'''
import os
import json
import ray

# Initialize Ray
ray.init()

# Import solstice components
from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators import (
    LanceTableSourceConfig,
    ImageResizeOperatorConfig,
    ImageFilterOperatorConfig,
    ImageMetadataOperatorConfig,
    FileSinkConfig,
)

# Storage options for S3
storage_options = {{
    "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    "aws_endpoint": os.environ.get("AWS_ENDPOINT_URL"),
    "aws_region": os.environ.get("AWS_DEFAULT_REGION", ""),
}}

# Create workflow
job = Job(name="image-process-simple-e2e-test")

# Source stage - read from Lance (simpler than Spark)
source_stage = Stage(
    name="source",
    config=LanceTableSourceConfig(
        table_uri="s3://nurion/lance/images_lance",
        batch_size=100,
        storage_options=storage_options,
    ),
)

# Resize stage
resize_stage = Stage(
    name="resize",
    config=ImageResizeOperatorConfig(
        max_dimension=512,
        output_format="JPEG",
        quality=85,
    ),
)

# Filter stage
filter_stage = Stage(
    name="filter",
    config=ImageFilterOperatorConfig(
        min_blur_score=50.0,
        min_brightness=30.0,
        add_quality_metrics=True,
    ),
)

# Metadata stage
metadata_stage = Stage(
    name="metadata",
    config=ImageMetadataOperatorConfig(
        extract_exif=True,
        compute_hash=True,
    ),
)

# Sink stage - write metadata to JSON
sink_stage = Stage(
    name="sink",
    config=FileSinkConfig(
        output_path="{output_location}",
        format="json",
    ),
)

# Build pipeline
job.add_stage(source_stage)
job.add_stage(resize_stage, depends_on=["source"])
job.add_stage(filter_stage, depends_on=["resize"])
job.add_stage(metadata_stage, depends_on=["filter"])
job.add_stage(sink_stage, depends_on=["metadata"])

# Run with limit
result = job.run(max_records={limit})
print(f"Job completed: {{result}}")
'''


class TestImageWorkflowSubmission:
    """Tests for image workflow submission."""
    
    def test_submit_simple_image_workflow(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        output_location: str,
        track_ray_job,
    ):
        """Test submitting simple image processing workflow."""
        entrypoint = create_simple_image_workflow_entrypoint(
            output_location=output_location,
            limit=50,  # Process only 50 images for fast testing
        )
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="image-process-simple-e2e",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": [
                    "solstice",
                    "pyarrow",
                    "lance",
                    "Pillow",
                    "scipy",
                ],
                "env_vars": {
                    "PIP_INDEX_URL": "https://mirrors.aliyun.com/pypi/simple/",
                },
            },
            metadata={
                "test": "e2e",
                "workflow": "image-process-simple",
            },
        )
        
        assert result is not None
        assert "job_id" in result
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Wait for job to complete
        status = aether_client.wait_for_rayjob(
            job_id=job_id,
            timeout=1800,  # 30 minutes
            interval=15,
        )
        
        assert status.is_success, f"Job failed: {status.message}"
    
    @pytest.mark.timeout(3600)  # 1 hour timeout
    def test_submit_spark_image_workflow(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        output_location: str,
        track_ray_job,
    ):
        """Test submitting Spark-based image processing workflow."""
        entrypoint = create_image_workflow_entrypoint(
            output_location=output_location,
            limit=100,
        )
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="image-process-spark-e2e",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": [
                    "solstice",
                    "pyarrow",
                    "pyspark",
                    "pyiceberg",
                    "Pillow",
                    "scipy",
                ],
                "env_vars": {
                    "PIP_INDEX_URL": "https://mirrors.aliyun.com/pypi/simple/",
                    "SPARK_HOME": "/opt/spark",
                },
            },
            metadata={
                "test": "e2e",
                "workflow": "image-process-spark",
            },
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Wait for completion
        status = aether_client.wait_for_rayjob(job_id, timeout=3600)
        assert status.is_success, f"Job failed: {status.message}"
    
    @pytest.mark.timeout(3600)
    def test_full_image_workflow_with_verification(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        test_data_manager: TestDataManager,
        output_location: str,
        track_ray_job,
        debug_collector,
    ):
        """Test full image workflow with output verification."""
        entrypoint = create_simple_image_workflow_entrypoint(
            output_location=output_location,
            limit=200,
        )
        
        # Submit job
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="image-full-e2e",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": ["solstice", "pyarrow", "lance", "Pillow", "scipy"],
                "env_vars": {
                    "PIP_INDEX_URL": "https://mirrors.aliyun.com/pypi/simple/",
                },
            },
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Wait for completion
        status = aether_client.wait_for_rayjob(job_id, timeout=3600)
        
        if not status.is_success:
            # Collect debug info on failure
            debug_collector.collect_all(job_ids=[job_id])
            pytest.fail(f"Job failed: {status.message}")
        
        # Verify JSON output exists
        # Check S3 for output files
        s3 = test_data_manager.s3_client
        bucket = "nurion"
        prefix = output_location.replace("s3://nurion/", "")
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
        objects = response.get("Contents", [])
        
        assert len(objects) > 0, "No output files found"
        
        # Verify at least one JSON file has expected content
        for obj in objects:
            if obj["Key"].endswith(".json"):
                response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                content = response["Body"].read().decode("utf-8")
                data = json.loads(content)
                
                # Check expected fields
                assert "sha256" in data or "metadata" in data
                break


class TestImageOperatorIntegration:
    """Tests for individual image operator integration."""
    
    def test_image_resize_operator(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        track_ray_job,
    ):
        """Test ImageResizeOperator in isolation."""
        entrypoint = '''
import io
from PIL import Image
from solstice.operators.image import ImageResizeOperator, ImageResizeOperatorConfig

# Create a test image
img = Image.new("RGB", (1024, 1024), color="red")
buffer = io.BytesIO()
img.save(buffer, format="JPEG")
image_bytes = buffer.getvalue()

# Create operator
config = ImageResizeOperatorConfig(max_dimension=256, output_format="JPEG")
operator = ImageResizeOperator(config, worker_id="test")

# Process
from solstice.core.models import Split, SplitPayload
import pyarrow as pa

batch = SplitPayload.from_arrow(
    pa.Table.from_pylist([{"id": "test", "image": image_bytes}]),
    split_id="test",
)
split = Split(split_id="test", stage_id="resize", data_range={})

result = operator.process_split(split, batch)
assert result is not None
print(f"Resized image size: {result.to_pylist()[0]['size_bytes']}")
'''
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="image-resize-test",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": ["solstice", "Pillow", "pyarrow"],
                "env_vars": {"PIP_INDEX_URL": "https://mirrors.aliyun.com/pypi/simple/"},
            },
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        status = aether_client.wait_for_rayjob(job_id, timeout=300)
        assert status.is_success, f"Resize operator test failed: {status.message}"
    
    def test_image_filter_operator(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        track_ray_job,
    ):
        """Test ImageFilterOperator in isolation."""
        entrypoint = '''
import io
from PIL import Image
from solstice.operators.image import ImageFilterOperator, ImageFilterOperatorConfig

# Create test images - one sharp, one blurry (simulated)
sharp_img = Image.new("RGB", (512, 512), color="white")
buffer = io.BytesIO()
sharp_img.save(buffer, format="JPEG", quality=95)
sharp_bytes = buffer.getvalue()

# Create operator with filter
config = ImageFilterOperatorConfig(
    min_width=100,
    min_height=100,
    add_quality_metrics=True,
)
operator = ImageFilterOperator(config, worker_id="test")

# Process
from solstice.core.models import Split, SplitPayload
import pyarrow as pa

batch = SplitPayload.from_arrow(
    pa.Table.from_pylist([
        {"id": "sharp", "image": sharp_bytes},
    ]),
    split_id="test",
)
split = Split(split_id="test", stage_id="filter", data_range={})

result = operator.process_split(split, batch)
assert result is not None
print(f"Filtered result count: {len(result)}")
'''
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="image-filter-test",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": ["solstice", "Pillow", "pyarrow", "scipy", "numpy"],
                "env_vars": {"PIP_INDEX_URL": "https://mirrors.aliyun.com/pypi/simple/"},
            },
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        status = aether_client.wait_for_rayjob(job_id, timeout=300)
        assert status.is_success, f"Filter operator test failed: {status.message}"


class TestImageWorkflowScaling:
    """Tests for workflow scaling with larger datasets."""
    
    @pytest.mark.slow
    @pytest.mark.timeout(7200)  # 2 hour timeout
    def test_large_scale_image_processing(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        test_data_manager: TestDataManager,
        output_location: str,
        track_ray_job,
    ):
        """Test processing larger number of images."""
        entrypoint = create_simple_image_workflow_entrypoint(
            output_location=output_location,
            limit=1000,  # Process 1000 images
        )
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="image-scale-e2e",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": ["solstice", "pyarrow", "lance", "Pillow", "scipy"],
                "env_vars": {
                    "PIP_INDEX_URL": "https://mirrors.aliyun.com/pypi/simple/",
                },
            },
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        status = aether_client.wait_for_rayjob(job_id, timeout=7200)
        assert status.is_success, f"Large scale job failed: {status.message}"
