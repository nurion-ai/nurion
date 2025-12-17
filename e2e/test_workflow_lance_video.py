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

"""E2E tests for Workflow 1: Lance Video Processing.

Pipeline:
    Lance Table (video metadata)
    → LanceTableSource (read)
    → VideoSliceOperator (extract frames)
    → VideoProcessOperator (inference/transform)
    → LanceSink (write results)
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict

import pytest

from utils.aether_client import AetherClient
from utils.test_data import TestDataManager

pytestmark = [pytest.mark.e2e, pytest.mark.nightly, pytest.mark.slow]


# Workflow configuration
VIDEO_WORKFLOW_CONFIG = {
    "name": "video-slice-workflow",
    "description": "Process videos from Lance, slice scenes, write to Lance",
    "stages": [
        {
            "name": "source",
            "operator": "LanceTableSource",
            "config": {
                "table_uri": "s3://nurion/lance/videos_lance",
                "batch_size": 10,
            },
        },
        {
            "name": "scene-detect",
            "operator": "FFmpegSceneDetectOperator",
            "config": {
                "scene_threshold": 0.4,
                "min_scene_duration": 1.0,
            },
        },
        {
            "name": "slice",
            "operator": "FFmpegSliceOperator",
            "config": {
                "min_scene_duration": 1.0,
            },
        },
        {
            "name": "sink",
            "operator": "LanceSink",
            "config": {
                "table_uri": "s3://nurion/test_outputs/video_slices",
                "mode": "overwrite",
            },
        },
    ],
}


def create_video_workflow_entrypoint(output_location: str, limit: int = 100) -> str:
    """Create the Python entrypoint for video workflow.
    
    Args:
        output_location: S3 location for output
        limit: Maximum number of videos to process
        
    Returns:
        Python script as string
    """
    return f'''
import os
import ray

# Initialize Ray
ray.init()

# Import solstice components
from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators import (
    LanceTableSourceConfig,
    FFmpegSceneDetectConfig,
    FFmpegSliceConfig,
    LanceSinkConfig,
)

# Storage options for S3
storage_options = {{
    "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    "aws_endpoint": os.environ.get("AWS_ENDPOINT_URL"),
    "aws_region": os.environ.get("AWS_DEFAULT_REGION", ""),
}}

# Create workflow
job = Job(name="video-slice-e2e-test")

# Source stage - read from Lance
source_stage = Stage(
    name="source",
    config=LanceTableSourceConfig(
        table_uri="s3://nurion/lance/videos_lance",
        batch_size=10,
        storage_options=storage_options,
    ),
)

# Scene detection stage
scene_detect_stage = Stage(
    name="scene-detect",
    config=FFmpegSceneDetectConfig(
        scene_threshold=0.4,
        min_scene_duration=1.0,
    ),
)

# Slice stage
slice_stage = Stage(
    name="slice",
    config=FFmpegSliceConfig(
        min_scene_duration=1.0,
    ),
)

# Sink stage - write to Lance
sink_stage = Stage(
    name="sink",
    config=LanceSinkConfig(
        table_uri="{output_location}",
        mode="overwrite",
        storage_options=storage_options,
    ),
)

# Build pipeline
job.add_stage(source_stage)
job.add_stage(scene_detect_stage, depends_on=["source"])
job.add_stage(slice_stage, depends_on=["scene-detect"])
job.add_stage(sink_stage, depends_on=["slice"])

# Run with limit for testing
result = job.run(max_records={limit})
print(f"Job completed: {{result}}")
'''


class TestVideoWorkflowSubmission:
    """Tests for video workflow submission."""
    
    def test_submit_video_workflow(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        output_location: str,
        track_ray_job,
    ):
        """Test submitting video processing workflow via Aether API."""
        entrypoint = create_video_workflow_entrypoint(
            output_location=output_location,
            limit=10,  # Process only 10 videos for fast testing
        )
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="video-slice-e2e-test",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": [
                    "solstice",
                    "pyarrow",
                    "lance",
                ],
                "env_vars": {},
            },
            metadata={
                "test": "e2e",
                "workflow": "video-slice",
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
    def test_full_video_workflow(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        test_data_manager: TestDataManager,
        output_location: str,
        track_ray_job,
    ):
        """Test full video workflow with verification."""
        entrypoint = create_video_workflow_entrypoint(
            output_location=output_location,
            limit=50,  # Process 50 videos
        )
        
        # Submit job
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="video-slice-full-test",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={
                "pip": ["solstice", "pyarrow", "lance"],
                "env_vars": {},
            },
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Wait for completion
        status = aether_client.wait_for_rayjob(job_id, timeout=3600)
        assert status.is_success, f"Job failed: {status.message}"
        
        # Verify output
        is_valid = test_data_manager.verify_output_table(
            location=output_location,
            expected_columns=["video_path", "slice_binary", "scene_index"],
            min_records=10,  # At least 10 slices expected
        )
        assert is_valid, "Output validation failed"


class TestVideoWorkflowMonitoring:
    """Tests for workflow monitoring."""
    
    def test_get_job_status(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        track_ray_job,
    ):
        """Test getting job status during execution."""
        # Submit a quick job
        entrypoint = "import time; time.sleep(30); print('done')"
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="status-test",
            entrypoint=f"python -c \"{entrypoint}\"",
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Check status immediately
        status = aether_client.get_rayjob_status(job_id)
        assert status.job_id == job_id
        assert status.status in ("PENDING", "RUNNING", "SUCCEEDED")
        
        # Wait for completion
        final_status = aether_client.wait_for_rayjob(job_id, timeout=120)
        assert final_status.is_success
    
    def test_get_job_logs(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        track_ray_job,
    ):
        """Test getting job logs."""
        entrypoint = "print('Hello from E2E test')"
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="logs-test",
            entrypoint=f"python -c \"{entrypoint}\"",
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Wait for completion
        aether_client.wait_for_rayjob(job_id, timeout=120)
        
        # Get logs
        logs = aether_client.get_rayjob_logs(job_id)
        assert "Hello from E2E test" in logs


class TestVideoWorkflowErrorHandling:
    """Tests for workflow error handling."""
    
    def test_invalid_source_table(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        track_ray_job,
    ):
        """Test workflow fails gracefully with invalid source."""
        entrypoint = '''
import lance
# This should fail - table doesn't exist
dataset = lance.dataset("s3://nurion/nonexistent/table")
'''
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="error-test",
            entrypoint=f"python -c '{entrypoint}'",
            runtime_env={"pip": ["lance"]},
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Job should fail
        status = aether_client.wait_for_rayjob(job_id, timeout=300)
        assert not status.is_success
        assert status.status == "FAILED"
    
    def test_stop_running_job(
        self,
        aether_client: AetherClient,
        k8s_cluster_id: int,
        track_ray_job,
    ):
        """Test stopping a running job."""
        # Submit a long-running job
        entrypoint = "import time; time.sleep(600)"
        
        result = aether_client.submit_rayjob(
            cluster_id=k8s_cluster_id,
            name="stop-test",
            entrypoint=f"python -c \"{entrypoint}\"",
        )
        
        job_id = result["job_id"]
        track_ray_job(job_id)
        
        # Wait for it to start running
        time.sleep(10)
        
        # Stop the job
        stop_result = aether_client.stop_rayjob(job_id)
        assert stop_result is not None
        
        # Verify it stopped
        time.sleep(5)
        status = aether_client.get_rayjob_status(job_id)
        assert status.status in ("STOPPED", "FAILED")
