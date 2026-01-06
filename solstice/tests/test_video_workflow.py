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

"""Ray-based end-to-end test for the video slice workflow.

Uses public HTTPS URLs for video files (no authentication required).

Local Debug Mode:
    Set VIDEO_CACHE_DIR environment variable to preserve output:

        export VIDEO_CACHE_DIR=~/.cache/solstice_test_videos
        pytest tests/test_video_workflow.py -v -m integration
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
from pathlib import Path

import lance
import pyarrow as pa
import pytest
import requests

logger = logging.getLogger("test")

# Public HTTPS endpoint (no auth required)
PUBLIC_VIDEO_URL = "https://pub-8bc1f1d3d1984bdfb056d0bc0bf97c3d.r2.dev/videos/raw"

# Video files available at the public endpoint
TEST_VIDEOS = [
    "-qwTw3PNXDE.mp4",
    "0wJO0eqVDho.mkv",
    "1UmhvUR_wtQ.mp4",
    "2R-gGLtYmdc.mp4",
    "3EIixA3E-rI.mp4",
    "3ETxXjGlxRo.mp4",
    "3WG6fgdFV74.mp4",
    "3jRDH1hSnpM.mp4",
    "4GIuKZbwl2w.mp4",
    "4kzJHyYtNhk.mp4",
]

# Local cache directory for debug mode (set via VIDEO_CACHE_DIR env var)
LOCAL_CACHE_DIR = os.environ.get("VIDEO_CACHE_DIR")


def create_test_lance_table(table_path: str) -> None:
    """Create a local Lance table with public video URLs for testing."""
    records = []
    for i, video in enumerate(TEST_VIDEOS):
        video_url = f"{PUBLIC_VIDEO_URL}/{video}"
        slug = video.rsplit(".", 1)[0]

        records.append(
            {
                "global_index": i,
                "video_uid": slug,
                "source_url": video_url,
                "video_path": video_url,
                "subset": "train" if i < 8 else "validation",
            }
        )

    table = pa.Table.from_pylist(records)
    lance.write_dataset(table, table_path, mode="overwrite")
    logger.info(f"Created test Lance table at {table_path} with {len(records)} videos")


def _check_video_access() -> bool:
    """Check if we can access the public video endpoint."""
    try:
        test_url = f"{PUBLIC_VIDEO_URL}/{TEST_VIDEOS[0]}"
        r = requests.head(test_url, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


@pytest.mark.integration
@pytest.mark.timeout(900)  # 15 minutes for video processing
def test_video_slice_workflow_with_ray(ray_cluster):
    """Verify scene detection, slicing, filtering, and hashing on public videos.

    Creates a local Lance table with 10 public video URLs, split_size=2 for 5 splits.

    Uses ray_cluster fixture to ensure Ray is initialized with correct Python version
    and runtime_env excludes.

    In local debug mode (VIDEO_CACHE_DIR set), output is preserved in the cache directory.
    """
    # Skip if public endpoint not accessible
    if not _check_video_access():
        pytest.skip("Public video endpoint not accessible.")

    # In local debug mode, use cache directory for output (preserved after test)
    # Otherwise use temp directory (cleaned up after test)
    if LOCAL_CACHE_DIR:
        cache_dir = Path(LOCAL_CACHE_DIR).expanduser()
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir = str(cache_dir / "test_output")
        Path(tmp_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Local debug mode: output will be preserved in {tmp_dir}")
    else:
        tmp_dir = tempfile.mkdtemp(prefix="video_workflow_test_")

    input_table_path = os.path.join(tmp_dir, "input_videos.lance")
    output_path = Path(tmp_dir) / "hashed_slices.lance"

    try:
        # Create local Lance table with public video URLs
        create_test_lance_table(input_table_path)

        # Verify table was created
        ds = lance.dataset(input_table_path)
        logger.info(f"Test dataset has {ds.count_rows()} rows")
        assert ds.count_rows() == 10, f"Expected 10 rows, got {ds.count_rows()}"

        from workflows.video_slice_workflow import create_job

        filter_modulo = 4  # Keep every 4th slice

        job = create_job(
            job_id="video_slice_ray_test",
            config={
                "input": input_table_path,
                "output": str(output_path),
                "output_format": "lance",
                "filter_modulo": filter_modulo,
                "scene_threshold": 0.4,
                "split_size": 2,  # 2 rows per split = 5 splits for 10 videos
                "tansu_storage_url": "memory://",  # Use memory for Tansu
                # Elastic worker counts (min=2, max=4) to test multi-worker scenarios
                # with resource backoff on limited CPU environments
                "scene_parallelism": (2, 4),
                "slice_parallelism": (2, 4),
                "filter_parallelism": (2, 4),
                "hash_parallelism": (2, 4),
                "sink_buffer_size": 16,
                # Low CPU/memory for local testing (4 CPU machine)
                "worker_num_cpus": 0.25,  # 0.25 CPU per worker = 16 workers max on 4 CPUs
                "worker_memory_mb": 256,  # 256MB per worker
            },
        )
        
        # Ray already initialized by ray_cluster fixture with correct excludes
        # Job config (queue_type, tansu_storage_url) is set in the workflow
        runner = job.create_ray_runner()

        async def run_pipeline():
            try:
                await runner.run(timeout=600)
            finally:
                await runner.stop()

        asyncio.run(run_pipeline())

        assert output_path.exists(), f"Output path {output_path} does not exist"
        result_ds = lance.dataset(str(output_path))
        rows = result_ds.to_table().to_pylist()

        logger.info(f"Output has {len(rows)} rows")
        assert rows, "Expected filtered slice payloads"

        for row in rows:
            # Check hash
            digest = row.get("slice_sha256")
            assert isinstance(digest, str) and len(digest) == 64, f"Invalid hash: {digest}"
            # Check filter modulo
            assert int(row["global_slice_rank"]) % filter_modulo == 0
            # Check binary slice data
            slice_binary = row.get("slice_binary")
            assert slice_binary is not None, "Missing slice_binary"
            assert len(slice_binary) > 0, "Empty slice_binary"

        logger.info(f"✓ Test passed with {len(rows)} output slices")

    finally:
        # Cleanup - skip in local debug mode to preserve output
        if LOCAL_CACHE_DIR:
            logger.info(f"Local debug mode: output preserved at {output_path}")
        elif Path(tmp_dir).exists():
            shutil.rmtree(tmp_dir)
