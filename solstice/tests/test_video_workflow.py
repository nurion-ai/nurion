"""Ray-based end-to-end test for the video slice workflow."""

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

logger = logging.getLogger("test")

# Public R2 endpoint for videos (no authentication needed)
PUBLIC_R2_ENDPOINT = "https://pub-8bc1f1d3d1984bdfb056d0bc0bf97c3d.r2.dev"

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


def create_test_lance_table(table_path: str) -> None:
    """Create a local Lance table with public video URLs for testing."""
    records = []
    for i, video in enumerate(TEST_VIDEOS):
        # Use public HTTPS URL (no auth needed)
        public_url = f"{PUBLIC_R2_ENDPOINT}/videos/raw/{video}"
        slug = video.rsplit(".", 1)[0]

        records.append({
            "global_index": i,
            "video_uid": slug,
            "source_url": public_url,
            "video_path": public_url,
            "subset": "train" if i < 8 else "validation",
        })

    table = pa.Table.from_pylist(records)
    lance.write_dataset(table, table_path, mode="overwrite")
    logger.info(f"Created test Lance table at {table_path} with {len(records)} videos")


@pytest.mark.integration
@pytest.mark.timeout(900)  # 15 minutes for video processing
def test_video_slice_workflow_with_ray():
    """Verify scene detection, slicing, filtering, and hashing on public videos.

    Creates a local Lance table with 10 public video URLs, split_size=2 for 5 splits.
    """
    # Create temp directory for test data
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
                "scene_parallelism": 1,
                "slice_parallelism": 1,
                "filter_parallelism": 1,
                "hash_parallelism": 1,
                "sink_buffer_size": 16,
            },
        )

        from solstice.core.stage_master import QueueType

        runner = job.create_ray_runner(
            queue_type=QueueType.TANSU,
            tansu_storage_url="memory://",  # Use memory for Tansu
            ray_init_kwargs={
                "num_cpus": 4,
                "log_to_driver": True,
                "logging_level": logging.INFO,
            }
        )

        try:
            asyncio.get_event_loop().run_until_complete(runner.run(timeout=600))
        finally:
            asyncio.get_event_loop().run_until_complete(runner.stop())

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
        # Cleanup
        if Path(tmp_dir).exists():
            shutil.rmtree(tmp_dir)
