#!/usr/bin/env python3

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

"""Test video_slice_workflow with S3 video paths (no pre-downloading)."""

import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
from lance.dataset import write_dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def setup_s3_credentials(rclone_remote: str = "s3"):
    """Setup S3 credentials from rclone config into environment variables.

    Note: solstice.utils.remote._load_s3_config will also try to load from
    rclone config directly, but setting env vars ensures other libraries
    (like lance) can also access the credentials.
    """
    import configparser

    # Set the S3 remote name for solstice
    os.environ.setdefault("SOLSTICE_S3_REMOTE", rclone_remote)

    rclone_config = Path.home() / ".config/rclone/rclone.conf"
    if not rclone_config.exists():
        rclone_config = Path("/root/.config/rclone/rclone.conf")

    if not rclone_config.exists():
        logger.warning("rclone config not found, S3 access may fail")
        return

    config = configparser.ConfigParser()
    config.read(rclone_config)

    if rclone_remote in config:
        section = config[rclone_remote]
        os.environ.setdefault("AWS_ACCESS_KEY_ID", section.get("access_key_id", ""))
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", section.get("secret_access_key", ""))
        os.environ.setdefault("AWS_ENDPOINT_URL", section.get("endpoint", ""))
        os.environ.setdefault("AWS_DEFAULT_REGION", section.get("region", "us-east-1"))
        logger.info(f"Loaded S3 credentials from rclone config [{rclone_remote}]")


def list_videos_from_s3(s3_path: str, max_videos: int = 5) -> List[tuple]:
    """List videos from S3 and return metadata.

    Args:
        s3_path: S3 path (s3://bucket/prefix)
        max_videos: Maximum number of videos to return

    Returns:
        List of (size, relative_path) tuples
    """
    logger.info(f"Listing videos from {s3_path}...")

    # Convert s3://bucket/prefix to rclone format s3:bucket/prefix
    rclone_path = s3_path.replace("s3://", "s3:")

    result = subprocess.run(
        ["rclone", "ls", rclone_path],
        capture_output=True,
        text=True,
        check=True,
    )

    videos = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.strip().split(maxsplit=1)
        if len(parts) == 2:
            size, filename = parts
            # Only include smaller videos for testing (< 10MB)
            if filename.endswith((".mp4", ".mkv")) and int(size) < 10_000_000:
                videos.append((int(size), filename))

    # Sort by size and take smallest ones
    videos.sort(key=lambda x: x[0])
    videos = videos[:max_videos]

    logger.info(f"Found {len(videos)} videos")
    return videos


def s3_join_path(base_path: str, relative_path: str) -> str:
    """Join S3 base path with relative path."""
    # Ensure base_path doesn't end with /
    base = base_path.rstrip("/")
    return f"{base}/{relative_path}"


def create_s3_lance_table(
    videos: List[tuple],
    s3_base_path: str,
    output_path: str,
) -> None:
    """Create a Lance table with S3 video paths.

    Args:
        videos: List of (size, relative_path) tuples
        s3_base_path: Base S3 path (e.g., s3://bucket/prefix)
        output_path: Output path (s3:// or local)
    """
    from solstice.utils.remote import get_lance_storage_options

    records: List[Dict[str, Any]] = []

    for idx, (size, rel_path) in enumerate(videos):
        s3_path = s3_join_path(s3_base_path, rel_path)
        video_name = Path(rel_path).stem

        records.append(
            {
                "global_index": idx,
                "video_uid": video_name,
                "source_url": s3_path,
                "video_path": s3_path,  # S3 path directly!
                "width": 0,
                "height": 0,
                "fps": 0.0,
                "duration_sec": 0.0,
                "subset": "test",
                "target_slice_count": 3,
            }
        )

    logger.info(f"Creating Lance table with {len(records)} videos (S3 paths) at {output_path}")
    table = pa.Table.from_pylist(records)

    # Write to S3 or local
    if output_path.startswith("s3://"):
        bucket = output_path[5:].split("/")[0]
        storage_options = get_lance_storage_options(bucket)
        write_dataset(table, output_path, mode="overwrite", storage_options=storage_options)
    else:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        write_dataset(table, output_path, mode="overwrite")

    # Show sample paths
    for r in records[:2]:
        logger.info(f"  video_path: {r['video_path']}")


async def run_workflow_async(
    input_path: str,
    output_path: str,
) -> None:
    """Run the video slice workflow with Lance blob storage.

    Args:
        input_path: Input path (local or S3)
        output_path: Output path (local or S3)
    """
    import ray
    from solstice.runtime import RayJobRunner
    from solstice.queue import QueueType

    from workflows.video_slice_workflow import create_job

    logger.info("Initializing Ray with num_cpus=10...")
    ray.init(
        ignore_reinit_error=True,
        logging_level=logging.WARNING,
        num_cpus=10,
    )

    try:
        logger.info(f"Creating job with input={input_path}, output={output_path}")

        config = {
            "input": input_path,
            "output": output_path,
            "output_format": "lance",
            "filter_modulo": 1,
            "scene_threshold": 0.3,
            "min_slice_duration": 0.5,
            "scene_parallelism": 1,
            "slice_parallelism": 1,
            "filter_parallelism": 1,
            "hash_parallelism": 1,
            "sink_buffer_size": 1,
            "checkpoint_interval_secs": 300,
        }

        # Create job with new API
        job = create_job(
            job_id="test_video_slice_s3",
            config=config,
        )

        logger.info(f"Job created with {len(job.stages)} stages")

        # Use new async RayJobRunner API with Tansu queue
        runner = RayJobRunner(job, queue_type=QueueType.TANSU)
        await runner.initialize()

        logger.info("Starting workflow execution (timeout=1800s)...")
        try:
            import asyncio

            status = await asyncio.wait_for(
                runner.run(timeout=1800),
                timeout=1820,  # Extra buffer for cleanup
            )
            logger.info(f"Workflow completed! Status: {status}")
        except asyncio.TimeoutError:
            logger.error("Workflow execution timed out after 300 seconds")
            await runner.stop()
            raise

        # Check results
        import lance

        try:
            # For S3 paths, need to provide storage options
            if output_path.startswith("s3://"):
                from solstice.utils.remote import get_lance_storage_options

                bucket = output_path[5:].split("/")[0]
                storage_options = get_lance_storage_options(bucket)
                ds = lance.dataset(output_path, storage_options=storage_options)
            else:
                ds = lance.dataset(output_path)

            logger.info(f"Output table has {ds.count_rows()} rows")
            logger.info(f"Schema: {ds.schema}")

            sample = ds.to_table().to_pylist()[:3]
            for i, row in enumerate(sample):
                slice_binary = row.get("slice_binary")
                binary_size = len(slice_binary) if slice_binary else 0
                logger.info(
                    f"Sample {i}: video_uid={row.get('video_uid')}, "
                    f"scene_index={row.get('scene_index')}, "
                    f"slice_size={row.get('slice_size_bytes')} bytes, "
                    f"blob_size={binary_size} bytes"
                )
        except Exception as e:
            logger.error(f"Failed to read output table: {e}")

    finally:
        ray.shutdown()


def run_workflow(input_path: str, output_path: str) -> None:
    """Sync wrapper for run_workflow_async."""
    import asyncio

    asyncio.run(run_workflow_async(input_path, output_path))


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Test video slice workflow with S3 paths")
    parser.add_argument(
        "--source",
        default="s3://nurion/raw",
        help="Source rclone path for videos (e.g., s3://bucket/path)",
    )
    parser.add_argument(
        "--input",
        default="s3://nurion/lance/videos_input",
        help="Input Lance table path (s3:// or local)",
    )
    parser.add_argument(
        "--output",
        default="s3://nurion/lance/test_videos_split/",
        help="Output Lance table path (s3:// or local)",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=2,
        help="Maximum number of videos to test with",
    )
    parser.add_argument(
        "--skip-create",
        action="store_true",
        help="Skip creating input table (use existing)",
    )
    args = parser.parse_args()

    # Setup S3 credentials
    setup_s3_credentials()

    if not args.skip_create:
        # Step 1: List videos (no download!)
        logger.info("=" * 60)
        logger.info("Step 1: Listing videos from S3 (no download)...")
        logger.info("=" * 60)
        videos = list_videos_from_s3(args.source, max_videos=args.max_videos)

        if not videos:
            logger.error("No videos found!")
            sys.exit(1)

        # Step 2: Create Lance table with S3 paths (directly to S3)
        logger.info("=" * 60)
        logger.info(f"Step 2: Creating input Lance table -> {args.input}")
        logger.info("=" * 60)
        create_s3_lance_table(videos, args.source, args.input)
    else:
        logger.info("=" * 60)
        logger.info("Skipping input table creation (using existing)")
        logger.info("=" * 60)

    # Step 3: Run workflow (videos downloaded on-demand, output directly to S3)
    logger.info("=" * 60)
    logger.info(f"Running video slice workflow: {args.input} -> {args.output}")
    logger.info("=" * 60)
    run_workflow(args.input, args.output)

    logger.info("=" * 60)
    logger.info("Test completed successfully!")
    logger.info(f"Input:  {args.input}")
    logger.info(f"Output: {args.output}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
