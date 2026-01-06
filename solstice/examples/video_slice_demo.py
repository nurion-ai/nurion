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

"""Video slice workflow demo with WebUI.

This script runs the video slice workflow and keeps the WebUI active for inspection.
It can be run locally or submitted to a Ray cluster.

Usage:
    python examples/video_slice_demo.py --job-id my_job --wait-time 300
"""

import asyncio
import logging
import os
import tempfile
import time
from typing import Optional

import click
import lance
import pyarrow as pa
import ray

from solstice.core.job import JobConfig, WebUIConfig
from solstice.runtime.ray_runner import RayJobRunner
from workflows.video_slice_workflow import create_job

logger = logging.getLogger(__name__)


def create_test_lance_table(table_path: str) -> None:
    """Create a local Lance table with public video URLs."""
    # Public videos
    videos = [
        "-qwTw3PNXDE.mp4", "0wJO0eqVDho.mkv", "1UmhvUR_wtQ.mp4",
        "2R-gGLtYmdc.mp4", "3EIixA3E-rI.mp4", "3ETxXjGlxRo.mp4",
        "3WG6fgdFV74.mp4", "3jRDH1hSnpM.mp4", "4GIuKZbwl2w.mp4",
        "4kzJHyYtNhk.mp4",
    ]
    base_url = "https://pub-8bc1f1d3d1984bdfb056d0bc0bf97c3d.r2.dev/videos/raw"
    
    records = []
    for i, video in enumerate(videos):
        records.append({
            "global_index": i,
            "video_uid": video.rsplit(".", 1)[0],
            "source_url": f"{base_url}/{video}",
            "video_path": f"{base_url}/{video}",
            "subset": "train" if i < 8 else "validation",
        })
    
    table = pa.Table.from_pylist(records)
    lance.write_dataset(table, table_path, mode="overwrite")
    logger.info(f"Created test table with {len(records)} videos")


@click.command()
@click.option("--job-id", default="video_slice_demo", help="Job identifier")
@click.option("--wait-time", default=0, type=int, help="Time to wait after completion (seconds)")
def main(job_id: str, wait_time: int):
    """Run video slice workflow demo."""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize Ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    
    # Use temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input_videos.lance")
        output_path = os.path.join(tmp_dir, "output_slices.lance")
        webui_storage = os.path.join(tmp_dir, "webui-storage")
        
        # Create input data
        create_test_lance_table(input_path)
        
        # Configure job
        config = {
            "input": input_path,
            "output": output_path,
            "output_format": "lance",
            "filter_modulo": 4,
            "scene_threshold": 0.4,
            "split_size": 2,
            "tansu_storage_url": "memory://",
            "scene_parallelism": (2, 4),
            "slice_parallelism": (2, 4),
            "filter_parallelism": (2, 4),
            "hash_parallelism": (2, 4),
            "worker_num_cpus": 0.25,
            "worker_memory_mb": 256,
        }
        
        # Create job
        job = create_job(job_id=job_id, config=config)
        
        # Enable WebUI
        job.config.webui = WebUIConfig(
            enabled=True,
            storage_path=webui_storage,
            prometheus_enabled=False,  # Disable for demo
            port=8000,
        )
        
        logger.info("=" * 80)
        logger.info(f"Starting job {job_id}")
        logger.info("=" * 80)
        
        runner = job.create_ray_runner()
        
        async def run():
            await runner.initialize()
            
            if runner.webui_port:
                logger.info(f"WebUI available at: http://localhost:{runner.webui_port}{runner.webui_path}")
                logger.info(f"Portal: http://localhost:{runner.webui_port}/solstice/")
            
            try:
                status = await runner.run(timeout=600)
                logger.info(f"Job finished: {status}")
                
                if wait_time > 0:
                    logger.info(f"Waiting {wait_time}s to keep WebUI active...")
                    await asyncio.sleep(wait_time)
                    
            finally:
                # Stop with timeout to avoid hanging
                try:
                    await asyncio.wait_for(runner.stop(), timeout=30)
                except asyncio.TimeoutError:
                    logger.warning("Stop timed out after 30s, forcing exit")
        
        asyncio.run(run())


if __name__ == "__main__":
    main()
