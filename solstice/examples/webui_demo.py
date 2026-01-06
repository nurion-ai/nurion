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

"""Example workflow demonstrating WebUI usage.

This example shows how to enable and use the Solstice Debug WebUI
for monitoring and debugging streaming jobs.

Usage:
    python examples/webui_demo.py

Then visit: http://localhost:8000/solstice/
"""

import asyncio
import time

from solstice.core.job import Job, JobConfig, WebUIConfig
from solstice.core.stage import Stage
from solstice.operators.sources import FileSource
from solstice.operators.map import MapOperator
from solstice.operators.sinks import PrintSink
from solstice.queue import QueueType


def create_demo_job() -> Job:
    """Create a demo job with WebUI enabled."""
    
    # Enable WebUI with S3 storage
    webui_config = WebUIConfig(
        enabled=True,
        storage_path="/tmp/solstice-webui/",  # Use S3 for production: s3://bucket/path/
        prometheus_enabled=True,
        metrics_snapshot_interval_s=10.0,  # Snapshot every 10 seconds
        archive_on_completion=True,
        port=8000,
    )
    
    job = Job(
        job_id=f"webui_demo_{int(time.time())}",
        config=JobConfig(
            queue_type=QueueType.MEMORY,  # Use memory for demo
            webui=webui_config,
        ),
    )
    
    # Source stage
    source = Stage(
        stage_id="source",
        operator_config=FileSource(
            file_pattern="/tmp/demo-data/*.json",
        ),
        parallelism=1,
    )
    job.add_stage(source)
    
    # Transform stage
    transform = Stage(
        stage_id="transform",
        operator_config=MapOperator(
            map_fn=lambda record: {**record, "processed": True}
        ),
        parallelism=(2, 4),  # Auto-scale between 2-4 workers
    )
    job.add_stage(transform, upstream_stages=["source"])
    
    # Sink stage
    sink = Stage(
        stage_id="sink",
        operator_config=PrintSink(),
        parallelism=1,
    )
    job.add_stage(sink, upstream_stages=["transform"])
    
    return job


async def main():
    """Run the demo job."""
    print("╔════════════════════════════════════════════╗")
    print("║   Solstice WebUI Demo                     ║")
    print("╚════════════════════════════════════════════╝")
    print()
    
    # Create job
    job = create_demo_job()
    runner = job.create_ray_runner()
    
    try:
        # Initialize (starts WebUI)
        await runner.initialize()
        
        print("✓ Job initialized")
        
        if runner.webui_port:
            print(f"✓ WebUI available at Ray Serve port {runner.webui_port}")
            print()
            print(f"  📊 Job Detail:    http://<your-host>:{runner.webui_port}{runner.webui_path}")
            print(f"  📈 Portal:        http://<your-host>:{runner.webui_port}/solstice/")
            print(f"  🔗 Ray Dashboard: http://<your-host>:8265")
            print()
            print("  Note: Replace <your-host> with your actual hostname or IP")
            print()
        
        print("Starting job execution...")
        print("Press Ctrl+C to stop")
        print()
        
        # Run job
        status = await runner.run()
        
        print()
        print("✓ Job completed successfully")
        print(f"  Elapsed time: {status.elapsed_time:.2f}s")
        
        if runner.webui_port:
            print()
            print("Job archived. View history at:")
            print(f"  http://<your-host>:{runner.webui_port}/solstice/completed")
        
    except KeyboardInterrupt:
        print("\n\nStopping job...")
        await runner.stop()
        print("✓ Job stopped")
    except Exception as e:
        print(f"\n✗ Job failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())

