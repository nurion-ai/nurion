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

"""
Main entry point for Solstice Streaming jobs

Example usage:
    python -m solstice.main \\
        --workflow workflows.simple_etl \\
        --job-id my_job_001 \\
        --input /data/input \\
        --output /data/output
"""

import asyncio
import logging
import sys
from typing import Optional
import time
import signal

import click
import ray

from solstice.core.job import Job


def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def load_workflow(workflow_module: str):
    """Dynamically load workflow module"""
    import importlib

    module = importlib.import_module(workflow_module)
    return module


def parse_kwargs(ctx, param, value):
    """Parse additional kwargs from CLI"""
    kwargs = {}
    if value:
        for item in value:
            if "=" not in item:
                raise click.BadParameter(f"Invalid format: {item}. Use key=value")
            key, val = item.split("=", 1)
            # Try to parse as number or boolean
            if val.lower() in ("true", "false"):
                val = val.lower() == "true"
            else:
                try:
                    val = int(val)
                except ValueError:
                    try:
                        val = float(val)
                    except ValueError:
                        pass  # Keep as string
            kwargs[key] = val
    return kwargs


@click.group()
def cli():
    """Solstice - Ray-based streaming data processing framework."""
    pass


@cli.command(name="run", context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.option(
    "--workflow", required=True, type=str, help="Workflow module (e.g., workflows.simple_etl)"
)
@click.option("--job-id", required=False, type=str, help="Job ID (auto-generated if not provided)")
@click.option("--log-level", default="INFO", type=str, help="Logging level")
@click.pass_context
def run_job(
    ctx,
    workflow: str,
    job_id: Optional[str],
    log_level: str,
):
    """
    Main entry point for running Solstice Streaming jobs
    
    All workflow parameters are defined in the workflow module.
    Additional parameters can be passed as --key=value and will be 
    forwarded to the workflow's create_job() function.
    
    Example:
        python -m solstice.main \\
            --workflow workflows.simple_etl \\
            --job-id my_job_001 \\
            --input /data/input \\
            --output /data/output \\
            --parallelism 4
    """
    # Setup logging
    setup_logging(log_level)
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("Solstice Streaming Job Runner")
    logger.info("=" * 80)

    # Parse additional kwargs from extra args
    extra_kwargs = {}
    args = ctx.args
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith("--"):
            key = arg[2:]
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                value = args[i + 1]
                # Try to parse as number or boolean
                if value.lower() in ("true", "false"):
                    value = value.lower() == "true"
                else:
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            pass  # Keep as string
                extra_kwargs[key] = value
                i += 2
            else:
                extra_kwargs[key] = True
                i += 1
        else:
            i += 1

    logger.info(f"Additional parameters: {extra_kwargs}")

    # Generate job ID if not provided
    if not job_id:
        job_id = f"job_{int(time.time())}"

    logger.info(f"Job ID: {job_id}")

    runner = None
    try:
        # Load workflow
        logger.info(f"Loading workflow: {workflow}")
        workflow_module = load_workflow(workflow)

        # Create job from workflow
        if not hasattr(workflow_module, "create_job"):
            raise ValueError(f"Workflow module {workflow} must have a create_job() function")

        # Merge workflow config with extra kwargs
        workflow_config = {
            **extra_kwargs,
        }

        job: Job = workflow_module.create_job(
            job_id=job_id,
            config=workflow_config,
        )

        runner = job.create_ray_runner()

        # Setup signal handler for graceful shutdown
        def signal_handler(signum, frame):
            logger.info("\nReceived interrupt signal. Shutting down...")
            if runner:
                asyncio.get_event_loop().run_until_complete(runner.stop())
            logger.info("Job stopped successfully")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Starting job execution...")
        logger.info("Job is running. Press Ctrl+C to stop.")
        logger.info("=" * 80)

        # Run the pipeline
        status = asyncio.get_event_loop().run_until_complete(runner.run())

        logger.info("Job completed successfully")
        logger.info("Final job status: %s", status)

    except Exception as e:
        logger.error(f"Job failed with error: {e}", exc_info=True)
        sys.exit(1)

    finally:
        if runner:
            asyncio.get_event_loop().run_until_complete(runner.stop())
        logger.info("Shutting down Ray")
        ray.shutdown()


@cli.command(name="history-server")
@click.option(
    "--storage-path",
    "-s",
    required=True,
    help="SlateDB storage path (e.g., s3://bucket/solstice-history/ or /tmp/solstice-webui/)",
)
@click.option(
    "--host",
    "-h",
    default="0.0.0.0",
    help="Host to bind (default: 0.0.0.0)",
)
@click.option(
    "--port",
    "-p",
    default=8080,
    type=int,
    help="Port to bind (default: 8080)",
)
@click.option(
    "--reload",
    is_flag=True,
    help="Enable auto-reload for development",
)
def history_server_cmd(storage_path: str, host: str, port: int, reload: bool):
    """Start History Server for viewing completed jobs.

    Example:
        solstice history-server -s s3://my-bucket/solstice-history/ -p 8080
    """
    from solstice.webui.history_server import history_server as hs_func

    # Call the actual function (can't use Click command directly)
    import sys

    sys.argv = [
        "history-server",
        "--storage-path",
        storage_path,
        "--host",
        host,
        "--port",
        str(port),
    ]
    if reload:
        sys.argv.append("--reload")

    hs_func.callback(storage_path, host, port, reload)


def main():
    """Main entry point (backwards compatibility)."""
    cli()


if __name__ == "__main__":
    cli()
