#!/usr/bin/env python3
"""
Main entry point for Solstice Streaming jobs

Example usage:
    python -m solstice.main \\
        --workflow workflows.simple_etl \\
        --job-id my_job_001 \\
        --input /data/input \\
        --output /data/output
"""

import logging
import sys
from typing import Optional
import time
import signal

import click
import ray

from solstice.state.store import (
    CheckpointStore,
    LocalCheckpointStore,
    S3CheckpointStore,
    SlateDBCheckpointStore,
)
from solstice.core.job import Job


def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def create_checkpoint_store_from_params(backend_type: str, **kwargs) -> CheckpointStore:
    """Create checkpoint store from parameters"""
    if backend_type == "local":
        local_path = kwargs.get("local_path", "/tmp/solstice")
        return LocalCheckpointStore(local_path)

    elif backend_type == "s3":
        s3_path = kwargs.get("s3_path")
        if not s3_path:
            raise ValueError("s3_path is required for S3 checkpoint store")
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return S3CheckpointStore(bucket, prefix)

    elif backend_type == "slatedb":
        # SlateDB with configurable object store
        path = kwargs.get("local_path", "/tmp/solstice")
        object_store = kwargs.get("object_store", "local")
        return SlateDBCheckpointStore(path, object_store)

    else:
        raise ValueError(f"Unknown backend type: {backend_type}")


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


@click.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.option(
    "--workflow", required=True, type=str, help="Workflow module (e.g., workflows.simple_etl)"
)
@click.option("--job-id", required=False, type=str, help="Job ID (auto-generated if not provided)")
@click.option("--log-level", default="INFO", type=str, help="Logging level")
@click.option("--checkpoint-interval", default=300, type=int, help="Checkpoint interval in seconds")
@click.option(
    "--checkpoint-store",
    default="local",
    type=click.Choice(["local", "s3", "slatedb"]),
    help="Checkpoint store type",
)
@click.option(
    "--checkpoint-path",
    default="/tmp/solstice",
    type=str,
    help="Checkpoint store path (local) or bucket (s3)",
)
@click.pass_context
def main(
    ctx,
    workflow: str,
    job_id: Optional[str],
    log_level: str,
    checkpoint_interval: int,
    checkpoint_store: str,
    checkpoint_path: str,
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

    try:
        # Create checkpoint store
        store = create_checkpoint_store_from_params(
            checkpoint_store, local_path=checkpoint_path, s3_path=checkpoint_path
        )
        logger.info(f"Created checkpoint store: {type(store).__name__}")

        # Load workflow
        logger.info(f"Loading workflow: {workflow}")
        workflow_module = load_workflow(workflow)

        # Create job from workflow
        if not hasattr(workflow_module, "create_job"):
            raise ValueError(f"Workflow module {workflow} must have a create_job() function")

        # Merge workflow config with extra kwargs
        workflow_config = {
            "checkpoint_interval_secs": checkpoint_interval,
            **extra_kwargs,
        }

        job: Job = workflow_module.create_job(
            job_id=job_id,
            config=workflow_config,
            checkpoint_store=store,
        )

        runner = job.create_ray_runner()
        runner.initialize()

        logger.info("Ray cluster info: %s", ray.cluster_resources())

        available_checkpoints = runner.list_checkpoints()
        if available_checkpoints:
            latest_checkpoint = available_checkpoints[-1]
            logger.info("Restoring from latest checkpoint: %s", latest_checkpoint)
            restored = runner.restore_from_checkpoint(latest_checkpoint)
            if not restored:
                logger.warning("Checkpoint restore failed; continuing with fresh state.")

        # Setup signal handler for graceful shutdown
        def signal_handler(signum, frame):
            logger.info("\nReceived interrupt signal. Shutting down...")
            runner.stop()
            logger.info("Job stopped successfully")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Starting job execution...")
        logger.info("Job is running. Press Ctrl+C to stop.")
        logger.info("=" * 80)

        runner.run()

        logger.info("Job completed successfully")

        status = runner.get_status()
        logger.info("Final job status: %s", status)

        metrics = runner.get_metrics()
        if metrics:
            logger.info("Final job metrics: %s", metrics)

    except Exception as e:
        logger.error(f"Job failed with error: {e}", exc_info=True)
        sys.exit(1)

    finally:
        if "runner" in locals():
            runner.shutdown()
        logger.info("Shutting down Ray")
        ray.shutdown()


if __name__ == "__main__":
    main()
