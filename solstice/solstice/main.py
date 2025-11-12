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

from solstice.state.backend import StateBackend, LocalStateBackend, S3StateBackend
from solstice.core.job import Job


def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def create_state_backend(backend_type: str, **kwargs) -> StateBackend:
    """Create state backend from parameters"""
    if backend_type == "local":
        local_path = kwargs.get("local_path", "/tmp/solstice")
        return LocalStateBackend(local_path)

    elif backend_type == "s3":
        s3_path = kwargs.get("s3_path")
        if not s3_path:
            raise ValueError("s3_path is required for S3 state backend")
        return S3StateBackend(s3_path)

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
@click.option("--restore-from", required=False, type=str, help="Checkpoint ID to restore from")
@click.option("--log-level", default="INFO", type=str, help="Logging level")
@click.option("--checkpoint-interval", default=300, type=int, help="Checkpoint interval in seconds")
@click.option("--checkpoint-records", default=None, type=int, help="Checkpoint interval in records")
@click.option(
    "--state-backend",
    default="local",
    type=click.Choice(["local", "s3"]),
    help="State backend type",
)
@click.option(
    "--state-path",
    default="/tmp/solstice",
    type=str,
    help="State backend path (local) or bucket (s3)",
)
@click.option(
    "--state-prefix", default="checkpoints", type=str, help="State backend prefix (for s3)"
)
@click.pass_context
def main(
    ctx,
    workflow: str,
    job_id: Optional[str],
    restore_from: Optional[str],
    log_level: str,
    checkpoint_interval: int,
    checkpoint_records: Optional[int],
    state_backend: str,
    state_path: str,
    state_prefix: str,
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

    logger.info("Starting local Ray cluster")
    ray.init(ignore_reinit_error=True)

    logger.info(f"Ray cluster info: {ray.cluster_resources()}")

    try:
        # Create state backend
        if state_backend == "local":
            backend = create_state_backend("local", base_path=state_path)
        else:  # s3
            backend = create_state_backend("s3", s3_path=state_path)

        logger.info(f"Created state backend: {type(backend).__name__}")

        # Load workflow
        logger.info(f"Loading workflow: {workflow}")
        workflow_module = load_workflow(workflow)

        # Create job from workflow
        if not hasattr(workflow_module, "create_job"):
            raise ValueError(f"Workflow module {workflow} must have a create_job() function")

        # Merge workflow config with extra kwargs
        workflow_config = {
            "checkpoint_interval_secs": checkpoint_interval,
            "checkpoint_interval_records": checkpoint_records,
            **extra_kwargs,
        }

        job: Job = workflow_module.create_job(
            job_id=job_id,
            config=workflow_config,
            state_backend=backend,
        )

        # Initialize job
        logger.info("Initializing job...")
        job.initialize()

        # Restore from checkpoint if requested
        if restore_from:
            logger.info(f"Restoring from checkpoint: {restore_from}")
            success = job.restore_from_checkpoint(restore_from)
            if not success:
                logger.error("Failed to restore from checkpoint")
                sys.exit(1)

        # Start job
        logger.info("Starting job execution...")
        job.start()

        # Monitor job
        logger.info("Job is running. Press Ctrl+C to stop.")
        logger.info("=" * 80)

        # Setup signal handler for graceful shutdown
        def signal_handler(signum, frame):
            logger.info("\nReceived interrupt signal. Shutting down...")
            job.stop()
            logger.info("Job stopped successfully")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Monitor loop
        while job.is_running:
            time.sleep(10)

            # Print status
            status = job.get_status()
            logger.info(f"Job Status: {status}")

            # Print metrics
            metrics = job.get_metrics()
            if metrics:
                logger.info(f"Job Metrics: {metrics}")

        logger.info("Job completed successfully")

    except Exception as e:
        logger.error(f"Job failed with error: {e}", exc_info=True)
        sys.exit(1)

    finally:
        logger.info("Shutting down Ray")
        ray.shutdown()


if __name__ == "__main__":
    main()
