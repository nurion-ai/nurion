"""Video workflow that performs ffmpeg scene detection, slicing, filtering, and hashing."""

from __future__ import annotations

import functools
import logging
from typing import Any, Dict

from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators.filter import FilterOperatorConfig
from solstice.operators.map import MapOperatorConfig
from solstice.operators.sinks import FileSinkConfig, LanceSinkConfig
from solstice.operators.sources import LanceTableSourceConfig
from solstice.operators.video import (
    FFmpegSceneDetectConfig,
    FFmpegSliceConfig,
    attach_slice_hash,
    keep_every_n,
)

DEFAULT_FILTER_MODULO = 10
DEFAULT_MIN_SLICE_DURATION = 0.5
DEFAULT_SCENE_THRESHOLD = 0.35


def create_job(
    job_id: str,
    config: Dict[str, Any],
) -> Job:
    """Create the ffmpeg-driven video slicing workflow."""

    logger = logging.getLogger(__name__)
    logger.info("Creating video slice workflow")

    input_path = config.get("input") or config.get("input_table")
    output_path = config.get("output") or config.get("output_path")

    if not input_path:
        raise ValueError("'input' or 'input_table' is required for video slice workflow")
    if not output_path:
        raise ValueError("'output' or 'output_path' is required for video slice workflow")

    filter_modulo = int(config.get("filter_modulo", DEFAULT_FILTER_MODULO))
    min_slice_duration = float(config.get("min_slice_duration", DEFAULT_MIN_SLICE_DURATION))
    scene_threshold = float(config.get("scene_threshold", DEFAULT_SCENE_THRESHOLD))

    job = Job(
        job_id=job_id,
        config=config,
    )

    # Source stage
    source_stage = Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri=input_path,
            split_size=10,
        ),
        parallelism=1,
    )

    # Detect stage
    scene_stage = Stage(
        stage_id="detect",
        operator_config=FFmpegSceneDetectConfig(
            scene_threshold=scene_threshold,
            min_scene_duration=min_slice_duration,
        ),
        parallelism=config.get("scene_parallelism", (2, 6)),
    )

    # Slice stage
    slice_stage = Stage(
        stage_id="slice",
        operator_config=FFmpegSliceConfig(
            min_scene_duration=min_slice_duration,
        ),
        parallelism=config.get("slice_parallelism", (2, 4)),
    )

    # Filter stage
    filter_stage = Stage(
        stage_id="filter",
        operator_config=FilterOperatorConfig(
            filter_fn=functools.partial(keep_every_n, modulo=filter_modulo),
        ),
        parallelism=config.get("filter_parallelism", 2),
    )

    # Hash stage
    hash_stage = Stage(
        stage_id="hash",
        operator_config=MapOperatorConfig(
            map_fn=attach_slice_hash,
        ),
        parallelism=config.get("hash_parallelism", 2),
    )

    output_format = config.get("output_format", "json")
    if output_format == "lance":
        sink_config = LanceSinkConfig(
            table_path=output_path,
            mode="overwrite",
            buffer_size=config.get("sink_buffer_size", 256),
            blob_columns=["slice_binary"],
        )
    else:
        sink_config = FileSinkConfig(
            output_path=output_path,
            format=output_format,
            buffer_size=config.get("sink_buffer_size", 256),
        )

    # Sink stage
    sink_stage = Stage(
        stage_id="sink",
        operator_config=sink_config,
        parallelism=1,
    )

    job.add_stage(source_stage)
    job.add_stage(scene_stage, upstream_stages=[source_stage.stage_id])
    job.add_stage(slice_stage, upstream_stages=[scene_stage.stage_id])
    job.add_stage(filter_stage, upstream_stages=[slice_stage.stage_id])
    job.add_stage(hash_stage, upstream_stages=[filter_stage.stage_id])
    job.add_stage(sink_stage, upstream_stages=[hash_stage.stage_id])

    logger.info(
        "Video slice workflow created with %d stages (filter_modulo=%d, threshold=%.2f)",
        len(job.stages),
        filter_modulo,
        scene_threshold,
    )

    return job
