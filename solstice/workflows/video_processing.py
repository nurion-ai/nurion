"""
Video processing workflow - inspired by fusionflow video_main.py

This demonstrates a more complex pipeline similar to the fusionflow blueprint:
1. Source: Read video metadata from Lance table
2. Classify: Classify and filter based on metadata
3. Process: Extract frames and run inference
4. Sink: Save results

All configuration is defined here with sensible defaults.
CLI parameters can override any setting.
"""

import logging
from typing import Any, Dict, List

from solstice.core.job import Job
from solstice.core.stage import Stage
from solstice.operators.source import LanceTableSource
from solstice.operators.map import MapOperator, FlatMapOperator
from solstice.operators.filter import FilterOperator
from solstice.operators.sink import LanceSink, FileSink
from solstice.state.backend import StateBackend


def classify_metadata(video_data: Dict[str, Any]) -> Dict[str, Any]:
    """Classify video metadata (similar to ClassifyMetaActor)"""
    # Example: check video duration, resolution, etc.
    info = video_data.get("info", {})

    # Mark as valid if meets criteria
    is_valid = (
        info.get("duration", 0) > 1.0
        and info.get("duration", 0) < 600.0
        and info.get("width", 0) >= 256
        and info.get("height", 0) >= 256
    )

    video_data["is_valid"] = is_valid
    video_data["classification"] = {
        "duration_ok": info.get("duration", 0) > 1.0,
        "resolution_ok": info.get("width", 0) >= 256,
    }

    return video_data


def filter_valid_videos(video_data: Dict[str, Any]) -> bool:
    """Filter to keep only valid videos"""
    return video_data.get("is_valid", False)


def detect_scenes(video_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Detect scenes in video (similar to DetectScenesActor)
    Returns multiple scene records from one video
    """
    # Placeholder - in real implementation would call scene detection
    num_scenes = video_data.get("info", {}).get("duration", 10.0) // 5.0
    num_scenes = max(1, int(num_scenes))

    scenes = []
    for i in range(num_scenes):
        scene = video_data.copy()
        scene["scene_id"] = i
        scene["scene_start"] = i * 5.0
        scene["scene_end"] = (i + 1) * 5.0
        scenes.append(scene)

    return scenes


def extract_features(scene_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract features from scene (similar to ExtractFramesActor + InferModel)
    """
    # Placeholder - in real implementation would extract frames and run inference
    scene_data["features"] = {
        "embeddings": [0.1, 0.2, 0.3],  # Dummy embeddings
        "tags": ["scene", "video"],
        "quality_score": 0.85,
    }

    return scene_data


def create_job(
    job_id: str,
    config: Dict[str, Any],
    state_backend: StateBackend,
) -> Job:
    """
    Create a video processing job.

    DAG structure:
        Source -> Classify -> Filter -> DetectScenes -> ExtractFeatures -> Sink

    Config parameters:
        - input: Input Lance table path (required)
        - output: Output path - Lance table or file (required)
        - output_format: Output format - lance/json/parquet (default: json)
        - classify_parallelism: Classify workers (default: (4, 10))
        - scenes_parallelism: Scene detection workers (default: (4, 20))
        - features_parallelism: Feature extraction workers (default: (2, 8))
        - features_gpus: GPUs per feature worker (default: 0)
        - checkpoint_interval_secs: Checkpoint interval (default: 600)
        - checkpoint_interval_records: Record-based checkpoint (default: 10000)
    """
    logger = logging.getLogger(__name__)
    logger.info("Creating Video Processing job")

    # Extract required parameters
    input_path = config.get("input")
    output_path = config.get("output")

    if not input_path:
        raise ValueError("'input' parameter is required (Lance table path)")
    if not output_path:
        raise ValueError("'output' parameter is required")

    # Create job with configuration
    job = Job(
        job_id=job_id,
        state_backend=state_backend,
        checkpoint_interval_secs=config.get("checkpoint_interval_secs", 600),
        checkpoint_interval_records=config.get("checkpoint_interval_records", 10000),
        config=config,
    )

    # Stage 1: Source - Read video metadata (fixed 1 worker)
    source_stage = Stage(
        stage_id="source",
        operator_class=LanceTableSource,
        operator_config={
            "table_path": input_path,
            "batch_size": config.get("source_batch_size", 100),
        },
        parallelism=1,  # Fixed 1 worker
        worker_resources={
            "num_cpus": 1,
            "memory": 4 * 1024**3,
        },
    )

    # Stage 2: Classify metadata (auto-scale 4-10 workers)
    classify_parallelism = config.get("classify_parallelism", (4, 10))
    classify_stage = Stage(
        stage_id="classify",
        operator_class=MapOperator,
        operator_config={
            "map_fn": classify_metadata,
        },
        parallelism=classify_parallelism,
        worker_resources={
            "num_cpus": 1,
            "memory": 2 * 1024**3,
        },
    )

    # Stage 3: Filter valid videos (fixed 2 workers)
    filter_parallelism = config.get("filter_parallelism", 2)
    filter_stage = Stage(
        stage_id="filter",
        operator_class=FilterOperator,
        operator_config={
            "filter_fn": filter_valid_videos,
        },
        parallelism=filter_parallelism,
        worker_resources={
            "num_cpus": 1,
            "memory": 1 * 1024**3,
        },
    )

    # Stage 4: Detect scenes (auto-scale 4-20 workers)
    scenes_parallelism = config.get("scenes_parallelism", (4, 20))
    scenes_stage = Stage(
        stage_id="detect_scenes",
        operator_class=FlatMapOperator,
        operator_config={
            "flatmap_fn": detect_scenes,
        },
        parallelism=scenes_parallelism,
        worker_resources={
            "num_cpus": 2,
            "memory": 4 * 1024**3,
        },
    )

    # Stage 5: Extract features with GPU (auto-scale 2-8 workers)
    features_parallelism = config.get("features_parallelism", (2, 8))
    features_gpus = config.get("features_gpus", 0)
    features_stage = Stage(
        stage_id="extract_features",
        operator_class=MapOperator,
        operator_config={
            "map_fn": extract_features,
        },
        parallelism=features_parallelism,
        worker_resources={
            "num_cpus": 2,
            "num_gpus": features_gpus,
            "memory": 8 * 1024**3,
        },
    )

    # Stage 6: Sink - Save results (fixed 2 workers)
    output_format = config.get("output_format", "json")

    if output_format == "lance":
        sink_class = LanceSink
        sink_config = {
            "table_path": output_path,
            "mode": config.get("output_mode", "append"),
            "buffer_size": config.get("sink_buffer_size", 1000),
        }
    else:
        sink_class = FileSink
        sink_config = {
            "output_path": output_path,
            "format": output_format,
            "buffer_size": config.get("sink_buffer_size", 1000),
        }

    sink_parallelism = config.get("sink_parallelism", 2)
    sink_stage = Stage(
        stage_id="sink",
        operator_class=sink_class,
        operator_config=sink_config,
        parallelism=sink_parallelism,
        worker_resources={
            "num_cpus": 1,
            "memory": 4 * 1024**3,
        },
    )

    # Build DAG
    job.add_stage(source_stage)
    job.add_stage(classify_stage, upstream_stages=["source"])
    job.add_stage(filter_stage, upstream_stages=["classify"])
    job.add_stage(scenes_stage, upstream_stages=["filter"])
    job.add_stage(features_stage, upstream_stages=["detect_scenes"])
    job.add_stage(sink_stage, upstream_stages=["extract_features"])

    logger.info(
        f"Created video processing job with {len(job.stages)} stages:\n"
        f"  Source -> Classify -> Filter -> DetectScenes -> ExtractFeatures -> Sink"
    )
    logger.info(f"Configuration: {config}")

    return job


# CLI usage example:
# python -m solstice.main \
#   --workflow workflows.video_processing \
#   --job-id video_001 \
#   --input /data/video_metadata \
#   --output /data/processed_videos.json \
#   --classify-parallelism 8 \
#   --scenes-parallelism 16 \
#   --features-parallelism 4 \
#   --features-gpus 1
