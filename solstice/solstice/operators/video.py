"""Video-specific operators for ffmpeg/ffprobe scene detection and slicing."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, List, Optional

from solstice.core.models import SplitPayload
from solstice.core.operator import Operator, OperatorConfig

import pyarrow as pa


def _lavfi_movie_expr(path: Path) -> str:
    escaped = str(path).replace("\\", "\\\\").replace("'", "\\'")
    return f"movie='{escaped}'"


def _run_ffprobe_scene_detection(video_path: Path, threshold: float) -> List[float]:
    import logging

    logger = logging.getLogger(__name__)
    movie_expr = _lavfi_movie_expr(video_path)
    filtergraph = f"{movie_expr},select=gt(scene\\,{threshold})"
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_frames",
        "-of",
        "json",
        "-f",
        "lavfi",
        filtergraph,
    ]
    logger.debug(f"Running ffprobe command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=300)
    logger.debug(
        f"ffprobe completed for {video_path}, stdout length={len(result.stdout)}, stderr length={len(result.stderr)}"
    )
    data = json.loads(result.stdout or "{}")
    frames = data.get("frames", [])
    boundaries = []
    for frame in frames:
        pts_time = frame.get("pts_time")
        if pts_time is None:
            continue
        try:
            boundaries.append(float(pts_time))
        except ValueError:
            continue
    return sorted(boundaries)


def _probe_video_metadata(video_path: Path) -> Dict[str, Any]:
    import logging

    logger = logging.getLogger(__name__)
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height,avg_frame_rate",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        str(video_path),
    ]
    logger.debug(f"Running ffprobe metadata command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=300)
    logger.debug(f"ffprobe metadata completed for {video_path}")
    payload = json.loads(result.stdout or "{}")
    streams = payload.get("streams", [])
    width = height = None
    fps = None
    if streams:
        stream = streams[0]
        width = stream.get("width")
        height = stream.get("height")
        avg_rate = stream.get("avg_frame_rate")
        if avg_rate and avg_rate != "0/0":
            try:
                fps = float(Fraction(avg_rate))
            except ZeroDivisionError:
                fps = None
    duration = None
    fmt = payload.get("format")
    if fmt and "duration" in fmt:
        try:
            duration = float(fmt["duration"])
        except ValueError:
            duration = None
    return {
        "width": width,
        "height": height,
        "fps": fps,
        "duration_sec": duration,
    }


def _compute_global_slice_rank(global_index: int, scene_index: int) -> int:
    return global_index + scene_index


@dataclass
class FFmpegSceneDetectConfig(OperatorConfig):
    """Configuration for FFmpegSceneDetectOperator."""

    scene_threshold: float = 0.4
    """Threshold for scene change detection (0.0-1.0)."""

    min_scene_duration: float = 0.5
    """Minimum scene duration in seconds."""


class FFmpegSceneDetectOperator(Operator):
    """Detect scenes for each video referenced in a batch."""

    def __init__(self, config: FFmpegSceneDetectConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        self.scene_threshold = config.scene_threshold
        self.min_scene_duration = config.min_scene_duration

    def process_split(
        self, split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            raise ValueError("FFmpegSceneDetectOperator requires a payload")

        rows = payload.to_table().to_pylist()
        self.logger.debug(
            f"FFmpegSceneDetectOperator processing {len(rows)} rows for split {split.split_id}"
        )
        output_records: List[Dict[str, Any]] = []

        for idx, row in enumerate(rows):
            video_path = row.get("video_path")
            if not video_path:
                raise ValueError(f"Missing video path for row {row}")

            local_path = Path(video_path)
            if not local_path.exists():
                self.logger.error("Missing video binary at %s", video_path)
                raise FileNotFoundError(f"Missing video binary at {video_path}")

            self.logger.debug(f"Processing video {idx + 1}/{len(rows)}: {video_path}")
            metadata = _probe_video_metadata(local_path)
            duration = metadata.get("duration_sec") or row.get("duration_sec")
            if not duration:
                duration = self.min_scene_duration

            self.logger.debug(
                f"Running scene detection for {video_path} (duration={duration:.2f}s, threshold={self.scene_threshold})"
            )
            boundaries = _run_ffprobe_scene_detection(local_path, self.scene_threshold)
            self.logger.debug(f"Found {len(boundaries)} scene boundaries for {video_path}")
            scenes: List[tuple[float, float]] = []
            previous = 0.0
            for boundary in boundaries:
                boundary = max(previous, min(boundary, duration))
                if boundary - previous >= self.min_scene_duration:
                    scenes.append((previous, boundary))
                    previous = boundary
            if duration - previous >= self.min_scene_duration:
                scenes.append((previous, duration))
            if not scenes:
                scenes = [(0.0, duration)]

            for idx, (start, end) in enumerate(scenes):
                record = dict(row)
                record.update(
                    {
                        "scene_index": idx,
                        "scene_start_sec": round(start, 3),
                        "scene_end_sec": round(end, 3),
                        "scene_duration_sec": round(end - start, 3),
                        "scene_count": len(scenes),
                        "video_width": metadata.get("width") or row.get("width"),
                        "video_height": metadata.get("height") or row.get("height"),
                        "video_fps": metadata.get("fps") or row.get("fps"),
                        "global_slice_rank": _compute_global_slice_rank(
                            int(row.get("global_index", 0)), idx
                        ),
                    }
                )
                output_records.append(record)

        self.logger.info(
            f"Produced {len(output_records)} output records for split {payload.split_id}"
        )
        if not output_records:
            return None

        return SplitPayload.from_arrow(
            pa.Table.from_pylist(output_records),
            split_id=f"{payload.split_id}:scene-detect-{self.worker_id}",
        )


# Set operator_class after class definition
FFmpegSceneDetectConfig.operator_class = FFmpegSceneDetectOperator


@dataclass
class FFmpegSliceConfig(OperatorConfig):
    """Configuration for FFmpegSliceOperator."""

    slice_dir: str
    """Directory to store sliced video files."""

    min_scene_duration: float = 0.5
    """Minimum scene duration in seconds."""


class FFmpegSliceOperator(Operator):
    """Materialize binary slices for each detected scene."""

    def __init__(self, config: FFmpegSliceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        if not config.slice_dir:
            raise ValueError("slice_dir is required for FFmpegSliceOperator")
        self.slice_dir = Path(config.slice_dir).expanduser().resolve()
        self.slice_dir.mkdir(parents=True, exist_ok=True)
        self.min_duration = config.min_scene_duration

    def _build_slice_path(self, record: Dict[str, Any]) -> Path:
        video_uid = record.get("video_uid") or "video"
        scene_index = int(record.get("scene_index", 0))
        filename = f"{video_uid}_scene_{scene_index:04d}.mp4"
        return self.slice_dir / filename

    def _cut_scene(self, source_path: Path, start: float, end: float, dest_path: Path) -> None:
        duration = max(0.0, end - start)
        if duration < self.min_duration:
            end = start + self.min_duration
            duration = self.min_duration
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            f"{start:.3f}",
            "-to",
            f"{end:.3f}",
            "-i",
            str(source_path),
            "-c",
            "copy",
            str(dest_path),
        ]
        subprocess.run(cmd, check=True)

    def process_split(self, split, batch: Optional[SplitPayload] = None) -> Optional[SplitPayload]:
        if batch is None:
            raise ValueError("FFmpegSliceOperator requires a batch")

        rows = batch.to_pylist()
        outputs: List[Dict[str, Any]] = []

        for row in rows:
            video_path = row.get("video_path")
            if not video_path:
                raise ValueError(f"Missing video path for row {row}")

            local_source = Path(video_path)
            if not local_source.exists():
                self.logger.error("Missing video binary for %s", video_path)
                raise FileNotFoundError(f"Missing video binary at {video_path}")

            start = float(row.get("scene_start_sec", 0.0))
            end = float(row.get("scene_end_sec", start + self.min_duration))
            dest_path = self._build_slice_path(row)

            self._cut_scene(local_source, start, end, dest_path)

            record = dict(row)
            record.update(
                {
                    "slice_path": str(dest_path),
                    "slice_duration_sec": round(end - start, 3),
                    "slice_size_bytes": dest_path.stat().st_size if dest_path.exists() else 0,
                }
            )
            outputs.append(record)

        if not outputs:
            return None

        return SplitPayload.from_arrow(
            pa.Table.from_pylist(outputs),
            split_id=f"{batch.split_id}:slice-{self.worker_id}",
        )


# Set operator_class after class definition
FFmpegSliceConfig.operator_class = FFmpegSliceOperator


def attach_slice_hash(record_value: Dict[str, Any]) -> Dict[str, Any]:
    """Map function compatible with MapOperator to hash emitted slice binaries."""
    slice_path = record_value.get("slice_path")
    if not slice_path:
        raise FileNotFoundError("slice_path missing for hashing")

    path = Path(slice_path)
    if not path.exists():
        raise FileNotFoundError(f"Slice binary missing for hashing: {slice_path}")

    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)

    enriched = dict(record_value)
    enriched["slice_sha256"] = hasher.hexdigest()
    enriched["slice_size_bytes"] = path.stat().st_size
    return enriched


def keep_every_n(record_value: Dict[str, Any], modulo: int) -> bool:
    rank = int(record_value.get("global_slice_rank", 0))
    if modulo <= 0:
        return True
    return rank % modulo == 0
