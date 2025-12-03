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
from solstice.utils.remote import ensure_local_file, is_remote_path

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

            self.logger.debug(f"Processing video {idx + 1}/{len(rows)}: {video_path}")
            
            # Handle both local and remote (S3) paths
            with ensure_local_file(video_path) as local_path:
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

                for scene_idx, (start, end) in enumerate(scenes):
                    record = dict(row)
                    record.update(
                        {
                            "scene_index": scene_idx,
                            "scene_start_sec": round(start, 3),
                            "scene_end_sec": round(end, 3),
                            "scene_duration_sec": round(end - start, 3),
                            "scene_count": len(scenes),
                            "video_width": metadata.get("width") or row.get("width"),
                            "video_height": metadata.get("height") or row.get("height"),
                            "video_fps": metadata.get("fps") or row.get("fps"),
                            "global_slice_rank": _compute_global_slice_rank(
                                int(row.get("global_index", 0)), scene_idx
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

    min_scene_duration: float = 0.5
    """Minimum scene duration in seconds."""


class FFmpegSliceOperator(Operator):
    """Materialize binary slices for each detected scene.
    
    Slices are stored as binary data (bytes) for Lance blob storage.
    """

    def __init__(self, config: FFmpegSliceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        self.min_duration = config.min_scene_duration

    def _build_slice_filename(self, record: Dict[str, Any]) -> str:
        video_uid = record.get("video_uid") or "video"
        scene_index = int(record.get("scene_index", 0))
        return f"{video_uid}_scene_{scene_index:04d}.mp4"

    def _cut_scene_to_bytes(self, source_path: Path, start: float, end: float) -> bytes:
        """Cut a scene and return the binary data."""
        import tempfile
        
        duration = max(0.0, end - start)
        if duration < self.min_duration:
            end = start + self.min_duration
        
        # Use temp file for ffmpeg output
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
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
                str(tmp_path),
            ]
            subprocess.run(cmd, check=True)
            
            # Read the binary data
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def process_split(self, split, batch: Optional[SplitPayload] = None) -> Optional[SplitPayload]:
        if batch is None:
            raise ValueError("FFmpegSliceOperator requires a batch")

        rows = batch.to_pylist()
        outputs: List[Dict[str, Any]] = []

        for row in rows:
            video_path = row.get("video_path")
            if not video_path:
                raise ValueError(f"Missing video path for row {row}")

            start = float(row.get("scene_start_sec", 0.0))
            end = float(row.get("scene_end_sec", start + self.min_duration))
            slice_filename = self._build_slice_filename(row)

            # Handle both local and remote (S3) paths
            with ensure_local_file(video_path) as local_source:
                slice_binary = self._cut_scene_to_bytes(local_source, start, end)

            record = dict(row)
            record.update({
                "slice_filename": slice_filename,
                "slice_duration_sec": round(end - start, 3),
                "slice_size_bytes": len(slice_binary),
                "slice_binary": slice_binary,
            })
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
    """Map function compatible with MapOperator to hash emitted slice binaries.
    
    Supports both embedded binary data (slice_binary) and file-based slices (slice_path).
    """
    enriched = dict(record_value)
    hasher = hashlib.sha256()
    
    # First try embedded binary (Lance blob mode)
    slice_binary = record_value.get("slice_binary")
    if slice_binary is not None:
        hasher.update(slice_binary)
        enriched["slice_sha256"] = hasher.hexdigest()
        enriched["slice_size_bytes"] = len(slice_binary)
        return enriched
    
    # Fall back to file-based mode
    slice_path = record_value.get("slice_path")
    if not slice_path:
        raise FileNotFoundError("Neither slice_binary nor slice_path available for hashing")

    path = Path(slice_path)
    if not path.exists():
        raise FileNotFoundError(f"Slice binary missing for hashing: {slice_path}")

    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)

    enriched["slice_sha256"] = hasher.hexdigest()
    enriched["slice_size_bytes"] = path.stat().st_size
    return enriched


def keep_every_n(record_value: Dict[str, Any], modulo: int) -> bool:
    rank = int(record_value.get("global_slice_rank", 0))
    if modulo <= 0:
        return True
    return rank % modulo == 0
