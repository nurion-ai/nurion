"""Utilities to materialize a Lance table backed by on-disk video binaries."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence
from urllib.request import urlopen

import pyarrow as pa
from lance.dataset import write_dataset


RESOURCE_ROOT = Path(__file__).resolve().parent.parent / "testdata" / "resources"
VIDEO_DIR = RESOURCE_ROOT / "videos"
LANCE_DIR = RESOURCE_ROOT / "lance"
SLICE_DIR = RESOURCE_ROOT / "slices"

SAMPLE_SOURCES: Sequence[Dict[str, str]] = [
    {
        "slug": "filesamples_640x360",
        "url": "https://filesamples.com/samples/video/mp4/sample_640x360.mp4",
    },
    {
        "slug": "filesamples_ocean_audio",
        "url": "https://filesamples.com/samples/video/mp4/sample_960x400_ocean_with_audio.mp4",
    },
    {
        "slug": "filesamples_960x540",
        "url": "https://filesamples.com/samples/video/mp4/sample_960x540.mp4",
    },
    {
        "slug": "samplelib_5s",
        "url": "https://samplelib.com/lib/preview/mp4/sample-5s.mp4",
    },
    {
        "slug": "samplelib_10s",
        "url": "https://samplelib.com/lib/preview/mp4/sample-10s.mp4",
    },
    {
        "slug": "samplelib_15s",
        "url": "https://samplelib.com/lib/preview/mp4/sample-15s.mp4",
    },
    {
        "slug": "testvideos_bbb_720p",
        "url": "https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/720/Big_Buck_Bunny_720_10s_1MB.mp4",
    },
    {
        "slug": "gtv_blazes",
        "url": "https://storage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4",
    },
    {
        "slug": "gtv_escapes",
        "url": "https://storage.googleapis.com/gtv-videos-bucket/sample/ForBiggerEscapes.mp4",
    },
    {
        "slug": "gtv_joyrides",
        "url": "https://storage.googleapis.com/gtv-videos-bucket/sample/ForBiggerJoyrides.mp4",
    },
]

COPIES_PER_SOURCE = 10


@dataclass
class VideoDatasetInfo:
    lance_path: Path
    video_root: Path
    slice_root: Path


def _download_if_missing(url: str, dest: Path) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(url) as response, dest.open("wb") as fh:  # nosec B310
        shutil.copyfileobj(response, fh)


def _probe_video(path: Path) -> Dict[str, float]:
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
        str(path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(result.stdout or "{}")
    stream = (payload.get("streams") or [{}])[0]
    width = stream.get("width", 0)
    height = stream.get("height", 0)
    avg_rate = stream.get("avg_frame_rate", "0/1")
    fps = 0.0
    if avg_rate and avg_rate != "0/0":
        num, _, den = avg_rate.partition("/")
        try:
            fps = float(num) / float(den or 1)
        except ZeroDivisionError:
            fps = 0.0
    duration = 0.0
    fmt = payload.get("format") or {}
    if "duration" in fmt:
        try:
            duration = float(fmt["duration"])
        except ValueError:
            duration = 0.0
    return {
        "width": width,
        "height": height,
        "fps": fps,
        "duration_sec": duration,
    }


def ensure_video_metadata_table(dataset_root: Path | None = None) -> VideoDatasetInfo:
    base_root = Path(dataset_root) if dataset_root else RESOURCE_ROOT
    video_dir = base_root / "videos"
    lance_path = base_root / "lance" / "video_metadata"
    slice_root = base_root / "slices"

    video_dir.mkdir(parents=True, exist_ok=True)
    slice_root.mkdir(parents=True, exist_ok=True)

    if not lance_path.exists():
        records: List[Dict[str, Any]] = []
        global_index = 0

        for source in SAMPLE_SOURCES:
            base_file = video_dir / f"{source['slug']}.mp4"
            _download_if_missing(source["url"], base_file)
            meta = _probe_video(base_file)
            abs_base = base_file.resolve()

            for copy_idx in range(COPIES_PER_SOURCE):
                video_uid = f"{source['slug']}_{copy_idx:02d}"
                subset = "train" if global_index < 80 else "validation"
                record = {
                    "global_index": global_index,
                    "video_uid": video_uid,
                    "source_url": source["url"],
                    "video_path": str(abs_base),
                    "width": meta["width"],
                    "height": meta["height"],
                    "fps": meta["fps"],
                    "duration_sec": meta["duration_sec"],
                    "subset": subset,
                    "target_slice_count": 5,
                }
                records.append(record)
                global_index += 1

        lance_path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(records)
        write_dataset(table, str(lance_path), mode="overwrite")

    return VideoDatasetInfo(
        lance_path=lance_path,
        video_root=video_dir,
        slice_root=slice_root,
    )
