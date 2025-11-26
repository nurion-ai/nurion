"""Utilities to materialize a Lance table backed by on-disk video binaries."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse
from urllib.request import urlopen

import pyarrow as pa
from lance.dataset import write_dataset


RESOURCE_ROOT = Path(__file__).resolve().parent.parent / "testdata" / "resources"
VIDEO_DIR = RESOURCE_ROOT / "videos"
VIDEO_DOWNLOAD_CACHE = VIDEO_DIR / ".cache"

DEFAULT_VIDEO_ARCHIVE_URL = (
    "https://huggingface.co/datasets/lmms-lab/LLaVA-Video-178K/resolve/main/"
    "1_2_m_youtube_v0_1/1_2_m_youtube_v0_1_videos_1.tar.gz"
)
VIDEO_ARCHIVE_URL_ENV = "SOLSTICE_TEST_VIDEO_ARCHIVE_URL"
VIDEO_ARCHIVE_SHA_ENV = "SOLSTICE_TEST_VIDEO_ARCHIVE_SHA256"
VIDEO_ARCHIVE_FILENAME_ENV = "SOLSTICE_TEST_VIDEO_ARCHIVE_FILENAME"
VIDEO_SOURCE_OVERRIDE_ENV = "SOLSTICE_TEST_VIDEO_SOURCE_DIR"
VIDEO_LIMIT_ENV = "SOLSTICE_TEST_VIDEO_LIMIT"

DEFAULT_VIDEO_LIMIT = 100
COPIES_PER_SOURCE = 1
LOGGER = logging.getLogger(__name__)


def _video_archive_url() -> str:
    url = os.environ.get(VIDEO_ARCHIVE_URL_ENV, DEFAULT_VIDEO_ARCHIVE_URL)
    if not url:
        raise ValueError(
            "No video dataset archive URL configured. "
            f"Set the {VIDEO_ARCHIVE_URL_ENV} environment variable."
        )
    return url


def _determine_archive_filename(url: str) -> str:
    override = os.environ.get(VIDEO_ARCHIVE_FILENAME_ENV)
    if override:
        return override
    parsed = urlparse(url)
    candidate = Path(parsed.path).name
    return candidate or "solstice_test_videos.tar.gz"


def _strip_archive_suffix(filename: str) -> str:
    lowered = filename.lower()
    for suffix in (".tar.gz", ".tgz", ".tar", ".zip"):
        if lowered.endswith(suffix):
            return filename[: -len(suffix)]
    return Path(filename).stem


def _verify_sha256(file_path: Path, expected: str) -> None:
    hasher = hashlib.sha256()
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    digest = hasher.hexdigest()
    if digest.lower() != expected.lower():
        raise ValueError(f"SHA256 mismatch for {file_path.name}: expected {expected}, got {digest}")


def _ensure_archive_download(url: str, refresh: bool = False) -> Path:
    VIDEO_DOWNLOAD_CACHE.mkdir(parents=True, exist_ok=True)
    filename = _determine_archive_filename(url)
    archive_path = VIDEO_DOWNLOAD_CACHE / filename
    if not archive_path.exists():
        parsed = urlparse(url)
        LOGGER.info(
            "Downloading test video archive %s from host %s",
            filename,
            parsed.netloc or "unknown",
        )
    _download_if_missing(url, archive_path)
    expected_sha = os.environ.get(VIDEO_ARCHIVE_SHA_ENV)
    if expected_sha:
        _verify_sha256(archive_path, expected_sha)
    return archive_path


def _safe_extract_tar(archive: tarfile.TarFile, dest: Path) -> None:
    dest = dest.resolve()
    for member in archive.getmembers():
        target_path = (dest / member.name).resolve(strict=False)
        if not str(target_path).startswith(str(dest)):
            raise ValueError(f"Archive member {member.name} would extract outside of {dest}")
    # Use filter="data" to avoid DeprecationWarning in Python 3.14+
    archive.extractall(dest, filter="data")


def _extract_videos_from_archive(archive_path: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="solstice_video_", dir=str(dest_dir.parent)) as tmp:
        tmp_dir = Path(tmp)
        LOGGER.info("Extracting %s into %s", archive_path.name, tmp_dir)
        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as tar:
                _safe_extract_tar(tar, tmp_dir)
        else:
            shutil.unpack_archive(str(archive_path), str(tmp_dir))
        mp4_candidates = sorted(p for p in tmp_dir.rglob("*.mp4") if p.is_file())
        if not mp4_candidates:
            raise ValueError(f"No .mp4 files found after extracting {archive_path.name}")
        for candidate in mp4_candidates:
            target = dest_dir / candidate.name
            if target.exists():
                continue
            shutil.move(str(candidate), str(target))
        LOGGER.info("Materialized %d video binaries under %s", len(mp4_candidates), dest_dir)


def _source_override_root() -> Path | None:
    override = os.environ.get(VIDEO_SOURCE_OVERRIDE_ENV)
    if not override:
        return None
    override_path = Path(override).expanduser()
    if not override_path.exists():
        raise FileNotFoundError(f"Configured override directory {override_path} does not exist")
    return override_path


def _populate_sources_from_directory(source_root: Path, dest_root: Path) -> List[Path]:
    mp4_candidates = sorted(p for p in source_root.rglob("*.mp4") if p.is_file())
    if not mp4_candidates:
        raise ValueError(f"No .mp4 files found under {source_root}")
    localized: List[Path] = []
    for candidate in mp4_candidates:
        target = dest_root / candidate.name
        if candidate.resolve() == target.resolve(strict=False):
            localized.append(candidate)
            continue
        localized.append(_ensure_video_copy(candidate, target))
    return localized


def _ensure_source_videos(source_dir: Path, refresh: bool) -> List[Path]:
    # Note: We intentionally do NOT delete source_dir on refresh.
    # Source videos are expensive to download and can be reused across refreshes.
    # Only the Lance metadata table needs to be regenerated.
    source_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted(p for p in source_dir.glob("*.mp4") if p.is_file())
    if existing:
        return existing

    override_root = _source_override_root()
    if override_root:
        LOGGER.info("Using pre-existing video dataset at %s", override_root)
        localized = _populate_sources_from_directory(override_root, source_dir)
        if localized:
            return localized

    archive_url = _video_archive_url()
    archive_path = _ensure_archive_download(archive_url, refresh=refresh)
    _extract_videos_from_archive(archive_path, source_dir)
    populated = sorted(p for p in source_dir.glob("*.mp4") if p.is_file())
    if not populated:
        raise ValueError(
            f"Failed to populate any video binaries under {source_dir} from {archive_path}"
        )
    return populated


def _resolve_video_limit() -> int:
    override = os.environ.get(VIDEO_LIMIT_ENV)
    if not override:
        return DEFAULT_VIDEO_LIMIT
    try:
        value = int(override)
    except ValueError:
        LOGGER.warning(
            "Invalid %s=%s; falling back to %d videos",
            VIDEO_LIMIT_ENV,
            override,
            DEFAULT_VIDEO_LIMIT,
        )
        return DEFAULT_VIDEO_LIMIT
    return max(1, value)


def _discover_external_sources(limit: int, source_dir: Path, refresh: bool) -> List[Dict[str, Any]]:
    available_videos = _ensure_source_videos(source_dir, refresh=refresh)
    if not available_videos:
        raise ValueError("No video binaries available to build the dataset")
    ordered = sorted(available_videos, key=lambda path: path.name)
    if len(ordered) < limit:
        LOGGER.warning("Only %d video binaries available; requested %d", len(ordered), limit)
    specs: List[Dict[str, Any]] = []
    for path in ordered[:limit]:
        specs.append(
            {
                "slug": path.stem,
                "mode": "local",
                "path": str(path.resolve()),
                "meta": _probe_video(path),
            }
        )
    return specs


@dataclass
class VideoDatasetInfo:
    lance_path: Path
    video_root: Path
    slice_root: Path


def _download_if_missing(url: str, dest: Path) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    with urlopen(url) as response, tmp_path.open("wb") as fh:  # nosec B310
        shutil.copyfileobj(response, fh)
    tmp_path.replace(dest)


def _materialize_source_video(spec: Dict[str, Any], dest: Path) -> Path:
    mode = spec.get("mode", "local")
    if mode == "local":
        source_path = Path(spec["path"]).expanduser()
        if not source_path.exists():
            raise FileNotFoundError(f"Local source {source_path} not found for {spec.get('slug')}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            if source_path.resolve() == dest.resolve(strict=False):
                return dest
        except FileNotFoundError:
            pass
        return _ensure_video_copy(source_path, dest)
    else:
        url = spec.get("url")
        if not url:
            raise ValueError(f"Source {spec.get('slug')} missing url for download mode")
        _download_if_missing(url, dest)
        return dest


def _synthesize_video(dest: Path, duration_sec: int, color: str, resolution: str) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=c={color}:s={resolution}:d={duration_sec}",
        "-vf",
        "fps=30",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "30",
        "-pix_fmt",
        "yuv420p",
        str(dest),
    ]
    subprocess.run(cmd, check=True)


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


def ensure_video_metadata_table(
    dataset_root: Path | None = None, refresh: bool = False
) -> VideoDatasetInfo:
    base_root = Path(dataset_root) if dataset_root else RESOURCE_ROOT
    video_dir = base_root / "videos"
    source_dir = video_dir / "sources"
    lance_path = base_root / "lance" / "video_metadata"
    slice_root = base_root / "slices"

    video_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)
    slice_root.mkdir(parents=True, exist_ok=True)

    if refresh and lance_path.exists():
        shutil.rmtree(lance_path, ignore_errors=True)

    if refresh and source_dir.exists():
        shutil.rmtree(source_dir, ignore_errors=True)
        source_dir.mkdir(parents=True, exist_ok=True)

    if refresh:
        for stray_file in video_dir.glob("*.mp4"):
            try:
                stray_file.unlink()
            except OSError:
                pass

    if not lance_path.exists():
        records: List[Dict[str, Any]] = []
        global_index = 0

        max_videos = _resolve_video_limit()
        source_specs = _discover_external_sources(max_videos, source_dir, refresh=refresh)

        for source in source_specs:
            slug = source["slug"]
            base_file = source_dir / f"{slug}.mp4"
            materialized_path = _materialize_source_video(source, base_file)
            meta = source.get("meta") or _probe_video(materialized_path)

            for copy_idx in range(COPIES_PER_SOURCE):
                video_uid = f"{slug}_{copy_idx:02d}"
                subset = "train" if global_index < 80 else "validation"
                source_url = (
                    source.get("url")
                    or source.get("path")
                    or f"synthetic:{source.get('color', 'unknown')}"
                )
                record = {
                    "global_index": global_index,
                    "video_uid": video_uid,
                    "source_url": source_url,
                    "video_path": str(materialized_path.resolve()),
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


def _ensure_video_copy(source_file: Path, target_file: Path) -> Path:
    if target_file.exists():
        return target_file

    source_resolved = source_file.resolve()
    target_resolved = target_file.resolve(strict=False)
    if source_resolved == target_resolved:
        return target_file

    target_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source_resolved, target_file)
    except OSError:
        shutil.copy2(source_resolved, target_file)
    return target_file
