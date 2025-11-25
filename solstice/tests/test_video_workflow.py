"""Ray-based end-to-end test for the video slice workflow."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path


from solstice.state.backend import LocalStateBackend
from solstice.tests.utils.video_dataset import ensure_video_metadata_table

logger = logging.getLogger("test")

testdata_root = Path(__file__).parent / "testdata" / "resources"

tmp_path = testdata_root / "tmp"

if tmp_path.exists():
    shutil.rmtree(tmp_path)
tmp_path.mkdir(parents=True, exist_ok=True)


def test_video_slice_workflow_with_ray():
    """Verify scene detection, slicing, filtering, and hashing on real binaries."""
    dataset_info = ensure_video_metadata_table()
    lance_path = str(dataset_info.lance_path)
    slice_root = dataset_info.slice_root

    if slice_root.exists():
        shutil.rmtree(slice_root)
    slice_root.mkdir(parents=True, exist_ok=True)

    output_path = tmp_path / "hashed_slices.json"
    backend = LocalStateBackend(str(tmp_path / "state"))

    filter_modulo = 10
    from workflows.video_slice_workflow import create_job

    job = create_job(
        job_id="video_slice_ray_test",
        config={
            "input": lance_path,
            "output": str(output_path),
            "filter_modulo": filter_modulo,
            "slice_dir": str(slice_root),
            "scene_threshold": 0.4,
            "source_batch_size": 16,
        },
        state_backend=backend,
    )
    logger

    runner = job.create_ray_runner(
        ray_init_kwargs={
            "include_dashboard": True,
            "log_to_driver": True,
            "logging_level": logging.DEBUG,
        }
    )
    try:
        runner.run(poll_interval=1, timeout=1000)
    finally:
        runner.shutdown()

    assert output_path.exists()
    with output_path.open() as fh:
        payloads = [json.loads(line) for line in fh if line.strip()]

    assert payloads, "Expected filtered slice payloads"
    for entry in payloads:
        value = entry["value"]
        digest = value.get("slice_sha256")
        assert isinstance(digest, str) and len(digest) == 64
        assert int(value["global_slice_rank"]) % filter_modulo == 0
        slice_path = value.get("slice_path")
        assert slice_path
        assert Path(slice_path).exists()


test_video_slice_workflow_with_ray()
