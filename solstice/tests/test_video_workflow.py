"""Ray-based end-to-end test for the video slice workflow."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import lance
import pytest

from solstice.state.store import LocalCheckpointStore
from tests.utils.video_dataset import ensure_video_metadata_table

logger = logging.getLogger("test")


@pytest.mark.timeout(1200)
def test_video_slice_workflow_with_ray():
    """Verify scene detection, slicing, filtering, and hashing on real binaries."""
    testdata_root = Path(__file__).parent / "testdata" / "resources"
    tmp_path = testdata_root / "tmp"

    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)

    dataset_info = ensure_video_metadata_table()
    lance_path = str(dataset_info.lance_path)

    output_path = tmp_path / "hashed_slices.lance"
    checkpoint_store = LocalCheckpointStore(str(tmp_path / "checkpoints"))

    filter_modulo = 10
    from workflows.video_slice_workflow import create_job

    job = create_job(
        job_id="video_slice_ray_test",
        config={
            "input": lance_path,
            "output": str(output_path),
            "output_format": "lance",
            "filter_modulo": filter_modulo,
            "scene_threshold": 0.4,
            "source_batch_size": 16,
            "sink_buffer_size": 64,
        },
        checkpoint_store=checkpoint_store,
    )

    runner = job.create_ray_runner(
        ray_init_kwargs={
            "num_cpus": 20,
            "include_dashboard": True,
            "log_to_driver": True,
            "logging_level": logging.DEBUG,
            "runtime_env": {
                "excludes": [
                    # Exclude large test data files from being uploaded to Ray cluster
                    "tests/testdata/resources/",
                    "*.mp4",
                    "*.tar.gz",
                    "*.tar",
                    ".cache/",
                    # Exclude virtual environments to avoid module conflicts
                    ".venv/",
                    "venv/",
                    "__pycache__/",
                    "*.pyc",
                    # Exclude other large/unnecessary directories
                    ".git/",
                    "*.egg-info/",
                ],
            },
        }
    )
    try:
        runner.run(poll_interval=1, timeout=1000)
    finally:
        runner.shutdown()
        checkpoint_store.close()

    assert output_path.exists()
    ds = lance.dataset(str(output_path))
    rows = ds.to_table().to_pylist()

    assert rows, "Expected filtered slice payloads"
    for row in rows:
        # Check hash
        digest = row.get("slice_sha256")
        assert isinstance(digest, str) and len(digest) == 64, f"Invalid hash: {digest}"
        # Check filter modulo
        assert int(row["global_slice_rank"]) % filter_modulo == 0
        # Check binary slice data
        slice_binary = row.get("slice_binary")
        assert slice_binary is not None, "Missing slice_binary"
        assert len(slice_binary) > 0, "Empty slice_binary"
