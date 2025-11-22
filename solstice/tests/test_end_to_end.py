"""Tests for the local runner using lightweight operators."""

from __future__ import annotations

from typing import List
import pytest

from solstice.core.job import Job
from solstice.core.models import Record, Split, SplitPayload
from solstice.core.operator import SourceOperator
from solstice.core.stage import Stage
from solstice.operators.filter import FilterOperator
from solstice.operators.map import MapOperator
from solstice.runtime.local_runner import LocalJobRunner
from solstice.state.backend import LocalStateBackend


class ListSourceOperator(SourceOperator):
    """In-memory source that materializes configured batches."""

    def __init__(self, config=None, worker_id=None):
        super().__init__(config, worker_id)
        cfg = config or {}
        self._stage_id = cfg.get("stage_id", "source")
        self._batches: List[List[dict]] = [list(batch) for batch in cfg.get("batches", [])]

    def plan_splits(self) -> List[Split]:
        splits: List[Split] = []
        for idx, batch in enumerate(self._batches):
            splits.append(
                Split(
                    split_id=f"{self._stage_id}_split_{idx}",
                    stage_id=self._stage_id,
                    data_range={"records": batch, "batch_index": idx},
                )
            )
        return splits

    def read(self, split: Split) -> SplitPayload:
        records = [
            Record(
                key=f"{split.split_id}_{idx}",
                value=value,
            )
            for idx, value in enumerate(split.data_range["records"])
        ]
        return SplitPayload.from_records(records, split_id=split.split_id)


class ManualSourceOperator(SourceOperator):
    """SourceOperator that expects splits to be provided externally."""

    def read(self, split: Split) -> SplitPayload:
        payload = [
            Record(key=f"{split.split_id}_{idx}", value=value)
            for idx, value in enumerate(split.data_range["records"])
        ]
        return SplitPayload.from_records(payload, split_id=split.split_id)


def make_job(tmp_path, stages: List[Stage]) -> Job:
    backend = LocalStateBackend(str(tmp_path / "state"))
    job = Job(job_id="local-runner-tests", state_backend=backend)
    for stage in stages:
        upstream = []
        if stage.stage_id != stages[0].stage_id:
            idx = stages.index(stage)
            upstream = [stages[idx - 1].stage_id]
        job.add_stage(stage, upstream_stages=upstream or None)
    return job


def test_local_runner_executes_pipeline(tmp_path):
    source_stage = Stage(
        stage_id="source",
        operator_class=ListSourceOperator,
        operator_config={
            "stage_id": "source",
            "batches": [
                [{"value": 1}, {"value": 2}],
                [{"value": 3}, {"value": 4}],
            ],
        },
    )
    map_stage = Stage(
        stage_id="double",
        operator_class=MapOperator,
        operator_config={"map_fn": lambda val: {"value": val["value"] * 2}},
    )
    filter_stage = Stage(
        stage_id="filter",
        operator_class=FilterOperator,
        operator_config={"filter_fn": lambda val: val["value"] >= 6},
    )

    job = make_job(tmp_path, [source_stage, map_stage, filter_stage])
    runner = LocalJobRunner(job)
    results = runner.run()

    assert "filter" in results
    filtered_records = [
        record.value["value"] for batch in results["filter"] for record in batch.to_records()
    ]
    assert filtered_records == [6, 8]


def test_local_runner_accepts_source_splits_argument(tmp_path):
    source_stage = Stage(stage_id="manual_source", operator_class=ManualSourceOperator)
    map_stage = Stage(
        stage_id="increment",
        operator_class=MapOperator,
        operator_config={"map_fn": lambda val: {"value": val["value"] + 1}},
    )
    job = make_job(tmp_path, [source_stage, map_stage])

    splits = [
        Split(
            split_id=f"manual_{idx}",
            stage_id="manual_source",
            data_range={"records": batch},
        )
        for idx, batch in enumerate([[{"value": 10}], [{"value": 20}]])
    ]

    runner = LocalJobRunner(job)
    results = runner.run(source_splits={"manual_source": splits})

    mapped = [r.value["value"] for batch in results["increment"] for r in batch.to_records()]
    assert mapped == [11, 21]


def test_local_runner_hooks_and_failure_injection(tmp_path):
    source_stage = Stage(
        stage_id="source",
        operator_class=ListSourceOperator,
        operator_config={"batches": [[{"value": 1}], [{"value": 2}]]},
    )
    map_stage = Stage(
        stage_id="map",
        operator_class=MapOperator,
        operator_config={"map_fn": lambda val: {"value": val["value"]}},
    )
    job = make_job(tmp_path, [source_stage, map_stage])
    runner = LocalJobRunner(job)

    calls: dict[str, list[str]] = {"before_stage": [], "after_stage": [], "before_batch": []}

    def before_stage(stage_id, _op):
        calls["before_stage"].append(stage_id)

    def after_stage(stage_id, _op):
        calls["after_stage"].append(stage_id)

    def before_batch(stage_id, batch, _op):
        calls["before_batch"].append(f"{stage_id}:{len(batch)}")

    def failure_injector(stage_id, batch, _op):
        if (
            stage_id == "map"
            and len(batch.to_records()) == 1
            and batch.to_records()[0].value["value"] == 2
        ):
            raise RuntimeError("Injected failure")

    with pytest.raises(RuntimeError):
        runner.run(
            before_stage=before_stage,
            after_stage=after_stage,
            before_batch=before_batch,
            failure_injector=failure_injector,
        )

    assert calls["before_stage"] == ["source", "map"]
    assert calls["after_stage"] == ["source"]
    assert calls["before_batch"][0] == "map:1"
