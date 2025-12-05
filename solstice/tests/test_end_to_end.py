"""Tests for the local runner using lightweight operators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import pytest

from solstice.core.job import Job
from solstice.core.models import Record, Split, SplitPayload
from solstice.core.operator import SourceOperator, OperatorConfig
from solstice.core.stage import Stage
from solstice.operators.filter import FilterOperatorConfig
from solstice.operators.map import MapOperatorConfig
from solstice.runtime.local_runner import LocalJobRunner
from solstice.state.store import LocalCheckpointStore


@dataclass
class ListSourceConfig(OperatorConfig):
    """Config for ListSourceOperator."""

    stage_id: str = "source"
    batches: List[List[dict]] = field(default_factory=list)


class ListSourceOperator(SourceOperator):
    """In-memory source that materializes configured batches."""

    def __init__(self, config: ListSourceConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)
        self._stage_id = config.stage_id
        self._batches: List[List[dict]] = [list(batch) for batch in config.batches]

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


# Set operator_class after class definition
ListSourceConfig.operator_class = ListSourceOperator


@dataclass
class ManualSourceConfig(OperatorConfig):
    """Config for ManualSourceOperator."""

    pass


class ManualSourceOperator(SourceOperator):
    """SourceOperator that expects splits to be provided externally."""

    def read(self, split: Split) -> SplitPayload:
        payload = [
            Record(key=f"{split.split_id}_{idx}", value=value)
            for idx, value in enumerate(split.data_range["records"])
        ]
        return SplitPayload.from_records(payload, split_id=split.split_id)


# Set operator_class after class definition
ManualSourceConfig.operator_class = ManualSourceOperator


def make_job(tmp_path, stages: List[Stage]) -> Job:
    store = LocalCheckpointStore(str(tmp_path / "checkpoints"))
    job = Job(job_id="local-runner-tests", checkpoint_store=store)
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
        operator_config=ListSourceConfig(
            stage_id="source",
            batches=[
                [{"value": 1}, {"value": 2}],
                [{"value": 3}, {"value": 4}],
            ],
        ),
    )
    map_stage = Stage(
        stage_id="double",
        operator_config=MapOperatorConfig(
            map_fn=lambda val: {"value": val["value"] * 2},
        ),
    )
    filter_stage = Stage(
        stage_id="filter",
        operator_config=FilterOperatorConfig(
            filter_fn=lambda val: val["value"] >= 6,
        ),
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
    source_stage = Stage(
        stage_id="manual_source",
        operator_config=ManualSourceConfig(),
    )
    map_stage = Stage(
        stage_id="increment",
        operator_config=MapOperatorConfig(
            map_fn=lambda val: {"value": val["value"] + 1},
        ),
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
        operator_config=ListSourceConfig(
            batches=[[{"value": 1}], [{"value": 2}]],
        ),
    )
    map_stage = Stage(
        stage_id="map",
        operator_config=MapOperatorConfig(
            map_fn=lambda val: {"value": val["value"]},
        ),
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
