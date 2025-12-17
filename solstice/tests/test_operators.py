# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for built-in operators."""

from __future__ import annotations

import pyarrow as pa
import pytest

import json

from solstice.core.models import Record, Split, SplitPayload
from solstice.operators.filter import FilterOperatorConfig
from solstice.operators.map import (
    FlatMapOperatorConfig,
    MapBatchesOperatorConfig,
    MapOperatorConfig,
)
from solstice.operators.sinks.file import FileSinkConfig


def make_split(split_id: str = "split", stage_id: str = "stage") -> Split:
    return Split(split_id=split_id, stage_id=stage_id, data_range={})


def make_payload(values: list[dict], split_id: str = "split") -> SplitPayload:
    records = [Record(key=str(idx), value=value) for idx, value in enumerate(values)]
    return SplitPayload.from_records(records, split_id=split_id)


class TestMapOperator:
    def test_map_operator_transforms_records(self):
        def increment(value: dict) -> dict:
            return {"value": value["value"] + 1}

        config = MapOperatorConfig(map_fn=increment)
        operator = config.setup(worker_id="worker-1")
        split = make_split()
        batch = make_payload([{"value": 1}, {"value": 41}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert [row.value["value"] for row in result.to_records()] == [2, 42]
        assert result.split_id.startswith(f"{split.split_id}_worker-1")

    def test_map_operator_returns_none_on_failure(self):
        def explode(_: dict) -> dict:
            raise RuntimeError("boom")

        config = MapOperatorConfig(map_fn=explode)
        operator = config.setup()
        split = make_split()
        batch = make_payload([{"value": 1}])

        result = operator.process_split(split, batch)
        assert result is None


class TestFlatMapOperator:
    def test_flatmap_operator_expands_rows(self):
        def duplicate(table: pa.Table) -> pa.Table:
            rows = table.to_pylist()
            expanded = []
            for row in rows:
                expanded.append({**row, "copy": 0})
                expanded.append({**row, "copy": 1})
            return pa.Table.from_pylist(expanded)

        config = FlatMapOperatorConfig(flatmap_fn=duplicate)
        operator = config.setup(worker_id="w0")
        split = make_split()
        batch = make_payload([{"video": "a"}, {"video": "b"}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert len(result) == 4
        copies = [row.value["copy"] for row in result.to_records()]
        assert copies.count(0) == 2 and copies.count(1) == 2

    def test_flatmap_operator_empty_output(self):
        def drop_all(_: pa.Table) -> pa.Table:
            return pa.table({})

        config = FlatMapOperatorConfig(flatmap_fn=drop_all)
        operator = config.setup()
        split = make_split()
        batch = make_payload([{"video": "a"}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert len(result) == 0


class TestMapBatchesOperator:
    def test_map_batches_transforms_table(self):
        def add_flag(table: pa.Table) -> pa.Table:
            rows = [{**row, "flag": True} for row in table.to_pylist()]
            return pa.Table.from_pylist(rows)

        config = MapBatchesOperatorConfig(map_batches_fn=add_flag)
        operator = config.setup()
        split = make_split()
        batch = make_payload([{"value": 1}, {"value": 2}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert [row.value["flag"] for row in result.to_records()] == [True, True]

    def test_map_batches_enforces_length(self):
        def shrink(table: pa.Table) -> pa.Table:
            return table.slice(0, 1)

        config = MapBatchesOperatorConfig(map_batches_fn=shrink)
        operator = config.setup()
        split = make_split()
        batch = make_payload([{"value": 1}, {"value": 2}])

        with pytest.raises(ValueError):
            operator.process_split(split, batch)

    def test_map_batches_skip_on_error(self):
        def explode(_: pa.Table) -> pa.Table:
            raise RuntimeError("boom")

        config = MapBatchesOperatorConfig(map_batches_fn=explode, skip_on_error=True)
        operator = config.setup()
        split = make_split()
        batch = make_payload([{"value": 1}])

        result = operator.process_split(split, batch)
        assert result is not None
        assert result.is_empty()


class TestFilterOperator:
    def test_filter_operator_keeps_matching_rows(self):
        def is_even(record_value: dict) -> bool:
            return record_value["value"] % 2 == 0

        config = FilterOperatorConfig(filter_fn=is_even)
        operator = config.setup()
        split = make_split()
        batch = make_payload([{"value": 2}, {"value": 3}, {"value": 4}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert [row.value["value"] for row in result.to_records()] == [2, 4]

    def test_filter_operator_drops_all_rows_returns_none(self):
        config = FilterOperatorConfig(filter_fn=lambda record: record.get("keep", False))
        operator = config.setup()
        split = make_split()
        batch = make_payload([{"keep": False}])

        result = operator.process_split(split, batch)
        assert result is not None
        assert result.is_empty()


class TestFileSink:
    def test_json_sink_writes_to_explicit_file(self, tmp_path):
        output_file = tmp_path / "result.json"
        config = FileSinkConfig(
            output_path=str(output_file),
            format="json",
            buffer_size=1,
        )
        sink = config.setup(worker_id="sink_worker_0")
        split = make_split("sink-split")
        batch = make_payload([{"value": 1, "key": "k"}])

        sink.process_split(split, batch)
        sink.close()

        assert output_file.exists()
        with output_file.open() as fh:
            records = [json.loads(line) for line in fh if line.strip()]
        assert len(records) == 1
        assert records[0]["key"] == "0"
        assert records[0]["value"]["value"] == 1
