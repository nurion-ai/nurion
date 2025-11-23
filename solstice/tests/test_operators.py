"""Unit tests for built-in operators."""

from __future__ import annotations

import pyarrow as pa
import pytest

from solstice.core.models import Record, Split, SplitPayload
from solstice.operators.filter import FilterOperator
from solstice.operators.map import FlatMapOperator, MapBatchesOperator, MapOperator


def make_split(split_id: str = "split", stage_id: str = "stage") -> Split:
    return Split(split_id=split_id, stage_id=stage_id, data_range={})


def make_payload(values: list[dict], split_id: str = "split") -> SplitPayload:
    records = [Record(key=str(idx), value=value) for idx, value in enumerate(values)]
    return SplitPayload.from_records(records, split_id=split_id)


class TestMapOperator:
    def test_map_operator_transforms_records(self):
        def increment(value: dict) -> dict:
            return {"value": value["value"] + 1}

        operator = MapOperator({"map_fn": increment}, worker_id="worker-1")
        split = make_split()
        batch = make_payload([{"value": 1}, {"value": 41}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert [row.value["value"] for row in result.to_records()] == [2, 42]
        assert result.split_id.startswith(f"{split.split_id}_worker-1")

    def test_map_operator_returns_none_on_failure(self):
        def explode(_: dict) -> dict:
            raise RuntimeError("boom")

        operator = MapOperator({"map_fn": explode})
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

        operator = FlatMapOperator({"flatmap_fn": duplicate}, worker_id="w0")
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

        operator = FlatMapOperator({"flatmap_fn": drop_all})
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

        operator = MapBatchesOperator({"map_batches_fn": add_flag})
        split = make_split()
        batch = make_payload([{"value": 1}, {"value": 2}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert [row.value["flag"] for row in result.to_records()] == [True, True]

    def test_map_batches_enforces_length(self):
        def shrink(table: pa.Table) -> pa.Table:
            return table.slice(0, 1)

        operator = MapBatchesOperator({"map_batches_fn": shrink})
        split = make_split()
        batch = make_payload([{"value": 1}, {"value": 2}])

        with pytest.raises(ValueError):
            operator.process_split(split, batch)

    def test_map_batches_skip_on_error(self):
        def explode(_: pa.Table) -> pa.Table:
            raise RuntimeError("boom")

        operator = MapBatchesOperator({"map_batches_fn": explode, "skip_on_error": True})
        split = make_split()
        batch = make_payload([{"value": 1}])

        result = operator.process_split(split, batch)
        assert result is not None
        assert result.is_empty()


class TestFilterOperator:
    def test_filter_operator_keeps_matching_rows(self):
        def is_even(record_value: dict) -> bool:
            return record_value["value"] % 2 == 0

        operator = FilterOperator({"filter_fn": is_even})
        split = make_split()
        batch = make_payload([{"value": 2}, {"value": 3}, {"value": 4}])

        result = operator.process_split(split, batch)

        assert result is not None
        assert [row.value["value"] for row in result.to_records()] == [2, 4]

    def test_filter_operator_drops_all_rows_returns_none(self):
        operator = FilterOperator({"filter_fn": lambda record: record.get("keep", False)})
        split = make_split()
        batch = make_payload([{"keep": False}])

        result = operator.process_split(split, batch)
        assert result is not None
        assert result.is_empty()
