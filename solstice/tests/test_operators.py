"""Unit tests for operators (pure logic, no mocks)"""

from solstice.core.models import Record, Batch, Split, SplitStatus
from solstice.operators.map import MapOperator, FlatMapOperator
from solstice.operators.batch import MapBatchesOperator
from solstice.operators.filter import FilterOperator


class TestMapOperator:
    """Tests for MapOperator"""

    def test_map_operator_basic(self):
        """Test basic map operation"""

        def double_value(record):
            record["value"] *= 2
            return record

        operator = MapOperator({"map_fn": double_value})

        batch = Batch.from_records([Record(key="1", value={"value": 5})], batch_id="test")
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)
        result_batch = operator.process_split(split, batch)

        assert result_batch is not None
        results = result_batch.to_records()
        assert len(results) == 1
        assert results[0].value["value"] == 10

    def test_map_operator_with_error_skip(self):
        """Test map operator with error handling"""

        def failing_fn(record):
            raise ValueError("Test error")

        operator = MapOperator({"map_fn": failing_fn, "skip_on_error": True})

        batch = Batch.from_records([Record(key="1", value={"data": "test"})], batch_id="test")
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)
        result_batch = operator.process_split(split, batch)

        assert result_batch is None  # Empty result returns None

    def test_map_operator_multiple_fields(self):
        """Test map with multiple field transformations"""

        def transform(record):
            record["sum"] = record["a"] + record["b"]
            record["product"] = record["a"] * record["b"]
            return record

        operator = MapOperator({"map_fn": transform})

        batch = Batch.from_records([Record(key="1", value={"a": 3, "b": 4})], batch_id="test")
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)
        result_batch = operator.process_split(split, batch)

        assert result_batch is not None
        results = result_batch.to_records()
        assert results[0].value["sum"] == 7
        assert results[0].value["product"] == 12


class TestFlatMapOperator:
    """Tests for FlatMapOperator"""

    def test_flatmap_basic(self):
        """Test basic flatmap operation"""

        def split_fn(record):
            return [
                {"id": 1, "part": record["part1"]},
                {"id": 2, "part": record["part2"]},
            ]

        operator = FlatMapOperator({"flatmap_fn": split_fn})

        batch = Batch.from_records([Record(key="1", value={"part1": "A", "part2": "B"})], batch_id="test")
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)
        result_batch = operator.process_split(split, batch)

        assert result_batch is not None
        results = result_batch.to_records()
        assert len(results) == 2
        assert results[0].value["id"] == 1
        assert results[1].value["id"] == 2

    def test_flatmap_empty_result(self):
        """Test flatmap that returns empty list"""

        def empty_fn(record):
            return []

        operator = FlatMapOperator({"flatmap_fn": empty_fn})

        batch = Batch.from_records([Record(key="1", value={"data": "test"})], batch_id="test")
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)
        result_batch = operator.process_split(split, batch)

        assert result_batch is None  # Empty result returns None

    def test_flatmap_variable_output(self):
        """Test flatmap with variable number of outputs"""

        def split_by_count(record):
            count = record.get("count", 1)
            return [{"index": i, "data": record["data"]} for i in range(count)]

        operator = FlatMapOperator({"flatmap_fn": split_by_count})
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)

        # 1 output
        batch1 = Batch.from_records([Record(key="1", value={"count": 1, "data": "A"})], batch_id="test1")
        result1 = operator.process_split(split, batch1)
        assert result1 is not None
        assert len(result1.to_records()) == 1

        # 3 outputs
        batch2 = Batch.from_records([Record(key="2", value={"count": 3, "data": "B"})], batch_id="test2")
        result2 = operator.process_split(split, batch2)
        assert result2 is not None
        assert len(result2.to_records()) == 3

        # 0 outputs
        batch3 = Batch.from_records([Record(key="3", value={"count": 0, "data": "C"})], batch_id="test3")
        result3 = operator.process_split(split, batch3)
        assert result3 is None  # Empty result returns None


class TestMapBatchesOperator:
    """Tests for MapBatchesOperator"""

    def test_map_batches_basic(self):
        """Test batch mapping operation"""

        def process_batch(batch: Batch):
            doubled = []
            for record in batch.to_records():
                doubled.append(Record(key=record.key, value={"value": record.value["value"] * 2}))
            return Batch.from_records(
                doubled,
                batch_id=batch.batch_id,
                source_split=batch.source_split,
            )

        operator = MapBatchesOperator({"map_batches_fn": process_batch})

        batch = Batch.from_records(
            [
                Record(key="1", value={"value": 1}),
                Record(key="2", value={"value": 2}),
                Record(key="3", value={"value": 3}),
            ],
            batch_id="batch1",
        )

        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)
        result_batch = operator.process_split(split, batch)

        result_records = result_batch.to_records()
        assert len(result_records) == 3
        assert result_records[0].value["value"] == 2
        assert result_records[1].value["value"] == 4
        assert result_records[2].value["value"] == 6

    def test_map_batches_skip_on_error(self):
        """Test batch mapping with error handling"""

        def failing_fn(batch: Batch):
            raise ValueError("Batch processing error")

        operator = MapBatchesOperator({"map_batches_fn": failing_fn, "skip_on_error": True})

        batch = Batch.from_records([Record(key="1", value={"data": "test"})], batch_id="batch1")
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)

        result_batch = operator.process_split(split, batch)
        assert result_batch is not None
        assert result_batch.is_empty()

    def test_map_batches_aggregation(self):
        """Test batch-level aggregation"""

        def aggregate_batch(batch: Batch):
            # Sum all values in batch
            records = batch.to_records()
            total = sum(r.value["value"] for r in records)
            avg = total / len(records) if records else 0
            return Batch.from_records(
                [
                    Record(key="aggregated", value={"total": total, "count": len(records), "avg": avg})
                ],
                batch_id=batch.batch_id,
                source_split=batch.source_split,
            )

        operator = MapBatchesOperator({"map_batches_fn": aggregate_batch})

        batch = Batch.from_records(
            [
                Record(key="1", value={"value": 10}),
                Record(key="2", value={"value": 20}),
                Record(key="3", value={"value": 30}),
            ],
            batch_id="batch1",
        )

        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)
        result_batch = operator.process_split(split, batch)

        assert result_batch is not None
        result_records = result_batch.to_records()
        assert len(result_records) == 1
        assert result_records[0].value["total"] == 60
        assert result_records[0].value["count"] == 3
        assert result_records[0].value["avg"] == 20.0


class TestFilterOperator:
    """Tests for FilterOperator"""

    def test_filter_basic(self):
        """Test basic filtering"""

        def is_even(record):
            return record["value"] % 2 == 0

        operator = FilterOperator({"filter_fn": is_even})
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)

        # Test even number (should pass)
        batch1 = Batch.from_records([Record(key="1", value={"value": 4})], batch_id="test1")
        result1 = operator.process_split(split, batch1)
        assert result1 is not None
        assert len(result1.to_records()) == 1

        # Test odd number (should be filtered out)
        batch2 = Batch.from_records([Record(key="2", value={"value": 5})], batch_id="test2")
        result2 = operator.process_split(split, batch2)
        assert result2 is None  # Empty result returns None

    def test_filter_with_complex_condition(self):
        """Test filter with complex condition"""

        def is_valid(record):
            return record.get("score", 0) > 0.5 and record.get("count", 0) > 10

        operator = FilterOperator({"filter_fn": is_valid})
        split = Split(split_id="test_split", stage_id="test_stage", data_range={}, status=SplitStatus.PENDING)

        # Should pass
        batch1 = Batch.from_records([Record(key="1", value={"score": 0.8, "count": 20})], batch_id="test1")
        result1 = operator.process_split(split, batch1)
        assert result1 is not None
        assert len(result1.to_records()) == 1

        # Should fail (low score)
        batch2 = Batch.from_records([Record(key="2", value={"score": 0.3, "count": 20})], batch_id="test2")
        result2 = operator.process_split(split, batch2)
        assert result2 is None  # Empty result returns None

        # Should fail (low count)
        batch3 = Batch.from_records([Record(key="3", value={"score": 0.8, "count": 5})], batch_id="test3")
        result3 = operator.process_split(split, batch3)
        assert result3 is None  # Empty result returns None

