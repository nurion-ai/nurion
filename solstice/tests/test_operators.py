"""Unit tests for operators (pure logic, no mocks)"""

from solstice.core.operator import OperatorContext
from solstice.core.models import Record, Batch
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
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        record = Record(key="1", value={"value": 5})
        results = list(operator.process(record))

        assert len(results) == 1
        assert results[0].value["value"] == 10

    def test_map_operator_with_error_skip(self):
        """Test map operator with error handling"""

        def failing_fn(record):
            raise ValueError("Test error")

        operator = MapOperator({"map_fn": failing_fn, "skip_on_error": True})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        record = Record(key="1", value={"data": "test"})
        results = list(operator.process(record))

        assert len(results) == 0

    def test_map_operator_multiple_fields(self):
        """Test map with multiple field transformations"""

        def transform(record):
            record["sum"] = record["a"] + record["b"]
            record["product"] = record["a"] * record["b"]
            return record

        operator = MapOperator({"map_fn": transform})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        record = Record(key="1", value={"a": 3, "b": 4})
        results = list(operator.process(record))

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
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        record = Record(key="1", value={"part1": "A", "part2": "B"})
        results = list(operator.process(record))

        assert len(results) == 2
        assert results[0].value["id"] == 1
        assert results[1].value["id"] == 2

    def test_flatmap_empty_result(self):
        """Test flatmap that returns empty list"""

        def empty_fn(record):
            return []

        operator = FlatMapOperator({"flatmap_fn": empty_fn})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        record = Record(key="1", value={"data": "test"})
        results = list(operator.process(record))

        assert len(results) == 0

    def test_flatmap_variable_output(self):
        """Test flatmap with variable number of outputs"""

        def split_by_count(record):
            count = record.get("count", 1)
            return [{"index": i, "data": record["data"]} for i in range(count)]

        operator = FlatMapOperator({"flatmap_fn": split_by_count})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        # 1 output
        r1 = Record(key="1", value={"count": 1, "data": "A"})
        assert len(list(operator.process(r1))) == 1

        # 3 outputs
        r2 = Record(key="2", value={"count": 3, "data": "B"})
        assert len(list(operator.process(r2))) == 3

        # 0 outputs
        r3 = Record(key="3", value={"count": 0, "data": "C"})
        assert len(list(operator.process(r3))) == 0


class TestMapBatchesOperator:
    """Tests for MapBatchesOperator"""

    def test_map_batches_basic(self):
        """Test batch mapping operation"""

        def process_batch(records):
            # Double all values in batch
            return [Record(key=r.key, value={"value": r.value["value"] * 2}) for r in records]

        operator = MapBatchesOperator({"map_batches_fn": process_batch})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        batch = Batch(
            records=[
                Record(key="1", value={"value": 1}),
                Record(key="2", value={"value": 2}),
                Record(key="3", value={"value": 3}),
            ],
            batch_id="batch1",
        )

        result_batch = operator.process_batch(batch)

        assert len(result_batch.records) == 3
        assert result_batch.records[0].value["value"] == 2
        assert result_batch.records[1].value["value"] == 4
        assert result_batch.records[2].value["value"] == 6

    def test_map_batches_skip_on_error(self):
        """Test batch mapping with error handling"""

        def failing_fn(records):
            raise ValueError("Batch processing error")

        operator = MapBatchesOperator({"map_batches_fn": failing_fn, "skip_on_error": True})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        batch = Batch(records=[Record(key="1", value={"data": "test"})], batch_id="batch1")

        result_batch = operator.process_batch(batch)
        assert len(result_batch.records) == 0

    def test_map_batches_aggregation(self):
        """Test batch-level aggregation"""

        def aggregate_batch(records):
            # Sum all values in batch
            total = sum(r.value["value"] for r in records)
            avg = total / len(records) if records else 0

            return [
                Record(key="aggregated", value={"total": total, "count": len(records), "avg": avg})
            ]

        operator = MapBatchesOperator({"map_batches_fn": aggregate_batch})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        batch = Batch(
            records=[
                Record(key="1", value={"value": 10}),
                Record(key="2", value={"value": 20}),
                Record(key="3", value={"value": 30}),
            ],
            batch_id="batch1",
        )

        result_batch = operator.process_batch(batch)

        assert len(result_batch.records) == 1
        assert result_batch.records[0].value["total"] == 60
        assert result_batch.records[0].value["count"] == 3
        assert result_batch.records[0].value["avg"] == 20.0


class TestFilterOperator:
    """Tests for FilterOperator"""

    def test_filter_basic(self):
        """Test basic filtering"""

        def is_even(record):
            return record["value"] % 2 == 0

        operator = FilterOperator({"filter_fn": is_even})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        # Test even number (should pass)
        record1 = Record(key="1", value={"value": 4})
        results1 = list(operator.process(record1))
        assert len(results1) == 1

        # Test odd number (should be filtered out)
        record2 = Record(key="2", value={"value": 5})
        results2 = list(operator.process(record2))
        assert len(results2) == 0

    def test_filter_with_complex_condition(self):
        """Test filter with complex condition"""

        def is_valid(record):
            return record.get("score", 0) > 0.5 and record.get("count", 0) > 10

        operator = FilterOperator({"filter_fn": is_valid})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        # Should pass
        record1 = Record(key="1", value={"score": 0.8, "count": 20})
        assert len(list(operator.process(record1))) == 1

        # Should fail (low score)
        record2 = Record(key="2", value={"score": 0.3, "count": 20})
        assert len(list(operator.process(record2))) == 0

        # Should fail (low count)
        record3 = Record(key="3", value={"score": 0.8, "count": 5})
        assert len(list(operator.process(record3))) == 0


class TestOperatorCheckpointing:
    """Tests for operator checkpoint and restore"""

    def test_operator_checkpoint_restore(self):
        """Test operator state checkpoint and restore"""

        def transform(record):
            record["processed"] = True
            return record

        operator = MapOperator({"map_fn": transform})
        context = OperatorContext("task1", "stage1", "worker1")
        operator.open(context)

        # Set some state
        context.set_state("counter", 42)
        context.set_state("last_key", "key123")

        # Checkpoint
        state = operator.checkpoint()

        assert state["counter"] == 42
        assert state["last_key"] == "key123"

        # Create new operator and restore
        operator2 = MapOperator({"map_fn": transform})
        context2 = OperatorContext("task1", "stage1", "worker1")
        operator2.open(context2)
        operator2.restore(state)

        assert context2.get_state("counter") == 42
        assert context2.get_state("last_key") == "key123"
