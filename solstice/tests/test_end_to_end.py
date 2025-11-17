"""End-to-end tests for Solstice framework.

These tests verify the complete pipeline from source to sink,
including split planning, processing, and metrics collection.
"""

from solstice.core.operator import SourceOperator, Operator, SinkOperator
from solstice.core.models import Record, Batch, Split, SplitStatus


class TestSourceOperator(SourceOperator):
    """Test source operator that generates a fixed number of records."""

    def plan_splits(self):
        """Plan splits - create one split per batch."""
        num_splits = self.config.get("num_splits", 3)
        records_per_split = self.config.get("records_per_split", 10)

        splits = []
        for i in range(num_splits):
            split = Split(
                split_id=f"source_split_{i}",
                stage_id=self.config.get("stage_id", "source"),
                data_range={"split_index": i, "records_per_split": records_per_split},
                record_count=records_per_split,
                status=SplitStatus.PENDING,
            )
            splits.append(split)

        return splits

    def read(self, split: Split) -> Batch:
        """Read data for a split - generate test records."""
        records_per_split = split.data_range.get("records_per_split", 10)
        split_index = split.data_range.get("split_index", 0)

        records = []
        start_idx = split_index * records_per_split
        for i in range(records_per_split):
            records.append(
                Record(
                    key=f"key_{start_idx + i}",
                    value={"number": start_idx + i, "split": split_index},
                )
            )

        return Batch.from_records(
            records,
            batch_id=f"batch_{split.split_id}",
            source_split=split.split_id,
        )


class TestMapOperator(Operator):
    """Test map operator that doubles the number value."""

    def process_split(self, split: Split, batch: Batch = None) -> Batch:
        """Double the number value in each record."""
        if batch is None:
            raise ValueError("MapOperator requires batch")

        output_records = []
        for record in batch.to_records():
            new_value = record.value.copy()
            new_value["number"] = record.value["number"] * 2
            output_records.append(
                Record(
                    key=record.key,
                    value=new_value,
                    timestamp=record.timestamp,
                    metadata=record.metadata,
                )
            )

        return Batch.from_records(
            output_records,
            batch_id=batch.batch_id,
            source_split=batch.source_split,
        )


class TestSinkOperator(SinkOperator):
    """Test sink operator that collects records."""

    def __init__(self, config=None):
        super().__init__(config)
        self.collected_records = []

    def write(self, record: Record) -> None:
        """Collect the record."""
        self.collected_records.append(record)

    def get_collected(self):
        """Get all collected records."""
        return self.collected_records


class TestEndToEnd:
    """End-to-end tests for complete pipeline execution."""

    def test_simple_pipeline(self):
        """Test a simple source -> map -> sink pipeline."""
        # Create operators
        source_op = TestSourceOperator(
            {
                "num_splits": 2,
                "records_per_split": 5,
                "stage_id": "source",
            }
        )

        map_op = TestMapOperator()

        sink_op = TestSinkOperator()

        # Plan splits
        splits = source_op.plan_splits()
        assert len(splits) == 2

        # Process each split through the pipeline
        for split in splits:
            # Source: read data
            batch = source_op.read(split)
            assert batch is not None
            assert len(batch) == 5

            # Map: transform data
            result_batch = map_op.process_split(split, batch)
            assert result_batch is not None
            assert len(result_batch) == 5

            # Sink: write data
            for record in result_batch.to_records():
                sink_op.write(record)

        # Verify results
        collected = sink_op.get_collected()
        assert len(collected) == 10  # 2 splits * 5 records each

        # Verify all numbers were doubled
        for record in collected:
            original_number = record.value["number"] / 2
            assert record.value["number"] == original_number * 2
            assert record.value["number"] % 2 == 0  # All should be even

    def test_source_operator_master(self):
        """Test that SourceOperatorMaster correctly plans and generates splits."""
        from solstice.core.operator_master import SourceOperatorMaster

        # Create operator master
        master = SourceOperatorMaster(
            job_id="test_job",
            stage_id="source",
            operator_class=TestSourceOperator,
            operator_config={
                "num_splits": 3,
                "records_per_split": 5,
                "stage_id": "source",
            },
        )

        # Test split planning
        assert master.get_planned_count() == 3

        # Request splits
        splits = list(master.on_split_requested(max_count=2))
        assert len(splits) == 2
        assert splits[0].split_id == "source_split_0"
        assert splits[1].split_id == "source_split_1"

        # Request more splits
        splits = list(master.on_split_requested(max_count=2))
        assert len(splits) == 1  # Only one left
        assert splits[0].split_id == "source_split_2"

        # Request again - should be empty
        splits = list(master.on_split_requested(max_count=1))
        assert len(splits) == 0

        master.shutdown()

    def test_operator_process_split(self):
        """Test that operators correctly process splits."""
        # Test source operator
        source_op = TestSourceOperator(
            {
                "num_splits": 1,
                "records_per_split": 3,
                "stage_id": "source",
            }
        )

        # Plan splits
        splits = source_op.plan_splits()
        assert len(splits) == 1

        # Read data
        batch = source_op.read(splits[0])
        assert batch is not None
        assert len(batch) == 3

        # Test map operator
        map_op = TestMapOperator()
        result_batch = map_op.process_split(splits[0], batch)
        assert result_batch is not None
        assert len(result_batch) == 3

        records = result_batch.to_records()
        assert records[0].value["number"] == 0  # 0 * 2 = 0
        assert records[1].value["number"] == 2  # 1 * 2 = 2
        assert records[2].value["number"] == 4  # 2 * 2 = 4

        # Test sink operator
        sink_op = TestSinkOperator()
        for record in result_batch.to_records():
            sink_op.write(record)

        collected = sink_op.get_collected()
        assert len(collected) == 3
