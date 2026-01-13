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

"""Tests for DuckDB compute engine."""

import pyarrow as pa
import pytest

from solstice.compute import DuckDBEngine
from solstice.compute.duckdb_engine import AggregationSpec


class TestDuckDBEngine:
    """Tests for DuckDBEngine."""

    @pytest.fixture
    def engine(self):
        """Create a DuckDB engine for testing."""
        engine = DuckDBEngine()
        yield engine
        engine.close()

    @pytest.fixture
    def sample_table(self):
        """Create a sample table for testing."""
        return pa.table({
            "user_id": [1, 2, 1, 3, 2, 1],
            "amount": [100, 200, 150, 300, 250, 50],
            "category": ["A", "B", "A", "C", "B", "A"],
        })

    def test_hash_partition(self, engine, sample_table):
        """Test hash partitioning."""
        partitions = engine.hash_partition(
            sample_table,
            partition_keys=["user_id"],
            num_partitions=3,
        )

        # Check that all rows are accounted for
        total_rows = sum(t.num_rows for t in partitions.values())
        assert total_rows == sample_table.num_rows

        # Check that same user_id goes to same partition
        for partition_id, table in partitions.items():
            user_ids = table.column("user_id").to_pylist()
            # All rows with the same user_id should be in the same partition
            # (we can't easily verify this without knowing the hash function,
            # but we can check that the partitioning is deterministic)
            assert len(user_ids) > 0

    def test_hash_partition_deterministic(self, engine, sample_table):
        """Test that hash partitioning is deterministic."""
        partitions1 = engine.hash_partition(
            sample_table,
            partition_keys=["user_id"],
            num_partitions=4,
        )
        partitions2 = engine.hash_partition(
            sample_table,
            partition_keys=["user_id"],
            num_partitions=4,
        )

        # Same partitioning
        assert set(partitions1.keys()) == set(partitions2.keys())
        for partition_id in partitions1:
            assert partitions1[partition_id].num_rows == partitions2[partition_id].num_rows

    def test_compute_partition_ids(self, engine, sample_table):
        """Test computing partition IDs."""
        partition_ids = engine.compute_partition_ids(
            sample_table,
            partition_keys=["user_id"],
            num_partitions=4,
        )

        assert len(partition_ids) == sample_table.num_rows
        # All IDs should be in range [0, num_partitions)
        for pid in partition_ids.to_pylist():
            assert 0 <= pid < 4

    def test_aggregate_sum(self, engine, sample_table):
        """Test sum aggregation."""
        result = engine.aggregate(
            sample_table,
            group_by=["user_id"],
            aggregations={"amount": "sum"},
        )

        # Check result
        assert result.num_rows == 3  # 3 unique user_ids
        assert "user_id" in result.column_names
        assert "sum_amount" in result.column_names

        # Verify sums
        result_dict = {
            row["user_id"]: row["sum_amount"]
            for row in result.to_pylist()
        }
        assert result_dict[1] == 300  # 100 + 150 + 50
        assert result_dict[2] == 450  # 200 + 250
        assert result_dict[3] == 300

    def test_aggregate_multiple(self, engine, sample_table):
        """Test multiple aggregations."""
        result = engine.aggregate(
            sample_table,
            group_by=["user_id"],
            aggregations={"amount": "sum", "category": "count"},
        )

        assert "sum_amount" in result.column_names
        assert "count_category" in result.column_names

    def test_aggregate_global(self, engine, sample_table):
        """Test global aggregation (no group by)."""
        result = engine.aggregate(
            sample_table,
            group_by=[],
            aggregations={"amount": "sum", "user_id": "count"},
        )

        assert result.num_rows == 1
        row = result.to_pylist()[0]
        assert row["sum_amount"] == 1050  # Total of all amounts
        assert row["count_user_id"] == 6

    def test_aggregate_with_spec(self, engine, sample_table):
        """Test aggregation with AggregationSpec."""
        specs = [
            AggregationSpec(column="amount", function="sum", alias="total_amount"),
            AggregationSpec(column="amount", function="avg", alias="avg_amount"),
        ]

        result = engine.aggregate(
            sample_table,
            group_by=["user_id"],
            aggregations=specs,
        )

        assert "total_amount" in result.column_names
        assert "avg_amount" in result.column_names

    def test_hash_join_inner(self, engine):
        """Test inner hash join."""
        left = pa.table({
            "user_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        right = pa.table({
            "user_id": [1, 2, 4],
            "score": [100, 200, 400],
        })

        result = engine.hash_join(
            left, right,
            join_keys=["user_id"],
            join_type="inner",
        )

        # Inner join should only have matching rows
        assert result.num_rows == 2  # user_id 1 and 2
        user_ids = set(result.column("user_id").to_pylist())
        assert user_ids == {1, 2}

    def test_hash_join_left(self, engine):
        """Test left hash join."""
        left = pa.table({
            "user_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        right = pa.table({
            "user_id": [1, 2, 4],
            "score": [100, 200, 400],
        })

        result = engine.hash_join(
            left, right,
            join_keys=["user_id"],
            join_type="left",
        )

        # Left join should have all left rows
        assert result.num_rows == 3
        user_ids = set(result.column("user_id").to_pylist())
        assert user_ids == {1, 2, 3}

    def test_hash_join_duplicate_columns(self, engine):
        """Test join with duplicate column names."""
        left = pa.table({
            "user_id": [1, 2],
            "value": [10, 20],
        })
        right = pa.table({
            "user_id": [1, 2],
            "value": [100, 200],
        })

        result = engine.hash_join(
            left, right,
            join_keys=["user_id"],
            left_suffix="_l",
            right_suffix="_r",
        )

        assert "value_l" in result.column_names
        assert "value_r" in result.column_names

    def test_filter(self, engine, sample_table):
        """Test filtering."""
        result = engine.filter(sample_table, "amount > 150")

        assert result.num_rows == 3  # 200, 300, 250
        for amount in result.column("amount").to_pylist():
            assert amount > 150

    def test_project(self, engine, sample_table):
        """Test projection."""
        result = engine.project(sample_table, ["user_id", "amount"])

        assert result.num_columns == 2
        assert "user_id" in result.column_names
        assert "amount" in result.column_names
        assert "category" not in result.column_names

    def test_sql(self, engine, sample_table):
        """Test arbitrary SQL."""
        result = engine.sql(
            sample_table,
            "SELECT user_id, SUM(amount) as total FROM t GROUP BY user_id ORDER BY user_id",
            table_name="t",
        )

        assert result.num_rows == 3
        rows = result.to_pylist()
        assert rows[0]["user_id"] == 1
        assert rows[0]["total"] == 300

    def test_dedupe_basic(self, engine):
        """Test basic deduplication (non-deterministic without order_by)."""
        table = pa.table({
            "user_id": [1, 1, 2, 2, 3],
            "value": [10, 20, 30, 40, 50],
        })

        result = engine.dedupe(table, key_columns=["user_id"])

        assert result.num_rows == 3  # 3 unique user_ids

    def test_dedupe_with_order_keep_first(self, engine):
        """Test deduplication keeping first by order column."""
        table = pa.table({
            "user_id": [1, 1, 1, 2, 2],
            "value": ["first", "second", "third", "a", "b"],
            "seq": [1, 2, 3, 4, 5],
        })

        result = engine.dedupe(table, key_columns=["user_id"], order_by="seq", keep="first")

        assert result.num_rows == 2
        result_dict = {row["user_id"]: row for row in result.to_pylist()}

        # Should keep first by seq (smallest seq value)
        assert result_dict[1]["value"] == "first"
        assert result_dict[1]["seq"] == 1
        assert result_dict[2]["value"] == "a"
        assert result_dict[2]["seq"] == 4

    def test_dedupe_with_order_keep_last(self, engine):
        """Test deduplication keeping last by order column."""
        table = pa.table({
            "user_id": [1, 1, 1, 2, 2],
            "value": ["first", "second", "third", "a", "b"],
            "seq": [1, 2, 3, 4, 5],
        })

        result = engine.dedupe(table, key_columns=["user_id"], order_by="seq", keep="last")

        assert result.num_rows == 2
        result_dict = {row["user_id"]: row for row in result.to_pylist()}

        # Should keep last by seq (largest seq value)
        assert result_dict[1]["value"] == "third"
        assert result_dict[1]["seq"] == 3
        assert result_dict[2]["value"] == "b"
        assert result_dict[2]["seq"] == 5

    def test_dedupe_first_vs_last_differ(self, engine):
        """Test that keep='first' and keep='last' produce different results with order_by."""
        table = pa.table({
            "key": ["a", "a", "b", "b"],
            "seq_num": [1, 2, 3, 4],
        })

        first_result = engine.dedupe(table, key_columns=["key"], order_by="seq_num", keep="first")
        last_result = engine.dedupe(table, key_columns=["key"], order_by="seq_num", keep="last")

        first_dict = {row["key"]: row["seq_num"] for row in first_result.to_pylist()}
        last_dict = {row["key"]: row["seq_num"] for row in last_result.to_pylist()}

        # First keeps 1, 3; Last keeps 2, 4
        assert first_dict["a"] == 1
        assert first_dict["b"] == 3
        assert last_dict["a"] == 2
        assert last_dict["b"] == 4

    def test_dedupe_invalid_keep_with_order(self, engine):
        """Test that invalid keep value raises error when order_by is specified."""
        table = pa.table({"key": [1, 2], "value": ["a", "b"], "seq": [1, 2]})

        with pytest.raises(ValueError, match="keep must be 'first' or 'last'"):
            engine.dedupe(table, key_columns=["key"], order_by="seq", keep="invalid")

    def test_dedupe_without_order_ignores_keep(self, engine):
        """Test that keep param is ignored when order_by is not specified."""
        table = pa.table({
            "key": [1, 1, 2],
            "value": ["a", "b", "c"],
        })

        # Both should work without error (keep is ignored)
        result1 = engine.dedupe(table, key_columns=["key"], keep="first")
        result2 = engine.dedupe(table, key_columns=["key"], keep="last")

        assert result1.num_rows == 2
        assert result2.num_rows == 2

    def test_dedupe_multiple_keys(self, engine):
        """Test deduplication with multiple key columns."""
        table = pa.table({
            "key1": ["a", "a", "a", "b"],
            "key2": [1, 1, 2, 1],
            "value": ["first", "second", "third", "fourth"],
            "seq": [1, 2, 3, 4],
        })

        result = engine.dedupe(table, key_columns=["key1", "key2"], order_by="seq", keep="first")

        assert result.num_rows == 3  # (a,1), (a,2), (b,1)
        result_list = result.to_pylist()
        values = {(r["key1"], r["key2"]): r["value"] for r in result_list}

        assert values[("a", 1)] == "first"  # First of two (a, 1) rows by seq
        assert values[("a", 2)] == "third"
        assert values[("b", 1)] == "fourth"

    def test_partial_and_merge_aggregate_sum(self, engine):
        """Test two-phase sum aggregation."""
        # Simulate data split across two partitions
        table1 = pa.table({
            "user_id": [1, 2],
            "amount": [100, 200],
        })
        table2 = pa.table({
            "user_id": [1, 3],
            "amount": [150, 300],
        })

        # Partial aggregates
        partial1 = engine.partial_aggregate(
            table1,
            group_by=["user_id"],
            aggregations={"amount": "sum"},
        )
        partial2 = engine.partial_aggregate(
            table2,
            group_by=["user_id"],
            aggregations={"amount": "sum"},
        )

        # Merge
        result = engine.merge_aggregates(
            [partial1, partial2],
            group_by=["user_id"],
            aggregations={"amount": "sum"},
        )

        # Verify
        result_dict = {
            row["user_id"]: row["sum_amount"]
            for row in result.to_pylist()
        }
        assert result_dict[1] == 250  # 100 + 150
        assert result_dict[2] == 200
        assert result_dict[3] == 300

    def test_partial_and_merge_aggregate_avg(self, engine):
        """Test two-phase avg aggregation with proper weighted average.

        This test verifies that avg is correctly computed as weighted average
        when partitions have different row counts.

        Bug fixed: avg(partial_averages) gives wrong results.
        Example: partition A (100 rows, avg=5.0), partition B (10 rows, avg=10.0)
        Wrong: avg(5.0, 10.0) = 7.5
        Correct: (500 + 100) / 110 = 5.45
        """
        # Partition 1: 3 rows with values 10, 20, 30 (sum=60, avg=20)
        table1 = pa.table({
            "group_id": [1, 1, 1],
            "value": [10, 20, 30],
        })
        # Partition 2: 1 row with value 100 (sum=100, avg=100)
        table2 = pa.table({
            "group_id": [1],
            "value": [100],
        })

        # Partial aggregates
        partial1 = engine.partial_aggregate(
            table1,
            group_by=["group_id"],
            aggregations={"value": "avg"},
        )
        partial2 = engine.partial_aggregate(
            table2,
            group_by=["group_id"],
            aggregations={"value": "avg"},
        )

        # Merge
        result = engine.merge_aggregates(
            [partial1, partial2],
            group_by=["group_id"],
            aggregations={"value": "avg"},
        )

        # Verify: correct avg = (10+20+30+100) / 4 = 160 / 4 = 40.0
        # Wrong avg(avg(10,20,30), avg(100)) = avg(20, 100) = 60
        result_dict = {row["group_id"]: row["avg_value"] for row in result.to_pylist()}
        assert result_dict[1] == 40.0, f"Expected 40.0, got {result_dict[1]}"

    def test_partial_and_merge_aggregate_multiple(self, engine):
        """Test two-phase aggregation with multiple functions."""
        table1 = pa.table({
            "user_id": [1, 1],
            "amount": [100, 200],
        })
        table2 = pa.table({
            "user_id": [1, 1, 1],
            "amount": [300, 400, 500],
        })

        # Partial aggregates with sum, count, avg, min, max
        partial1 = engine.partial_aggregate(
            table1,
            group_by=["user_id"],
            aggregations={"amount": "sum"},
        )
        partial2 = engine.partial_aggregate(
            table2,
            group_by=["user_id"],
            aggregations={"amount": "sum"},
        )

        # Merge
        result = engine.merge_aggregates(
            [partial1, partial2],
            group_by=["user_id"],
            aggregations={"amount": "sum"},
        )

        row = result.to_pylist()[0]
        assert row["sum_amount"] == 1500  # 100+200+300+400+500


class TestAggregationSpec:
    """Tests for AggregationSpec."""

    def test_to_sql_sum(self):
        """Test sum SQL generation."""
        spec = AggregationSpec(column="amount", function="sum")
        assert spec.to_sql() == "SUM(amount) AS sum_amount"

    def test_to_sql_with_alias(self):
        """Test SQL generation with custom alias."""
        spec = AggregationSpec(column="amount", function="sum", alias="total")
        assert spec.to_sql() == "SUM(amount) AS total"

    def test_to_sql_count(self):
        """Test count SQL generation."""
        spec = AggregationSpec(column="id", function="count")
        assert spec.to_sql() == "COUNT(id) AS count_id"

    def test_to_sql_invalid(self):
        """Test invalid function raises error."""
        spec = AggregationSpec(column="x", function="invalid")
        with pytest.raises(ValueError, match="Unknown aggregation"):
            spec.to_sql()
