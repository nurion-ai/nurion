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

"""DuckDB compute engine for high-performance data processing.

This module provides a DuckDB-based compute engine for operations that
benefit from vectorized execution. Key features:

- **Per-worker instance**: Each worker creates its own DuckDB connection
- **Zero-copy Arrow**: Direct integration with PyArrow tables
- **Vectorized execution**: SIMD-optimized operations
- **SQL interface**: Familiar SQL for complex operations

Usage:
    engine = DuckDBEngine()

    # Hash partitioning
    partitioned = engine.hash_partition(table, ["user_id"], num_partitions=8)

    # Aggregation
    result = engine.aggregate(table, ["user_id"], {"amount": "sum", "count": "count"})

    # Join
    result = engine.hash_join(left, right, ["user_id"])

Note: DuckDB is embedded and cannot be shared across processes.
Each worker must create its own engine instance.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import pyarrow as pa

from solstice.utils.logging import create_ray_logger


@dataclass
class AggregationSpec:
    """Specification for an aggregation operation."""

    column: str
    function: str  # sum, count, min, max, avg, first, last
    alias: Optional[str] = None

    def to_sql(self) -> str:
        """Convert to SQL expression."""
        alias = self.alias or f"{self.function}_{self.column}"
        if self.function == "count":
            return f"COUNT({self.column}) AS {alias}"
        elif self.function == "sum":
            return f"SUM({self.column}) AS {alias}"
        elif self.function == "avg":
            return f"AVG({self.column}) AS {alias}"
        elif self.function == "min":
            return f"MIN({self.column}) AS {alias}"
        elif self.function == "max":
            return f"MAX({self.column}) AS {alias}"
        elif self.function == "first":
            return f"FIRST({self.column}) AS {alias}"
        elif self.function == "last":
            return f"LAST({self.column}) AS {alias}"
        else:
            raise ValueError(f"Unknown aggregation function: {self.function}")


class DuckDBEngine:
    """DuckDB-based compute engine for high-performance operations.

    This engine provides vectorized execution for common data operations:
    - Hash partitioning for shuffle
    - Aggregation with various functions
    - Hash join for combining tables
    - Filtering and projection

    Each worker should create its own instance. DuckDB connections
    cannot be shared across processes.

    Example:
        engine = DuckDBEngine()

        # Partition data for shuffle
        partitions = engine.hash_partition(
            table,
            partition_keys=["user_id"],
            num_partitions=8,
        )

        # Aggregate data
        result = engine.aggregate(
            table,
            group_by=["user_id"],
            aggregations={"amount": "sum", "count": "count"},
        )
    """

    def __init__(self, memory_limit: Optional[str] = None):
        """Initialize the DuckDB engine.

        Args:
            memory_limit: Optional memory limit (e.g., "1GB", "512MB")
        """
        import duckdb

        self.logger = create_ray_logger("DuckDBEngine")

        # Create in-memory database
        self.conn = duckdb.connect(":memory:")

        # Configure memory limit if specified
        if memory_limit:
            self.conn.execute(f"SET memory_limit='{memory_limit}'")

        # Enable parallel execution
        self.conn.execute("SET threads TO 4")

        self.logger.debug("DuckDB engine initialized")

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]

    def __del__(self):
        """Cleanup on garbage collection."""
        self.close()

    # === Hash Partitioning ===

    def hash_partition(
        self,
        table: pa.Table,
        partition_keys: List[str],
        num_partitions: int,
    ) -> Dict[int, pa.Table]:
        """Partition a table by hash of partition keys.

        This is used for shuffle operations to route data to the correct
        downstream partition.

        Args:
            table: Input Arrow table
            partition_keys: Columns to hash for partitioning
            num_partitions: Number of output partitions

        Returns:
            Dictionary mapping partition ID to Arrow table
        """
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")

        if not partition_keys:
            raise ValueError("partition_keys cannot be empty")

        # Register the table
        self.conn.register("input_table", table)

        # Build hash expression
        key_expr = ", ".join(partition_keys)
        hash_expr = f"hash({key_expr})"

        # Query with partition assignment
        query = f"""
            SELECT *, 
                   ABS({hash_expr}) % {num_partitions} AS __partition_id
            FROM input_table
        """

        result = self.conn.execute(query).fetch_arrow_table()

        # Split by partition
        partitions: Dict[int, pa.Table] = {}
        partition_col = result.column("__partition_id")

        for partition_id in range(num_partitions):
            # Filter rows for this partition
            mask = pa.compute.equal(partition_col, partition_id)
            partition_table = result.filter(mask)

            # Remove the partition column
            partition_table = partition_table.drop(["__partition_id"])

            if partition_table.num_rows > 0:
                partitions[partition_id] = partition_table

        # Cleanup
        self.conn.unregister("input_table")

        return partitions

    def compute_partition_ids(
        self,
        table: pa.Table,
        partition_keys: List[str],
        num_partitions: int,
    ) -> pa.Array:
        """Compute partition IDs for each row without splitting.

        This is useful when you need the partition assignment but want
        to handle the splitting yourself.

        Args:
            table: Input Arrow table
            partition_keys: Columns to hash for partitioning
            num_partitions: Number of partitions

        Returns:
            Arrow array of partition IDs (int32)
        """
        self.conn.register("input_table", table)

        key_expr = ", ".join(partition_keys)
        hash_expr = f"hash({key_expr})"

        query = f"""
            SELECT ABS({hash_expr}) % {num_partitions} AS partition_id
            FROM input_table
        """

        result = self.conn.execute(query).fetch_arrow_table()
        partition_ids = result.column("partition_id")

        self.conn.unregister("input_table")

        return partition_ids

    # === Aggregation ===

    def aggregate(
        self,
        table: pa.Table,
        group_by: List[str],
        aggregations: Union[Dict[str, str], List[AggregationSpec]],
    ) -> pa.Table:
        """Perform aggregation on a table.

        Args:
            table: Input Arrow table
            group_by: Columns to group by
            aggregations: Either a dict {column: function} or list of AggregationSpec

        Returns:
            Aggregated Arrow table
        """
        self.conn.register("input_table", table)

        # Build aggregation expressions
        if isinstance(aggregations, dict):
            agg_specs = [
                AggregationSpec(column=col, function=func) for col, func in aggregations.items()
            ]
        else:
            agg_specs = aggregations

        agg_exprs = [spec.to_sql() for spec in agg_specs]

        # Build query
        select_cols = group_by + agg_exprs
        select_clause = ", ".join(select_cols)

        if group_by:
            group_clause = ", ".join(group_by)
            query = f"""
                SELECT {select_clause}
                FROM input_table
                GROUP BY {group_clause}
            """
        else:
            # Global aggregation (no group by)
            agg_only = ", ".join(agg_exprs)
            query = f"""
                SELECT {agg_only}
                FROM input_table
            """

        result = self.conn.execute(query).fetch_arrow_table()
        self.conn.unregister("input_table")

        return result

    def partial_aggregate(
        self,
        table: pa.Table,
        group_by: List[str],
        aggregations: Dict[str, str],
    ) -> pa.Table:
        """Perform partial (map-side) aggregation.

        This is the first phase of a two-phase aggregation:
        1. Partial aggregate (map-side combine)
        2. Final aggregate (reduce-side)

        For partial aggregation, we compute partial results that can
        be merged later. For example:
        - sum -> partial sum
        - count -> partial count
        - avg -> partial sum + partial count (for correct weighted merge)

        Args:
            table: Input Arrow table
            group_by: Columns to group by
            aggregations: Dict {column: function}

        Returns:
            Partially aggregated Arrow table
        """
        self.conn.register("input_table", table)

        # Build aggregation expressions
        # For avg, we output both sum and count for proper merging
        agg_exprs = []
        for col, func in aggregations.items():
            if func == "avg":
                # For avg, store sum and count separately
                agg_exprs.append(f"SUM({col}) AS __avg_sum_{col}")
                agg_exprs.append(f"COUNT({col}) AS __avg_count_{col}")
            elif func == "sum":
                agg_exprs.append(f"SUM({col}) AS sum_{col}")
            elif func == "count":
                agg_exprs.append(f"COUNT({col}) AS count_{col}")
            elif func == "min":
                agg_exprs.append(f"MIN({col}) AS min_{col}")
            elif func == "max":
                agg_exprs.append(f"MAX({col}) AS max_{col}")
            else:
                raise ValueError(f"Unknown aggregation function: {func}")

        select_cols = group_by + agg_exprs
        select_clause = ", ".join(select_cols)

        if group_by:
            group_clause = ", ".join(group_by)
            query = f"""
                SELECT {select_clause}
                FROM input_table
                GROUP BY {group_clause}
            """
        else:
            agg_only = ", ".join(agg_exprs)
            query = f"""
                SELECT {agg_only}
                FROM input_table
            """

        result = self.conn.execute(query).fetch_arrow_table()
        self.conn.unregister("input_table")
        return result

    def merge_aggregates(
        self,
        tables: List[pa.Table],
        group_by: List[str],
        aggregations: Dict[str, str],
    ) -> pa.Table:
        """Merge partial aggregates into final result.

        This is the second phase of a two-phase aggregation.
        Properly handles avg by computing weighted average from sum/count.

        Args:
            tables: List of partially aggregated tables (from partial_aggregate)
            group_by: Columns to group by
            aggregations: Dict {column: function}

        Returns:
            Final aggregated Arrow table
        """
        if not tables:
            raise ValueError("No tables to merge")

        # Concatenate all partial results
        combined = pa.concat_tables(tables)
        self.conn.register("partial_table", combined)

        # Build merge expressions
        # For avg, compute SUM(partial_sum) / SUM(partial_count)
        agg_exprs = []
        for col, func in aggregations.items():
            if func == "sum":
                agg_exprs.append(f"SUM(sum_{col}) AS sum_{col}")
            elif func == "count":
                agg_exprs.append(f"SUM(count_{col}) AS count_{col}")
            elif func == "min":
                agg_exprs.append(f"MIN(min_{col}) AS min_{col}")
            elif func == "max":
                agg_exprs.append(f"MAX(max_{col}) AS max_{col}")
            elif func == "avg":
                # Proper weighted average: total_sum / total_count
                agg_exprs.append(
                    f"SUM(__avg_sum_{col}) * 1.0 / SUM(__avg_count_{col}) AS avg_{col}"
                )
            else:
                raise ValueError(f"Unknown aggregation function: {func}")

        select_cols = group_by + agg_exprs
        select_clause = ", ".join(select_cols)

        if group_by:
            group_clause = ", ".join(group_by)
            query = f"""
                SELECT {select_clause}
                FROM partial_table
                GROUP BY {group_clause}
            """
        else:
            agg_only = ", ".join(agg_exprs)
            query = f"""
                SELECT {agg_only}
                FROM partial_table
            """

        result = self.conn.execute(query).fetch_arrow_table()
        self.conn.unregister("partial_table")
        return result

    # === Join Operations ===

    def hash_join(
        self,
        left: pa.Table,
        right: pa.Table,
        join_keys: List[str],
        join_type: str = "inner",
        left_suffix: str = "_left",
        right_suffix: str = "_right",
    ) -> pa.Table:
        """Perform a hash join between two tables.

        Args:
            left: Left table
            right: Right table
            join_keys: Columns to join on (must exist in both tables)
            join_type: Type of join (inner, left, right, full)
            left_suffix: Suffix for duplicate columns from left table
            right_suffix: Suffix for duplicate columns from right table

        Returns:
            Joined Arrow table
        """
        self.conn.register("left_table", left)
        self.conn.register("right_table", right)

        # Build join condition
        join_conditions = [f"left_table.{k} = right_table.{k}" for k in join_keys]
        join_clause = " AND ".join(join_conditions)

        # Handle column selection (avoid duplicates)
        left_cols = set(left.column_names)
        right_cols = set(right.column_names)
        common_cols = left_cols & right_cols - set(join_keys)

        select_parts = []

        # Add join keys from left table
        for k in join_keys:
            select_parts.append(f"left_table.{k}")

        # Add left columns
        for col in left.column_names:
            if col in join_keys:
                continue
            if col in common_cols:
                select_parts.append(f"left_table.{col} AS {col}{left_suffix}")
            else:
                select_parts.append(f"left_table.{col}")

        # Add right columns
        for col in right.column_names:
            if col in join_keys:
                continue
            if col in common_cols:
                select_parts.append(f"right_table.{col} AS {col}{right_suffix}")
            else:
                select_parts.append(f"right_table.{col}")

        select_clause = ", ".join(select_parts)

        # Map join type to SQL
        join_type_sql = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN",
            "right": "RIGHT JOIN",
            "full": "FULL OUTER JOIN",
        }.get(join_type.lower(), "INNER JOIN")

        query = f"""
            SELECT {select_clause}
            FROM left_table
            {join_type_sql} right_table ON {join_clause}
        """

        result = self.conn.execute(query).fetch_arrow_table()

        self.conn.unregister("left_table")
        self.conn.unregister("right_table")

        return result

    # === Filtering and Projection ===

    def filter(self, table: pa.Table, predicate: str) -> pa.Table:
        """Filter a table using a SQL predicate.

        Args:
            table: Input Arrow table
            predicate: SQL WHERE clause predicate (e.g., "age > 18")

        Returns:
            Filtered Arrow table
        """
        self.conn.register("input_table", table)

        query = f"""
            SELECT * FROM input_table
            WHERE {predicate}
        """

        result = self.conn.execute(query).fetch_arrow_table()
        self.conn.unregister("input_table")

        return result

    def project(self, table: pa.Table, columns: List[str]) -> pa.Table:
        """Project specific columns from a table.

        Args:
            table: Input Arrow table
            columns: List of column names to select

        Returns:
            Projected Arrow table
        """
        self.conn.register("input_table", table)

        select_clause = ", ".join(columns)
        query = f"SELECT {select_clause} FROM input_table"

        result = self.conn.execute(query).fetch_arrow_table()
        self.conn.unregister("input_table")

        return result

    def sql(self, table: pa.Table, query: str, table_name: str = "t") -> pa.Table:
        """Execute arbitrary SQL on a table.

        Args:
            table: Input Arrow table
            query: SQL query (use table_name to reference the table)
            table_name: Name to use for the table in the query

        Returns:
            Result Arrow table
        """
        self.conn.register(table_name, table)
        result = self.conn.execute(query).fetch_arrow_table()
        self.conn.unregister(table_name)
        return result

    # === Deduplication ===

    def dedupe(
        self,
        table: pa.Table,
        key_columns: List[str],
        order_by: Optional[str] = None,
        keep: str = "first",
    ) -> pa.Table:
        """Deduplicate a table by key columns.

        Args:
            table: Input Arrow table
            key_columns: Columns that define uniqueness
            order_by: Column to order by for deterministic first/last selection.
                      If None, selection is non-deterministic (faster).
            keep: Which duplicate to keep ("first" or "last"), only used when
                  order_by is specified.

        Returns:
            Deduplicated Arrow table
        """
        self.conn.register("input_table", table)
        key_clause = ", ".join(key_columns)

        if order_by is None:
            # Fast path: non-deterministic selection using DISTINCT ON
            query = f"""
                SELECT DISTINCT ON ({key_clause}) *
                FROM input_table
            """
        else:
            # Deterministic path: order by specified column
            if keep not in ("first", "last"):
                raise ValueError(f"keep must be 'first' or 'last', got '{keep}'")
            order_dir = "ASC" if keep == "first" else "DESC"
            query = f"""
                SELECT DISTINCT ON ({key_clause}) *
                FROM input_table
                ORDER BY {key_clause}, {order_by} {order_dir}
            """

        result = self.conn.execute(query).fetch_arrow_table()
        self.conn.unregister("input_table")

        return result
