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

"""Data consistency validation utilities for distributed tests."""

from typing import Callable, Dict, List, Set


class DataValidator:
    """Data consistency validation utility for distributed tests.

    Provides methods to verify:
    - Record count matches expected
    - No duplicate records
    - All expected IDs present
    - Checksum integrity
    - Transform correctness
    - Filter/Explode correctness (row count changes)
    """

    @staticmethod
    def verify_count(records: List[Dict], expected: int) -> bool:
        """Verify that record count matches expected."""
        return len(records) == expected

    @staticmethod
    def verify_no_duplicates(records: List[Dict], id_field: str = "id") -> bool:
        """Verify that there are no duplicate records by ID."""
        ids = [r[id_field] for r in records]
        return len(ids) == len(set(ids))

    @staticmethod
    def verify_no_duplicates_composite(
        records: List[Dict], id_fields: List[str]
    ) -> bool:
        """Verify no duplicates using composite key (multiple fields).

        Useful for exploded data where (id, copy_idx) forms unique key.
        """
        keys = [tuple(r[f] for f in id_fields) for r in records]
        return len(keys) == len(set(keys))

    @staticmethod
    def verify_all_ids_present(records: List[Dict], expected_ids: Set[int]) -> bool:
        """Verify that all expected IDs are present in records."""
        actual_ids = {r["id"] for r in records}
        return actual_ids == expected_ids

    @staticmethod
    def verify_checksums(source: List[Dict], sink: List[Dict]) -> bool:
        """Verify that checksums match between source and sink data.

        Args:
            source: Source records with 'id' and 'checksum' fields
            sink: Sink records with 'id' and 'checksum' fields

        Returns:
            True if all checksums match, False otherwise
        """
        source_map = {r["id"]: r.get("checksum") for r in source}
        for record in sink:
            expected = source_map.get(record["id"])
            if expected is not None and record.get("checksum") != expected:
                return False
        return True

    @staticmethod
    def verify_transform_correctness(
        source: List[Dict],
        sink: List[Dict],
        transform_fn: Callable[[Dict], Dict],
    ) -> bool:
        """Verify that transform results are correct.

        Args:
            source: Original source records
            sink: Transformed sink records
            transform_fn: Function that transforms source to expected sink format

        Returns:
            True if all transforms are correct, False otherwise
        """
        source_map = {r["id"]: r for r in source}
        for record in sink:
            source_record = source_map.get(record["id"])
            if source_record is None:
                return False
            expected = transform_fn(source_record)
            # Compare relevant fields (excluding metadata that may differ)
            for key in expected:
                if key in record and record[key] != expected[key]:
                    return False
        return True

    @staticmethod
    def get_missing_ids(records: List[Dict], expected_ids: Set[int]) -> Set[int]:
        """Get the set of IDs that are missing from records."""
        actual_ids = {r["id"] for r in records}
        return expected_ids - actual_ids

    @staticmethod
    def get_duplicate_ids(records: List[Dict], id_field: str = "id") -> List:
        """Get list of duplicate IDs in records."""
        ids = [r[id_field] for r in records]
        seen = set()
        duplicates = []
        for id_val in ids:
            if id_val in seen:
                duplicates.append(id_val)
            seen.add(id_val)
        return duplicates

    # ========================================================================
    # Filter/Explode validation methods
    # ========================================================================

    @staticmethod
    def verify_filter_result(
        records: List[Dict],
        source_count: int,
        filter_modulo: int,
        filter_remainder: int,
        id_field: str = "id",
    ) -> bool:
        """Verify filter operation result.

        Args:
            records: Output records after filtering
            source_count: Original number of source records
            filter_modulo: Filter modulo value
            filter_remainder: Filter remainder value
            id_field: Field name for ID

        Returns:
            True if filter result is correct
        """
        # Calculate expected count
        expected_count = sum(
            1 for i in range(source_count)
            if i % filter_modulo == filter_remainder
        )

        if len(records) != expected_count:
            return False

        # Verify all IDs match filter condition
        for record in records:
            if record[id_field] % filter_modulo != filter_remainder:
                return False

        return True

    @staticmethod
    def verify_explode_result(
        records: List[Dict],
        source_count: int,
        explode_factor: int,
        id_field: str = "id",
    ) -> bool:
        """Verify explode operation result.

        Args:
            records: Output records after exploding
            source_count: Original number of source records
            explode_factor: Number of copies per row
            id_field: Field name for ID

        Returns:
            True if explode result is correct
        """
        expected_count = source_count * explode_factor

        if len(records) != expected_count:
            return False

        # Verify each source ID appears exactly explode_factor times
        id_counts = {}
        for record in records:
            rid = record[id_field]
            id_counts[rid] = id_counts.get(rid, 0) + 1

        if len(id_counts) != source_count:
            return False

        for count in id_counts.values():
            if count != explode_factor:
                return False

        return True

    @staticmethod
    def verify_filter_explode_result(
        records: List[Dict],
        source_count: int,
        filter_modulo: int,
        filter_remainder: int,
        explode_factor: int,
        id_field: str = "id",
    ) -> bool:
        """Verify combined filter-then-explode result.

        Args:
            records: Output records
            source_count: Original number of source records
            filter_modulo: Filter modulo value
            filter_remainder: Filter remainder value
            explode_factor: Number of copies per row after filter
            id_field: Field name for ID

        Returns:
            True if result is correct
        """
        # Calculate expected count
        filtered_count = sum(
            1 for i in range(source_count)
            if i % filter_modulo == filter_remainder
        )
        expected_count = filtered_count * explode_factor

        if len(records) != expected_count:
            return False

        # Verify each filtered ID appears exactly explode_factor times
        expected_ids = {
            i for i in range(source_count)
            if i % filter_modulo == filter_remainder
        }

        id_counts = {}
        for record in records:
            rid = record[id_field]
            if rid not in expected_ids:
                return False  # ID should have been filtered out
            id_counts[rid] = id_counts.get(rid, 0) + 1

        if set(id_counts.keys()) != expected_ids:
            return False

        for count in id_counts.values():
            if count != explode_factor:
                return False

        return True

    @staticmethod
    def calculate_filter_expected_count(
        source_count: int,
        filter_modulo: int,
        filter_remainder: int,
    ) -> int:
        """Calculate expected record count after filter."""
        return sum(
            1 for i in range(source_count)
            if i % filter_modulo == filter_remainder
        )

    @staticmethod
    def calculate_filter_explode_expected_count(
        source_count: int,
        filter_modulo: int,
        filter_remainder: int,
        explode_factor: int,
    ) -> int:
        """Calculate expected record count after filter + explode."""
        filtered = sum(
            1 for i in range(source_count)
            if i % filter_modulo == filter_remainder
        )
        return filtered * explode_factor
