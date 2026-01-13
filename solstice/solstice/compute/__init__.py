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

"""Compute engines for high-performance data processing.

This module provides embedded compute engines for operations that benefit
from vectorized execution, such as:

- Hash partitioning for shuffle
- Aggregation (sum, count, min, max, avg)
- Join operations
- Filtering and projection

Key design principle: Each worker has its own engine instance.
DuckDB is embedded and cannot be shared across processes.
"""

from solstice.compute.duckdb_engine import DuckDBEngine

__all__ = [
    "DuckDBEngine",
]
