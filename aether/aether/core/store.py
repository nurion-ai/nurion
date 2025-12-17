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

"""Storage utilities for Lance datasets."""

from __future__ import annotations


def normalized_path_and_storage_options(
    lance_path: str, storage_options: dict | None = None
) -> tuple[str, dict]:
    """Return normalized path and merged storage options.

    For now this implementation simply returns the provided path and options.
    It mirrors the interface used by the data-api service so that the logic can
    evolve alongside it without requiring callers to change.
    """

    return lance_path, storage_options or {}
