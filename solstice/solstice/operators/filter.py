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

"""Filter operator"""

from dataclasses import dataclass
from typing import Any, Callable, Optional

from solstice.core.operator import Operator, OperatorConfig
from solstice.core.models import Split, SplitPayload


@dataclass
class FilterOperatorConfig(OperatorConfig):
    """Configuration for FilterOperator."""

    filter_fn: Callable[[Any], bool]
    """Predicate function that returns True for records to keep."""


class FilterOperator(Operator):
    """Operator that filters records based on a predicate"""

    def __init__(self, config: FilterOperatorConfig, worker_id: Optional[str] = None):
        super().__init__(config, worker_id)

        if not callable(config.filter_fn):
            raise ValueError("filter_fn must be a callable returning bool")
        self.filter_fn = config.filter_fn

    def process_split(
        self, split: Split, batch: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Filter record based on predicate"""
        try:
            # Apply filter
            new_data = []
            for record in batch.to_records():
                if self.filter_fn(record.value):
                    new_data.append(record)
            return batch.with_new_data(new_data, split_id=f"{split.split_id}_{self.worker_id}")

        except Exception as e:
            self.logger.error(f"Error filtering split {split.split_id}: {e}")
            return None


# Set operator_class after class definition
FilterOperatorConfig.operator_class = FilterOperator
