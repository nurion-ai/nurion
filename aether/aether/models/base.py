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

"""Declarative base for ORM models."""

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


class BaseModel(DeclarativeBase):
    """Base class for SQLAlchemy models using dataclass integration."""

    metadata = MetaData()
    repr_cols_num = 3

    def __repr__(self) -> str:  # pragma: no cover - repr utility
        attrs = ", ".join(
            f"{key}={value!r}" for key, value in self.__dict__.items() if not key.startswith("_")
        )
        return f"{self.__class__.__name__}({attrs})"
