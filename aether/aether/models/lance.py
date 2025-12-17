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

"""Catalog models for Lance namespace support."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, BigInteger, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import BaseModel


class LanceNamespace(BaseModel):
    """Represents a Lance namespace grouping tables under a logical path."""

    __tablename__ = "catalog_namespaces"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True, default=None)
    delimiter: Mapped[str] = mapped_column(String(10), default=".")
    properties: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)

    created_by: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    updated_by: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    tables: Mapped[list[LanceTable]] = relationship(
        "LanceTable",
        back_populates="namespace",
        cascade="all, delete-orphan",
    )


class LanceTable(BaseModel):
    """Represents a Lance dataset registered within the catalog."""

    __tablename__ = "catalog_lance_tables"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True, default=None)
    lance_path: Mapped[str] = mapped_column(String(255), nullable=False)
    lance_schema: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True, default=None)
    storage_options: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)
    tags: Mapped[dict | None] = mapped_column(JSONB, nullable=True, default=None)
    custom_values: Mapped[dict | None] = mapped_column(JSONB, nullable=True, default=None)
    last_updated_by: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)

    namespace_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("catalog_namespaces.id"),
        nullable=True,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    namespace: Mapped[LanceNamespace | None] = relationship(
        "LanceNamespace", back_populates="tables"
    )

    __table_args__ = (
        Index(
            "idx_catalog_lance_tables_tags",
            "tags",
            unique=False,
            postgresql_using="gin",
        ),
        Index(
            "idx_catalog_lance_tables_custom_values",
            "custom_values",
            unique=False,
            postgresql_using="gin",
        ),
    )
