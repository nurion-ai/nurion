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

"""Iceberg table models for catalog support."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import BaseModel


class IcebergNamespace(BaseModel):
    """Represents an Iceberg namespace, independent from Lance namespaces."""

    __tablename__ = "catalog_iceberg_namespaces"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    properties: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    tables: Mapped[list[IcebergTable]] = relationship(
        "IcebergTable",
        back_populates="namespace",
        cascade="all, delete-orphan",
    )


class IcebergTable(BaseModel):
    """Represents an Iceberg table in the catalog.

    Only stores minimal metadata. The actual table metadata (schema, partition-spec, etc.)
    is stored in the metadata_location file and should be read from storage.
    """

    __tablename__ = "catalog_iceberg_tables"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    # Only essential fields - metadata should be read from storage
    metadata_location: Mapped[str] = mapped_column(String(512), nullable=False)

    namespace_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("catalog_iceberg_namespaces.id"),
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

    namespace: Mapped[IcebergNamespace | None] = relationship(
        "IcebergNamespace", back_populates="tables", foreign_keys=[namespace_id]
    )

    __table_args__ = (
        Index(
            "ix_catalog_iceberg_tables_namespace_name",
            "namespace_id",
            "name",
            unique=True,
        ),
    )
