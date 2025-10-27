"""Catalog models for Lance namespace support."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, BigInteger, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import BaseModel


class Namespace(BaseModel):
    """Represents a Lance namespace grouping tables under a logical path."""

    __tablename__ = "catalog_namespaces"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True, default=None)
    delimiter: Mapped[str] = mapped_column(String(10), default=".")
    properties: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True, default=None)

    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default=None)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    tables: Mapped[list["LanceTable"]] = relationship(
        "LanceTable",
        back_populates="namespace",
        cascade="all, delete-orphan",
    )


class LanceTable(BaseModel):
    """Represents a Lance dataset registered within the catalog."""

    __tablename__ = "catalog_lance_tables"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True, default=None)
    lance_path: Mapped[str] = mapped_column(String(255), nullable=False)
    lance_schema: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True, default=None)
    row_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, default=None)
    storage_options: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True, default=None)
    tags: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    custom_values: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    last_updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default=None)

    namespace_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("catalog_namespaces.id"),
        nullable=True,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    namespace: Mapped[Optional[Namespace]] = relationship("Namespace", back_populates="tables")

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


