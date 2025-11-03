"""Add Iceberg namespaces and tables support."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002_add_iceberg_namespaces_and_tables"
down_revision: str = "0001_create_catalog_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Create Iceberg namespaces table
    op.create_table(
        "catalog_iceberg_namespaces",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("properties", sa.JSON(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")
        ),
    )
    op.create_index("ix_catalog_iceberg_namespaces_name", "catalog_iceberg_namespaces", ["name"], unique=True)

    # Create Iceberg tables table (minimal metadata only)
    op.create_table(
        "catalog_iceberg_tables",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("metadata_location", sa.String(length=512), nullable=False),
        sa.Column(
            "namespace_id",
            sa.Integer(),
            sa.ForeignKey("catalog_iceberg_namespaces.id"),
            index=True,
            nullable=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")
        ),
    )
    op.create_index("ix_catalog_iceberg_tables_name", "catalog_iceberg_tables", ["name"], unique=True)
    op.create_index(
        "ix_catalog_iceberg_tables_namespace_name",
        "catalog_iceberg_tables",
        ["namespace_id", "name"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_catalog_iceberg_tables_namespace_name", table_name="catalog_iceberg_tables")
    op.drop_index("ix_catalog_iceberg_tables_name", table_name="catalog_iceberg_tables")
    op.drop_table("catalog_iceberg_tables")
    
    op.drop_index("ix_catalog_iceberg_namespaces_name", table_name="catalog_iceberg_namespaces")
    op.drop_table("catalog_iceberg_namespaces")

