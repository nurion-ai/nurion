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

"""Create catalog tables for Lance namespace"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_create_catalog_tables"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "catalog_namespaces",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("description", sa.String(length=1024), nullable=True),
        sa.Column("delimiter", sa.String(length=10), nullable=False, server_default=sa.text("'.'")),
        sa.Column("properties", sa.JSON(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("updated_by", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_index("ix_catalog_namespaces_name", "catalog_namespaces", ["name"], unique=True)

    op.create_table(
        "catalog_lance_tables",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("description", sa.String(length=1024), nullable=True),
        sa.Column("lance_path", sa.String(length=255), nullable=False),
        sa.Column("lance_schema", sa.JSON(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("storage_options", sa.JSON(), nullable=True),
        sa.Column("tags", postgresql.JSONB(), nullable=True),
        sa.Column("custom_values", postgresql.JSONB(), nullable=True),
        sa.Column("last_updated_by", sa.String(length=255), nullable=True),
        sa.Column(
            "namespace_id",
            sa.Integer(),
            sa.ForeignKey("catalog_namespaces.id"),
            index=True,
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_index("ix_catalog_lance_tables_name", "catalog_lance_tables", ["name"], unique=True)
    op.create_index(
        "idx_catalog_lance_tables_tags",
        "catalog_lance_tables",
        ["tags"],
        postgresql_using="gin",
    )
    op.create_index(
        "idx_catalog_lance_tables_custom_values",
        "catalog_lance_tables",
        ["custom_values"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("idx_catalog_lance_tables_custom_values", table_name="catalog_lance_tables")
    op.drop_index("idx_catalog_lance_tables_tags", table_name="catalog_lance_tables")
    op.drop_index("ix_catalog_lance_tables_name", table_name="catalog_lance_tables")
    op.drop_table("catalog_lance_tables")

    op.drop_index("ix_catalog_namespaces_name", table_name="catalog_namespaces")
    op.drop_table("catalog_namespaces")
