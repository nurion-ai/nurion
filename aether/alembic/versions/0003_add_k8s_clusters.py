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

"""Add Kubernetes clusters and RayJobs tables."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_add_k8s_clusters"
down_revision: str = "0002_add_iceberg_namespaces_and_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Create K8s clusters table
    op.create_table(
        "k8s_clusters",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("kubeconfig", sa.Text(), nullable=True),
        sa.Column("default_queue", sa.String(length=255), nullable=False),
        sa.Column("default_ray_image", sa.String(length=512), nullable=False),
        sa.Column("image_pull_secret", sa.String(length=255), nullable=True),
        sa.Column("is_default", sa.Boolean(), nullable=False, default=False),
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
    op.create_index("ix_k8s_clusters_name", "k8s_clusters", ["name"], unique=True)
    op.create_index("ix_k8s_clusters_is_default", "k8s_clusters", ["is_default"], unique=False)

    # Create RayJobs table
    op.create_table(
        "rayjobs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("job_name", sa.String(length=255), nullable=False),
        sa.Column("namespace", sa.String(length=255), nullable=False),
        sa.Column("queue_name", sa.String(length=255), nullable=False),
        # Job configuration
        sa.Column("entrypoint", sa.Text(), nullable=False),
        sa.Column("ray_image", sa.String(length=512), nullable=True),
        sa.Column("num_cpus", sa.Integer(), nullable=False, default=16),
        sa.Column("num_gpus", sa.Integer(), nullable=False, default=0),
        sa.Column("memory_gb", sa.Integer(), nullable=False, default=128),
        sa.Column("worker_replicas", sa.Integer(), nullable=False, default=3),
        sa.Column("runtime_env", sa.JSON(), nullable=True),
        sa.Column("env_vars", sa.JSON(), nullable=True),
        # Job status
        sa.Column("status", sa.String(length=50), nullable=False, default="PENDING"),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("dashboard_url", sa.String(length=512), nullable=True),
        # User info
        sa.Column("user", sa.String(length=255), nullable=True),
        # Cluster reference
        sa.Column(
            "cluster_id",
            sa.Integer(),
            sa.ForeignKey("k8s_clusters.id"),
            nullable=True,
        ),
        # Timestamps
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
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_rayjobs_job_name", "rayjobs", ["job_name"], unique=False)
    op.create_index("ix_rayjobs_cluster_job", "rayjobs", ["cluster_id", "job_name"], unique=False)
    op.create_index("ix_rayjobs_status", "rayjobs", ["status"], unique=False)
    op.create_index("ix_rayjobs_user", "rayjobs", ["user"], unique=False)
    op.create_index("ix_rayjobs_cluster_id", "rayjobs", ["cluster_id"], unique=False)


def downgrade() -> None:
    # Drop RayJobs table
    op.drop_index("ix_rayjobs_cluster_id", table_name="rayjobs")
    op.drop_index("ix_rayjobs_user", table_name="rayjobs")
    op.drop_index("ix_rayjobs_status", table_name="rayjobs")
    op.drop_index("ix_rayjobs_cluster_job", table_name="rayjobs")
    op.drop_index("ix_rayjobs_job_name", table_name="rayjobs")
    op.drop_table("rayjobs")

    # Drop K8s clusters table
    op.drop_index("ix_k8s_clusters_is_default", table_name="k8s_clusters")
    op.drop_index("ix_k8s_clusters_name", table_name="k8s_clusters")
    op.drop_table("k8s_clusters")
