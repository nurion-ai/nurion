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

"""Models for Kubernetes cluster and RayJob management."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import BaseModel


class K8sCluster(BaseModel):
    """Represents a Kubernetes cluster configuration."""

    __tablename__ = "k8s_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    kubeconfig: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    default_queue: Mapped[str] = mapped_column(String(255), nullable=False)
    default_ray_image: Mapped[str] = mapped_column(String(512), nullable=False)
    image_pull_secret: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    # Relationship to RayJobs
    rayjobs: Mapped[list[RayJob]] = relationship(
        "RayJob", back_populates="cluster", cascade="all, delete-orphan"
    )


class RayJob(BaseModel):
    """Represents a RayJob submitted to Kubernetes."""

    __tablename__ = "rayjobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Job configuration
    entrypoint: Mapped[str] = mapped_column(Text, nullable=False)
    ray_image: Mapped[str | None] = mapped_column(String(512), nullable=True)
    num_cpus: Mapped[int] = mapped_column(Integer, default=16)
    num_gpus: Mapped[int] = mapped_column(Integer, default=0)
    memory_gb: Mapped[int] = mapped_column(Integer, default=128)
    worker_replicas: Mapped[int] = mapped_column(Integer, default=3)
    runtime_env: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)
    env_vars: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)

    # Job status
    status: Mapped[str] = mapped_column(String(50), default="PENDING", nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    dashboard_url: Mapped[str | None] = mapped_column(String(512), nullable=True, default=None)

    # User info
    user: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)

    # Cluster reference
    cluster_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("k8s_clusters.id"), nullable=True, index=True
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationship
    cluster: Mapped[K8sCluster | None] = relationship("K8sCluster", back_populates="rayjobs")

    __table_args__ = (
        Index("ix_rayjobs_cluster_job", "cluster_id", "job_name"),
        Index("ix_rayjobs_status", "status"),
        Index("ix_rayjobs_user", "user"),
    )
