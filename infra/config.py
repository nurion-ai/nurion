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

"""Shared configuration for Pulumi infrastructure."""

import os
from dataclasses import dataclass
from typing import List, Optional

import pulumi


@dataclass
class InfraConfig:
    """Infrastructure configuration loaded from Pulumi config."""

    # Kubernetes
    k8s_context: str
    namespace: str

    # Container registry
    registry_url: str
    registry_username: Optional[str]
    registry_password: Optional[str]

    # Aether
    aether_replicas: int
    aether_image_tag: str

    # PostgreSQL
    postgres_storage_size: str
    postgres_password: str

    # GitHub Actions Runner
    runner_replicas_min: int
    runner_replicas_max: int
    runner_labels: List[str]
    github_token: str
    github_repo: str

    # S3/Object Storage
    s3_bucket: str
    s3_endpoint: str
    s3_access_key: Optional[str]
    s3_secret_key: Optional[str]


def load_config() -> InfraConfig:
    """Load configuration from Pulumi config and secrets."""
    config = pulumi.Config()
    
    # Parse runner labels from JSON string
    import json
    runner_labels_str = config.get("runner_labels") or '["self-hosted", "linux"]'
    runner_labels = json.loads(runner_labels_str)

    return InfraConfig(
        # Kubernetes
        k8s_context=config.require("k8s_context"),
        namespace=config.get("namespace") or "nurion-nightly",
        
        # Container registry (from config or CR_URL environment variable)
        registry_url=config.get("registry_url") or os.environ.get("CR_URL", ""),
        registry_username=config.get_secret("registry_username"),
        registry_password=config.get_secret("registry_password"),
        
        # Aether
        aether_replicas=config.get_int("aether_replicas") or 1,
        aether_image_tag=config.get("aether_image_tag") or "nightly",
        
        # PostgreSQL (default password for ephemeral test instance)
        postgres_storage_size=config.get("postgres_storage_size") or "10Gi",
        postgres_password=config.get_secret("postgres_password") or "nurion-nightly-pg",
        
        # GitHub Actions Runner
        runner_replicas_min=config.get_int("runner_replicas_min") or 1,
        runner_replicas_max=config.get_int("runner_replicas_max") or 3,
        runner_labels=runner_labels,
        github_token=config.require_secret("github_token"),
        github_repo=config.require("github_repo"),
        
        # S3/Object Storage (endpoint from environment variable)
        s3_bucket=config.get("s3_bucket") or "nurion",
        s3_endpoint=config.get("s3_endpoint") or os.environ.get("AWS_ENDPOINT_URL", "").replace("https://", ""),
        s3_access_key=config.get_secret("s3_access_key"),
        s3_secret_key=config.get_secret("s3_secret_key"),
    )


# Common labels for all resources
def get_common_labels(component: str) -> dict:
    """Get common labels for Kubernetes resources."""
    return {
        "app.kubernetes.io/name": "nurion",
        "app.kubernetes.io/component": component,
        "app.kubernetes.io/managed-by": "pulumi",
        "environment": "nightly",
    }
