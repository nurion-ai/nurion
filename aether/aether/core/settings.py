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

"""Application settings management."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IcebergCatalogSettings(BaseSettings):
    """Iceberg catalog specific configuration."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    storage_backend: Literal["s3", "local"] = "s3"
    warehouse: str | None = None
    local_root_path: str = "/tmp/iceberg"
    s3_endpoint: str | None = Field(default=None, description="Internal S3 endpoint")
    s3_external_endpoint: str | None = Field(
        default=None,
        description="External S3 endpoint for clients",
    )
    s3_access_key_id: str = "minioadmin"
    s3_secret_access_key: str = "minioadmin"
    s3_region: str = "us-east-1"

    @property
    def is_s3(self) -> bool:
        return self.storage_backend == "s3"

    @property
    def is_local(self) -> bool:
        return self.storage_backend == "local"

    def endpoint_for_clients(self) -> str | None:
        """Return the endpoint to expose to external clients."""
        if not self.is_s3:
            return None
        return self.s3_external_endpoint or self.s3_endpoint

    def endpoint_for_backend(self) -> str | None:
        """Return the endpoint used by backend services."""
        if not self.is_s3:
            return None
        return self.s3_endpoint

    def warehouse_uri(self) -> str:
        """Return the resolved warehouse URI based on backend configuration."""
        if self.warehouse:
            return self.warehouse.rstrip("/")

        if self.is_s3:
            return "s3://warehouse"

        path = self.local_root_path.rstrip("/") or "/"
        if not path.startswith("/"):
            path = f"/{path}"
        return f"file://{path}"


class Settings(BaseSettings):
    """Pydantic model for platform configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

    app_name: str = "Aether Data Platform"
    environment: str = "development"
    database_url: str = "postgresql+asyncpg://aether:aether@localhost:5432/aether"
    iceberg: IcebergCatalogSettings = Field(default_factory=IcebergCatalogSettings)


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""

    return Settings()
