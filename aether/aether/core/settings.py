"""Application settings management."""

from functools import lru_cache
from typing import Literal

from pydantic import AliasChoices, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IcebergCatalogSettings(BaseModel):
    """Iceberg catalog specific configuration."""

    storage_backend: Literal["s3", "local"] = Field(
        default="s3",
        validation_alias=AliasChoices(
            "ICEBERG__STORAGE_BACKEND",
            "ICEBERG_STORAGE_BACKEND",
        ),
    )
    warehouse: str | None = Field(
        default=None,
        validation_alias=AliasChoices("ICEBERG__WAREHOUSE", "ICEBERG_WAREHOUSE"),
    )
    local_root_path: str = Field(
        default="/tmp/iceberg",
        validation_alias=AliasChoices(
            "ICEBERG__LOCAL_ROOT_PATH",
            "ICEBERG_LOCAL_ROOT_PATH",
        ),
    )
    s3_endpoint: str | None = Field(
        default=None,
        description="Internal S3 endpoint",
        validation_alias=AliasChoices("ICEBERG__S3_ENDPOINT", "AWS_ENDPOINT_URL"),
    )
    s3_external_endpoint: str | None = Field(
        default="http://localhost:9000",
        description="External S3 endpoint for clients",
        validation_alias=AliasChoices("ICEBERG__S3_EXTERNAL_ENDPOINT", "AWS_S3_ENDPOINT_EXTERNAL"),
    )
    s3_access_key_id: str = Field(
        default="minioadmin",
        validation_alias=AliasChoices("ICEBERG__S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
    )
    s3_secret_access_key: str = Field(
        default="minioadmin",
        validation_alias=AliasChoices("ICEBERG__S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"),
    )
    s3_region: str = Field(
        default="us-east-1",
        validation_alias=AliasChoices("ICEBERG__S3_REGION", "AWS_REGION"),
    )

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
