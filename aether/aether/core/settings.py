"""Application settings management."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pydantic model for platform configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "Aether Data Platform"
    environment: str = "development"
    database_url: str = "postgresql+asyncpg://aether:aether@localhost:5432/aether"


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""

    return Settings()
