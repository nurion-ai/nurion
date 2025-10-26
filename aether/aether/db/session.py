"""Database session and engine configuration."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..core.settings import get_settings

settings = get_settings()

async_engine = create_async_engine(settings.database_url, echo=False)
async_session_factory = async_sessionmaker(async_engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a transactional scope around a series of operations."""

    async with async_session_factory() as session:
        yield session

