"""Service for managing Iceberg namespaces and tables in the catalog."""

from __future__ import annotations

import logging
import re

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..db.session import get_session
from ..models.iceberg import IcebergNamespace, IcebergTable

logger = logging.getLogger(__name__)


async def _resolve_session(db: AsyncSession | None) -> AsyncSession:
    if db is None:
        async with get_session() as session:
            return session
    return db


# Iceberg Namespace operations
async def get_iceberg_namespace_by_name(
    name: str, db: AsyncSession | None = None
) -> IcebergNamespace | None:
    """Get an Iceberg namespace by name."""
    session = await _resolve_session(db)

    stmt = select(IcebergNamespace).where(IcebergNamespace.name == name)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_all_iceberg_namespaces(
    db: AsyncSession | None = None,
) -> list[IcebergNamespace]:
    """Get all Iceberg namespaces."""
    session = await _resolve_session(db)

    stmt = select(IcebergNamespace)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def create_iceberg_namespace(
    name: str,
    properties: dict[str, str] | None = None,
    db: AsyncSession | None = None,
) -> IcebergNamespace:
    """Create a new Iceberg namespace."""
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError(
            "Namespace name must start with a letter or underscore and contain only "
            "letters, digits, and underscores."
        )

    session = await _resolve_session(db)

    stmt = select(IcebergNamespace).where(IcebergNamespace.name == name)
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing:
        raise ValueError(f"Iceberg namespace with name '{name}' already exists")

    namespace = IcebergNamespace(
        name=name,
        properties=properties,
    )

    session.add(namespace)
    await session.commit()
    await session.refresh(namespace)

    return namespace


async def ensure_default_iceberg_namespace(
    db: AsyncSession | None = None,
) -> IcebergNamespace:
    """Ensure the default Iceberg namespace exists."""
    session = await _resolve_session(db)

    namespace = await get_iceberg_namespace_by_name("default", session)
    if not namespace:
        namespace = await create_iceberg_namespace("default", properties={}, db=session)
        logger.info("Created default Iceberg namespace")

    return namespace


async def delete_iceberg_namespace(namespace_id: int, db: AsyncSession | None = None) -> bool:
    """Delete an Iceberg namespace."""
    session = await _resolve_session(db)

    stmt = select(IcebergNamespace).where(IcebergNamespace.id == namespace_id)
    result = await session.execute(stmt)
    namespace = result.scalar_one_or_none()
    if not namespace:
        return False

    await session.delete(namespace)
    await session.commit()

    return True


async def update_iceberg_namespace_properties(
    name: str,
    removals: list[str] | None = None,
    updates: dict[str, str] | None = None,
    db: AsyncSession | None = None,
) -> IcebergNamespace:
    """Update Iceberg namespace properties."""
    session = await _resolve_session(db)

    namespace = await get_iceberg_namespace_by_name(name, session)
    if not namespace:
        raise ValueError(f"Iceberg namespace '{name}' not found")

    # Update properties
    current_props = namespace.properties or {}
    if isinstance(current_props, str):
        import json

        try:
            current_props = json.loads(current_props)
        except json.JSONDecodeError:
            current_props = {}
    properties = dict(current_props)

    for key in removals or []:
        properties.pop(key, None)

    for key, value in (updates or {}).items():
        properties[key] = value

    namespace.properties = properties
    await session.commit()
    await session.refresh(namespace)

    return namespace


# Iceberg Table operations
async def get_iceberg_table_by_name(
    table_name: str, namespace_name: str, db: AsyncSession | None = None
) -> IcebergTable | None:
    """Get an Iceberg table by name in a namespace."""
    session = await _resolve_session(db)

    namespace = await get_iceberg_namespace_by_name(namespace_name, session)
    if not namespace:
        return None

    stmt = (
        select(IcebergTable)
        .where(IcebergTable.name == table_name)
        .where(IcebergTable.namespace_id == namespace.id)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_iceberg_tables_by_namespace(
    namespace_name: str, db: AsyncSession | None = None
) -> list[IcebergTable]:
    """Get all Iceberg tables in a namespace."""
    session = await _resolve_session(db)

    namespace = await get_iceberg_namespace_by_name(namespace_name, session)
    if not namespace:
        return []

    stmt = (
        select(IcebergTable)
        .where(IcebergTable.namespace_id == namespace.id)
        .options(selectinload(IcebergTable.namespace))
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def create_iceberg_table(
    table_name: str,
    namespace_name: str,
    metadata_location: str,
    db: AsyncSession | None = None,
) -> IcebergTable:
    """Create a new Iceberg table in the catalog.

    Only stores the metadata_location pointer. Actual Iceberg metadata
    should be read from storage at metadata_location.
    """
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", table_name):
        raise ValueError(
            "Table name must start with a letter or underscore and contain only "
            "letters, digits, and underscores."
        )

    session = await _resolve_session(db)

    # Get or create namespace
    namespace = await get_iceberg_namespace_by_name(namespace_name, session)
    if not namespace:
        if namespace_name == "default":
            namespace = await ensure_default_iceberg_namespace(session)
        else:
            raise ValueError(f"Iceberg namespace '{namespace_name}' not found")

    # Check if table already exists
    stmt = (
        select(IcebergTable)
        .where(IcebergTable.name == table_name)
        .where(IcebergTable.namespace_id == namespace.id)
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing:
        raise ValueError(
            f"Iceberg table '{table_name}' already exists in namespace '{namespace_name}'"
        )

    # Create the table - only store metadata_location
    iceberg_table = IcebergTable(
        name=table_name,
        namespace_id=namespace.id,
        metadata_location=metadata_location,
    )

    session.add(iceberg_table)
    await session.commit()
    await session.refresh(iceberg_table)

    return iceberg_table


async def update_iceberg_table_metadata_location(
    table_name: str,
    namespace_name: str,
    metadata_location: str,
    db: AsyncSession | None = None,
) -> IcebergTable:
    """Update an Iceberg table's metadata location."""
    session = await _resolve_session(db)

    table = await get_iceberg_table_by_name(table_name, namespace_name, session)
    if not table:
        raise ValueError(f"Iceberg table '{table_name}' not found in namespace '{namespace_name}'")

    table.metadata_location = metadata_location
    await session.commit()
    await session.refresh(table)

    return table


async def delete_iceberg_table(
    table_name: str, namespace_name: str, db: AsyncSession | None = None
) -> bool:
    """Delete an Iceberg table from the catalog."""
    session = await _resolve_session(db)

    table = await get_iceberg_table_by_name(table_name, namespace_name, session)
    if not table:
        return False

    await session.delete(table)
    await session.commit()

    return True
