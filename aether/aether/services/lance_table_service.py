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

"""Catalog service mirroring Lance namespace operations."""

from __future__ import annotations

import logging
import re
from typing import Any

import lance
from lance import schema_to_json
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.store import normalized_path_and_storage_options
from ..db.session import get_session
from ..models.lance import LanceNamespace, LanceTable

logger = logging.getLogger(__name__)


async def _resolve_session(db: AsyncSession | None) -> AsyncSession:
    if db is None:
        async with get_session() as session:
            return session
    return db


async def create_namespace(
    name: str,
    description: str | None = None,
    delimiter: str = ".",
    properties: dict[str, Any] | None = None,
    created_by: str | None = None,
    db: AsyncSession | None = None,
) -> LanceNamespace:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError(
            "Namespace name must start with a letter or underscore and contain only "
            "letters, digits, and underscores."
        )

    session = await _resolve_session(db)

    stmt = select(LanceNamespace).where(LanceNamespace.name == name)
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing:
        raise ValueError(f"Namespace with name '{name}' already exists")

    namespace = LanceNamespace(
        name=name,
        description=description,
        delimiter=delimiter,
        properties=properties or {},
        created_by=created_by,
    )
    session.add(namespace)
    await session.commit()
    await session.refresh(namespace)
    return namespace


async def ensure_default_namespace(db: AsyncSession | None = None) -> LanceNamespace:
    session = await _resolve_session(db)
    stmt = select(LanceNamespace).where(LanceNamespace.name == "default")
    result = await session.execute(stmt)
    namespace = result.scalar_one_or_none()
    if namespace:
        return namespace

    namespace = await create_namespace(
        name="default",
        description="Default namespace for tables",
        delimiter=".",
        properties={"system": True, "default": True},
        created_by="system",
        db=session,
    )
    logger.info("Created default namespace")
    return namespace


async def get_namespace_by_name(name: str, db: AsyncSession | None = None) -> LanceNamespace | None:
    session = await _resolve_session(db)
    stmt = select(LanceNamespace).where(LanceNamespace.name == name)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_all_namespaces(db: AsyncSession | None = None) -> list[LanceNamespace]:
    session = await _resolve_session(db)
    stmt = select(LanceNamespace).order_by(LanceNamespace.name)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_available_namespaces(db: AsyncSession | None = None) -> list[str]:
    session = await _resolve_session(db)
    stmt = select(LanceNamespace.name).order_by(LanceNamespace.name)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def delete_namespace(namespace_id: int, db: AsyncSession | None = None) -> bool:
    session = await _resolve_session(db)

    stmt = select(func.count(LanceTable.id)).where(LanceTable.namespace_id == namespace_id)
    result = await session.execute(stmt)
    table_count = result.scalar()
    if table_count and table_count > 0:
        raise ValueError(f"Cannot delete namespace: it contains {table_count} tables")

    stmt = select(LanceNamespace).where(LanceNamespace.id == namespace_id)
    result = await session.execute(stmt)
    namespace = result.scalar_one_or_none()
    if not namespace:
        return False

    await session.delete(namespace)
    await session.commit()
    return True


async def create_lance_table(
    lance_path: str,
    name: str,
    storage_options: dict | None = None,
    namespace_name: str | None = None,
    db: AsyncSession | None = None,
    **kwargs: Any,
) -> LanceTable:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError(
            "Table name must start with a letter or underscore and contain only "
            "letters, digits, and underscores."
        )

    session = await _resolve_session(db)

    if namespace_name:
        namespace_obj = await get_namespace_by_name(namespace_name, session)
        if namespace_obj:
            namespace_id = namespace_obj.id
        else:
            namespace_obj = await create_namespace(namespace_name, db=session)
            namespace_id = namespace_obj.id
    else:
        default_namespace = await ensure_default_namespace(session)
        namespace_id = default_namespace.id

    normalized_path, storage_options = normalized_path_and_storage_options(
        lance_path, storage_options
    )

    stmt = select(LanceTable).where(
        or_(LanceTable.name == name, LanceTable.lance_path == normalized_path)
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing:
        raise ValueError(f"Table with name {name} or path {lance_path} already exists")

    try:
        dataset = lance.dataset(normalized_path, storage_options=storage_options)
        row_count = dataset.count_rows()
        schema_dict = schema_to_json(dataset.schema)
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"Failed to open lance dataset: {exc}") from exc

    table = LanceTable(
        name=name,
        lance_path=normalized_path,
        lance_schema=schema_dict,
        row_count=row_count,
        storage_options=storage_options,
        namespace_id=namespace_id,
        **kwargs,
    )
    session.add(table)
    await session.commit()
    await session.refresh(table)
    return table


async def get_lance_table(
    table_id_or_name: str, db: AsyncSession | None = None
) -> LanceTable | None:
    session = await _resolve_session(db)

    try:
        table_id = int(table_id_or_name)
        stmt = select(LanceTable).where(LanceTable.id == table_id)
        result = await session.execute(stmt)
        table = result.scalar_one_or_none()
        if table:
            return table
    except ValueError:
        pass

    stmt = select(LanceTable).where(LanceTable.name == table_id_or_name)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_tables_by_namespace(
    namespace: str, db: AsyncSession | None = None
) -> list[LanceTable]:
    session = await _resolve_session(db)
    namespace_name = namespace if namespace not in {"", "."} else "default"

    namespace_obj = await get_namespace_by_name(namespace_name, session)
    if not namespace_obj:
        return []

    stmt = select(LanceTable).where(LanceTable.namespace_id == namespace_obj.id)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def create_empty_lance_table(
    table_name: str,
    location: str,
    namespace_id: int | None = None,
    storage_options: dict | None = None,
    properties: dict[str, Any] | None = None,
    db: AsyncSession | None = None,
) -> LanceTable:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", table_name):
        raise ValueError(
            "Table name must start with a letter or underscore and contain only "
            "letters, digits, and underscores."
        )

    session = await _resolve_session(db)
    normalized_path, storage_options = normalized_path_and_storage_options(
        location, storage_options
    )

    stmt = select(LanceTable).where(
        or_(LanceTable.name == table_name, LanceTable.lance_path == normalized_path)
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing:
        raise ValueError(f"Table with name {table_name} or path {location} already exists")

    table = LanceTable(
        name=table_name,
        lance_path=normalized_path,
        lance_schema={},
        row_count=0,
        namespace_id=namespace_id,
        storage_options=storage_options,
        **(properties or {}),
    )
    session.add(table)
    await session.commit()
    await session.refresh(table)
    return table


async def drop_table_by_id(
    table_id: str, delimiter: str, db: AsyncSession | None = None
) -> LanceTable:
    session = await _resolve_session(db)
    table_info = await get_lance_table(table_id, session)

    if not table_info and delimiter in table_id:
        table_name = table_id.split(delimiter)[-1]
        table_info = await get_lance_table(table_name, session)

    if not table_info:
        raise ValueError(f"Table '{table_id}' not found")

    await session.delete(table_info)
    await session.commit()
    return table_info


async def deregister_table_by_id(
    table_id: str, delimiter: str, db: AsyncSession | None = None
) -> LanceTable:
    session = await _resolve_session(db)
    table_info = await get_lance_table(table_id, session)

    if not table_info and delimiter in table_id:
        table_name = table_id.split(delimiter)[-1]
        table_info = await get_lance_table(table_name, session)

    if not table_info:
        raise ValueError(f"Table '{table_id}' not found")

    await session.delete(table_info)
    await session.commit()
    return table_info


async def update_lance_table(
    table_id_or_name: str, db: AsyncSession | None = None, **update_data: Any
) -> LanceTable | None:
    session = await _resolve_session(db)
    table = await get_lance_table(table_id_or_name, session)
    if not table:
        return None

    allowed_fields = {
        "lance_path",
        "description",
        "tags",
        "custom_values",
        "last_updated_by",
    }
    for key, value in update_data.items():
        if key in allowed_fields and value is not None:
            setattr(table, key, value)

    await session.commit()
    await session.refresh(table)
    return table


async def delete_lance_table(table_id_or_name: str, db: AsyncSession | None = None) -> bool:
    session = await _resolve_session(db)
    table = await get_lance_table(table_id_or_name, session)
    if not table:
        return False
    await session.delete(table)
    await session.commit()
    return True


async def refresh_lance_table_metadata(
    table_id_or_name: str, db: AsyncSession | None = None
) -> LanceTable:
    session = await _resolve_session(db)
    table = await get_lance_table(table_id_or_name, session)
    if not table:
        raise ValueError("Table not found")

    dataset = lance.dataset(table.lance_path, storage_options=table.storage_options)
    row_count = dataset.count_rows()
    schema_dict = schema_to_json(dataset.schema)
    table.row_count = row_count
    table.lance_schema = schema_dict
    await session.commit()
    await session.refresh(table)
    return table


async def list_table_indices(
    table_id_or_name: str, db: AsyncSession | None = None
) -> list[dict[str, Any]]:
    session = await _resolve_session(db)
    table = await get_lance_table(table_id_or_name, session)
    if not table:
        raise ValueError("Table not found")

    dataset = lance.dataset(table.lance_path, storage_options=table.storage_options)
    indices: list[dict[str, Any]] = []
    schema = dataset.schema
    for field in schema:
        if field.name.endswith("_vector") or "embedding" in field.name:
            indices.append(
                {
                    "name": f"{field.name}_index",
                    "columns": [field.name],
                    "type": "VECTOR",
                    "status": "unknown",
                }
            )

    return indices
