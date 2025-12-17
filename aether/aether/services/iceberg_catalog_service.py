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

"""Service layer that backs the Iceberg REST catalog routes.

Uses pyiceberg's SqlCatalog as the internal implementation, which properly handles
metadata storage in PostgreSQL and file operations for S3-compatible stores.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import unquote

from fastapi import HTTPException, status
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import (
    CommitTableRequest,
    Table,
)
from pyiceberg.table import (
    CommitTableResponse as PyIcebergCommitTableResponse,
)
from pyiceberg.typedef import Identifier

from ..core.settings import Settings, get_settings
from ..schemas.iceberg import (
    CatalogConfigResponse,
    CommitTableResponse,
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    CreateTableRequest,
    CreateTableResponse,
    DropTableResponse,
    ListNamespacesResponse,
    ListTablesResponse,
    LoadTableResponse,
    NamespaceResponse,
    RegisterTableRequest,
    RegisterTableResponse,
    TableIdentifier,
    UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "default"


def parse_namespace(namespace_str: str | None) -> list[str]:
    """Parse encoded namespace strings into a list of segments."""
    if not namespace_str:
        return []
    decoded = unquote(namespace_str)
    if not decoded:
        return []
    return [segment for segment in decoded.split(".") if segment]


def format_namespace(segments: list[str] | tuple[str, ...]) -> str:
    """Format namespace segments into the canonical catalog name."""
    return ".".join(segments) if segments else DEFAULT_NAMESPACE


def normalize_namespace_list(namespace: list[str] | tuple[str, ...] | None) -> list[str]:
    """Normalize namespace lists that may be empty or contain blanks."""
    if not namespace:
        return []
    return [segment for segment in namespace if segment]


_sql_catalog_instance: SqlCatalog | None = None


def _get_sql_catalog() -> SqlCatalog:
    """Create or return cached SqlCatalog instance."""
    global _sql_catalog_instance
    if _sql_catalog_instance is not None:
        return _sql_catalog_instance

    settings = get_settings()
    cfg = settings.iceberg

    # Build catalog properties for pyiceberg SqlCatalog
    # See: https://py.iceberg.apache.org/configuration/#sql-catalog
    # Convert async database URL to sync psycopg (v3) URL for SqlCatalog
    db_url = settings.database_url.replace("+asyncpg", "+psycopg").replace(
        "+psycopg+psycopg", "+psycopg"
    )
    catalog_props: dict[str, str] = {
        "uri": db_url,
        "warehouse": cfg.warehouse_uri(),
    }

    # Add S3 configuration if using S3 backend
    if cfg.is_s3:
        if endpoint := cfg.endpoint_for_backend():
            catalog_props["s3.endpoint"] = endpoint
        catalog_props["s3.access-key-id"] = cfg.s3_access_key_id
        catalog_props["s3.secret-access-key"] = cfg.s3_secret_access_key
        catalog_props["s3.region"] = cfg.s3_region
        # Use path-style addressing for S3-compatible stores like MinIO
        catalog_props["s3.path-style-access"] = "true"
    # Local storage configuration is handled automatically by pyiceberg
    # when warehouse starts with file://

    _LOGGER.info(
        "Creating SqlCatalog with warehouse=%s, uri=%s", cfg.warehouse_uri(), catalog_props["uri"]
    )

    _sql_catalog_instance = SqlCatalog("aether_catalog", **catalog_props)
    return _sql_catalog_instance


def clear_catalog_cache() -> None:
    """Clear the cached SqlCatalog instance (useful for testing)."""
    global _sql_catalog_instance
    _sql_catalog_instance = None


class IcebergCatalogService:
    """Facade around pyiceberg's SqlCatalog for Iceberg REST catalog operations."""

    def __init__(self, settings: Settings | None = None):
        self._settings = settings or get_settings()
        self._catalog: SqlCatalog | None = None

    @property
    def catalog(self) -> SqlCatalog:
        """Get the underlying SqlCatalog instance."""
        if self._catalog is None:
            self._catalog = _get_sql_catalog()
        return self._catalog

    def warehouse_location(self) -> str:
        """Return the catalog warehouse base location."""
        return self._settings.iceberg.warehouse_uri()

    # --------------------------------------------------------------------- #
    # Public API – mirrors the Iceberg REST spec
    # --------------------------------------------------------------------- #

    def get_config(self) -> CatalogConfigResponse:
        """Return catalog configuration for clients."""
        cfg = self._settings.iceberg
        defaults: dict[str, Any] = {
            "warehouse": self.warehouse_location(),
            "type": "rest",
        }

        if cfg.is_s3:
            if s3_endpoint := cfg.endpoint_for_clients():
                defaults["s3.endpoint"] = s3_endpoint
            defaults["s3.access-key-id"] = cfg.s3_access_key_id
            defaults["s3.secret-access-key"] = cfg.s3_secret_access_key
            defaults["s3.region"] = cfg.s3_region
        elif cfg.is_local:
            defaults["fileio.local.root-path"] = cfg.local_root_path.rstrip("/")

        return CatalogConfigResponse(defaults=defaults, overrides={})

    async def list_namespaces(self, parent: str | None) -> ListNamespacesResponse:
        """List all namespaces, optionally filtered by parent."""
        parent_ns: Identifier = ()
        if parent:
            parent_segments = parse_namespace(parent)
            parent_ns = tuple(parent_segments)

        try:
            namespaces = await asyncio.to_thread(self.catalog.list_namespaces, parent_ns)
        except NoSuchNamespaceError:
            # Parent doesn't exist, return empty list
            namespaces = []

        namespace_rows = [list(ns) for ns in namespaces]
        return ListNamespacesResponse(namespaces=namespace_rows)

    async def create_namespace(
        self,
        request: CreateNamespaceRequest,
        *,
        namespace_override: list[str] | None = None,
    ) -> CreateNamespaceResponse:
        """Create a new namespace."""
        namespace_segments = normalize_namespace_list(
            namespace_override
        ) or normalize_namespace_list(request.namespace)
        if not namespace_segments:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Namespace name must be provided in request"
            )

        namespace_tuple = tuple(namespace_segments)
        properties = request.properties or {}

        try:
            await asyncio.to_thread(self.catalog.create_namespace, namespace_tuple, properties)
        except NamespaceAlreadyExistsError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except Exception as exc:
            _LOGGER.exception("Failed to create namespace %s", namespace_segments)
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

        # Load the created namespace to get merged properties
        try:
            loaded_props = await asyncio.to_thread(
                self.catalog.load_namespace_properties, namespace_tuple
            )
        except NoSuchNamespaceError:
            loaded_props = properties

        return CreateNamespaceResponse(
            namespace=namespace_segments,
            properties={k: str(v) for k, v in loaded_props.items()},
        )

    async def get_namespace(self, namespace: list[str]) -> NamespaceResponse:
        """Get namespace metadata."""
        namespace_segments = namespace or [DEFAULT_NAMESPACE]
        namespace_tuple = tuple(namespace_segments)

        try:
            properties = await asyncio.to_thread(
                self.catalog.load_namespace_properties, namespace_tuple
            )
        except NoSuchNamespaceError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Namespace '{format_namespace(namespace_segments)}' not found",
            ) from exc

        return NamespaceResponse(
            namespace=namespace_segments,
            properties={k: str(v) for k, v in properties.items()},
        )

    async def delete_namespace(self, namespace: list[str]) -> None:
        """Delete a namespace."""
        namespace_name = format_namespace(namespace)
        if namespace_name in {"", DEFAULT_NAMESPACE}:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cannot delete default namespace")

        namespace_tuple = tuple(namespace)

        try:
            await asyncio.to_thread(self.catalog.drop_namespace, namespace_tuple)
        except NoSuchNamespaceError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Namespace '{namespace_name}' not found",
            ) from exc
        except NamespaceNotEmptyError as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"Namespace '{namespace_name}' is not empty",
            ) from exc

    async def update_namespace_properties(
        self,
        namespace: list[str],
        request: UpdateNamespacePropertiesRequest,
    ) -> UpdateNamespacePropertiesResponse:
        """Update namespace properties."""
        namespace_tuple = tuple(namespace)
        removals = set(request.removals or [])
        updates = request.updates or {}

        try:
            await asyncio.to_thread(
                self.catalog.update_namespace_properties,
                namespace_tuple,
                removals,
                updates,
            )
        except NoSuchNamespaceError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Namespace '{format_namespace(namespace)}' not found",
            ) from exc

        return UpdateNamespacePropertiesResponse(
            removed=list(removals),
            updated=list(updates.keys()),
            missing=[],
        )

    async def list_tables(self, namespace: list[str]) -> ListTablesResponse:
        """List all tables in a namespace."""
        namespace_tuple = tuple(namespace) if namespace else (DEFAULT_NAMESPACE,)

        try:
            tables = await asyncio.to_thread(self.catalog.list_tables, namespace_tuple)
        except NoSuchNamespaceError:
            tables = []

        identifiers = [
            TableIdentifier(namespace=list(table_id[:-1]), name=table_id[-1]) for table_id in tables
        ]
        return ListTablesResponse(identifiers=identifiers)

    async def create_table(
        self,
        namespace: list[str],
        request: CreateTableRequest,
    ) -> CreateTableResponse:
        """Create a new table."""
        namespace_tuple = tuple(namespace) if namespace else (DEFAULT_NAMESPACE,)
        table_identifier = (*namespace_tuple, request.name)

        _LOGGER.info(
            "Creating table %s with schema %s",
            table_identifier,
            request.table_schema,
        )

        # Parse the schema from the request
        try:
            iceberg_schema = IcebergSchema.model_validate(request.table_schema)
        except Exception as exc:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"Invalid schema: {exc}",
            ) from exc

        # Build table properties
        properties = request.properties or {}
        if request.location:
            properties["location"] = request.location

        try:
            table: Table = await asyncio.to_thread(
                self.catalog.create_table,
                table_identifier,
                iceberg_schema,
                properties=properties,
            )
        except NamespaceAlreadyExistsError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except TableAlreadyExistsError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except NoSuchNamespaceError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Namespace '{format_namespace(namespace)}' not found",
            ) from exc
        except Exception as exc:
            _LOGGER.exception("Failed to create table %s", table_identifier)
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

        metadata = table.metadata.model_dump(mode="json")
        metadata_location = table.metadata_location

        return CreateTableResponse(
            metadata_location=metadata_location,
            metadata=metadata,
            config={},
        )

    async def load_table(
        self,
        namespace: list[str],
        table_name: str,
    ) -> LoadTableResponse:
        """Load a table's metadata."""
        namespace_tuple = tuple(namespace) if namespace else (DEFAULT_NAMESPACE,)
        table_identifier = (*namespace_tuple, table_name)

        try:
            table: Table = await asyncio.to_thread(self.catalog.load_table, table_identifier)
        except NoSuchTableError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Table '{format_namespace(namespace)}.{table_name}' not found",
            ) from exc

        metadata = table.metadata.model_dump(mode="json")
        return LoadTableResponse(
            metadata_location=table.metadata_location,
            metadata=metadata,
            config={},
        )

    async def register_table(
        self,
        namespace: list[str],
        table_name: str,
        request: RegisterTableRequest,
    ) -> RegisterTableResponse:
        """Register an existing table from a metadata file."""
        namespace_tuple = tuple(namespace) if namespace else (DEFAULT_NAMESPACE,)
        table_identifier = (*namespace_tuple, table_name)

        try:
            table: Table = await asyncio.to_thread(
                self.catalog.register_table,
                table_identifier,
                request.metadata_location,
            )
        except NoSuchNamespaceError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Namespace '{format_namespace(namespace)}' not found",
            ) from exc
        except TableAlreadyExistsError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except Exception as exc:
            _LOGGER.exception("Failed to register table %s", table_identifier)
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

        metadata = table.metadata.model_dump(mode="json")
        return RegisterTableResponse(
            metadata_location=table.metadata_location,
            metadata=metadata,
            config={},
        )

    async def update_table(
        self,
        namespace: list[str],
        table_name: str,
        payload: dict[str, Any],
    ) -> CommitTableResponse:
        """Commit updates to a table.

        This is the main endpoint used by pyiceberg for all table modifications
        including schema evolution, property updates, and snapshot commits.
        """
        namespace_tuple = tuple(namespace) if namespace else (DEFAULT_NAMESPACE,)
        table_identifier = (*namespace_tuple, table_name)

        # Load the current table
        try:
            table: Table = await asyncio.to_thread(self.catalog.load_table, table_identifier)
        except NoSuchTableError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Table '{format_namespace(namespace)}.{table_name}' not found",
            ) from exc

        # Parse the commit request
        try:
            commit_request = CommitTableRequest.model_validate(payload)
        except Exception as exc:
            _LOGGER.exception("Failed to parse commit request for %s", table_identifier)
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"Invalid commit request: {exc}",
            ) from exc

        _LOGGER.info(
            "Committing updates to table %s: %d requirements, %d updates",
            table_identifier,
            len(commit_request.requirements),
            len(commit_request.updates),
        )

        # Commit the updates using SqlCatalog
        try:
            response: PyIcebergCommitTableResponse = await asyncio.to_thread(
                self.catalog.commit_table,
                table,
                commit_request.requirements,
                commit_request.updates,
            )
        except Exception as exc:
            _LOGGER.exception("Failed to commit table %s", table_identifier)
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"Failed to commit table: {exc}",
            ) from exc

        return CommitTableResponse(
            metadata_location=response.metadata_location,
            metadata=response.metadata.model_dump(mode="json"),
        )

    async def drop_table(
        self,
        namespace: list[str],
        table_name: str,
    ) -> DropTableResponse:
        """Drop a table."""
        namespace_tuple = tuple(namespace) if namespace else (DEFAULT_NAMESPACE,)
        table_identifier = (*namespace_tuple, table_name)

        try:
            await asyncio.to_thread(self.catalog.drop_table, table_identifier)
        except NoSuchTableError as exc:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Table '{format_namespace(namespace)}.{table_name}' not found",
            ) from exc

        return DropTableResponse(dropped=True)


__all__ = [
    "IcebergCatalogService",
    "parse_namespace",
    "format_namespace",
    "normalize_namespace_list",
    "clear_catalog_cache",
    "DEFAULT_NAMESPACE",
]
