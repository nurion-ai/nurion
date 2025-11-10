"""Service layer that backs the Iceberg REST catalog routes."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from fastapi import HTTPException, status
from pyiceberg.schema import Schema as IcebergSchema  # type: ignore
from pyiceberg.table import UNPARTITIONED_PARTITION_SPEC  # type: ignore
from pyiceberg.table.metadata import new_table_metadata  # type: ignore
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER  # type: ignore
from sqlalchemy.ext.asyncio import AsyncSession

from ..core import object_store
from ..core.object_store import ObjectStoreError
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
from ..services import iceberg_table_service

_LOGGER = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "default"
METADATA_FILE_SUFFIX = ".metadata.json"


def parse_namespace(namespace_str: str | None) -> list[str]:
    """Parse encoded namespace strings into a list of segments."""
    if not namespace_str:
        return []
    decoded = unquote(namespace_str)
    if not decoded:
        return []
    return [segment for segment in decoded.split(".") if segment]


def format_namespace(segments: Sequence[str]) -> str:
    """Format namespace segments into the canonical catalog name."""
    return ".".join(segments) if segments else DEFAULT_NAMESPACE


def normalize_namespace_list(namespace: Sequence[str] | None) -> list[str]:
    """Normalize namespace lists that may be empty or contain blanks."""
    if not namespace:
        return []
    return [segment for segment in namespace if segment]


@dataclass(slots=True)
class IcebergCatalogService:
    """Facade around database and object store helpers for Iceberg metadata."""

    settings: Settings = field(default_factory=get_settings)

    def warehouse_location(self) -> str:
        """Return the catalog warehouse base location."""
        return self.settings.iceberg.warehouse_uri()

    # --------------------------------------------------------------------- #
    # Public API – mirrors the Java RESTCatalogAdapter methods.
    # --------------------------------------------------------------------- #

    def get_config(self) -> CatalogConfigResponse:
        warehouse = self.warehouse_location()
        cfg = self.settings.iceberg
        defaults: dict[str, Any] = {
            "warehouse": warehouse,
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

    async def list_namespaces(self, parent: str | None, db: AsyncSession) -> ListNamespacesResponse:
        namespaces = await iceberg_table_service.get_all_iceberg_namespaces(db)
        parent_filter = None
        if parent:
            parent_segments = parse_namespace(parent)
            parent_filter = format_namespace(parent_segments)

        namespace_rows = []
        for namespace in namespaces:
            segments = namespace.name.split(".") if namespace.name else [DEFAULT_NAMESPACE]
            if parent_filter and not namespace.name.startswith(parent_filter):
                continue
            namespace_rows.append(segments)

        return ListNamespacesResponse(namespaces=namespace_rows)

    async def create_namespace(
        self,
        request: CreateNamespaceRequest,
        db: AsyncSession,
        *,
        namespace_override: list[str] | None = None,
    ) -> CreateNamespaceResponse:
        namespace_segments = normalize_namespace_list(
            namespace_override
        ) or normalize_namespace_list(request.namespace)
        if not namespace_segments:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Namespace name must be provided in request"
            )

        namespace_name = format_namespace(namespace_segments)
        properties = request.properties or {}

        try:
            ns = await iceberg_table_service.create_iceberg_namespace(
                name=namespace_name,
                properties=properties,
                db=db,
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

        merged_properties = {
            **{k: str(v) for k, v in (ns.properties or {}).items()},
            **{k: str(v) for k, v in properties.items()},
        }

        return CreateNamespaceResponse(namespace=namespace_segments, properties=merged_properties)

    async def get_namespace(self, namespace: list[str], db: AsyncSession) -> NamespaceResponse:
        namespace_name = format_namespace(namespace)
        namespace_segments = namespace or [DEFAULT_NAMESPACE]

        ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
        if not ns:
            if namespace_name == DEFAULT_NAMESPACE:
                ns = await iceberg_table_service.ensure_default_iceberg_namespace(db)
            else:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f"Namespace '{namespace_name}' not found",
                )

        properties = {k: str(v) for k, v in (ns.properties or {}).items()}
        return NamespaceResponse(namespace=namespace_segments, properties=properties)

    async def delete_namespace(self, namespace: list[str], db: AsyncSession) -> None:
        namespace_name = format_namespace(namespace)
        if namespace_name in {"", DEFAULT_NAMESPACE}:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cannot delete default namespace")

        ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
        if not ns:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Namespace '{namespace_name}' not found",
            )

        try:
            deleted = await iceberg_table_service.delete_iceberg_namespace(ns.id, db)
        except ValueError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

        if not deleted:
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                f"Failed to delete namespace '{namespace_name}'",
            )

    async def update_namespace_properties(
        self,
        namespace: list[str],
        request: UpdateNamespacePropertiesRequest,
        db: AsyncSession,
    ) -> UpdateNamespacePropertiesResponse:
        namespace_name = format_namespace(namespace)

        try:
            await iceberg_table_service.update_iceberg_namespace_properties(
                name=namespace_name,
                removals=request.removals or [],
                updates=request.updates or {},
                db=db,
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from exc

        removed_keys = request.removals or []
        updated_keys = list((request.updates or {}).keys())

        return UpdateNamespacePropertiesResponse(
            removed=removed_keys,
            updated=updated_keys,
            missing=[],
        )

    async def list_tables(self, namespace: list[str], db: AsyncSession) -> ListTablesResponse:
        namespace_name = format_namespace(namespace)
        tables = await iceberg_table_service.get_iceberg_tables_by_namespace(namespace_name, db)
        identifiers = [
            TableIdentifier(namespace=namespace or [DEFAULT_NAMESPACE], name=table.name)
            for table in tables
        ]
        return ListTablesResponse(identifiers=identifiers)

    async def create_table(
        self,
        namespace: list[str],
        request: CreateTableRequest,
        db: AsyncSession,
    ) -> CreateTableResponse:
        namespace_name = format_namespace(namespace)

        ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
        if not ns:
            if namespace_name == DEFAULT_NAMESPACE:
                ns = await iceberg_table_service.ensure_default_iceberg_namespace(db)
            else:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f"Namespace '{namespace_name}' not found",
                )

        table_location = request.location.rstrip("/") if request.location else None
        if request.write_metadata_location:
            metadata_location = request.write_metadata_location
        elif table_location:
            metadata_location = f"{table_location}/metadata/metadata.json"
        else:
            metadata_location = self.default_metadata_location(namespace_name, request.name)

        try:
            await iceberg_table_service.create_iceberg_table(
                table_name=request.name,
                namespace_name=namespace_name,
                metadata_location=metadata_location,
                db=db,
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

        table_metadata = self._build_table_metadata(metadata_location, request)

        await self._write_metadata(metadata_location, table_metadata)

        return CreateTableResponse(
            metadata_location=metadata_location,
            metadata=table_metadata,
            config={},
        )

    async def update_table(
        self,
        namespace: list[str],
        table_name: str,
        updates: dict[str, Any],
        db: AsyncSession,
    ) -> LoadTableResponse:
        namespace_name = format_namespace(namespace)
        table = await self._get_table(table_name, namespace_name, db)

        metadata = await self._read_metadata(table.metadata_location, allow_missing=True)
        if metadata is None:
            metadata = self._build_empty_metadata(table.metadata_location)

        self._apply_table_updates(metadata, updates.get("updates", []))

        await self._write_metadata(table.metadata_location, metadata)

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
        db: AsyncSession,
    ) -> RegisterTableResponse:
        namespace_name = format_namespace(namespace)
        ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
        if not ns:
            if namespace_name == DEFAULT_NAMESPACE:
                ns = await iceberg_table_service.ensure_default_iceberg_namespace(db)
            else:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f"Namespace '{namespace_name}' not found",
                )

        try:
            iceberg_table = await iceberg_table_service.create_iceberg_table(
                table_name=table_name,
                namespace_name=namespace_name,
                metadata_location=request.metadata_location,
                db=db,
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

        metadata = await self._read_metadata(iceberg_table.metadata_location)

        return RegisterTableResponse(
            metadata_location=iceberg_table.metadata_location,
            metadata=metadata,
            config={},
        )

    async def load_table(
        self,
        namespace: list[str],
        table_name: str,
        db: AsyncSession,
    ) -> LoadTableResponse:
        namespace_name = format_namespace(namespace)
        table = await self._get_table(table_name, namespace_name, db)
        metadata = await self._read_metadata(table.metadata_location)
        return LoadTableResponse(
            metadata_location=table.metadata_location,
            metadata=metadata,
            config={},
        )

    async def commit_table(
        self,
        namespace: list[str],
        table_name: str,
        payload: dict[str, Any],
        db: AsyncSession,
    ) -> CommitTableResponse:
        namespace_name = format_namespace(namespace)
        table = await self._get_table(table_name, namespace_name, db)

        new_metadata_location = await self._extract_metadata_location_from_updates(
            payload.get("updates", [])
        )

        if new_metadata_location and new_metadata_location != table.metadata_location:
            try:
                updated_table = await iceberg_table_service.update_iceberg_table_metadata_location(
                    table_name=table_name,
                    namespace_name=namespace_name,
                    metadata_location=new_metadata_location,
                    db=db,
                )
            except ValueError as exc:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    f"Failed to update table: {exc}",
                ) from exc
            table = updated_table

        metadata = await self._read_metadata(table.metadata_location)

        return CommitTableResponse(
            metadata_location=table.metadata_location,
            metadata=metadata,
        )

    async def drop_table(
        self,
        namespace: list[str],
        table_name: str,
        db: AsyncSession,
    ) -> DropTableResponse:
        namespace_name = format_namespace(namespace)
        deleted = await iceberg_table_service.delete_iceberg_table(table_name, namespace_name, db)
        if not deleted:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Table '{namespace_name}.{table_name}' not found",
            )
        return DropTableResponse(dropped=True)

    # ------------------------------------------------------------------ #
    # Helper utilities
    # ------------------------------------------------------------------ #

    def default_metadata_location(self, namespace_name: str, table_name: str) -> str:
        warehouse = self.warehouse_location()
        namespace_path = namespace_name.replace(".", "/")
        return f"{warehouse}/{namespace_path}/{table_name}/metadata/metadata.json"

    @staticmethod
    def table_location_from_metadata(metadata_location: str) -> str:
        if "/metadata/" in metadata_location:
            return metadata_location.rsplit("/metadata/", 1)[0]
        return metadata_location.rstrip("/")

    async def _read_metadata(
        self, metadata_location: str, allow_missing: bool = False
    ) -> dict[str, Any] | None:
        try:
            return await object_store.read_json(metadata_location)
        except FileNotFoundError:
            if allow_missing:
                return None
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Metadata not found at {metadata_location}",
            ) from None
        except ObjectStoreError as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                f"Failed to read metadata from {metadata_location}: {exc}",
            ) from exc

    async def _write_metadata(self, metadata_location: str, metadata: dict[str, Any]) -> None:
        try:
            await object_store.write_json(metadata_location, metadata)
        except ObjectStoreError as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                f"Failed to write metadata to {metadata_location}: {exc}",
            ) from exc

    async def _list_metadata_files(self, prefix: str) -> list[str]:
        try:
            return await object_store.list_objects(prefix)
        except ObjectStoreError as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                f"Failed to list metadata under {prefix}: {exc}",
            ) from exc

    def _build_table_metadata(
        self, metadata_location: str, request: CreateTableRequest
    ) -> dict[str, Any]:
        iceberg_schema = IcebergSchema.model_validate(request.table_schema)
        table_metadata = new_table_metadata(
            location=self.table_location_from_metadata(metadata_location),
            schema=iceberg_schema,
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
            sort_order=UNSORTED_SORT_ORDER,
            properties=request.properties or {},
        )
        return table_metadata.model_dump(mode="json")

    def _build_empty_metadata(self, metadata_location: str) -> dict[str, Any]:
        table_metadata = new_table_metadata(
            location=self.table_location_from_metadata(metadata_location),
            schema=IcebergSchema(),
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
            sort_order=UNSORTED_SORT_ORDER,
            properties={},
        )
        return table_metadata.model_dump(mode="json")

    @staticmethod
    def _apply_table_updates(metadata: dict[str, Any], updates: Iterable[dict[str, Any]]) -> None:
        for update in updates:
            action = update.get("action")
            if action == "add-snapshot":
                snapshot = update.get("snapshot", {})
                snapshot_id = snapshot.get("snapshot-id")
                if snapshot_id is None:
                    continue
                metadata.setdefault("snapshots", []).append(snapshot)
                metadata["current-snapshot-id"] = snapshot_id
                _LOGGER.info("Applied add-snapshot %s", snapshot_id)
            elif action == "set-snapshot-ref":
                ref_name = update.get("ref-name", "main")
                snapshot_id = update.get("snapshot-id")
                if snapshot_id is None:
                    continue
                metadata.setdefault("refs", {})[ref_name] = {
                    "snapshot-id": snapshot_id,
                    "type": update.get("type", "branch"),
                }
                _LOGGER.info("Applied set-snapshot-ref %s -> %s", ref_name, snapshot_id)

    async def _extract_metadata_location_from_updates(
        self, updates: Iterable[dict[str, Any]]
    ) -> str | None:
        for update in updates:
            if update.get("action") != "add-snapshot":
                continue
            snapshot = update.get("snapshot", {})
            manifest_list = snapshot.get("manifest-list")
            if not manifest_list:
                continue
            metadata_dir = manifest_list.rsplit("/", 1)[0]
            candidates = await self._list_metadata_files(metadata_dir)
            metadata_files = [
                candidate for candidate in candidates if candidate.endswith(METADATA_FILE_SUFFIX)
            ]
            if metadata_files:
                latest = sorted(metadata_files)[-1]
                _LOGGER.info("Resolved latest metadata file %s", latest)
                return latest
        return None

    async def _get_table(self, table_name: str, namespace_name: str, db: AsyncSession):
        table = await iceberg_table_service.get_iceberg_table_by_name(
            table_name, namespace_name, db
        )
        if not table:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Table '{namespace_name}.{table_name}' not found",
            )
        return table


__all__ = [
    "IcebergCatalogService",
    "parse_namespace",
    "format_namespace",
    "normalize_namespace_list",
    "DEFAULT_NAMESPACE",
]
