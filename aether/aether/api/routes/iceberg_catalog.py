"""Iceberg REST Catalog API routes."""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import unquote

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from ...db.session import get_session
from ...schemas.iceberg import (
    CatalogConfigResponse,
    CommitTableRequest,
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
from ...services import iceberg_table_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/iceberg-catalog/v1", tags=["iceberg-rest-catalog"])


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    async for session in get_session():
        yield session


def parse_namespace(namespace_str: str) -> list[str]:
    """Parse namespace string into list of segments."""
    if not namespace_str:
        return []
    # URL decode and split by dot
    decoded = unquote(namespace_str)
    return decoded.split(".") if decoded else []


def format_namespace(namespace_list: list[str]) -> str:
    """Format namespace list into dot-separated string."""
    return ".".join(namespace_list) if namespace_list else "default"


def extract_last_column_id(schema_data: dict[str, Any] | list[dict[str, Any]] | None) -> int:
    """Extract last-column-id from Iceberg schema."""
    if not schema_data:
        return 0
    
    if isinstance(schema_data, dict):
        if "fields" in schema_data:
            # Schema is in Iceberg JSON format
            field_ids = [
                field.get("id", 0) for field in schema_data.get("fields", [])
                if isinstance(field, dict) and "id" in field
            ]
            return max(field_ids) if field_ids else 0
        elif "schema-id" in schema_data:
            # Alternative format
            return schema_data.get("schema-id", 0)
    elif isinstance(schema_data, list):
        # List of fields
        field_ids = [
            field.get("id", 0) for field in schema_data
            if isinstance(field, dict) and "id" in field
        ]
        return max(field_ids) if field_ids else 0
    
    return 0


def build_iceberg_metadata(
    format_version: int,
    table_uuid: str,
    location: str,
    schema: dict[str, Any] | None,
    partition_spec: dict[str, Any] | list | None,
    properties: dict[str, str] | None,
) -> dict[str, Any]:
    """Build Iceberg metadata dictionary with required fields."""
    last_column_id = extract_last_column_id(schema)
    return {
        "format-version": format_version,
        "table-uuid": table_uuid,
        "location": location,
        "last-column-id": last_column_id,
        "schema": schema or {},
        "partition-spec": partition_spec or [],
        "properties": properties or {},
    }


@router.get("/config", response_model=CatalogConfigResponse)
async def get_config() -> CatalogConfigResponse:
    """Get catalog configuration."""
    return CatalogConfigResponse(
        defaults={
            "warehouse": "file:///tmp/warehouse",
            "type": "rest",
        },
        overrides={},
    )


@router.get("/namespaces", response_model=ListNamespacesResponse)
async def list_namespaces(
    parent: str | None = Query(None, description="Parent namespace to list"),
    db: AsyncSession = Depends(get_db_session),
) -> ListNamespacesResponse:
    """List all Iceberg namespaces."""
    namespaces = await iceberg_table_service.get_all_iceberg_namespaces(db)
    namespace_list = [[ns.name] for ns in namespaces]

    if parent:
        parent_list = parse_namespace(parent)
        namespace_list = [
            [ns.name]
            for ns in namespaces
            if ns.name.startswith(format_namespace(parent_list))
        ]

    return ListNamespacesResponse(namespaces=namespace_list)


@router.post("/namespaces", response_model=CreateNamespaceResponse)
async def create_namespace_post(
    request: CreateNamespaceRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> CreateNamespaceResponse:
    """Create a namespace (Iceberg REST Catalog standard endpoint)."""
    if not request.namespace:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Namespace name must be provided in request",
        )
    
    namespace_name = format_namespace(request.namespace)
    
    try:
        ns = await iceberg_table_service.create_iceberg_namespace(
            name=namespace_name,
            properties=request.properties or {},
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    properties = {k: str(v) for k, v in (ns.properties or {}).items()}
    properties.update({k: str(v) for k, v in (request.properties or {}).items()})

    return CreateNamespaceResponse(namespace=request.namespace, properties=properties)


@router.post("/namespaces/{namespace}", response_model=CreateNamespaceResponse)
async def create_namespace(
    namespace: str = Path(..., description="Namespace identifier"),
    request: CreateNamespaceRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> CreateNamespaceResponse:
    """Create a namespace."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    try:
        ns = await iceberg_table_service.create_iceberg_namespace(
            name=namespace_name,
            properties=request.properties or {},
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    properties = {k: str(v) for k, v in (ns.properties or {}).items()}
    properties.update({k: str(v) for k, v in (request.properties or {}).items()})

    return CreateNamespaceResponse(namespace=namespace_list, properties=properties)


@router.get("/namespaces/{namespace}", response_model=NamespaceResponse)
async def get_namespace(
    namespace: str = Path(..., description="Namespace identifier"),
    db: AsyncSession = Depends(get_db_session),
) -> NamespaceResponse:
    """Get namespace information."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    if namespace_name == "":
        namespace_name = "default"
        namespace_list = ["default"]

    ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
    if not ns:
        if namespace_name == "default":
            ns = await iceberg_table_service.ensure_default_iceberg_namespace(db)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Namespace '{namespace}' not found",
            )

    properties = {k: str(v) for k, v in (ns.properties or {}).items()}
    return NamespaceResponse(namespace=namespace_list, properties=properties)


@router.delete("/namespaces/{namespace}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_namespace(
    namespace: str = Path(..., description="Namespace identifier"),
    db: AsyncSession = Depends(get_db_session),
):
    """Delete a namespace."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    if namespace_name in {"", "default"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete default namespace",
        )

    ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
    if not ns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace '{namespace}' not found",
        )

    try:
        deleted = await iceberg_table_service.delete_iceberg_namespace(ns.id, db)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from exc

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete namespace '{namespace}'",
        )


@router.post(
    "/namespaces/{namespace}/properties",
    response_model=UpdateNamespacePropertiesResponse,
)
async def update_namespace_properties(
    namespace: str = Path(..., description="Namespace identifier"),
    request: UpdateNamespacePropertiesRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> UpdateNamespacePropertiesResponse:
    """Update namespace properties."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    try:
        ns = await iceberg_table_service.update_iceberg_namespace_properties(
            name=namespace_name,
            removals=request.removals or [],
            updates=request.updates or {},
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc

    # Determine which keys were removed, updated, missing
    # This is a simplified approach - the service returns the updated namespace
    removed_keys = request.removals or []
    updated_keys = list((request.updates or {}).keys())
    missing_keys = []  # Would need to track this separately
    
    return UpdateNamespacePropertiesResponse(
        removed=removed_keys,
        updated=updated_keys,
        missing=missing_keys,
    )


@router.get("/namespaces/{namespace}/tables", response_model=ListTablesResponse)
async def list_tables(
    namespace: str = Path(..., description="Namespace identifier"),
    db: AsyncSession = Depends(get_db_session),
) -> ListTablesResponse:
    """List all tables in a namespace."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    tables = await iceberg_table_service.get_iceberg_tables_by_namespace(namespace_name, db)
    identifiers = [TableIdentifier(namespace=namespace_list, name=table.name) for table in tables]

    return ListTablesResponse(identifiers=identifiers)


@router.post(
    "/namespaces/{namespace}/tables",
    response_model=CreateTableResponse,
)
async def create_table_post(
    namespace: str = Path(..., description="Namespace identifier"),
    request: CreateTableRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> CreateTableResponse:
    """Create a new Iceberg table (Iceberg REST Catalog standard endpoint)."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)
    table_name = request.name

    # Ensure namespace exists
    ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
    if not ns:
        if namespace_name == "default":
            ns = await iceberg_table_service.ensure_default_iceberg_namespace(db)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Namespace '{namespace}' not found",
            )

    # Generate metadata location if not provided
    metadata_location = request.write_metadata_location or ""
    if not metadata_location:
        metadata_location = f"s3://warehouse/{namespace_name}/{table_name}/metadata/metadata.json"

    try:
        iceberg_table = await iceberg_table_service.create_iceberg_table(
            table_name=table_name,
            namespace_name=namespace_name,
            metadata_location=metadata_location,
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    # Build Iceberg metadata from request (metadata should be read from storage)
    location = metadata_location.rsplit("/metadata/", 1)[0] if "/metadata/" in metadata_location else metadata_location
    table_uuid_str = str(uuid.uuid4())
    metadata = build_iceberg_metadata(
        format_version=1,  # Default, should be read from metadata_location
        table_uuid=table_uuid_str,
        location=location,
        schema=request.schema,
        partition_spec=request.partition_spec,
        properties=request.properties or {},
    )

    return CreateTableResponse(
        metadata_location=metadata_location,
        metadata=metadata,
        config={},
    )


@router.post(
    "/namespaces/{namespace}/tables/{table}",
    response_model=CreateTableResponse,
)
async def create_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    request: CreateTableRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> CreateTableResponse:
    """Create a new Iceberg table (alternative endpoint with table name in path)."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    # Ensure namespace exists
    ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
    if not ns:
        if namespace_name == "default":
            ns = await iceberg_table_service.ensure_default_iceberg_namespace(db)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Namespace '{namespace}' not found",
            )

    # For Iceberg, we store the metadata location in lance_path
    # The actual table data location is determined by Iceberg
    metadata_location = request.write_metadata_location or ""
    if not metadata_location:
        # Generate a default metadata location
        metadata_location = f"s3://warehouse/{namespace_name}/{table}/metadata/metadata.json"

    try:
        iceberg_table = await iceberg_table_service.create_iceberg_table(
            table_name=table,
            namespace_name=namespace_name,
            metadata_location=metadata_location,
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    # Build Iceberg metadata from request (metadata should be read from storage)
    location = metadata_location.rsplit("/metadata/", 1)[0] if "/metadata/" in metadata_location else metadata_location
    table_uuid_str = str(uuid.uuid4())
    metadata = build_iceberg_metadata(
        format_version=1,  # Default, should be read from metadata_location
        table_uuid=table_uuid_str,
        location=location,
        schema=request.schema,
        partition_spec=request.partition_spec,
        properties=request.properties or {},
    )

    return CreateTableResponse(
        metadata_location=iceberg_table.metadata_location,
        metadata=metadata,
        config={},
    )


@router.post(
    "/namespaces/{namespace}/tables/{table}/register",
    response_model=RegisterTableResponse,
)
async def register_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    request: RegisterTableRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> RegisterTableResponse:
    """Register an existing Iceberg table."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    # Ensure namespace exists
    ns = await iceberg_table_service.get_iceberg_namespace_by_name(namespace_name, db)
    if not ns:
        if namespace_name == "default":
            ns = await iceberg_table_service.ensure_default_iceberg_namespace(db)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Namespace '{namespace}' not found",
            )

    try:
        iceberg_table = await iceberg_table_service.create_iceberg_table(
            table_name=table,
            namespace_name=namespace_name,
            metadata_location=request.metadata_location,
            location=request.metadata_location.rsplit("/metadata/", 1)[0] if "/metadata/" in request.metadata_location else request.metadata_location,
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    # Build Iceberg metadata (should read from metadata_location file)
    # For now, return minimal metadata - actual metadata should be read from storage
    location = request.metadata_location.rsplit("/metadata/", 1)[0] if "/metadata/" in request.metadata_location else request.metadata_location
    table_uuid_str = str(uuid.uuid4())
    metadata = build_iceberg_metadata(
        format_version=1,  # Should be read from metadata_location
        table_uuid=table_uuid_str,
        location=location,
        schema={},  # Should be read from metadata_location
        partition_spec=[],
        properties={},
    )

    return RegisterTableResponse(
        metadata_location=iceberg_table.metadata_location,
        metadata=metadata,
        config={},
    )


@router.get(
    "/namespaces/{namespace}/tables/{table}",
    response_model=LoadTableResponse,
)
async def load_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    db: AsyncSession = Depends(get_db_session),
) -> LoadTableResponse:
    """Load table metadata."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    table_info = await iceberg_table_service.get_iceberg_table_by_name(table, namespace_name, db)
    if not table_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{namespace}.{table}' not found",
        )

    # Build Iceberg metadata (should read from metadata_location file)
    # For now, return minimal metadata - actual metadata should be read from storage
    # TODO: Read actual metadata from table_info.metadata_location
    location = table_info.metadata_location.rsplit("/metadata/", 1)[0] if "/metadata/" in table_info.metadata_location else table_info.metadata_location
    table_uuid_str = str(uuid.uuid4())
    metadata = build_iceberg_metadata(
        format_version=1,  # Should be read from metadata_location
        table_uuid=table_uuid_str,
        location=location,
        schema={},  # Should be read from metadata_location
        partition_spec=[],
        properties={},
    )

    return LoadTableResponse(
        metadata_location=table_info.metadata_location,
        metadata=metadata,
        config={},
    )


@router.post(
    "/namespaces/{namespace}/tables/{table}/metadata",
    response_model=CommitTableResponse,
)
async def commit_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    request: CommitTableRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
) -> CommitTableResponse:
    """Commit table updates."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    table_info = await iceberg_table_service.get_iceberg_table_by_name(table, namespace_name, db)
    if not table_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{namespace}.{table}' not found",
        )

    # Process updates (simplified implementation)
    # In a real implementation, this would handle Iceberg table evolution
    # Only update metadata_location if provided
    updated_table = table_info
    if request.write_metadata_location:
        try:
            updated_table = await iceberg_table_service.update_iceberg_table_metadata_location(
                table_name=table,
                namespace_name=namespace_name,
                metadata_location=request.write_metadata_location,
                db=db,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update table: {exc}",
            ) from exc

    # Build Iceberg metadata (should read from metadata_location file)
    # For now, return minimal metadata - actual metadata should be read from storage
    # TODO: Read actual metadata from updated_table.metadata_location
    location = updated_table.metadata_location.rsplit("/metadata/", 1)[0] if "/metadata/" in updated_table.metadata_location else updated_table.metadata_location
    table_uuid_str = str(uuid.uuid4())
    metadata = build_iceberg_metadata(
        format_version=1,  # Should be read from metadata_location
        table_uuid=table_uuid_str,
        location=location,
        schema={},  # Should be read from metadata_location
        partition_spec=[],  # Should be read from metadata_location
        properties={},  # Should be read from metadata_location
    )

    return CommitTableResponse(
        metadata_location=updated_table.metadata_location,
        metadata=metadata,
    )


@router.delete(
    "/namespaces/{namespace}/tables/{table}",
    response_model=DropTableResponse,
)
async def drop_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    db: AsyncSession = Depends(get_db_session),
) -> DropTableResponse:
    """Drop a table."""
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    deleted = await iceberg_table_service.delete_iceberg_table(table, namespace_name, db)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{namespace}.{table}' not found",
        )

    return DropTableResponse(dropped=True)

