"""Iceberg REST Catalog API routes."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import unquote

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from ...db.session import get_session
from ...schemas.iceberg import (
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


def get_s3_client():
    """Get S3 client configured from environment variables."""
    import os

    import boto3

    s3_kwargs = {}
    if endpoint := os.getenv("AWS_ENDPOINT_URL") or os.getenv("AWS_S3_ENDPOINT"):
        s3_kwargs["endpoint_url"] = endpoint
    if access_key := os.getenv("AWS_ACCESS_KEY_ID"):
        s3_kwargs["aws_access_key_id"] = access_key
    if secret_key := os.getenv("AWS_SECRET_ACCESS_KEY"):
        s3_kwargs["aws_secret_access_key"] = secret_key
    if region := os.getenv("AWS_REGION"):
        s3_kwargs["region_name"] = region

    return boto3.client("s3", **s3_kwargs)


async def read_metadata_from_s3(metadata_location: str) -> dict[str, Any]:
    """Read Iceberg metadata from S3/MinIO storage. Fails fast if not found."""
    import json as json_lib

    if not metadata_location.startswith("s3://"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only s3:// metadata locations are supported, got: {metadata_location}",
        )

    # Parse S3 URL
    s3_path = metadata_location.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)

    # Get S3 client
    s3_client = get_s3_client()

    # Read object - let exceptions propagate
    response = s3_client.get_object(Bucket=bucket, Key=key)
    metadata = json_lib.loads(response["Body"].read())
    logger.debug(f"Successfully read metadata from {metadata_location}")
    return metadata


@router.get("/config", response_model=CatalogConfigResponse)
async def get_config() -> CatalogConfigResponse:
    """Get catalog configuration from environment variables."""
    import os

    # Read configuration from environment
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse")

    # For S3 endpoint, prefer external endpoint for clients outside container
    # Default to localhost:9000 for external clients
    s3_endpoint = os.getenv("AWS_S3_ENDPOINT_EXTERNAL") or os.getenv(
        "AWS_ENDPOINT_URL", "http://localhost:9000"
    )
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    s3_region = os.getenv("AWS_REGION", "us-east-1")

    return CatalogConfigResponse(
        defaults={
            "warehouse": warehouse,
            "type": "rest",
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.region": s3_region,
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
            [ns.name] for ns in namespaces if ns.name.startswith(format_namespace(parent_list))
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

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
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

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
        await iceberg_table_service.update_iceberg_namespace_properties(
            name=namespace_name,
            removals=request.removals or [],
            updates=request.updates or {},
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

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
        await iceberg_table_service.create_iceberg_table(
            table_name=table_name,
            namespace_name=namespace_name,
            metadata_location=metadata_location,
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    # Use pyiceberg to create proper metadata
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

    # Convert schema dict to pyiceberg Schema
    iceberg_schema = IcebergSchema.model_validate(request.schema)

    # Determine location
    if "/metadata/" in metadata_location:
        location = metadata_location.rsplit("/metadata/", 1)[0]
    else:
        location = metadata_location

    # Create proper table metadata using pyiceberg
    table_metadata = new_table_metadata(
        location=location,
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        properties=request.properties or {},
    )

    # Convert to dict for API response
    metadata = table_metadata.model_dump()

    # Write metadata to S3 using pyiceberg's JSON serialization
    s3_client = get_s3_client()
    s3_path = metadata_location.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)

    # Use model_dump_json for proper serialization
    metadata_json = table_metadata.model_dump_json()

    s3_client.put_object(
        Bucket=bucket, Key=key, Body=metadata_json.encode("utf-8"), ContentType="application/json"
    )
    logger.info(f"Wrote initial metadata to {metadata_location}")

    return CreateTableResponse(
        metadata_location=metadata_location,
        metadata=metadata,
        config={},
    )


@router.post(
    "/namespaces/{namespace}/tables/{table}",
    response_model=LoadTableResponse,
)
async def update_table(
    req: Request,
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    db: AsyncSession = Depends(get_db_session),
) -> LoadTableResponse:
    """
    Update Iceberg table (commit changes).
    This is the standard Iceberg REST endpoint for commits.
    Reference: Java RESTCatalogAdapter UPDATE_TABLE
    """
    # Parse raw request - this is UpdateTableRequest with requirements and updates
    request_data = await req.json()

    logger.info(f"UPDATE_TABLE: {namespace}.{table}")

    # Get table info
    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    table_info = await iceberg_table_service.get_iceberg_table_by_name(table, namespace_name, db)
    if not table_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{namespace}.{table}' not found",
        )

    # Read current metadata from S3
    try:
        current_metadata = await read_metadata_from_s3(table_info.metadata_location)
    except Exception:
        # If no metadata exists, create minimal one
        if "/metadata/" in table_info.metadata_location:
            location = table_info.metadata_location.rsplit("/metadata/", 1)[0]
        else:
            location = table_info.metadata_location

        from pyiceberg.schema import Schema as IcebergSchema
        from pyiceberg.table import UNPARTITIONED_PARTITION_SPEC
        from pyiceberg.table.metadata import new_table_metadata
        from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

        # Create minimal schema
        minimal_schema = IcebergSchema()
        table_metadata = new_table_metadata(
            location=location,
            schema=minimal_schema,
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
            sort_order=UNSORTED_SORT_ORDER,
            properties={},
        )
        current_metadata = table_metadata.model_dump()

    # Apply updates from request
    if updates := request_data.get("updates"):
        for update in updates:
            action = update.get("action")

            if action == "add-snapshot":
                snapshot = update.get("snapshot", {})
                snapshot_id = snapshot.get("snapshot-id")
                if snapshot_id:
                    if "snapshots" not in current_metadata:
                        current_metadata["snapshots"] = []
                    current_metadata["snapshots"].append(snapshot)
                    current_metadata["current-snapshot-id"] = snapshot_id
                    logger.info(f"Applied add-snapshot: {snapshot_id}")

            elif action == "set-snapshot-ref":
                ref_name = update.get("ref-name", "main")
                snapshot_id = update.get("snapshot-id")
                if snapshot_id:
                    if "refs" not in current_metadata:
                        current_metadata["refs"] = {}
                    current_metadata["refs"][ref_name] = {
                        "snapshot-id": snapshot_id,
                        "type": update.get("type", "branch"),
                    }
                    logger.info(f"Applied set-snapshot-ref: {ref_name} -> {snapshot_id}")

    # Write updated metadata back to S3
    import json as json_lib

    s3_client = get_s3_client()
    s3_path = table_info.metadata_location.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_lib.dumps(current_metadata).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Wrote updated metadata to {table_info.metadata_location}")

    return LoadTableResponse(
        metadata_location=table_info.metadata_location,
        metadata=current_metadata,
        config={},
    )


# Keep old endpoint for backwards compatibility
@router.post(
    "/namespaces/{namespace}/tables/{table}/old",
    response_model=CreateTableResponse,
)
async def create_table_legacy(
    req: Request,
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    db: AsyncSession = Depends(get_db_session),
) -> CreateTableResponse:
    """Legacy endpoint - delegates to update_table"""
    # Parse raw request
    request_data = await req.json()

    # Check if this is a commit/update request
    if "updates" in request_data or "identifier" in request_data:
        # This is an update, use the new endpoint
        update_resp = await update_table(req, namespace, table, db)
        return CreateTableResponse(
            metadata_location=update_resp.metadata_location,
            metadata=update_resp.metadata,
            config={},
        )

    # Otherwise it's create - but this shouldn't happen on this endpoint
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Use POST /namespaces/{namespace}/tables to create tables",
    )


# Old create_table function - now removed
# The correct endpoint is:
#   - POST /namespaces/{namespace}/tables for CREATE
#   - POST /namespaces/{namespace}/tables/{table} for UPDATE (commit)


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

    # Calculate location from metadata location
    if "/metadata/" in request.metadata_location:
        location = request.metadata_location.rsplit("/metadata/", 1)[0]
    else:
        location = request.metadata_location

    try:
        iceberg_table = await iceberg_table_service.create_iceberg_table(
            table_name=table,
            namespace_name=namespace_name,
            metadata_location=request.metadata_location,
            location=location,
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    # Read metadata from S3
    metadata = await read_metadata_from_s3(iceberg_table.metadata_location)

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

    metadata = await read_metadata_from_s3(table_info.metadata_location)

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
    request: Request,
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    db: AsyncSession = Depends(get_db_session),
) -> CommitTableResponse:
    """Commit table updates - read metadata from S3."""

    # Parse request body as raw JSON
    request_data = await request.json()

    namespace_list = parse_namespace(namespace)
    namespace_name = format_namespace(namespace_list)

    table_info = await iceberg_table_service.get_iceberg_table_by_name(table, namespace_name, db)
    if not table_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{namespace}.{table}' not found",
        )

    # Extract new metadata location from updates
    # pyiceberg writes metadata first, the location is NOT predictable from snapshot
    # We need to look in the manifest-list directory for the latest metadata file
    new_metadata_location = None

    if updates := request_data.get("updates"):
        for update in updates:
            if update.get("action") == "add-snapshot":
                snapshot = update.get("snapshot", {})
                if manifest_list := snapshot.get("manifest-list"):
                    # pyiceberg writes metadata files with timestamp/uuid names
                    # We'll list the metadata directory and find the latest one
                    metadata_dir = manifest_list.rsplit("/", 1)[0]

                    # Parse S3 path to list files
                    s3_path = metadata_dir.replace("s3://", "")
                    bucket, prefix = s3_path.split("/", 1)

                    # Get S3 client
                    s3_client = get_s3_client()

                    try:
                        # List metadata files
                        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix + "/")
                        if "Contents" in response:
                            # Find .metadata.json files
                            metadata_files = [
                                obj["Key"]
                                for obj in response["Contents"]
                                if obj["Key"].endswith(".metadata.json")
                            ]
                            if metadata_files:
                                # Get the latest one (sort by name, they have timestamps)
                                latest = sorted(metadata_files)[-1]
                                new_metadata_location = f"s3://{bucket}/{latest}"
                                logger.info(f"Found latest metadata: {new_metadata_location}")
                    except Exception as e:
                        logger.warning(f"Could not list metadata files: {e}")

                    break

    # Update table metadata location in DB
    if new_metadata_location:
        try:
            updated_table = await iceberg_table_service.update_iceberg_table_metadata_location(
                table_name=table,
                namespace_name=namespace_name,
                metadata_location=new_metadata_location,
                db=db,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update table: {exc}",
            ) from exc
    else:
        updated_table = table_info

    metadata = await read_metadata_from_s3(updated_table.metadata_location)

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
