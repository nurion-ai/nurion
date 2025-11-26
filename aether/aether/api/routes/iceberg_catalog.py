"""Iceberg REST Catalog API routes.

Uses pyiceberg's SqlCatalog as backend - no separate db session needed.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, Path, Query, Request, status

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
    UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
)
from ...services.iceberg_catalog_service import (
    IcebergCatalogService,
    parse_namespace,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/iceberg-catalog/v1", tags=["iceberg-rest-catalog"])

# Module-level service instance - uses SqlCatalog internally
_catalog_service: IcebergCatalogService | None = None


def get_catalog_service() -> IcebergCatalogService:
    """Get or create the catalog service instance."""
    global _catalog_service
    if _catalog_service is None:
        _catalog_service = IcebergCatalogService()
    return _catalog_service


@router.get("/config", response_model=CatalogConfigResponse)
async def get_config(
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> CatalogConfigResponse:
    """Return catalog configuration."""
    return service.get_config()


@router.get("/namespaces", response_model=ListNamespacesResponse)
async def list_namespaces(
    parent: str | None = Query(None, description="Parent namespace to list"),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> ListNamespacesResponse:
    """List Iceberg namespaces."""
    return await service.list_namespaces(parent)


@router.post("/namespaces", response_model=CreateNamespaceResponse)
async def create_namespace_post(
    request: CreateNamespaceRequest = Body(...),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> CreateNamespaceResponse:
    """Create a namespace (standard REST endpoint)."""
    return await service.create_namespace(request)


@router.post(
    "/namespaces/{namespace}",
    response_model=CreateNamespaceResponse,
)
async def create_namespace(
    namespace: str = Path(..., description="Namespace identifier"),
    request: CreateNamespaceRequest = Body(...),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> CreateNamespaceResponse:
    """Create a namespace using a path parameter."""
    namespace_segments = parse_namespace(namespace)
    return await service.create_namespace(
        request,
        namespace_override=namespace_segments,
    )


@router.get(
    "/namespaces/{namespace}",
    response_model=NamespaceResponse,
)
async def get_namespace(
    namespace: str = Path(..., description="Namespace identifier"),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> NamespaceResponse:
    """Retrieve namespace metadata."""
    namespace_segments = parse_namespace(namespace)
    return await service.get_namespace(namespace_segments)


@router.delete(
    "/namespaces/{namespace}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_namespace(
    namespace: str = Path(..., description="Namespace identifier"),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> None:
    """Delete a namespace."""
    namespace_segments = parse_namespace(namespace)
    await service.delete_namespace(namespace_segments)


@router.post(
    "/namespaces/{namespace}/properties",
    response_model=UpdateNamespacePropertiesResponse,
)
async def update_namespace_properties(
    namespace: str = Path(..., description="Namespace identifier"),
    request: UpdateNamespacePropertiesRequest = Body(...),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> UpdateNamespacePropertiesResponse:
    """Update namespace properties."""
    namespace_segments = parse_namespace(namespace)
    return await service.update_namespace_properties(namespace_segments, request)


@router.get(
    "/namespaces/{namespace}/tables",
    response_model=ListTablesResponse,
)
async def list_tables(
    namespace: str = Path(..., description="Namespace identifier"),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> ListTablesResponse:
    """List tables within a namespace."""
    namespace_segments = parse_namespace(namespace)
    return await service.list_tables(namespace_segments)


@router.post(
    "/namespaces/{namespace}/tables",
    response_model=CreateTableResponse,
)
async def create_table_post(
    namespace: str = Path(..., description="Namespace identifier"),
    request: CreateTableRequest = Body(...),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> CreateTableResponse:
    """Create a new Iceberg table."""
    namespace_segments = parse_namespace(namespace)
    return await service.create_table(namespace_segments, request)


@router.post(
    "/namespaces/{namespace}/tables/{table}",
    response_model=CommitTableResponse,
)
async def update_table(
    request: Request,
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> CommitTableResponse:
    """Commit updates to an Iceberg table.

    This is the main endpoint for all table modifications including
    schema evolution, property updates, and snapshot commits.
    """
    namespace_segments = parse_namespace(namespace)
    payload: dict[str, Any] = await request.json()
    logger.info("UPDATE_TABLE %s.%s", ".".join(namespace_segments) or "default", table)
    return await service.update_table(namespace_segments, table, payload)


@router.post(
    "/namespaces/{namespace}/tables/{table}/register",
    response_model=RegisterTableResponse,
)
async def register_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    request: RegisterTableRequest = Body(...),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> RegisterTableResponse:
    """Register an existing table with the catalog."""
    namespace_segments = parse_namespace(namespace)
    return await service.register_table(namespace_segments, table, request)


@router.get(
    "/namespaces/{namespace}/tables/{table}",
    response_model=LoadTableResponse,
)
async def load_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> LoadTableResponse:
    """Load Iceberg table metadata."""
    namespace_segments = parse_namespace(namespace)
    return await service.load_table(namespace_segments, table)


@router.delete(
    "/namespaces/{namespace}/tables/{table}",
    response_model=DropTableResponse,
)
async def drop_table(
    namespace: str = Path(..., description="Namespace identifier"),
    table: str = Path(..., description="Table name"),
    service: IcebergCatalogService = Depends(get_catalog_service),
) -> DropTableResponse:
    """Drop a table from the catalog."""
    namespace_segments = parse_namespace(namespace)
    return await service.drop_table(namespace_segments, table)
