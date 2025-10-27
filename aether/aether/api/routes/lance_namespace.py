"""Lance namespace REST API routes."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import lance
from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncSession

from ...core.store import normalized_path_and_storage_options
from ...db.session import get_session
from ...schemas.catalog import (
    CountTableRowsRequest,
    CountTableRowsResponse,
    CreateEmptyTableRequest,
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    CreateTableResponse,
    CreateTableTagRequest,
    DeleteTableTagRequest,
    DeregisterTableResponse,
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    DescribeTableResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
    DropTableResponse,
    GetTableStatsResponse,
    GetTableTagVersionRequest,
    GetTableTagVersionResponse,
    HealthCheckResponse,
    ListNamespacesResponse,
    ListTableIndicesResponse,
    ListTablesResponse,
    ListTableTagsResponse,
    NamespaceExistsRequest,
    RegisterTableRequest,
    RegisterTableResponse,
    UpdateTableTagRequest,
)
from ...services import catalog_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lance-namespace/v1", tags=["lance-namespace"])


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    async for session in get_session():
        yield session


def _is_root_namespace(identifier: str, delimiter: str) -> bool:
    return identifier in {delimiter, "$", "", "."}


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _stringify_properties(properties: dict[str, Any] | None) -> dict[str, str]:
    return {key: _stringify_value(val) for key, val in (properties or {}).items()}


@router.post("/namespace/{id}/create", response_model=CreateNamespaceResponse)
async def create_namespace(
    id: str = Path(..., description="Namespace identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: CreateNamespaceRequest = Body(default_factory=CreateNamespaceRequest),
    db: AsyncSession = Depends(get_db_session),
):
    properties = request.properties or {}
    description = properties.get("description")
    created_by = properties.get("created_by")

    try:
        namespace = await catalog_service.create_namespace(
            name=id,
            description=description,
            delimiter=delimiter,
            properties=properties,
            created_by=created_by,
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    response_properties = {
        "id": _stringify_value(namespace.id),
        "description": _stringify_value(namespace.description),
        "created_at": _stringify_value(
            namespace.created_at.isoformat() if namespace.created_at else ""
        ),
        "delimiter": _stringify_value(namespace.delimiter),
    }
    response_properties.update(_stringify_properties(namespace.properties))

    return CreateNamespaceResponse(namespace=namespace.name, properties=response_properties)


@router.post("/namespace/{id}/describe", response_model=DescribeNamespaceResponse)
async def describe_namespace(
    id: str = Path(..., description="Namespace identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: DescribeNamespaceRequest = Body(default_factory=DescribeNamespaceRequest),
    db: AsyncSession = Depends(get_db_session),
):
    if _is_root_namespace(id, delimiter):
        namespace = await catalog_service.ensure_default_namespace(db)
        tables_in_namespace = await catalog_service.get_tables_by_namespace(namespace.name, db)
        return DescribeNamespaceResponse(
            namespace="default",
            properties={
                "id": _stringify_value(namespace.id),
                "namespace": _stringify_value("default"),
                "description": _stringify_value(namespace.description or "Default namespace"),
                "table_count": _stringify_value(len(tables_in_namespace)),
                "delimiter": _stringify_value(namespace.delimiter),
                "created_at": _stringify_value(
                    namespace.created_at.isoformat() if namespace.created_at else ""
                ),
                "updated_at": _stringify_value(
                    namespace.updated_at.isoformat() if namespace.updated_at else ""
                ),
                "is_default": "true",
                **_stringify_properties(namespace.properties),
            },
        )

    namespace = await catalog_service.get_namespace_by_name(id, db)
    if not namespace:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Namespace '{id}' not found"
        )

    tables_in_namespace = await catalog_service.get_tables_by_namespace(id, db)
    return DescribeNamespaceResponse(
        namespace=namespace.name,
        properties={
            "id": _stringify_value(namespace.id),
            "namespace": _stringify_value(namespace.name),
            "description": _stringify_value(namespace.description or ""),
            "table_count": _stringify_value(len(tables_in_namespace)),
            "delimiter": _stringify_value(namespace.delimiter),
            "created_at": _stringify_value(
                namespace.created_at.isoformat() if namespace.created_at else ""
            ),
            "updated_at": _stringify_value(
                namespace.updated_at.isoformat() if namespace.updated_at else ""
            ),
            **_stringify_properties(namespace.properties),
        },
    )


@router.post("/namespace/{id}/drop", response_model=DropNamespaceResponse)
async def drop_namespace(
    id: str = Path(..., description="Namespace identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: DropNamespaceRequest = Body(default_factory=DropNamespaceRequest),
    db: AsyncSession = Depends(get_db_session),
):
    if id in {".", ""}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot drop root namespace"
        )

    namespace = await catalog_service.get_namespace_by_name(id, db)
    if not namespace:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Namespace '{id}' not found"
        )

    try:
        deleted = await catalog_service.delete_namespace(namespace.id, db)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete namespace '{id}'",
        )

    return DropNamespaceResponse(namespace=id, dropped=True)


@router.post("/namespace/{id}/exists", status_code=status.HTTP_200_OK)
async def namespace_exists(
    id: str = Path(..., description="Namespace identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: NamespaceExistsRequest = Body(default_factory=NamespaceExistsRequest),
    db: AsyncSession = Depends(get_db_session),
):
    if id in {".", "", "default"}:
        await catalog_service.ensure_default_namespace(db)
        return

    namespace = await catalog_service.get_namespace_by_name(id, db)
    if not namespace:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Namespace '{id}' not found"
        )


@router.get("/namespace/{id}/list", response_model=ListNamespacesResponse)
async def list_namespaces(
    id: str = Path(..., description="Parent namespace identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    page_token: int | None = Query(None, description="Page token for pagination"),
    limit: int | None = Query(None, description="Maximum number of results to return"),
    db: AsyncSession = Depends(get_db_session),
):
    if _is_root_namespace(id, delimiter):
        await catalog_service.ensure_default_namespace(db)
        namespaces = await catalog_service.get_available_namespaces(db)
    else:
        all_namespaces = await catalog_service.get_all_namespaces(db)
        namespaces = [
            namespace.name for namespace in all_namespaces if namespace.name.startswith(id)
        ]

    if page_token is not None or limit is not None:
        start_idx = page_token or 0
        end_idx = len(namespaces)
        if limit:
            end_idx = min(start_idx + limit, len(namespaces))
        paginated = namespaces[start_idx:end_idx]
        next_token = str(end_idx) if end_idx < len(namespaces) else None
        return ListNamespacesResponse(namespaces=paginated, next_page_token=next_token)

    return ListNamespacesResponse(namespaces=namespaces)


@router.get("/namespace/{id}/table/list", response_model=ListTablesResponse)
async def list_tables(
    id: str = Path(..., description="Parent namespace identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    page_token: int | None = Query(None, description="Page token for pagination"),
    limit: int | None = Query(None, description="Maximum number of results to return"),
    db: AsyncSession = Depends(get_db_session),
):
    tables = await catalog_service.get_tables_by_namespace(id, db)
    table_names = [table.name for table in tables]

    if page_token is not None or limit is not None:
        start_idx = page_token or 0
        end_idx = len(table_names)
        if limit:
            end_idx = min(start_idx + limit, len(table_names))
        paginated = table_names[start_idx:end_idx]
        next_token = str(end_idx) if end_idx < len(table_names) else None
        return ListTablesResponse(tables=paginated, next_page_token=next_token)

    return ListTablesResponse(tables=table_names)


@router.post("/table/{id}/register", response_model=RegisterTableResponse)
async def register_table(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: RegisterTableRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
):
    properties = request.properties or {}

    if delimiter in id:
        parts = id.split(delimiter)
        namespace_name = delimiter.join(parts[:-1])
        table_name = parts[-1]
    else:
        namespace_name = "default"
        table_name = id

    try:
        table = await catalog_service.create_lance_table(
            lance_path=request.location,
            name=table_name,
            storage_options=request.storage_options,
            namespace_name=namespace_name,
            db=db,
            **properties,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return RegisterTableResponse(
        version=1,
        location=table.lance_path,
        properties={
            "name": table.name,
            "created_at": table.created_at.isoformat() if table.created_at else "",
            "updated_at": table.updated_at.isoformat() if table.updated_at else "",
        },
    )


@router.post("/table/{id}/drop", response_model=DropTableResponse)
async def drop_table(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    db: AsyncSession = Depends(get_db_session),
):
    try:
        table_info = await catalog_service.drop_table_by_id(id, delimiter, db)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return DropTableResponse(
        id=[id],
        location=table_info.lance_path,
        properties={
            "name": table_info.name,
            "created_at": table_info.created_at.isoformat() if table_info.created_at else "",
            "updated_at": table_info.updated_at.isoformat() if table_info.updated_at else "",
        },
    )


@router.post("/table/{id}/deregister", response_model=DeregisterTableResponse)
async def deregister_table(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    db: AsyncSession = Depends(get_db_session),
):
    try:
        table_info = await catalog_service.deregister_table_by_id(id, delimiter, db)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return DeregisterTableResponse(
        id=[id],
        location=table_info.lance_path,
        properties={
            "name": table_info.name,
            "created_at": table_info.created_at.isoformat() if table_info.created_at else "",
            "updated_at": table_info.updated_at.isoformat() if table_info.updated_at else "",
        },
    )


@router.post("/table/{id}/stats", response_model=GetTableStatsResponse)
async def get_table_stats(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    try:
        dataset = lance.dataset(table_info.lance_path, storage_options=table_info.storage_options)
        row_count = dataset.count_rows()
        num_fragments = len(dataset.get_fragments())
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not get fresh row count for table '%s': %s", id, exc)
        return GetTableStatsResponse(num_rows=table_info.row_count or 0, num_fragments=None)

    return GetTableStatsResponse(num_rows=row_count, num_fragments=num_fragments)


@router.post("/table/{id}/describe", response_model=DescribeTableResponse)
async def describe_table(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    version = 1
    if table_info.updated_at:
        import calendar

        version = int(calendar.timegm(table_info.updated_at.timetuple()))

    response = DescribeTableResponse(
        version=version,
        location=table_info.lance_path,
        table_schema=table_info.lance_schema or {},
        properties={
            "name": table_info.name,
            "created_at": table_info.created_at.isoformat() if table_info.created_at else "",
            "updated_at": table_info.updated_at.isoformat() if table_info.updated_at else "",
        },
        storage_options=table_info.storage_options or {},
    )
    return response


@router.post("/table/{id}/exists", status_code=status.HTTP_200_OK)
async def table_exists(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")


@router.post("/table/{id}/count_rows", response_model=CountTableRowsResponse)
async def count_table_rows(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: CountTableRowsRequest = Body(default_factory=CountTableRowsRequest),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    try:
        normalized_path, storage_options = normalized_path_and_storage_options(
            table_info.lance_path, table_info.storage_options
        )
        dataset = lance.dataset(normalized_path, storage_options=storage_options)
        row_count = dataset.count_rows()

        if request.filter:
            filtered_count = len(dataset.to_table(filter=request.filter))
            return CountTableRowsResponse(count=filtered_count)

        return CountTableRowsResponse(count=row_count)
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not get fresh row count for table '%s': %s", id, exc)
        return CountTableRowsResponse(count=table_info.row_count or 0)


@router.post("/table/{id}/create-empty", response_model=CreateTableResponse)
async def create_empty_table(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: CreateEmptyTableRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
):
    table_name = id.split(delimiter)[-1] if delimiter in id else id

    try:
        table = await catalog_service.create_empty_lance_table(
            table_name=table_name,
            location=request.location,
            storage_options=request.storage_options,
            properties=request.properties,
            db=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return CreateTableResponse(
        version=1,
        location=table.lance_path,
        properties={
            "name": table.name,
            "created_at": table.created_at.isoformat() if table.created_at else "",
            "updated_at": table.updated_at.isoformat() if table.updated_at else "",
            "empty_table": True,
        },
    )


@router.post("/table/{id}/index/list", response_model=ListTableIndicesResponse)
async def list_table_indices_endpoint(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    indices = await catalog_service.list_table_indices(table_info.name, db)
    return ListTableIndicesResponse(indices=indices)


@router.get("/table/{id}/tags/list", response_model=ListTableTagsResponse)
async def list_table_tags(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    page_token: str | None = Query(None, description="Page token for pagination"),
    limit: int | None = Query(None, description="Maximum number of results to return"),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    tags = {"latest": {"version": 1}, "stable": {"version": 1}}

    if page_token is not None or limit is not None:
        items = list(tags.items())
        start_idx = 0
        if page_token:
            try:
                start_idx = int(page_token)
            except (ValueError, TypeError):
                start_idx = 0
        end_idx = len(items)
        if limit:
            end_idx = min(start_idx + limit, len(items))
        paginated = dict(items[start_idx:end_idx])
        next_token = str(end_idx) if end_idx < len(items) else None
        return ListTableTagsResponse(tags=paginated, next_page_token=next_token)

    return ListTableTagsResponse(tags=tags)


@router.post("/table/{id}/tags/version", response_model=GetTableTagVersionResponse)
async def get_table_tag_version(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: GetTableTagVersionRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    if request.tag in {"latest", "stable"}:
        return GetTableTagVersionResponse(version=1)

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail=f"Tag '{request.tag}' not found"
    )


@router.post("/table/{id}/tags/create", status_code=status.HTTP_201_CREATED)
async def create_table_tag(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: CreateTableTagRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    return None


@router.post("/table/{id}/tags/update", status_code=status.HTTP_200_OK)
async def update_table_tag(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: UpdateTableTagRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    if request.tag not in {"latest", "stable"}:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Tag '{request.tag}' not found"
        )

    return None


@router.post("/table/{id}/tags/delete", status_code=status.HTTP_204_NO_CONTENT)
async def delete_table_tag(
    id: str = Path(..., description="Table identifier"),
    delimiter: str = Query(".", description="Delimiter used to parse object string identifiers"),
    request: DeleteTableTagRequest = Body(...),
    db: AsyncSession = Depends(get_db_session),
):
    table_info = await catalog_service.get_lance_table(id, db)

    if not table_info and delimiter in id:
        parts = id.split(delimiter)
        table_name = parts[-1]
        table_info = await catalog_service.get_lance_table(table_name, db)

    if not table_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{id}' not found")

    if request.tag not in {"latest", "stable"}:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Tag '{request.tag}' not found"
        )

    return None


@router.get("/health", response_model=HealthCheckResponse)
async def health_check() -> HealthCheckResponse:
    return HealthCheckResponse(status="healthy", service="lance-namespace-catalog", version="1.0.0")
