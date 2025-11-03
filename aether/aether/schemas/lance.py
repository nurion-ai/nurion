"""Pydantic schemas for Lance namespace REST API."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


PageToken = str | None
PageLimit = int | None


class CreateNamespaceRequest(ApiModel):
    properties: dict[str, Any] | None = Field(default_factory=dict)


class CreateNamespaceResponse(ApiModel):
    namespace: str
    properties: dict[str, Any] = Field(default_factory=dict)


class DescribeNamespaceRequest(ApiModel):
    pass


class DescribeNamespaceResponse(ApiModel):
    namespace: str
    properties: dict[str, Any] = Field(default_factory=dict)


class DropNamespaceRequest(ApiModel):
    pass


class DropNamespaceResponse(ApiModel):
    namespace: str
    dropped: bool = True


class NamespaceExistsRequest(ApiModel):
    pass


class ListNamespacesResponse(ApiModel):
    namespaces: list[str]
    next_page_token: str | None = None


class ListTablesResponse(ApiModel):
    tables: list[str]
    next_page_token: str | None = None


class RegisterTableRequest(ApiModel):
    location: str
    properties: dict[str, Any] | None = Field(default_factory=dict)
    storage_options: dict[str, Any] | None = None


class RegisterTableResponse(ApiModel):
    version: int = 1
    location: str
    properties: dict[str, Any] = Field(default_factory=dict)


class DropTableResponse(ApiModel):
    id: list[str]
    location: str
    properties: dict[str, Any] = Field(default_factory=dict)


class DeregisterTableResponse(ApiModel):
    id: list[str]
    location: str
    properties: dict[str, Any] = Field(default_factory=dict)


class GetTableStatsResponse(ApiModel):
    num_rows: int
    num_fragments: int | None = None
    size_bytes: int | None = None


class DescribeTableResponse(ApiModel):
    version: int
    location: str
    table_schema: dict[str, Any] = Field(default_factory=dict, alias="schema")
    properties: dict[str, Any] = Field(default_factory=dict)
    storage_options: dict[str, Any] | None = None

    @property
    def schema(self) -> dict[str, Any]:
        return self.table_schema


class CountTableRowsRequest(ApiModel):
    filter: str | None = None


class CountTableRowsResponse(ApiModel):
    count: int


class CreateEmptyTableRequest(ApiModel):
    location: str
    properties: dict[str, Any] | None = Field(default_factory=dict)
    storage_options: dict[str, Any] | None = None


class CreateTableResponse(ApiModel):
    version: int = 1
    location: str
    properties: dict[str, Any] = Field(default_factory=dict)


class TableIndex(ApiModel):
    name: str
    columns: list[str]
    type: str = "VECTOR"
    status: str = "unknown"


class ListTableIndicesResponse(ApiModel):
    indices: list[TableIndex] = Field(default_factory=list)


class ListTableTagsResponse(ApiModel):
    tags: dict[str, dict[str, Any]] = Field(default_factory=dict)
    next_page_token: str | None = None


class GetTableTagVersionRequest(ApiModel):
    tag: str


class GetTableTagVersionResponse(ApiModel):
    version: int


class CreateTableTagRequest(ApiModel):
    tag: str
    version: int


class UpdateTableTagRequest(ApiModel):
    tag: str
    version: int


class DeleteTableTagRequest(ApiModel):
    tag: str


class HealthCheckResponse(ApiModel):
    status: str = "healthy"
    service: str = "lance-namespace-catalog"
    version: str = "1.0.0"


class FieldType(str, Enum):
    TEXT = "text"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OPTIONS = "options"


class FilterOperator(str, Enum):
    CONTAINS = "contains"
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"


class CustomColumnDefinitionBase(ApiModel):
    name: str
    field_type: FieldType
    default_value: Any | None = None
    required: bool = False
    options: list[dict[str, str]] | None = None


class CustomColumnDefinitionCreate(CustomColumnDefinitionBase):
    pass


class CustomColumnDefinitionUpdate(BaseModel):
    name: str | None = None
    field_type: FieldType | None = None
    default_value: Any | None = None
    required: bool | None = None
    display_order: int | None = None
    options: list[dict[str, str]] | None = None


class CustomColumnDefinitionResponse(CustomColumnDefinitionBase):
    id: int
    display_order: int
    is_active: bool
    deleted_at: datetime | None = None
    deleted_by: str | None = None
    created_at: datetime
    updated_at: datetime
    created_by: str | None = None


class CustomColumnFilter(BaseModel):
    column_id: str
    operator: FilterOperator
    value: str


class TableCreate(BaseModel):
    name: str
    lance_path: str
    description: str | None = None
    storage_options: dict[str, Any] | None = None
    tags: list[str] | None = None
    custom_values: dict[str, Any] | None = None


class TableUpdate(BaseModel):
    name: str | None = None
    lance_path: str | None = None
    description: str | None = None
    storage_options: dict[str, Any] | None = None
    tags: list[str] | None = None
    custom_values: dict[str, Any] | None = None


class TableResponse(BaseModel):
    id: int
    lance_path: str
    name: str
    description: str | None = None
    lance_schema: dict[str, Any] | None = None
    row_count: int | None = None
    storage_options: dict[str, Any] | None = None
    custom_values: dict[str, Any] | None = None
    last_updated_by: str | None = None
    created_at: datetime
    updated_at: datetime


class TablePaginatedResponse(BaseModel):
    tables: list[TableResponse]
    total_count: int


class TableSampleRequest(BaseModel):
    select_columns: list[str] | None = Field(default=None)
    row_ids: list[int] | None = Field(default=None)
    indices: list[int] | None = Field(default=None)
    limit: int | None = Field(default=10, ge=1, le=100)


class TableSampleResponse(BaseModel):
    id: int
    columns: list[str]
    rows: list[dict[str, Any]]
    sampled_row_count: int
    total_row_count: int


class FileSampleRequest(BaseModel):
    file_path: str
    select_columns: list[str] | None = Field(default=None)
    indices: list[int] | None = Field(default=None)
    row_ids: list[int] | None = Field(default=None)
    limit: int | None = Field(default=10, ge=1, le=100)
    storage_options: dict | None = None
    return_random_rows: bool = False


class FileSampleResponse(BaseModel):
    file_path: str
    columns: list[str]
    rows: list[dict[str, Any]]
    sampled_row_count: int
    total_row_count: int
