"""Pydantic schemas for Lance namespace REST API."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


PageToken = Optional[str]
PageLimit = Optional[int]


class CreateNamespaceRequest(ApiModel):
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)


class CreateNamespaceResponse(ApiModel):
    namespace: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class DescribeNamespaceRequest(ApiModel):
    pass


class DescribeNamespaceResponse(ApiModel):
    namespace: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class DropNamespaceRequest(ApiModel):
    pass


class DropNamespaceResponse(ApiModel):
    namespace: str
    dropped: bool = True


class NamespaceExistsRequest(ApiModel):
    pass


class ListNamespacesResponse(ApiModel):
    namespaces: List[str]
    next_page_token: Optional[str] = None


class ListTablesResponse(ApiModel):
    tables: List[str]
    next_page_token: Optional[str] = None


class RegisterTableRequest(ApiModel):
    location: str
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    storage_options: Optional[Dict[str, Any]] = None


class RegisterTableResponse(ApiModel):
    version: int = 1
    location: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class DropTableResponse(ApiModel):
    id: List[str]
    location: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class DeregisterTableResponse(ApiModel):
    id: List[str]
    location: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class GetTableStatsResponse(ApiModel):
    num_rows: int
    num_fragments: Optional[int] = None
    size_bytes: Optional[int] = None


class DescribeTableResponse(ApiModel):
    version: int
    location: str
    table_schema: Dict[str, Any] = Field(default_factory=dict, alias="schema")
    properties: Dict[str, Any] = Field(default_factory=dict)
    storage_options: Optional[Dict[str, Any]] = None

    @property
    def schema(self) -> Dict[str, Any]:
        return self.table_schema


class CountTableRowsRequest(ApiModel):
    filter: Optional[str] = None


class CountTableRowsResponse(ApiModel):
    count: int


class CreateEmptyTableRequest(ApiModel):
    location: str
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    storage_options: Optional[Dict[str, Any]] = None


class CreateTableResponse(ApiModel):
    version: int = 1
    location: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class TableIndex(ApiModel):
    name: str
    columns: List[str]
    type: str = "VECTOR"
    status: str = "unknown"


class ListTableIndicesResponse(ApiModel):
    indices: List[TableIndex] = Field(default_factory=list)


class ListTableTagsResponse(ApiModel):
    tags: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    next_page_token: Optional[str] = None


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
    default_value: Optional[Any] = None
    required: bool = False
    options: Optional[List[Dict[str, str]]] = None


class CustomColumnDefinitionCreate(CustomColumnDefinitionBase):
    pass


class CustomColumnDefinitionUpdate(BaseModel):
    name: Optional[str] = None
    field_type: Optional[FieldType] = None
    default_value: Optional[Any] = None
    required: Optional[bool] = None
    display_order: Optional[int] = None
    options: Optional[List[Dict[str, str]]] = None


class CustomColumnDefinitionResponse(CustomColumnDefinitionBase):
    id: int
    display_order: int
    is_active: bool
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None


class CustomColumnFilter(BaseModel):
    column_id: str
    operator: FilterOperator
    value: str


class TableCreate(BaseModel):
    name: str
    lance_path: str
    description: Optional[str] = None
    storage_options: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    custom_values: Optional[Dict[str, Any]] = None


class TableUpdate(BaseModel):
    name: Optional[str] = None
    lance_path: Optional[str] = None
    description: Optional[str] = None
    storage_options: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    custom_values: Optional[Dict[str, Any]] = None


class TableResponse(BaseModel):
    id: int
    lance_path: str
    name: str
    description: Optional[str] = None
    lance_schema: Optional[Dict[str, Any]] = None
    row_count: Optional[int] = None
    storage_options: Optional[Dict[str, Any]] = None
    custom_values: Optional[Dict[str, Any]] = None
    last_updated_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class TablePaginatedResponse(BaseModel):
    tables: List[TableResponse]
    total_count: int


class TableSampleRequest(BaseModel):
    select_columns: Optional[List[str]] = Field(default=None)
    row_ids: Optional[List[int]] = Field(default=None)
    indices: Optional[List[int]] = Field(default=None)
    limit: Optional[int] = Field(default=10, ge=1, le=100)


class TableSampleResponse(BaseModel):
    id: int
    columns: List[str]
    rows: List[Dict[str, Any]]
    sampled_row_count: int
    total_row_count: int


class FileSampleRequest(BaseModel):
    file_path: str
    select_columns: Optional[List[str]] = Field(default=None)
    indices: Optional[List[int]] = Field(default=None)
    row_ids: Optional[List[int]] = Field(default=None)
    limit: Optional[int] = Field(default=10, ge=1, le=100)
    storage_options: Optional[dict] = None
    return_random_rows: bool = False


class FileSampleResponse(BaseModel):
    file_path: str
    columns: List[str]
    rows: List[Dict[str, Any]]
    sampled_row_count: int
    total_row_count: int

