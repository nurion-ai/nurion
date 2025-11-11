"""Pydantic schemas for Iceberg REST Catalog API."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


# Config endpoints
class CatalogConfigResponse(ApiModel):
    """Catalog configuration response."""

    defaults: dict[str, str] = Field(default_factory=dict)
    overrides: dict[str, str] = Field(default_factory=dict)


# Namespace endpoints
class CreateNamespaceRequest(ApiModel):
    """Request to create a namespace."""

    namespace: list[str] | None = None
    properties: dict[str, str] | None = Field(default_factory=dict)


class CreateNamespaceResponse(ApiModel):
    """Response after creating a namespace."""

    namespace: list[str]
    properties: dict[str, str] = Field(default_factory=dict)


class NamespaceResponse(ApiModel):
    """Namespace information."""

    namespace: list[str]
    properties: dict[str, str] = Field(default_factory=dict)


class ListNamespacesResponse(ApiModel):
    """Response listing all namespaces."""

    namespaces: list[list[str]]


class UpdateNamespacePropertiesRequest(ApiModel):
    """Request to update namespace properties."""

    removals: list[str] | None = Field(default_factory=list)
    updates: dict[str, str] = Field(default_factory=dict)


class UpdateNamespacePropertiesResponse(ApiModel):
    """Response after updating namespace properties."""

    removed: list[str] = Field(default_factory=list)
    updated: list[str] = Field(default_factory=list)
    missing: list[str] = Field(default_factory=list)


# Table endpoints
class TableIdentifier(ApiModel):
    """Table identifier."""

    namespace: list[str]
    name: str


class TableRequirement(ApiModel):
    """Table requirement for conditional operations."""

    type: str
    requirement: dict[str, Any]


class TableMetadata(ApiModel):
    """Table metadata."""

    metadata_location: str | None = None
    metadata_file: str | None = None
    previous_metadata_files: list[str] | None = None


class LoadTableResponse(ApiModel):
    """Response when loading a table."""

    metadata_location: str
    metadata: dict[str, Any]
    config: dict[str, str] = Field(default_factory=dict)


class CreateTableRequest(ApiModel):
    """Request to create a table."""

    name: str
    location: str | None = None
    table_schema: dict[str, Any] = Field(
        validation_alias=AliasChoices("table_schema", "schema"),
        serialization_alias="schema",
    )
    partition_spec: dict[str, Any] | None = None
    write_metadata_location: str | None = None
    stage_create: bool = False
    properties: dict[str, str] | None = Field(default_factory=dict)


class CreateTableResponse(ApiModel):
    """Response after creating a table."""

    metadata_location: str
    metadata: dict[str, Any]
    config: dict[str, str] = Field(default_factory=dict)


class RegisterTableRequest(ApiModel):
    """Request to register an existing table."""

    metadata_location: str


class RegisterTableResponse(ApiModel):
    """Response after registering a table."""

    metadata_location: str
    metadata: dict[str, Any]
    config: dict[str, str] = Field(default_factory=dict)


class CommitTableRequest(ApiModel):
    """Request to commit table updates."""

    model_config = {"extra": "allow"}  # Allow extra fields from pyiceberg client

    identifier: TableIdentifier | None = None
    requirements: list[TableRequirement] | None = None
    updates: list[dict[str, Any]] | None = None

    # Optional fields that pyiceberg may send
    name: str | None = None
    schema: dict[str, Any] | None = None
    partition_spec: dict[str, Any] | None = None
    write_order: dict[str, Any] | None = None
    properties: dict[str, str] | None = None

    # For update operations
    write_metadata_location: str | None = None


class CommitTableResponse(ApiModel):
    """Response after committing table updates."""

    metadata_location: str
    metadata: dict[str, Any]


class ListTablesResponse(ApiModel):
    """Response listing all tables in a namespace."""

    identifiers: list[TableIdentifier]


class UpdateTablePropertiesRequest(ApiModel):
    """Request to update table properties."""

    removals: list[str] | None = Field(default_factory=list)
    updates: dict[str, str] = Field(default_factory=dict)


class RenameTableRequest(ApiModel):
    """Request to rename a table."""

    source: TableIdentifier
    destination: TableIdentifier


class DropTableResponse(ApiModel):
    """Response after dropping a table."""

    dropped: bool = True
