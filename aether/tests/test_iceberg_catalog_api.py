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

"""Integration tests for Iceberg catalog API using pyiceberg SDK.

Tests the Iceberg REST catalog endpoints via pyiceberg RestCatalog
to verify API compatibility with the open source Iceberg REST spec.
"""

from __future__ import annotations

import uuid

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def iceberg_catalog(app_server: str) -> RestCatalog:
    """Create a pyiceberg RestCatalog connected to the test server."""
    # pyiceberg expects the URI to point to the catalog root
    # Our API is at /api/iceberg-catalog/v1/...
    # pyiceberg prepends /v1/ to all requests
    catalog_uri = f"{app_server}/api/iceberg-catalog"
    return RestCatalog(name="test_catalog", uri=catalog_uri)


def test_list_namespaces(iceberg_catalog: RestCatalog) -> None:
    """Test listing namespaces via pyiceberg SDK."""
    namespaces = iceberg_catalog.list_namespaces()
    # Default namespace should be present
    assert ("default",) in namespaces


def test_create_namespace(iceberg_catalog: RestCatalog) -> None:
    """Test creating a namespace via pyiceberg SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"test_db_{unique_id}"

    # Create namespace
    iceberg_catalog.create_namespace(namespace, {"description": "Test database"})

    # Verify namespace exists in list
    namespaces = iceberg_catalog.list_namespaces()
    assert (namespace,) in namespaces


def test_load_namespace_metadata(iceberg_catalog: RestCatalog) -> None:
    """Test loading namespace metadata via pyiceberg SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"test_meta_{unique_id}"
    properties = {"description": "Test metadata namespace", "custom_key": "custom_value"}

    # Create namespace with properties
    iceberg_catalog.create_namespace(namespace, properties)

    # Load namespace properties
    loaded_props = iceberg_catalog.load_namespace_properties(namespace)
    assert loaded_props.get("description") == "Test metadata namespace"
    assert loaded_props.get("custom_key") == "custom_value"


def test_update_namespace_properties(iceberg_catalog: RestCatalog) -> None:
    """Test updating namespace properties via pyiceberg SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"test_update_{unique_id}"

    # Create namespace first
    iceberg_catalog.create_namespace(namespace, {"original": "value"})

    # Update properties
    iceberg_catalog.update_namespace_properties(
        namespace,
        removals=set(),
        updates={"description": "Updated description", "new_prop": "new_value"},
    )

    # Verify properties were updated
    loaded_props = iceberg_catalog.load_namespace_properties(namespace)
    assert loaded_props.get("description") == "Updated description"
    assert loaded_props.get("new_prop") == "new_value"


def test_drop_namespace(iceberg_catalog: RestCatalog) -> None:
    """Test dropping a namespace via pyiceberg SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"temp_ns_{unique_id}"

    # Create namespace first
    iceberg_catalog.create_namespace(namespace)

    # Verify it exists
    namespaces = iceberg_catalog.list_namespaces()
    assert (namespace,) in namespaces

    # Drop namespace
    iceberg_catalog.drop_namespace(namespace)

    # Verify namespace is gone
    namespaces = iceberg_catalog.list_namespaces()
    assert (namespace,) not in namespaces


def test_namespace_already_exists_error(iceberg_catalog: RestCatalog) -> None:
    """Test that creating a duplicate namespace raises an error."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"dup_ns_{unique_id}"

    # Create namespace first
    iceberg_catalog.create_namespace(namespace)

    # Try to create again - should raise error
    with pytest.raises(NamespaceAlreadyExistsError):
        iceberg_catalog.create_namespace(namespace)


def test_no_such_namespace_error(iceberg_catalog: RestCatalog) -> None:
    """Test that loading a non-existent namespace raises an error."""
    with pytest.raises(NoSuchNamespaceError):
        iceberg_catalog.load_namespace_properties("nonexistent_namespace_12345")


def test_list_tables_empty_namespace(iceberg_catalog: RestCatalog) -> None:
    """Test listing tables in an empty namespace via pyiceberg SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"empty_ns_{unique_id}"

    # Create namespace
    iceberg_catalog.create_namespace(namespace)

    # List tables - should be empty
    tables = iceberg_catalog.list_tables(namespace)
    assert tables == []


def test_create_and_load_table(iceberg_catalog: RestCatalog) -> None:
    """Test creating and loading a table via pyiceberg SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"tbl_ns_{unique_id}"
    table_name = f"users_{unique_id}"

    # Create namespace
    iceberg_catalog.create_namespace(namespace)

    # Define schema
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )

    # Create table
    table = iceberg_catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        properties={"write.format.default": "parquet"},
    )

    assert table is not None
    assert table.name() == (namespace, table_name)

    # Verify table appears in list
    tables = iceberg_catalog.list_tables(namespace)
    assert (namespace, table_name) in tables

    # Load table
    loaded_table = iceberg_catalog.load_table((namespace, table_name))
    assert loaded_table is not None
    assert len(loaded_table.schema().fields) == 2


def test_drop_table(iceberg_catalog: RestCatalog) -> None:
    """Test dropping a table via pyiceberg SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"drop_tbl_ns_{unique_id}"
    table_name = f"temp_tbl_{unique_id}"

    # Create namespace and table
    iceberg_catalog.create_namespace(namespace)
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    )
    iceberg_catalog.create_table(identifier=(namespace, table_name), schema=schema)

    # Verify table exists
    tables = iceberg_catalog.list_tables(namespace)
    assert (namespace, table_name) in tables

    # Drop table
    iceberg_catalog.drop_table((namespace, table_name))

    # Verify table is gone
    tables = iceberg_catalog.list_tables(namespace)
    assert (namespace, table_name) not in tables


def test_update_table_properties(iceberg_catalog: RestCatalog) -> None:
    """Test updating table properties via pyiceberg SDK.

    This tests whether the update_table endpoint is actually used.
    """
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"update_tbl_ns_{unique_id}"
    table_name = f"update_tbl_{unique_id}"

    # Create namespace and table
    iceberg_catalog.create_namespace(namespace)
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    )
    table = iceberg_catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        properties={"original.property": "original_value"},
    )

    # Update table properties using transaction
    with table.transaction() as txn:
        txn.set_properties({"new.property": "new_value"})

    # Reload table and verify properties
    loaded_table = iceberg_catalog.load_table((namespace, table_name))
    props = loaded_table.properties
    assert props.get("new.property") == "new_value"


def test_schema_evolution_add_column(iceberg_catalog: RestCatalog) -> None:
    """Test schema evolution (adding a column) via pyiceberg SDK.

    This tests whether the commit_table endpoint is actually used for schema changes.
    """
    unique_id = str(uuid.uuid4())[:8]
    namespace = f"schema_evo_ns_{unique_id}"
    table_name = f"schema_evo_tbl_{unique_id}"

    # Create namespace and table with simple schema
    iceberg_catalog.create_namespace(namespace)
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    )
    table = iceberg_catalog.create_table(identifier=(namespace, table_name), schema=schema)

    # Evolve schema - add a new column
    with table.update_schema() as update:
        update.add_column("name", StringType())

    # Reload table and verify schema was updated
    loaded_table = iceberg_catalog.load_table((namespace, table_name))
    assert len(loaded_table.schema().fields) == 2
    assert loaded_table.schema().find_field("name") is not None


def test_schema_creation() -> None:
    """Test creating Iceberg schema objects locally (no server needed)."""
    from pyiceberg.types import TimestampType

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType(), required=True),
        NestedField(field_id=3, name="created_at", field_type=TimestampType(), required=False),
    )

    assert len(schema.fields) == 3
    assert schema.find_field("id") is not None
    assert schema.find_field("email") is not None
    assert schema.find_field("created_at") is not None
