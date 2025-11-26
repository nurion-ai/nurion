"""Integration tests for Lance namespace API using lance-namespace-urllib3-client SDK.

Tests the lance-namespace REST endpoints via the lance_namespace_urllib3_client
to verify API compatibility with the open source Lance Namespace REST spec.
"""

from __future__ import annotations

import uuid

import pytest
from lance_namespace_urllib3_client import ApiClient, Configuration, NamespaceApi
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    DescribeNamespaceRequest,
    DropNamespaceRequest,
    NamespaceExistsRequest,
)

pytestmark = pytest.mark.integration

# Use $ as delimiter per lance-namespace spec (. has issues with URL path handling)
DELIMITER = "$"


@pytest.fixture(scope="module")
def lance_client(app_server: str) -> NamespaceApi:
    """Create a lance_namespace_urllib3_client connected to the test server."""
    # The lance-namespace SDK adds /v1/namespace/... to the host URL
    # Our API is at /api/lance-namespace/v1/namespace/...
    # So the host should be /api/lance-namespace (without /v1)
    namespace_uri = f"{app_server}/api/lance-namespace"
    config = Configuration(host=namespace_uri)
    api_client = ApiClient(configuration=config)
    return NamespaceApi(api_client)


def test_list_namespaces(lance_client: NamespaceApi) -> None:
    """Test listing namespaces via lance-namespace SDK."""
    response = lance_client.list_namespaces(
        id=DELIMITER,  # Root namespace
        delimiter=DELIMITER,
    )

    # Default namespace should be present
    assert response.namespaces is not None
    assert "default" in response.namespaces


def test_describe_namespace(lance_client: NamespaceApi) -> None:
    """Test describing a namespace via lance-namespace SDK."""
    request = DescribeNamespaceRequest(delimiter=DELIMITER)
    response = lance_client.describe_namespace(
        id="default",
        describe_namespace_request=request,
        delimiter=DELIMITER,
    )

    assert response.properties is not None
    assert response.properties.get("namespace") == "default"


def test_create_namespace(lance_client: NamespaceApi) -> None:
    """Test creating a namespace via lance-namespace SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace_name = f"test_ns_{unique_id}"

    # Create namespace
    request = CreateNamespaceRequest(
        id=[namespace_name],
        delimiter=DELIMITER,
        properties={"description": "Test namespace created by SDK"},
    )
    response = lance_client.create_namespace(
        id=namespace_name,
        create_namespace_request=request,
        delimiter=DELIMITER,
    )

    assert response is not None

    # Verify namespace exists in list
    list_response = lance_client.list_namespaces(
        id=DELIMITER,
        delimiter=DELIMITER,
    )
    assert namespace_name in list_response.namespaces


def test_namespace_exists(lance_client: NamespaceApi) -> None:
    """Test checking if a namespace exists via lance-namespace SDK."""
    # Default namespace should exist (no exception means it exists)
    request = NamespaceExistsRequest(delimiter=DELIMITER)
    # This should not raise an exception
    lance_client.namespace_exists(
        id="default",
        namespace_exists_request=request,
        delimiter=DELIMITER,
    )


def test_drop_namespace(lance_client: NamespaceApi) -> None:
    """Test dropping a namespace via lance-namespace SDK."""
    unique_id = str(uuid.uuid4())[:8]
    namespace_name = f"drop_ns_{unique_id}"

    # Create namespace first
    create_request = CreateNamespaceRequest(
        id=[namespace_name],
        delimiter=DELIMITER,
        properties={},
    )
    lance_client.create_namespace(
        id=namespace_name,
        create_namespace_request=create_request,
        delimiter=DELIMITER,
    )

    # Verify it exists
    list_response = lance_client.list_namespaces(
        id=DELIMITER,
        delimiter=DELIMITER,
    )
    assert namespace_name in list_response.namespaces

    # Drop namespace
    drop_request = DropNamespaceRequest(delimiter=DELIMITER)
    lance_client.drop_namespace(
        id=namespace_name,
        drop_namespace_request=drop_request,
        delimiter=DELIMITER,
    )

    # Verify namespace is gone
    list_response = lance_client.list_namespaces(
        id=DELIMITER,
        delimiter=DELIMITER,
    )
    assert namespace_name not in list_response.namespaces


def test_describe_namespace_properties(lance_client: NamespaceApi) -> None:
    """Test that namespace properties are properly returned."""
    unique_id = str(uuid.uuid4())[:8]
    namespace_name = f"props_ns_{unique_id}"

    # Create namespace with custom properties
    create_request = CreateNamespaceRequest(
        id=[namespace_name],
        delimiter=DELIMITER,
        properties={"custom_key": "custom_value", "description": "Namespace with properties"},
    )
    lance_client.create_namespace(
        id=namespace_name,
        create_namespace_request=create_request,
        delimiter=DELIMITER,
    )

    # Describe namespace and verify properties
    describe_request = DescribeNamespaceRequest(delimiter=DELIMITER)
    response = lance_client.describe_namespace(
        id=namespace_name,
        describe_namespace_request=describe_request,
        delimiter=DELIMITER,
    )

    assert response.properties is not None
    # The properties should include the namespace name
    assert response.properties.get("namespace") == namespace_name


def test_list_namespaces_pagination(lance_client: NamespaceApi) -> None:
    """Test listing namespaces with pagination parameters."""
    # Create a few namespaces for testing
    unique_id = str(uuid.uuid4())[:8]
    created_namespaces = []
    for i in range(3):
        ns_name = f"page_ns_{unique_id}_{i}"
        create_request = CreateNamespaceRequest(
            id=[ns_name],
            delimiter=DELIMITER,
            properties={},
        )
        lance_client.create_namespace(
            id=ns_name,
            create_namespace_request=create_request,
            delimiter=DELIMITER,
        )
        created_namespaces.append(ns_name)

    # List with limit
    response = lance_client.list_namespaces(
        id=DELIMITER,
        delimiter=DELIMITER,
        limit=2,
    )

    # Should return some namespaces (at least 2)
    assert response.namespaces is not None
    assert len(response.namespaces) >= 2


def test_namespace_with_parent_property(lance_client: NamespaceApi) -> None:
    """Test creating namespaces with parent property for logical hierarchy."""
    unique_id = str(uuid.uuid4())[:8]
    parent_ns = f"parent_{unique_id}"
    child_ns = f"child_{unique_id}"

    # Create parent namespace first
    parent_request = CreateNamespaceRequest(
        id=[parent_ns],
        delimiter=DELIMITER,
        properties={"description": "Parent namespace"},
    )
    lance_client.create_namespace(
        id=parent_ns,
        create_namespace_request=parent_request,
        delimiter=DELIMITER,
    )

    # Create child namespace with parent property (logical hierarchy)
    child_request = CreateNamespaceRequest(
        id=[child_ns],
        delimiter=DELIMITER,
        properties={"parent": parent_ns, "description": "Child namespace"},
    )
    lance_client.create_namespace(
        id=child_ns,
        create_namespace_request=child_request,
        delimiter=DELIMITER,
    )

    # Verify parent namespace exists
    describe_request = DescribeNamespaceRequest(delimiter=DELIMITER)
    response = lance_client.describe_namespace(
        id=parent_ns,
        describe_namespace_request=describe_request,
        delimiter=DELIMITER,
    )
    assert response.properties is not None

    # Verify child namespace exists
    response = lance_client.describe_namespace(
        id=child_ns,
        describe_namespace_request=describe_request,
        delimiter=DELIMITER,
    )
    assert response.properties is not None
