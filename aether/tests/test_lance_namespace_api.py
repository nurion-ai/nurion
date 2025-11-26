"""Integration tests that exercise the public lance-namespace spec client against the service."""

from __future__ import annotations

import os

import pytest
from lance_namespace_urllib3_client import ApiClient, Configuration, NamespaceApi
from lance_namespace_urllib3_client.models import DescribeNamespaceRequest

DEFAULT_BASE_URL = "http://localhost:8000/api/lance-namespace"


@pytest.fixture(scope="session")
def lance_client() -> NamespaceApi:
    """Create a NamespaceApi client for testing."""
    base_url = os.environ.get("LANCE_BASE_URL", DEFAULT_BASE_URL)
    config = Configuration(host=base_url)
    api_client = ApiClient(configuration=config)
    return NamespaceApi(api_client)


def test_spec_list_namespaces(lance_client: NamespaceApi) -> None:
    response = lance_client.list_namespaces(id="$", delimiter="$")
    assert response.namespaces and "default" in response.namespaces


def test_spec_describe_namespace(lance_client: NamespaceApi) -> None:
    response = lance_client.describe_namespace(
        id="default",
        describe_namespace_request=DescribeNamespaceRequest(delimiter="$"),
        delimiter="$",
    )
    assert (response.properties or {}).get("namespace") == "default"
