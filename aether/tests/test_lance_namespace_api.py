"""Integration tests that exercise the public lance-namespace spec client against the service."""

from __future__ import annotations

import os

import pytest
from lance_namespace import LanceNamespace
from lance_namespace.rest import LanceRestNamespace
from lance_namespace_urllib3_client.models import (
    DescribeNamespaceRequest,
    ListNamespacesRequest,
)

DEFAULT_BASE_URL = "http://localhost:8000/api/lance-namespace"


@pytest.fixture(scope="session")
def lance_client() -> LanceNamespace:
    base_url = os.environ.get("LANCE_BASE_URL", DEFAULT_BASE_URL)
    return LanceRestNamespace(uri=base_url, delimiter="$")


def test_spec_list_namespaces(lance_client: LanceNamespace) -> None:
    response = lance_client.list_namespaces(ListNamespacesRequest(id=["$"], delimiter="$"))
    assert response.namespaces and "default" in response.namespaces


def test_spec_describe_namespace(lance_client: LanceNamespace) -> None:
    response = lance_client.describe_namespace(
        DescribeNamespaceRequest(id=["default"], delimiter="$")
    )
    assert (response.properties or {}).get("namespace") == "default"
