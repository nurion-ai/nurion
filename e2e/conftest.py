"""Pytest configuration and fixtures for E2E tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator, Optional

import pytest

from utils.aether_client import AetherClient
from utils.debug_collector import DebugCollector
from utils.test_data import TestDataManager


# Environment configuration (all values from GitHub Secrets)
AETHER_URL = os.environ.get("AETHER_URL")
K8S_NAMESPACE = os.environ.get("K8S_NAMESPACE")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "")

# Track Ray job IDs for log collection
_ray_job_ids: list[str] = []


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "e2e: end-to-end tests")
    config.addinivalue_line("markers", "nightly: nightly test suite")
    config.addinivalue_line("markers", "slow: slow running tests")


def pytest_sessionfinish(session, exitstatus):
    """Collect debug artifacts on session finish."""
    # Only collect on failure
    if exitstatus != 0:
        output_dir = os.environ.get("DEBUG_ARTIFACTS_DIR")
        collector = DebugCollector(
            output_dir=output_dir,
            namespace=K8S_NAMESPACE,
        )
        collector.collect_all(job_ids=_ray_job_ids)
        collector.create_archive()


@pytest.fixture(scope="session")
def aether_url() -> str:
    """Get Aether API URL."""
    return AETHER_URL


@pytest.fixture(scope="session")
def k8s_namespace() -> str:
    """Get Kubernetes namespace."""
    return K8S_NAMESPACE


@pytest.fixture(scope="session")
def aether_client(aether_url: str) -> Generator[AetherClient, None, None]:
    """Create Aether API client.
    
    Yields:
        Configured AetherClient instance
    """
    client = AetherClient(base_url=aether_url, timeout=60.0)
    
    # Wait for Aether to be ready
    if not client.wait_for_health(timeout=120.0):
        pytest.fail("Aether service is not healthy")
    
    yield client
    client.close()


@pytest.fixture(scope="session")
def test_data_manager() -> TestDataManager:
    """Create test data manager.
    
    Returns:
        Configured TestDataManager instance
    """
    return TestDataManager(
        s3_endpoint=S3_ENDPOINT,
        s3_access_key=S3_ACCESS_KEY,
        s3_secret_key=S3_SECRET_KEY,
        s3_region=S3_REGION,
    )


@pytest.fixture(scope="session")
def debug_collector(k8s_namespace: str) -> DebugCollector:
    """Create debug collector.
    
    Returns:
        Configured DebugCollector instance
    """
    output_dir = os.environ.get("DEBUG_ARTIFACTS_DIR")
    return DebugCollector(
        output_dir=output_dir,
        namespace=k8s_namespace,
    )


@pytest.fixture(scope="session")
def k8s_cluster_id(aether_client: AetherClient) -> int:
    """Get or create K8s cluster registration.
    
    Returns:
        Cluster ID for the test cluster
    """
    # Check if cluster already exists
    clusters = aether_client.list_k8s_clusters()
    for cluster in clusters:
        if cluster.get("name") == "nurion-sh":
            return cluster["id"]
    
    # Register new cluster (uses in-cluster config)
    result = aether_client.register_k8s_cluster(
        name="nurion-sh",
        context="nurion-sh",
    )
    return result["id"]


@pytest.fixture
def track_ray_job():
    """Fixture to track Ray job IDs for log collection.
    
    Usage:
        def test_something(track_ray_job):
            job_id = submit_job()
            track_ray_job(job_id)
    """
    def _track(job_id: str):
        _ray_job_ids.append(job_id)
    
    return _track


@pytest.fixture(scope="function")
def output_location(test_data_manager: TestDataManager, request) -> Generator[str, None, None]:
    """Get a unique output location for a test.
    
    Automatically cleans up after the test.
    
    Yields:
        S3 URI for test output
    """
    test_name = request.node.name
    location = test_data_manager.get_output_location(test_name)
    
    yield location
    
    # Cleanup after test
    test_data_manager.cleanup_output(location)


# Pytest hooks for better error reporting

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Capture test results for debug collection."""
    outcome = yield
    report = outcome.get_result()
    
    if report.when == "call" and report.failed:
        # Save test failure info
        debug_dir = Path(os.environ.get("DEBUG_ARTIFACTS_DIR", "debug-artifacts"))
        debug_dir.mkdir(parents=True, exist_ok=True)
        
        failure_file = debug_dir / "test-failures.log"
        with open(failure_file, "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"Test: {item.nodeid}\n")
            f.write(f"{'='*60}\n")
            if report.longrepr:
                f.write(str(report.longrepr))
            f.write("\n")
