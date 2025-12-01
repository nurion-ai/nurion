"""Pytest configuration and fixtures for Solstice tests."""

import pytest


@pytest.fixture(scope="session", autouse=True)
def ensure_spark_testdata():
    """Ensure Spark test data files exist before any tests run."""
    from tests.testdata.generate_spark_testdata import ensure_spark_testdata as generate

    generate()

