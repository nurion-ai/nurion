"""Storage utilities for Lance datasets."""

from __future__ import annotations


def normalized_path_and_storage_options(
    lance_path: str, storage_options: dict | None = None
) -> tuple[str, dict]:
    """Return normalized path and merged storage options.

    For now this implementation simply returns the provided path and options.
    It mirrors the interface used by the data-api service so that the logic can
    evolve alongside it without requiring callers to change.
    """

    return lance_path, storage_options or {}
