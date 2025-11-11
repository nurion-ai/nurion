"""Storage helpers backed by fsspec for the Iceberg REST catalog."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import urlparse

import fsspec
from fsspec.core import url_to_fs

from .settings import get_settings

logger = logging.getLogger(__name__)

class ObjectStoreError(RuntimeError):
    """Raised when an object store operation fails."""


async def read_json(uri: str) -> dict[str, Any]:
    """Read a JSON object from object storage using fsspec."""
    return await asyncio.to_thread(_read_json_sync, uri)


async def write_json(uri: str, payload: dict[str, Any]) -> None:
    """Write a JSON payload to object storage using fsspec."""
    await asyncio.to_thread(_write_json_sync, uri, payload)


async def list_objects(uri: str) -> list[str]:
    """List objects under the provided URI prefix/directory."""
    return await asyncio.to_thread(_list_objects_sync, uri)


def filter_by_suffix(objects: Sequence[str], suffixes: Iterable[str]) -> list[str]:
    """Return objects whose names end with one of the desired suffixes."""
    suffix_tuple = tuple(suffixes)
    return [obj for obj in objects if obj.endswith(suffix_tuple)]


# --------------------------------------------------------------------------- #
# Internal helpers (sync implementations used via asyncio.to_thread)
# --------------------------------------------------------------------------- #


def _read_json_sync(uri: str) -> dict[str, Any]:
    storage_options = _storage_options(uri)
    try:
        with fsspec.open(uri, "r", encoding="utf-8", **storage_options) as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise
    except OSError as exc:  # pragma: no cover - transport errors surfaced as OSError
        raise ObjectStoreError(f"Failed to read object: {uri}") from exc


def _write_json_sync(uri: str, payload: dict[str, Any]) -> None:
    storage_options = _storage_options(uri)
    data = json.dumps(payload)
    try:
        with fsspec.open(uri, "w", encoding="utf-8", **storage_options) as fh:
            fh.write(data)
    except OSError as exc: 
        logger.error(f"Failed to write object: {uri}, options: {storage_options}", exc_info=True)
        raise ObjectStoreError(f"Failed to write object: {uri}") from exc


def _list_objects_sync(uri: str) -> list[str]:
    storage_options = _storage_options(uri)
    try:
        fs, path = url_to_fs(uri, **storage_options)
        try:
            entries = fs.find(path)
        except FileNotFoundError:
            return []
        return [fs.unstrip_protocol(entry) for entry in entries]
    except OSError as exc:  # pragma: no cover
        logger.error(f"Failed to list objects: {uri}, options: {storage_options}", exc_info=True)
        raise ObjectStoreError(f"Failed to list objects under: {uri}") from exc


def _storage_options(uri: str) -> dict[str, Any]:
    """Derive storage options for fsspec based on URI scheme and configured settings."""
    scheme = (urlparse(uri).scheme or "file").lower()
    if scheme not in {"file", "s3"}:
        raise ObjectStoreError(f"Unsupported URI scheme: {scheme}")

    if scheme == "s3":
        iceberg_cfg = get_settings().iceberg
        if not iceberg_cfg.is_s3:
            raise ObjectStoreError(
                "S3 URI requested but ICEBERG storage backend is not configured for S3 operations."
            )
        client_kwargs: dict[str, Any] = {}
        if endpoint := iceberg_cfg.endpoint_for_backend():
            client_kwargs["endpoint_url"] = endpoint
        if iceberg_cfg.s3_region:
            client_kwargs["region_name"] = iceberg_cfg.s3_region

        options: dict[str, Any] = {}
        if iceberg_cfg.s3_access_key_id:
            options["key"] = iceberg_cfg.s3_access_key_id
        if iceberg_cfg.s3_secret_access_key:
            options["secret"] = iceberg_cfg.s3_secret_access_key
        if client_kwargs:
            options["client_kwargs"] = client_kwargs
        return options

    return {}


__all__ = ["read_json", "write_json", "list_objects", "filter_by_suffix", "ObjectStoreError"]
