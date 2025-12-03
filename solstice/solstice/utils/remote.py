"""Utilities for accessing remote files (S3, etc.)."""

from __future__ import annotations

import configparser
import hashlib
import logging
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Cache directory for downloaded files
_CACHE_DIR: Optional[Path] = None
_S3_CONFIG: Optional[Dict[str, Any]] = None


def reset_s3_config() -> None:
    """Reset the cached S3 configuration. Useful for testing or reloading config."""
    global _S3_CONFIG
    _S3_CONFIG = None


def _load_s3_config_from_env() -> Optional[Dict[str, Any]]:
    """Load S3 configuration from environment variables."""
    key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    if key and secret:
        config = {
            "key": key,
            "secret": secret,
            "endpoint_url": os.environ.get(
                "AWS_ENDPOINT_URL", os.environ.get("FSSPEC_S3_ENDPOINT_URL", "")
            ),
            "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            "source": "environment",
        }
        logger.info(
            f"Loaded S3 config from environment variables: endpoint={config['endpoint_url']}, region={config['region_name']}"
        )
        return config
    return None


def _load_s3_config_from_aws(profile: str = "default") -> Optional[Dict[str, Any]]:
    """Load S3 configuration from AWS config files (~/.aws/credentials, ~/.aws/config)."""
    aws_creds_paths = [
        Path.home() / ".aws/credentials",
        Path("/root/.aws/credentials"),
    ]
    aws_config_paths = [
        Path.home() / ".aws/config",
        Path("/root/.aws/config"),
    ]

    key, secret, region, endpoint = "", "", "us-east-1", ""

    # Load credentials
    for creds_path in aws_creds_paths:
        if creds_path.exists():
            config = configparser.ConfigParser()
            config.read(creds_path)
            if profile in config:
                section = config[profile]
                key = section.get("aws_access_key_id", "")
                secret = section.get("aws_secret_access_key", "")
                if key and secret:
                    logger.debug(f"Loaded AWS credentials from {creds_path} [{profile}]")
                    break

    # Load config (region, endpoint)
    for config_path in aws_config_paths:
        if config_path.exists():
            config = configparser.ConfigParser()
            config.read(config_path)
            # AWS config uses "profile xxx" sections for non-default profiles
            section_name = profile if profile == "default" else f"profile {profile}"
            if section_name in config:
                section = config[section_name]
                region = section.get("region", region)
                endpoint = section.get("endpoint_url", endpoint)
                logger.debug(f"Loaded AWS config from {config_path} [{section_name}]")
                break

    if key and secret:
        result = {
            "key": key,
            "secret": secret,
            "endpoint_url": endpoint,
            "region_name": region,
            "source": f"aws_config:{profile}",
        }
        logger.info(
            f"Loaded S3 config from AWS config [{profile}]: endpoint={endpoint}, region={region}"
        )
        return result
    return None


def _load_s3_config_from_rclone(remote_name: str = "s3") -> Optional[Dict[str, Any]]:
    """Load S3 configuration from rclone config."""
    rclone_paths = [
        Path.home() / ".config/rclone/rclone.conf",
        Path("/root/.config/rclone/rclone.conf"),
    ]

    for rclone_config in rclone_paths:
        if rclone_config.exists():
            config = configparser.ConfigParser()
            config.read(rclone_config)

            if remote_name in config:
                section = config[remote_name]
                result = {
                    "key": section.get("access_key_id", ""),
                    "secret": section.get("secret_access_key", ""),
                    "endpoint_url": section.get("endpoint", ""),
                    "region_name": section.get("region", "us-east-1"),
                    "source": f"rclone:{remote_name}",
                }
                logger.info(
                    f"Loaded S3 config from {rclone_config} [{remote_name}]: endpoint={result['endpoint_url']}, region={result['region_name']}"
                )
                return result
    return None


def _load_s3_config(
    rclone_remote: Optional[str] = None,
    aws_profile: str = "default",
) -> Dict[str, Any]:
    """Load S3 configuration from multiple sources.

    Priority order:
    1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc.)
    2. AWS config files (~/.aws/credentials, ~/.aws/config)
    3. rclone config (~/.config/rclone/rclone.conf)

    Args:
        rclone_remote: Remote name in rclone config. If None, uses
                       SOLSTICE_S3_REMOTE env var or "s3" as default.
        aws_profile: Profile name in AWS config (default: "default")

    Returns:
        Dict with keys: key, secret, endpoint_url, region_name, source
    """
    if rclone_remote is None:
        rclone_remote = os.environ.get("SOLSTICE_S3_REMOTE", "s3")
    global _S3_CONFIG
    if _S3_CONFIG is not None:
        return _S3_CONFIG

    # Try environment variables first
    config = _load_s3_config_from_env()
    if config and config.get("key") and config.get("secret"):
        _S3_CONFIG = config
        return _S3_CONFIG

    # Try AWS config files
    config = _load_s3_config_from_aws(aws_profile)
    if config and config.get("key") and config.get("secret"):
        _S3_CONFIG = config
        return _S3_CONFIG

    # Try rclone config
    config = _load_s3_config_from_rclone(rclone_remote)
    if config and config.get("key") and config.get("secret"):
        _S3_CONFIG = config
        return _S3_CONFIG

    # No config found, return empty config
    logger.warning("No S3 configuration found from any source (env, aws, rclone)")
    _S3_CONFIG = {
        "key": "",
        "secret": "",
        "endpoint_url": "",
        "region_name": "us-east-1",
        "source": "none",
    }
    return _S3_CONFIG


def get_s3_storage_options(
    rclone_remote: Optional[str] = None,
    aws_profile: str = "default",
) -> Dict[str, Any]:
    """Get S3 storage options for fsspec.

    Args:
        rclone_remote: Remote name in rclone config. If None, uses
                       SOLSTICE_S3_REMOTE env var or "s3" as default.
        aws_profile: Profile name in AWS config (default: "default")

    Returns:
        Dict of storage options for fsspec.open()
    """
    if rclone_remote is None:
        rclone_remote = os.environ.get("SOLSTICE_S3_REMOTE", "s3")
    config = _load_s3_config(rclone_remote, aws_profile)

    options: Dict[str, Any] = {
        "key": config["key"],
        "secret": config["secret"],
        # Use virtual-hosted style addressing for S3-compatible providers
        "config_kwargs": {
            "signature_version": "s3v4",
            "s3": {"addressing_style": "virtual"},
        },
    }

    if config["endpoint_url"]:
        options["endpoint_url"] = config["endpoint_url"]

    if config["region_name"]:
        options["client_kwargs"] = {"region_name": config["region_name"]}

    return options


def get_lance_storage_options(
    bucket: str,
    rclone_remote: Optional[str] = None,
    aws_profile: str = "default",
) -> Dict[str, str]:
    """Get S3 storage options for Lance.

    Lance uses object_store crate which requires specific options format.
    For S3-compatible providers with custom endpoints, we need to use
    virtual-hosted style with bucket in the endpoint URL.

    Args:
        bucket: S3 bucket name (needed for virtual-hosted endpoint)
        rclone_remote: Remote name in rclone config
        aws_profile: Profile name in AWS config

    Returns:
        Dict of storage options for lance.write_dataset()
    """
    if rclone_remote is None:
        rclone_remote = os.environ.get("SOLSTICE_S3_REMOTE", "s3")
    config = _load_s3_config(rclone_remote, aws_profile)

    options: Dict[str, str] = {
        "aws_access_key_id": config["key"],
        "aws_secret_access_key": config["secret"],
        "aws_region": config["region_name"] or "us-east-1",
    }

    # For custom endpoints, use virtual-hosted style with bucket in endpoint
    if config["endpoint_url"]:
        endpoint = config["endpoint_url"]
        # Insert bucket name into endpoint for virtual-hosted style
        # https://endpoint.com -> https://bucket.endpoint.com
        if endpoint.startswith("https://"):
            options["aws_endpoint"] = f"https://{bucket}.{endpoint[8:]}"
        elif endpoint.startswith("http://"):
            options["aws_endpoint"] = f"http://{bucket}.{endpoint[7:]}"
        else:
            options["aws_endpoint"] = f"https://{bucket}.{endpoint}"
        options["aws_virtual_hosted_style_request"] = "true"

    return options


def get_cache_dir() -> Path:
    """Get or create the cache directory for downloaded files."""
    global _CACHE_DIR
    if _CACHE_DIR is None:
        cache_base = os.environ.get("SOLSTICE_CACHE_DIR", "/tmp/solstice_cache")
        _CACHE_DIR = Path(cache_base)
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR


def is_remote_path(path: str) -> bool:
    """Check if a path is a remote URL (s3://, gs://, http://, etc.)."""
    if not path:
        return False
    return path.startswith(("s3://", "gs://", "http://", "https://", "az://"))


def _get_cache_path(remote_url: str) -> Path:
    """Generate a deterministic cache path for a remote URL."""
    url_hash = hashlib.md5(remote_url.encode()).hexdigest()[:16]
    parsed = urlparse(remote_url)
    filename = Path(parsed.path).name or "file"
    cache_dir = get_cache_dir()
    return cache_dir / f"{url_hash}_{filename}"


def download_file(remote_url: str, local_path: Optional[Path] = None) -> Path:
    """Download a file from a remote URL to local storage.

    Args:
        remote_url: The remote URL (s3://, gs://, etc.)
        local_path: Optional local path to save to. If None, uses cache.

    Returns:
        Path to the local file.
    """
    import fsspec

    if local_path is None:
        local_path = _get_cache_path(remote_url)

    # Check if already cached
    if local_path.exists():
        logger.debug(f"Using cached file: {local_path}")
        return local_path

    local_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading {remote_url} to {local_path}")

    # Get storage options for S3
    storage_options = get_s3_storage_options() if remote_url.startswith("s3://") else {}

    with fsspec.open(remote_url, "rb", **storage_options) as remote_file:
        with open(local_path, "wb") as local_file:
            while True:
                chunk = remote_file.read(8 * 1024 * 1024)  # 8MB chunks
                if not chunk:
                    break
                local_file.write(chunk)

    logger.debug(f"Downloaded {remote_url} ({local_path.stat().st_size} bytes)")
    return local_path


@contextmanager
def ensure_local_file(
    path: str,
    use_cache: bool = True,
) -> Generator[Path, None, None]:
    """Context manager that ensures a file is available locally.

    For local files, returns the path directly.
    For remote files, downloads to a temp/cache location.

    Args:
        path: Local path or remote URL.
        use_cache: If True, cache downloaded files for reuse.

    Yields:
        Path to the local file.
    """
    if not is_remote_path(path):
        # Local file - just return the path
        local_path = Path(path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {path}")
        yield local_path
        return

    # Remote file - download it
    if use_cache:
        local_path = download_file(path)
        yield local_path
        # Don't delete cached files
    else:
        # Use temp file without caching
        with tempfile.NamedTemporaryFile(
            suffix=Path(urlparse(path).path).suffix or ".tmp",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)

        try:
            download_file(path, tmp_path)
            yield tmp_path
        finally:
            # Clean up temp file
            if tmp_path.exists():
                tmp_path.unlink()


def clear_cache() -> None:
    """Clear the download cache."""
    import shutil

    cache_dir = get_cache_dir()
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cleared cache directory: {cache_dir}")
