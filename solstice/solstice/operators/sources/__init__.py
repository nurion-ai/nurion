"""Built-in source operators."""

from solstice.operators.sources.file import FileSource, FileSourceConfig
from solstice.operators.sources.iceberg import IcebergSource, IcebergSourceConfig
from solstice.operators.sources.lance import (
    LanceTableSource,
    LanceTableSourceConfig,
    LanceSourceMaster,
)
from solstice.operators.sources.source import (
    SourceMaster,
    SourceConfig,
)
from solstice.operators.sources.spark import (
    SparkSource,
    SparkSourceConfig,
    SparkSourceMaster,
)

__all__ = [
    # File source
    "FileSource",
    "FileSourceConfig",
    # Iceberg source
    "IcebergSource",
    "IcebergSourceConfig",
    # Lance source
    "LanceTableSource",
    "LanceTableSourceConfig",
    "LanceSourceMaster",
    # Source base
    "SourceMaster",
    "SourceConfig",
    # Spark source
    "SparkSource",
    "SparkSourceConfig",
    "SparkSourceMaster",
]
